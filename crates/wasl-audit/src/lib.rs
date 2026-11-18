//! wasl-audit — the audit trail of a Wasl node: one [`AuditEntry`] per delegation, decision,
//! policy change and outcome, appended as one JSON line to a file.
//!
//! [`AuditLog::open`] starts a writer task fed through a bounded queue; [`AuditLog::record`]
//! resolves once its entry is durable — written, flushed and synced — so a caller that records
//! a decision before applying it never acts on a decision the trail could lose (callers never
//! hold a lock across it). Entries queued together share one sync.
//! [`AuditLog::flush`] resolves once everything recorded before it is on disk.
//! [`read_entries`] reads a trail back.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use wasl_proto::{AgentId, EdgePolicy, HandoffDecision, TaskId, TaskMode};

/// Bound on entries queued for the writer.
const QUEUE: usize = 1024;

/// What an entry records.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AuditKind {
    /// A delegation was requested (whatever happened next).
    Delegated {
        mode: TaskMode,
        policy: EdgePolicy,
        /// Truncated instruction.
        instruction: String,
    },
    /// A delegation was rejected before any approval (no edge, cycle, depth, budget, deny).
    Rejected { code: String, message: String },
    /// A handoff was decided (gate, elicitation, or an `always_allow` edge).
    Decided {
        decision: HandoffDecision,
        /// Who decided: `gate`, `elicitation`, `policy`.
        by: String,
    },
    /// An edge's policy changed.
    PolicyChanged { policy: EdgePolicy, cause: String },
    /// A task reached a terminal state.
    Finished {
        state: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
}

/// One audit record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Ms since the Unix epoch.
    pub t_ms: u64,
    pub workstream: String,
    /// Who acted (the sending agent).
    pub actor: AgentId,
    pub target: AgentId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskId>,
    #[serde(flatten)]
    pub kind: AuditKind,
}

/// Why the trail could not be written or read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditError(pub String);

impl std::fmt::Display for AuditError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "audit trail: {}", self.0)
    }
}

impl std::error::Error for AuditError {}

enum Msg {
    Entry(Box<AuditEntry>, oneshot::Sender<Result<(), AuditError>>),
    Flush(oneshot::Sender<Result<(), AuditError>>),
}

/// Handle to an audit trail (cheap to clone).
#[derive(Clone)]
pub struct AuditLog {
    tx: mpsc::Sender<Msg>,
    path: PathBuf,
}

impl AuditLog {
    /// Open (creating) `path` for append and start its writer task.
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self, AuditError> {
        let path = path.into();
        if let Some(dir) = path.parent() {
            tokio::fs::create_dir_all(dir)
                .await
                .map_err(|e| AuditError(format!("create {}: {e}", dir.display())))?;
        }
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| AuditError(format!("open {}: {e}", path.display())))?;
        let (tx, rx) = mpsc::channel(QUEUE);
        tokio::spawn(writer(file, path.clone(), rx));
        Ok(AuditLog { tx, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one entry; resolves once it is durable (written, flushed and synced), or with
    /// the error that kept it from being so.
    pub async fn record(&self, entry: AuditEntry) -> Result<(), AuditError> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Msg::Entry(Box::new(entry), tx)).await.is_err() {
            return Err(AuditError("writer is gone".into()));
        }
        rx.await.map_err(|_| AuditError("writer is gone".into()))?
    }

    /// Resolve once every entry recorded before this call is written (and synced).
    pub async fn flush(&self) -> Result<(), AuditError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Msg::Flush(tx))
            .await
            .map_err(|_| AuditError("writer is gone".into()))?;
        rx.await.map_err(|_| AuditError("writer is gone".into()))?
    }
}

async fn writer(mut file: tokio::fs::File, path: PathBuf, mut rx: mpsc::Receiver<Msg>) {
    while let Some(first) = rx.recv().await {
        // Everything queued right now is written and synced together.
        let mut batch = vec![first];
        while let Ok(m) = rx.try_recv() {
            batch.push(m);
        }
        let mut waiters = Vec::with_capacity(batch.len());
        let mut failed: Option<AuditError> = None;
        for msg in batch {
            match msg {
                Msg::Entry(e, done) => {
                    let r = match serde_json::to_vec(&e) {
                        Ok(mut line) => {
                            line.push(b'\n');
                            match &failed {
                                Some(f) => Err(f.clone()),
                                None => file
                                    .write_all(&line)
                                    .await
                                    .map_err(|err| AuditError(format!("append: {err}"))),
                            }
                        }
                        Err(err) => Err(AuditError(format!("encode: {err}"))),
                    };
                    if let Err(err) = &r {
                        failed.get_or_insert_with(|| err.clone());
                    }
                    waiters.push(Waiter::Entry(done, r));
                }
                Msg::Flush(reply) => waiters.push(Waiter::Flush(reply)),
            }
        }
        // tokio's `File` hands writes to a blocking thread: `flush` waits for them to reach the
        // OS, `sync_data` for the OS to reach the disk.
        let synced = match file.flush().await {
            Ok(()) => file.sync_data().await,
            Err(e) => Err(e),
        }
        .map_err(|e| AuditError(format!("sync: {e}")));
        if let Err(e) = &synced {
            tracing::error!(path = %path.display(), error = %e, "cannot sync the audit trail");
        }
        for w in waiters {
            match w {
                Waiter::Entry(done, r) => {
                    if let Err(e) = &r {
                        tracing::error!(path = %path.display(), error = %e, "audit entry lost");
                    }
                    let r = r.and(synced.clone());
                    let _ = done.send(r); // the recorder may have given up
                }
                Waiter::Flush(reply) => {
                    let r = match &failed {
                        Some(f) => Err(f.clone()),
                        None => synced.clone(),
                    };
                    let _ = reply.send(r); // the requester may have given up
                }
            }
        }
    }
}

enum Waiter {
    Entry(
        oneshot::Sender<Result<(), AuditError>>,
        Result<(), AuditError>,
    ),
    Flush(oneshot::Sender<Result<(), AuditError>>),
}

/// Read a trail back (blank lines skipped).
pub async fn read_entries(path: impl AsRef<Path>) -> Result<Vec<AuditEntry>, AuditError> {
    let text = tokio::fs::read_to_string(path.as_ref())
        .await
        .map_err(|e| AuditError(format!("read {}: {e}", path.as_ref().display())))?;
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).map_err(|e| AuditError(format!("bad entry {l:?}: {e}"))))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn entries_round_trip_through_the_file() {
        let dir = std::env::temp_dir().join(format!("wasl-audit-{}", fastrand_id()));
        let path = dir.join("audit.ndjson");
        let log = AuditLog::open(&path).await.unwrap();
        let e = AuditEntry {
            t_ms: 1,
            workstream: "w".into(),
            actor: AgentId::new("h", "a"),
            target: AgentId::new("h", "b"),
            task: Some(TaskId("t1".into())),
            kind: AuditKind::Decided {
                decision: HandoffDecision::AlwaysAllow,
                by: "gate".into(),
            },
        };
        // Durable when `record` returns: readable through another handle at once.
        log.record(e.clone()).await.unwrap();
        assert_eq!(read_entries(&path).await.unwrap(), vec![e.clone()]);
        for i in 0..20 {
            log.record(AuditEntry {
                t_ms: i,
                ..e.clone()
            })
            .await
            .unwrap();
        }
        assert_eq!(read_entries(&path).await.unwrap().len(), 21);
        log.flush().await.unwrap();
        let _ = std::fs::remove_dir_all(&dir); // best-effort cleanup
    }

    fn fastrand_id() -> String {
        format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        )
    }
}
