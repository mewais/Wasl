//! wasl-audit — the audit trail of a Wasl node: one [`AuditEntry`] per delegation, decision,
//! policy change and outcome, appended as one JSON line to a file.
//!
//! [`AuditLog::open`] starts a writer task fed through a bounded queue; [`AuditLog::record`]
//! resolves once its entry is written (callers never hold a lock across it).
//! [`AuditLog::flush`] resolves once everything recorded before it is on disk (tests, shutdown).
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
    Entry(Box<AuditEntry>, oneshot::Sender<()>),
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

    /// Append one entry; resolves once it is written (not yet synced; see
    /// [`AuditLog::flush`]).
    pub async fn record(&self, entry: AuditEntry) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Msg::Entry(Box::new(entry), tx)).await.is_err() {
            tracing::error!(path = %self.path.display(), "audit writer is gone; entry lost");
            return;
        }
        // The writer always answers unless it is gone (already logged there).
        let _ = rx.await;
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
    let mut last_error: Option<AuditError> = None;
    while let Some(msg) = rx.recv().await {
        match msg {
            Msg::Entry(e, done) => {
                let mut line = match serde_json::to_vec(&e) {
                    Ok(l) => l,
                    Err(err) => {
                        tracing::error!(error = %err, "cannot encode an audit entry");
                        continue;
                    }
                };
                line.push(b'\n');
                if let Err(err) = file.write_all(&line).await {
                    tracing::error!(path = %path.display(), error = %err, "cannot append to the audit trail");
                    last_error = Some(AuditError(err.to_string()));
                }
                let _ = done.send(()); // the recorder may have given up
            }
            Msg::Flush(reply) => {
                let r = match last_error.take() {
                    Some(e) => Err(e),
                    None => file
                        .sync_data()
                        .await
                        .map_err(|e| AuditError(e.to_string())),
                };
                let _ = reply.send(r); // the requester may have given up
            }
        }
    }
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
        log.record(e.clone()).await;
        log.flush().await.unwrap();
        assert_eq!(read_entries(&path).await.unwrap(), vec![e]);
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
