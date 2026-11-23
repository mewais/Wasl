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
use wasit_diag::{Code, ErrorInfo, HomePath};
use wasl_proto::diag::{emit, WaslEvent};
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

impl AuditKind {
    /// The kind as the `kind` field of its JSON line (`delegated`, `rejected`, ...).
    pub fn name(&self) -> &'static str {
        match self {
            AuditKind::Delegated { .. } => "delegated",
            AuditKind::Rejected { .. } => "rejected",
            AuditKind::Decided { .. } => "decided",
            AuditKind::PolicyChanged { .. } => "policy_changed",
            AuditKind::Finished { .. } => "finished",
        }
    }
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

/// Why the trail could not be written or read. The text names an operation and the reason the
/// OS (or the writer) gave; it never quotes an entry (a trail holds what agents and people
/// wrote), which is what lets [`AuditError::diag`] keep it in a diagnostics record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditError(pub String);

impl AuditError {
    /// The error as a diagnostics field: its text is the operation and the OS's reason only
    /// (see the type's documentation).
    pub fn diag(&self) -> ErrorInfo {
        ErrorInfo::ours(Code::st("audit"), self)
    }
}

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
                    let (kind, task) = (e.kind.name(), e.task.clone());
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
                    waiters.push(Waiter::Entry(done, r, kind, task));
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
            emit(WaslEvent::AuditSyncFailed {
                path: HomePath::new(&path),
                error: e.diag(),
            });
        }
        for w in waiters {
            match w {
                Waiter::Entry(done, r, kind, task) => {
                    if let Err(e) = &r {
                        emit(WaslEvent::AuditEntryLost {
                            path: HomePath::new(&path),
                            kind: Code::st(kind),
                            task: task.as_ref().map(wasl_proto::diag::task),
                            error: e.diag(),
                        });
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
        &'static str,
        Option<TaskId>,
    ),
    Flush(oneshot::Sender<Result<(), AuditError>>),
}

/// Read a trail back (blank lines skipped).
pub async fn read_entries(path: impl AsRef<Path>) -> Result<Vec<AuditEntry>, AuditError> {
    let text = tokio::fs::read_to_string(path.as_ref())
        .await
        .map_err(|e| AuditError(format!("read {}: {e}", path.as_ref().display())))?;
    // The error says where and what kind of failure, not what the line held: `serde_json`'s
    // text quotes the offending value, and the trail holds what agents and people wrote.
    text.lines()
        .enumerate()
        .filter(|(_, l)| !l.trim().is_empty())
        .map(|(i, l)| {
            serde_json::from_str(l).map_err(|e| {
                AuditError(format!(
                    "bad entry on line {}: {} at column {}",
                    i + 1,
                    match e.classify() {
                        serde_json::error::Category::Io => "i/o error",
                        serde_json::error::Category::Syntax => "syntax error",
                        serde_json::error::Category::Data => "unexpected shape",
                        serde_json::error::Category::Eof => "unexpected end",
                    },
                    e.column()
                ))
            })
        })
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

    /// A trail that cannot be read back reports where and what kind of failure, not what the
    /// line held (`serde_json` quotes the offending value, and the trail holds what agents and
    /// people wrote): this is what lets `AuditError::diag` keep the text.
    #[tokio::test]
    async fn an_unreadable_entry_is_reported_without_its_text() {
        let dir = std::env::temp_dir().join(format!("wasl-audit-bad-{}", fastrand_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("audit.ndjson");
        std::fs::write(
            &path,
            "{\"t_ms\":\"AUDIT-MARKER-5e2a\",\"workstream\":\"w\"}\n",
        )
        .unwrap();
        let e = read_entries(&path).await.unwrap_err();
        assert!(e.0.starts_with("bad entry on line 1"), "{e}");
        assert!(!e.0.contains("AUDIT-MARKER-5e2a"), "{e}");
        assert!(!format!("{:?}", e.diag()).contains("AUDIT-MARKER-5e2a"));
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
