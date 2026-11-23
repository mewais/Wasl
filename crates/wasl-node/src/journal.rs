//! The node's durable state: an append-only JSON-lines journal (`<dir>/journal.ndjson`).
//!
//! Every record carries the node-wide revision `rev` it was produced with and a journal `seq`.
//! A record *replaces* the previous record with the same [`Key`]; recovery keeps the newest
//! revision per key. On open the file is compacted (rewritten with the latest record per key,
//! original `seq`s preserved, via an atomic rename).
//!
//! Records are submitted *under the node's state lock* with [`Journal::submit`] (a
//! non-blocking push into a bounded queue, so journal order equals lock order) and the caller
//! awaits durability *after* releasing the lock. A single writer task appends, `fsync`s once
//! per batch, publishes the records to topic subscribers and acknowledges.
//!
//! Topic subscribers ([`Journal::subscribe`]) get the latest record of every key with
//! `seq > from` (coalesced replay) followed by live records, with no gap and no duplicate: the
//! writer publishes under the same mutex the subscription snapshot is taken under.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::{broadcast, mpsc, oneshot};
use wasit_diag::{Code, ErrorInfo, HomePath, Ident};
use wasl_proto::diag::{count, emit, foreign, task, WaslEvent};
use wasl_proto::{EdgeEvent, TaskId, Topology};

use crate::state::{InPersist, OutPersist, SpawnPersist};

/// Bound on records waiting for the writer.
const QUEUE: usize = 8192;
/// Bound on live records buffered per topic subscriber.
const SUB_QUEUE: usize = 1024;
/// Edge events kept across compactions.
const KEEP_EDGE_EVENTS: usize = 1000;

/// One journal entry's payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "rec", rename_all = "snake_case")]
pub enum Rec {
    Out(Box<OutPersist>),
    In(Box<InPersist>),
    Topology(Box<Topology>),
    TopologyRemoved { workstream: String },
    Edge(Box<EdgeEvent>),
    Spawn(Box<SpawnPersist>),
}

/// What a record replaces.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Key {
    Out(TaskId),
    In(TaskId),
    Topology(String),
    /// Edge events are history, not state: each is its own key.
    Edge(u64),
    Spawn(String),
}

impl Rec {
    /// What the record is, for diagnostics: its kind and, for a task or a spawn request, its
    /// id (a topology is named by the supervisor, and a name is content).
    fn describe(&self) -> (Code, Option<Ident>) {
        match self {
            Rec::Out(o) => (Code::st("out"), Some(task(&o.task.id))),
            Rec::In(i) => (Code::st("in"), Some(task(&i.task.id))),
            Rec::Topology(_) => (Code::st("topology"), None),
            Rec::TopologyRemoved { .. } => (Code::st("topology_removed"), None),
            Rec::Edge(_) => (Code::st("edge"), None),
            Rec::Spawn(s) => (Code::st("spawn"), Some(Ident::new(&s.request_id))),
        }
    }

    fn key(&self, seq: u64) -> Key {
        match self {
            Rec::Out(o) => Key::Out(o.task.id.clone()),
            Rec::In(i) => Key::In(i.task.id.clone()),
            Rec::Topology(t) => Key::Topology(t.workstream.clone()),
            Rec::TopologyRemoved { workstream } => Key::Topology(workstream.clone()),
            Rec::Edge(_) => Key::Edge(seq),
            Rec::Spawn(s) => Key::Spawn(s.request_id.clone()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub seq: u64,
    pub rev: u64,
    pub t: u64,
    #[serde(flatten)]
    pub rec: Rec,
}

/// Why a submit failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalError {
    /// The queue is full (the disk is not keeping up).
    Busy,
    Io(String),
}

impl JournalError {
    /// The error as a diagnostics field. The text names an operation and the OS's reason (or
    /// says the queue is full or the writer gone); it is built here from `io::Error`s and
    /// never from a record's content, so it is kept (scrubbed).
    pub fn diag(&self) -> ErrorInfo {
        ErrorInfo::ours(Code::st("journal"), self)
    }
}

impl std::fmt::Display for JournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JournalError::Busy => write!(f, "the node journal is not keeping up"),
            JournalError::Io(e) => write!(f, "node journal: {e}"),
        }
    }
}

/// Resolves once the submitted record is durable.
pub type Ack = oneshot::Receiver<Result<u64, JournalError>>;

struct Pending {
    rev: u64,
    t: u64,
    rec: Rec,
    ack: oneshot::Sender<Result<u64, JournalError>>,
}

struct Published {
    next_seq: u64,
    latest: BTreeMap<Key, Entry>,
    live: broadcast::Sender<Entry>,
}

/// See the module docs. Cheap to clone.
#[derive(Clone)]
pub struct Journal {
    tx: mpsc::Sender<Pending>,
    published: Arc<Mutex<Published>>,
}

fn lock(m: &Mutex<Published>) -> std::sync::MutexGuard<'_, Published> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Keep `e` in `latest` if it is newer than what is there.
fn fold(latest: &mut BTreeMap<Key, Entry>, e: Entry) -> bool {
    let key = e.rec.key(e.seq);
    match latest.get(&key) {
        Some(old) if old.rev > e.rev => false,
        _ => {
            latest.insert(key, e);
            true
        }
    }
}

impl Journal {
    /// Open (creating and compacting) the journal in `dir`; returns the recovered records in
    /// `seq` order.
    pub async fn open(dir: &Path) -> Result<(Journal, Vec<Entry>), JournalError> {
        tokio::fs::create_dir_all(dir)
            .await
            .map_err(|e| JournalError::Io(format!("create {}: {e}", dir.display())))?;
        let path = dir.join("journal.ndjson");
        let mut latest = BTreeMap::new();
        let mut next_seq = 1;
        let mut skipped = 0u32;
        match tokio::fs::read_to_string(&path).await {
            Ok(text) => {
                for (n, line) in text
                    .lines()
                    .enumerate()
                    .filter(|(_, l)| !l.trim().is_empty())
                {
                    match serde_json::from_str::<Entry>(line) {
                        Ok(e) => {
                            next_seq = next_seq.max(e.seq + 1);
                            fold(&mut latest, e);
                        }
                        // A torn last line after a crash: everything before it is intact.
                        // The parse error quotes the line, so only its size and fingerprint
                        // are recorded.
                        Err(err) => {
                            skipped = skipped.saturating_add(1);
                            emit(WaslEvent::JournalLineSkipped {
                                line: n as u64 + 1,
                                error: foreign("journal_line", &err.to_string()),
                            });
                        }
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(JournalError::Io(format!("read {}: {e}", path.display()))),
        }
        // Bound the edge history.
        let edges: Vec<Key> = latest
            .keys()
            .filter(|k| matches!(k, Key::Edge(_)))
            .cloned()
            .collect();
        if edges.len() > KEEP_EDGE_EVENTS {
            for k in &edges[..edges.len() - KEEP_EDGE_EVENTS] {
                latest.remove(k);
            }
        }
        let mut entries: Vec<Entry> = latest.values().cloned().collect();
        entries.sort_by_key(|e| e.seq);
        compact(&path, &entries).await?;
        emit(WaslEvent::JournalOpened {
            path: HomePath::new(&path),
            records: count(entries.len()),
            skipped,
        });
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| JournalError::Io(format!("open {}: {e}", path.display())))?;
        let (tx, rx) = mpsc::channel(QUEUE);
        let published = Arc::new(Mutex::new(Published {
            next_seq,
            latest,
            live: broadcast::channel(SUB_QUEUE).0,
        }));
        tokio::spawn(writer(file, path, rx, published.clone()));
        Ok((Journal { tx, published }, entries))
    }

    /// Queue `rec` (call under the state lock; never blocks). Await the returned [`Ack`] after
    /// releasing the lock.
    pub fn submit(&self, rev: u64, t: u64, rec: Rec) -> Ack {
        let (ack, rx) = oneshot::channel();
        match self.tx.try_send(Pending { rev, t, rec, ack }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(p)) => {
                let (record, id) = p.rec.describe();
                let e = JournalError::Busy;
                emit(WaslEvent::JournalSubmitFailed {
                    record,
                    id,
                    error: e.diag(),
                });
                let _ = p.ack.send(Err(e)); // receiver is `rx` below
            }
            Err(mpsc::error::TrySendError::Closed(p)) => {
                let (record, id) = p.rec.describe();
                let e = JournalError::Io("writer is gone".into());
                emit(WaslEvent::JournalSubmitFailed {
                    record,
                    id,
                    error: e.diag(),
                });
                let _ = p.ack.send(Err(e));
                // as above
            }
        }
        rx
    }

    /// The latest record of every key with `seq > from` (oldest first) and a receiver of every
    /// record published after them.
    pub fn subscribe(&self, from: Option<u64>) -> (Vec<Entry>, broadcast::Receiver<Entry>) {
        let p = lock(&self.published);
        let after = from.unwrap_or(0);
        let mut backlog: Vec<Entry> = p
            .latest
            .values()
            .filter(|e| e.seq > after)
            .cloned()
            .collect();
        backlog.sort_by_key(|e| e.seq);
        (backlog, p.live.subscribe())
    }
}

/// Wait for an [`Ack`].
pub async fn durable(ack: Ack) -> Result<u64, JournalError> {
    ack.await
        .unwrap_or_else(|_| Err(JournalError::Io("writer is gone".into())))
}

async fn compact(path: &Path, entries: &[Entry]) -> Result<(), JournalError> {
    let tmp: PathBuf = path.with_extension("ndjson.tmp");
    let mut buf = Vec::new();
    for e in entries {
        serde_json::to_writer(&mut buf, e).map_err(|e| JournalError::Io(e.to_string()))?;
        buf.push(b'\n');
    }
    let io = |what: &str, e: std::io::Error| JournalError::Io(format!("{what}: {e}"));
    let mut f = tokio::fs::File::create(&tmp)
        .await
        .map_err(|e| io("create the compacted journal", e))?;
    f.write_all(&buf)
        .await
        .map_err(|e| io("write the compacted journal", e))?;
    f.sync_all()
        .await
        .map_err(|e| io("sync the compacted journal", e))?;
    drop(f);
    tokio::fs::rename(&tmp, path)
        .await
        .map_err(|e| io("replace the journal", e))
}

async fn writer(
    mut file: tokio::fs::File,
    path: PathBuf,
    mut rx: mpsc::Receiver<Pending>,
    published: Arc<Mutex<Published>>,
) {
    let mut batch: Vec<Pending> = Vec::new();
    while rx.recv_many(&mut batch, 256).await > 0 {
        // Assign seqs and encode.
        let mut buf = Vec::new();
        let mut entries = Vec::with_capacity(batch.len());
        {
            let mut p = lock(&published);
            for item in &batch {
                let e = Entry {
                    seq: p.next_seq,
                    rev: item.rev,
                    t: item.t,
                    rec: item.rec.clone(),
                };
                p.next_seq += 1;
                entries.push(e);
            }
        }
        for e in &entries {
            if let Err(err) = serde_json::to_writer(&mut buf, e) {
                let (record, id) = e.rec.describe();
                emit(WaslEvent::JournalEncodeFailed {
                    record,
                    id,
                    error: foreign("encode", &err.to_string()),
                });
            }
            buf.push(b'\n');
        }
        let res = match file.write_all(&buf).await {
            Ok(()) => file.sync_data().await,
            Err(e) => Err(e),
        };
        match res {
            Ok(()) => {
                let mut p = lock(&published);
                for (item, e) in batch.drain(..).zip(entries) {
                    let seq = e.seq;
                    if fold(&mut p.latest, e.clone()) {
                        // No subscribers is fine.
                        let _ = p.live.send(e);
                    }
                    let _ = item.ack.send(Ok(seq)); // the submitter may have given up
                }
            }
            Err(err) => {
                emit(WaslEvent::JournalAppendFailed {
                    path: HomePath::new(&path),
                    records: count(entries.len()),
                    ids: entries.iter().filter_map(|e| e.rec.describe().1).collect(),
                    error: ErrorInfo::io(&err),
                });
                for item in batch.drain(..) {
                    let _ = item.ack.send(Err(JournalError::Io(err.to_string())));
                    // as above
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The premise of the `foreign` classification of an unreadable line: `serde_json`'s error
    /// quotes the offending value, so its text must never reach a record.
    #[test]
    fn the_parse_error_of_a_journal_line_quotes_the_line() {
        let line = r#"{"seq":"JOURNAL-MARKER-7c1e","rev":1,"t":1,"rec":"topology_removed","workstream":"w"}"#;
        let e = serde_json::from_str::<Entry>(line).unwrap_err();
        assert!(e.to_string().contains("JOURNAL-MARKER-7c1e"), "{e}");
        let info = foreign("journal_line", &e.to_string());
        assert!(!format!("{info:?}").contains("JOURNAL-MARKER-7c1e"));
    }

    /// What is recorded about a journal record: its kind, and an id only for tasks and spawn
    /// requests (a topology is named by the supervisor).
    #[test]
    fn a_record_is_described_by_kind_and_task_or_request_id_only() {
        let (kind, id) = Rec::TopologyRemoved {
            workstream: "Secret plan".into(),
        }
        .describe();
        assert_eq!(kind, Code::st("topology_removed"));
        assert!(id.is_none());
    }
}
