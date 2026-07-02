//! Black-box error-path tests for `wasl_filesink` per docs/specs/filesink.md
//! §Test plan. Real files via `tempfile::tempdir()`; write errors via /dev/full.

use std::fs;
use std::path::Path;

use wasl_audit::{AuditEntry, AuditError, EntryKind, Sink};
use wasl_filesink::{read_entries, FileSink};
use wasl_proto::AgentId;

fn agent(id: &str) -> AgentId {
    AgentId(id.to_string())
}

fn entry(seq: u64, summary: &str) -> AuditEntry {
    AuditEntry {
        seq,
        kind: EntryKind::Message,
        actor: agent("alice"),
        targets: vec![agent("bob")],
        summary: summary.to_string(),
        decision: None,
    }
}

// ---------------------------------------------------------------------
// open error: parent directory does not exist -> Err(AuditError::Sink(_)).
// ---------------------------------------------------------------------

#[test]
fn open_missing_parent_dir_errors() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let path = dir.path().join("does-not-exist").join("audit.jsonl");
    assert!(
        !path.parent().unwrap().exists(),
        "test precondition: parent dir must not exist"
    );

    let result = FileSink::open(&path);
    match result {
        Err(AuditError::Sink(detail)) => {
            assert!(!detail.is_empty(), "sink error detail should be non-empty");
        }
        Ok(_) => panic!("expected open to fail when parent dir is missing"),
    }
}

// ---------------------------------------------------------------------
// write error: open /dev/full then append -> Err(AuditError::Sink(_)).
// /dev/full always returns ENOSPC on write. Required in this env.
// ---------------------------------------------------------------------

#[test]
fn append_to_dev_full_errors() {
    assert!(
        Path::new("/dev/full").exists(),
        "this environment must provide /dev/full"
    );

    let mut sink = FileSink::open("/dev/full").expect("open /dev/full for append");
    let result = sink.append(&entry(1, "this write must hit ENOSPC"));
    match result {
        Err(AuditError::Sink(detail)) => {
            assert!(!detail.is_empty(), "write error detail should be non-empty");
        }
        Ok(()) => panic!("expected append to /dev/full to fail with ENOSPC"),
    }
}

// ---------------------------------------------------------------------
// read: missing file -> Err(AuditError::Sink(_)).
// ---------------------------------------------------------------------

#[test]
fn read_missing_file_errors() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let path = dir.path().join("nope.jsonl");
    assert!(!path.exists(), "test precondition: file must not exist");

    let result = read_entries(&path);
    match result {
        Err(AuditError::Sink(detail)) => {
            assert!(!detail.is_empty(), "read error detail should be non-empty");
        }
        Ok(entries) => panic!("expected read of missing file to fail, got {entries:?}"),
    }
}

// ---------------------------------------------------------------------
// read: malformed line -> Err(AuditError::Sink(_)).
// ---------------------------------------------------------------------

#[test]
fn read_malformed_line_errors() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let path = dir.path().join("bad.jsonl");
    fs::write(&path, "not json\n").expect("write malformed file");

    let result = read_entries(&path);
    match result {
        Err(AuditError::Sink(detail)) => {
            assert!(!detail.is_empty(), "parse error detail should be non-empty");
        }
        Ok(entries) => panic!("expected read of malformed line to fail, got {entries:?}"),
    }
}
