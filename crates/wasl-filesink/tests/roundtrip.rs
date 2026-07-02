//! Black-box happy-path + persistence tests for `wasl_filesink` per
//! docs/specs/filesink.md §Test plan. Real files via `tempfile::tempdir()`.

use std::fs::OpenOptions;
use std::io::Write as _;

use wasl_audit::{AuditEntry, EntryKind, Sink};
use wasl_filesink::{read_entries, FileSink};
use wasl_proto::{AgentId, GateDecision};

fn agent(id: &str) -> AgentId {
    AgentId(id.to_string())
}

fn targets(ids: &[&str]) -> Vec<AgentId> {
    ids.iter().map(|s| agent(s)).collect()
}

fn entry(
    seq: u64,
    kind: EntryKind,
    actor: &str,
    targets_: &[&str],
    summary: &str,
    decision: Option<GateDecision>,
) -> AuditEntry {
    AuditEntry {
        seq,
        kind,
        actor: agent(actor),
        targets: targets(targets_),
        summary: summary.to_string(),
        decision,
    }
}

// ---------------------------------------------------------------------
// open + append + read round-trip: append a Decision entry with
// Some(Approve) and a plain entry with decision: None; read_entries
// returns them in order, field-for-field equal; path() == opened path.
// ---------------------------------------------------------------------

#[test]
fn roundtrip_open_append_read_preserves_entries_in_order() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let path = dir.path().join("audit.jsonl");

    let decision_entry = entry(
        1,
        EntryKind::Decision,
        "alice",
        &["bob"],
        "approved the request",
        Some(GateDecision::Approve),
    );
    let plain_entry = entry(
        2,
        EntryKind::Consult,
        "bob",
        &["carol", "dave"],
        "asked a question",
        None,
    );
    let deny_entry = entry(
        3,
        EntryKind::Decision,
        "carol",
        &["alice"],
        "denied the request",
        Some(GateDecision::Deny {
            reason: "not authorized".to_string(),
        }),
    );

    {
        let mut sink = FileSink::open(&path).expect("open sink");
        assert_eq!(sink.path(), path.as_path(), "path() must equal opened path");

        sink.append(&decision_entry).expect("append decision entry");
        sink.append(&plain_entry).expect("append plain entry");
        sink.append(&deny_entry).expect("append deny entry");
    }

    let read_back = read_entries(&path).expect("read entries back");
    assert_eq!(
        read_back,
        vec![
            decision_entry.clone(),
            plain_entry.clone(),
            deny_entry.clone()
        ],
        "entries must round-trip in append order, field-for-field"
    );

    // Explicit field-for-field spot checks on the decision/None contrast.
    assert_eq!(read_back[0].seq, 1);
    assert_eq!(read_back[0].kind, EntryKind::Decision);
    assert_eq!(read_back[0].actor, agent("alice"));
    assert_eq!(read_back[0].targets, targets(&["bob"]));
    assert_eq!(read_back[0].summary, "approved the request");
    assert_eq!(read_back[0].decision, Some(GateDecision::Approve));

    assert_eq!(read_back[1].decision, None);
    assert_eq!(read_back[1].targets, targets(&["carol", "dave"]));

    assert_eq!(
        read_back[2].decision,
        Some(GateDecision::Deny {
            reason: "not authorized".to_string()
        })
    );
}

// ---------------------------------------------------------------------
// persistence across "restart": open, append one, drop; re-open SAME
// path, append a second, drop; read_entries returns BOTH in order.
// ---------------------------------------------------------------------

#[test]
fn persistence_reopen_same_path_appends_after_prior_records() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let path = dir.path().join("restart.jsonl");

    let first = entry(
        1,
        EntryKind::Message,
        "alice",
        &["bob"],
        "before restart",
        None,
    );
    let second = entry(
        2,
        EntryKind::Command,
        "alice",
        &["bob"],
        "after restart",
        None,
    );

    {
        let mut sink = FileSink::open(&path).expect("first open");
        sink.append(&first).expect("append first");
    } // drop -> simulates process exit

    {
        let mut sink = FileSink::open(&path).expect("re-open same path");
        sink.append(&second).expect("append second");
    } // drop

    let read_back = read_entries(&path).expect("read entries back");
    assert_eq!(
        read_back,
        vec![first, second],
        "re-open must append after prior records (both entries, in order)"
    );
}

// ---------------------------------------------------------------------
// read: blank line skipped. Build a file with a valid JSON line, then a
// blank line, then another valid JSON line (blank inserted via std::fs);
// read_entries returns exactly the two entries.
// ---------------------------------------------------------------------

#[test]
fn read_skips_blank_lines() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let path = dir.path().join("blank.jsonl");

    let first = entry(1, EntryKind::Consult, "alice", &["bob"], "first line", None);
    let second = entry(
        2,
        EntryKind::Message,
        "bob",
        &["alice"],
        "second line",
        None,
    );

    // Write the first valid JSON line via FileSink (file ends "json1\n").
    {
        let mut sink = FileSink::open(&path).expect("open sink for first entry");
        sink.append(&first).expect("append first");
    }

    // Insert a blank line by appending a bare newline: file -> "json1\n\n".
    {
        let mut f = OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open for blank line");
        f.write_all(b"\n").expect("write blank line");
    }

    // Append the second valid JSON line -> "json1\n\njson2\n".
    {
        let mut sink = FileSink::open(&path).expect("open sink for second entry");
        sink.append(&second).expect("append second");
    }

    let read_back = read_entries(&path).expect("read entries back");
    assert_eq!(
        read_back,
        vec![first, second],
        "blank line between records must be skipped; exactly two entries"
    );
}
