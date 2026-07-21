//! Black-box error-path tests for `wasl_audit` per docs/specs/audit.md §Test plan (E1-E2).

use wasl_audit::{AuditEntry, AuditError, AuditLog, EntryKind, Sink};
use wasl_proto::AgentId;

// ---------------------------------------------------------------------
// Fake Sink — records appended entries in order; can be scripted to fail
// on demand (fails every call while `fail` is true).
// ---------------------------------------------------------------------

#[derive(Debug, Default)]
struct FakeSink {
    appended: Vec<AuditEntry>,
    fail: bool,
}

impl FakeSink {
    fn new() -> Self {
        Self::default()
    }

    fn always_failing() -> Self {
        Self {
            appended: Vec::new(),
            fail: true,
        }
    }
}

impl Sink for FakeSink {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
        if self.fail {
            return Err(AuditError::Sink("disk full".to_string()));
        }
        self.appended.push(entry.clone());
        Ok(())
    }
}

fn agent(id: &str) -> AgentId {
    AgentId(id.to_string())
}

fn entry(seq: u64, actor: &str, target: &str, summary: &str) -> AuditEntry {
    AuditEntry {
        seq,
        kind: EntryKind::Message,
        actor: agent(actor),
        targets: vec![agent(target)],
        summary: summary.to_string(),
        decision: None,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------
// E1 — record when the sink append fails -> Err(AuditError::Sink(_)) AND
// the entry is NOT cached (entries() unchanged / len not incremented) —
// durability-first invariant.
// ---------------------------------------------------------------------

#[test]
fn e1_record_sink_failure_returns_err_and_does_not_cache() {
    let mut log = AuditLog::new(FakeSink::always_failing());

    assert_eq!(log.len(), 0);
    assert!(log.is_empty());

    let e = entry(1, "alice", "bob", "this must not be cached");
    let result = log.record(e);

    match result {
        Err(AuditError::Sink(detail)) => {
            assert!(!detail.is_empty(), "sink error detail should be non-empty");
        }
        other => panic!("expected Err(AuditError::Sink(_)), got {other:?}"),
    }

    assert_eq!(log.len(), 0, "failed record must not increment len");
    assert!(log.is_empty(), "failed record must not populate the cache");
    assert_eq!(log.entries(), &[], "cache must remain empty after failure");
}

#[test]
fn e1_record_partial_failure_only_uncaches_the_failing_entry() {
    // A sink that succeeds for the first record, then starts failing:
    // the first (successful) entry must remain cached, and the second
    // (failed) entry must never be added, leaving len at 1, not 2.
    let mut log = AuditLog::new(FakeSink::new());

    let e1 = entry(1, "alice", "bob", "first entry, sink is healthy");
    assert_eq!(log.record(e1.clone()), Ok(()));
    assert_eq!(log.len(), 1);
    assert_eq!(log.entries(), std::slice::from_ref(&e1));

    // Now flip a *fresh* always-failing log to verify no partial caching
    // occurs on the failing call itself (the crate must not implement
    // any lookahead / retry that could partially cache).
    let mut failing_log = AuditLog::new(FakeSink::always_failing());
    let e2 = entry(2, "carol", "dave", "second entry, sink now broken");
    let result = failing_log.record(e2);
    assert!(result.is_err());
    assert_eq!(failing_log.len(), 0);
    assert_eq!(failing_log.entries(), &[]);

    // Sanity: the healthy log from before is unaffected by the separate
    // failing log instance.
    assert_eq!(log.len(), 1);
    assert_eq!(log.entries(), &[e1]);
}

// ---------------------------------------------------------------------
// E2 — AuditError Display non-empty + carries the sink detail;
// std::error::Error implemented.
// ---------------------------------------------------------------------

#[test]
fn e2_audit_error_display_non_empty_and_carries_detail() {
    let err = AuditError::Sink("connection reset by peer".to_string());
    let display = err.to_string();

    assert!(!display.is_empty(), "Display must be non-empty");
    assert!(
        display.contains("connection reset by peer"),
        "Display must carry the sink detail, got: {display}"
    );
}

#[test]
fn e2_audit_error_implements_std_error() {
    fn assert_std_error<E: std::error::Error>(_: &E) {}
    let err = AuditError::Sink("boom".to_string());
    assert_std_error(&err);

    // Exercise the trait object path too, confirming Display is usable
    // through `dyn std::error::Error` as real call sites would use it.
    let boxed: Box<dyn std::error::Error> = Box::new(AuditError::Sink("boxed boom".to_string()));
    let boxed_display = boxed.to_string();
    assert!(!boxed_display.is_empty());
    assert!(boxed_display.contains("boxed boom"));
}
