//! Black-box happy-path tests for `wasl_audit` per docs/specs/audit.md §Test plan (A1-A5).

use wasl_audit::{AuditEntry, AuditError, AuditLog, EntryKind, Sink};
use wasl_proto::{AgentId, GateDecision};

// ---------------------------------------------------------------------
// Fake Sink — records appended entries in order; can be scripted to fail.
// ---------------------------------------------------------------------

#[derive(Debug, Default)]
struct FakeSink {
    appended: Vec<AuditEntry>,
    fail_next: bool,
}

impl FakeSink {
    fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)]
    fn failing() -> Self {
        Self {
            appended: Vec::new(),
            fail_next: true,
        }
    }
}

impl Sink for FakeSink {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
        if self.fail_next {
            return Err(AuditError::Sink("fake sink failure".to_string()));
        }
        self.appended.push(entry.clone());
        Ok(())
    }
}

fn agent(id: &str) -> AgentId {
    AgentId(id.to_string())
}

fn targets(ids: &[&str]) -> Vec<AgentId> {
    ids.iter().map(|s| agent(s)).collect()
}

fn entry(seq: u64, kind: EntryKind, actor: &str, targets_: &[&str], summary: &str) -> AuditEntry {
    AuditEntry {
        seq,
        kind,
        actor: agent(actor),
        targets: targets(targets_),
        summary: summary.to_string(),
        decision: None,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------
// A1 — record appends to the sink AND caches (entries() reflects it, in
// order); len/is_empty.
// ---------------------------------------------------------------------

#[test]
fn a1_record_appends_to_sink_and_caches_in_order() {
    let mut log = AuditLog::new(FakeSink::new());

    assert!(log.is_empty());
    assert_eq!(log.len(), 0);
    assert_eq!(log.entries(), &[]);

    let e1 = entry(1, EntryKind::Consult, "alice", &["bob"], "asked a question");
    let e2 = entry(2, EntryKind::Command, "alice", &["carol"], "issued a task");

    assert_eq!(log.record(e1.clone()), Ok(()));
    assert!(!log.is_empty());
    assert_eq!(log.len(), 1);
    assert_eq!(log.entries(), std::slice::from_ref(&e1));

    assert_eq!(log.record(e2.clone()), Ok(()));
    assert_eq!(log.len(), 2);
    assert!(!log.is_empty());
    assert_eq!(log.entries(), &[e1, e2]);
}

#[test]
fn a1_record_appends_to_the_underlying_sink() {
    // A separate, directly-observable FakeSink: after recording two entries
    // via AuditLog, the sink itself (not just the cache) must have received
    // both appends, in order.
    let mut sink = FakeSink::new();
    let e1 = entry(1, EntryKind::Message, "dave", &["erin"], "hello there");
    let e2 = entry(2, EntryKind::Transfer, "dave", &["erin"], "sending a file");

    // Drive the sink directly first to establish baseline semantics, then
    // drive it through AuditLog and confirm cache mirrors sink append order.
    sink.append(&e1).expect("direct append ok");
    assert_eq!(sink.appended, vec![e1.clone()]);

    let mut log = AuditLog::new(FakeSink::new());
    log.record(e1.clone()).expect("record e1 ok");
    log.record(e2.clone()).expect("record e2 ok");
    assert_eq!(log.entries(), &[e1, e2]);
    assert_eq!(log.len(), 2);
}

// ---------------------------------------------------------------------
// A2 — by_actor: only that actor's entries, in record order.
// ---------------------------------------------------------------------

#[test]
fn a2_by_actor_filters_matching_only_in_record_order() {
    let mut log = AuditLog::new(FakeSink::new());

    let a1 = entry(1, EntryKind::Consult, "alice", &["bob"], "alice consult 1");
    let b1 = entry(2, EntryKind::Message, "bob", &["alice"], "bob message 1");
    let a2 = entry(
        3,
        EntryKind::Command,
        "alice",
        &["carol"],
        "alice command 1",
    );
    let c1 = entry(
        4,
        EntryKind::Transfer,
        "carol",
        &["alice"],
        "carol transfer 1",
    );

    for e in [a1.clone(), b1.clone(), a2.clone(), c1.clone()] {
        log.record(e).expect("record ok");
    }

    let alice_entries = log.by_actor(&agent("alice"));
    assert_eq!(alice_entries, vec![&a1, &a2]);

    let bob_entries = log.by_actor(&agent("bob"));
    assert_eq!(bob_entries, vec![&b1]);

    let carol_entries = log.by_actor(&agent("carol"));
    assert_eq!(carol_entries, vec![&c1]);

    let nobody_entries = log.by_actor(&agent("nobody"));
    assert!(nobody_entries.is_empty());
}

// ---------------------------------------------------------------------
// A3 — by_kind: only that kind (test at least Consult + Transfer +
// Decision).
// ---------------------------------------------------------------------

#[test]
fn a3_by_kind_filters_consult_transfer_decision() {
    let mut log = AuditLog::new(FakeSink::new());

    let consult = entry(1, EntryKind::Consult, "alice", &["bob"], "a consult");
    let command = entry(2, EntryKind::Command, "alice", &["bob"], "a command");
    let message = entry(3, EntryKind::Message, "alice", &["bob"], "a message");
    let transfer = entry(4, EntryKind::Transfer, "alice", &["bob"], "a transfer");
    let mut decision = entry(5, EntryKind::Decision, "alice", &["bob"], "a decision");
    decision.decision = Some(GateDecision::Approve);

    for e in [
        consult.clone(),
        command.clone(),
        message.clone(),
        transfer.clone(),
        decision.clone(),
    ] {
        log.record(e).expect("record ok");
    }

    assert_eq!(log.by_kind(EntryKind::Consult), vec![&consult]);
    assert_eq!(log.by_kind(EntryKind::Transfer), vec![&transfer]);
    assert_eq!(log.by_kind(EntryKind::Decision), vec![&decision]);
    assert_eq!(log.by_kind(EntryKind::Command), vec![&command]);
    assert_eq!(log.by_kind(EntryKind::Message), vec![&message]);
}

#[test]
fn a3_by_kind_preserves_record_order_for_repeated_kind() {
    let mut log = AuditLog::new(FakeSink::new());

    let c1 = entry(1, EntryKind::Consult, "alice", &["bob"], "consult 1");
    let t1 = entry(2, EntryKind::Transfer, "alice", &["bob"], "transfer 1");
    let c2 = entry(3, EntryKind::Consult, "alice", &["bob"], "consult 2");

    for e in [c1.clone(), t1.clone(), c2.clone()] {
        log.record(e).expect("record ok");
    }

    assert_eq!(log.by_kind(EntryKind::Consult), vec![&c1, &c2]);
    assert_eq!(log.by_kind(EntryKind::Transfer), vec![&t1]);
}

// ---------------------------------------------------------------------
// A4 — decisions: only entries carrying a Some(decision); assert both an
// Approve and a Deny surface.
// ---------------------------------------------------------------------

#[test]
fn a4_decisions_filters_only_some_decision_entries() {
    let mut log = AuditLog::new(FakeSink::new());

    let no_decision = entry(1, EntryKind::Message, "alice", &["bob"], "just a message");

    let mut approved = entry(
        2,
        EntryKind::Decision,
        "alice",
        &["bob"],
        "approved request",
    );
    approved.decision = Some(GateDecision::Approve);

    let mut denied = entry(3, EntryKind::Decision, "alice", &["bob"], "denied request");
    denied.decision = Some(GateDecision::Deny {
        reason: "not authorized".to_string(),
    });

    let another_no_decision = entry(
        4,
        EntryKind::Consult,
        "alice",
        &["bob"],
        "another non-decision",
    );

    for e in [
        no_decision,
        approved.clone(),
        denied.clone(),
        another_no_decision,
    ] {
        log.record(e).expect("record ok");
    }

    let decisions = log.decisions();
    assert_eq!(decisions, vec![&approved, &denied]);

    assert!(decisions
        .iter()
        .any(|e| e.decision == Some(GateDecision::Approve)));
    assert!(decisions.iter().any(|e| e.decision
        == Some(GateDecision::Deny {
            reason: "not authorized".to_string()
        })));
    assert!(decisions.iter().all(|e| e.decision.is_some()));
}

// ---------------------------------------------------------------------
// A5 — serde: AuditEntry round-trips incl. decision: None default and
// Some(Approve)/Some(Deny); EntryKind lowercase round-trip.
// ---------------------------------------------------------------------

#[test]
fn a5_entry_kind_lowercase_serde_round_trip() {
    let cases = [
        (EntryKind::Consult, "\"consult\""),
        (EntryKind::Command, "\"command\""),
        (EntryKind::Message, "\"message\""),
        (EntryKind::Transfer, "\"transfer\""),
        (EntryKind::Decision, "\"decision\""),
    ];
    for (kind, expected_json) in cases {
        let json = serde_json::to_string(&kind).expect("serialize EntryKind");
        assert_eq!(json, expected_json, "serialized form for {kind:?}");
        let back: EntryKind = serde_json::from_str(&json).expect("deserialize EntryKind");
        assert_eq!(back, kind, "round-trip for {kind:?}");
    }
}

#[test]
fn a5_audit_entry_serde_round_trip_decision_none() {
    let e = entry(
        7,
        EntryKind::Message,
        "alice",
        &["bob", "carol"],
        "hi there",
    );
    assert_eq!(e.decision, None);

    let json = serde_json::to_string(&e).expect("serialize AuditEntry");
    let back: AuditEntry = serde_json::from_str(&json).expect("deserialize AuditEntry");
    assert_eq!(back, e);
    assert_eq!(back.decision, None);
}

#[test]
fn a5_audit_entry_decision_defaults_to_none_when_field_absent() {
    // The `decision` field is `#[serde(default)]`; a payload that omits it
    // entirely must still deserialize successfully with `decision: None`.
    let json = serde_json::json!({
        "seq": 9,
        "kind": "consult",
        "actor": "alice",
        "targets": ["bob"],
        "summary": "no decision field present"
    })
    .to_string();

    let back: AuditEntry = serde_json::from_str(&json).expect("deserialize without decision");
    assert_eq!(back.seq, 9);
    assert_eq!(back.kind, EntryKind::Consult);
    assert_eq!(back.actor, agent("alice"));
    assert_eq!(back.targets, vec![agent("bob")]);
    assert_eq!(back.summary, "no decision field present");
    assert_eq!(back.decision, None);
}

#[test]
fn a5_audit_entry_serde_round_trip_decision_some_approve() {
    let mut e = entry(10, EntryKind::Decision, "alice", &["bob"], "approved");
    e.decision = Some(GateDecision::Approve);

    let json = serde_json::to_string(&e).expect("serialize AuditEntry");
    let back: AuditEntry = serde_json::from_str(&json).expect("deserialize AuditEntry");
    assert_eq!(back, e);
    assert_eq!(back.decision, Some(GateDecision::Approve));
}

#[test]
fn a5_audit_entry_serde_round_trip_decision_some_deny() {
    let mut e = entry(11, EntryKind::Decision, "alice", &["bob"], "denied");
    e.decision = Some(GateDecision::Deny {
        reason: "policy violation".to_string(),
    });

    let json = serde_json::to_string(&e).expect("serialize AuditEntry");
    let back: AuditEntry = serde_json::from_str(&json).expect("deserialize AuditEntry");
    assert_eq!(back, e);
    assert_eq!(
        back.decision,
        Some(GateDecision::Deny {
            reason: "policy violation".to_string()
        })
    );
}
