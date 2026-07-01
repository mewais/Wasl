//! Error-path black-box suite for `wasl_server` (spec §Test plan E1-E5).

mod common;

use common::{FakeSink, FakeSurfacer};

use wasl_gate::{ClientCaps, GateError, GateOutcome};
use wasl_proto::{AgentId, ApprovalToken, GateDecision, GateKind};
use wasl_server::{Gatekeeper, ServerError};

fn from_agent() -> AgentId {
    AgentId("alice".to_string())
}

fn targets() -> Vec<AgentId> {
    vec![AgentId("bob".to_string())]
}

fn caps(elicitation: bool, marsad: bool) -> ClientCaps {
    ClientCaps {
        elicitation,
        marsad,
    }
}

/// E1 — Tier-1 audit failure: the sink fails → `authorize` returns `Err(ServerError::Audit(_))`
/// and the audit log stays empty (durability-first: no decision durably recorded, so no cache).
#[test]
fn e1_tier1_audit_failure() {
    let sink = FakeSink::failing();
    let sink_log = sink.log();
    let mut gk = Gatekeeper::new(
        caps(true, false),
        FakeSurfacer::new(GateOutcome::Approved),
        sink,
    );

    let err = gk
        .authorize(
            "req-e1".to_string(),
            GateKind::Consult,
            from_agent(),
            targets(),
            "preview".to_string(),
            ApprovalToken("tok-e1".to_string()),
            1,
        )
        .expect_err("sink failure must propagate");

    match err {
        ServerError::Audit(_) => {}
        other => panic!("expected ServerError::Audit, got {other:?}"),
    }

    assert!(
        gk.audit_entries().is_empty(),
        "durability-first: nothing cached when the sink rejects"
    );
    assert!(
        sink_log.borrow().appended.is_empty(),
        "failing sink records nothing"
    );
}

/// E2 — Tier-2 commit with an unknown token → `Err(ServerError::Gate(GateError::UnknownToken))`.
#[test]
fn e2_commit_unknown_token() {
    let mut gk = Gatekeeper::new(
        caps(false, false),
        FakeSurfacer::new(GateOutcome::Approved),
        FakeSink::ok(),
    );

    let err = gk
        .commit(
            &ApprovalToken("never-prepared".to_string()),
            GateDecision::Approve,
            1,
        )
        .expect_err("unknown token must error");

    assert_eq!(err, ServerError::Gate(GateError::UnknownToken));
    assert!(
        gk.audit_entries().is_empty(),
        "no decision recorded on error"
    );
}

/// E3 — Tier-2 commit already-resolved: commit once (ok), then commit the same token again →
/// `Err(ServerError::Gate(GateError::AlreadyResolved))`.
#[test]
fn e3_commit_already_resolved() {
    let token = ApprovalToken("tok-e3".to_string());
    let mut gk = Gatekeeper::new(
        caps(false, false),
        FakeSurfacer::new(GateOutcome::Approved),
        FakeSink::ok(),
    );

    gk.authorize(
        "req-e3".to_string(),
        GateKind::Command,
        from_agent(),
        targets(),
        "preview".to_string(),
        token.clone(),
        10,
    )
    .expect("prepare should succeed");

    gk.commit(&token, GateDecision::Approve, 11)
        .expect("first commit should succeed");

    let err = gk
        .commit(&token, GateDecision::Approve, 12)
        .expect_err("second commit must error");

    assert_eq!(err, ServerError::Gate(GateError::AlreadyResolved));

    // Only the first (successful) commit was recorded.
    assert_eq!(
        gk.audit_entries().len(),
        1,
        "the re-commit must not record again"
    );
}

/// E4 — Tier-2 commit audit failure: prepare ok, sink fails on commit → `Err(ServerError::Audit(_))`.
#[test]
fn e4_commit_audit_failure() {
    let token = ApprovalToken("tok-e4".to_string());
    let sink = FakeSink::failing();
    let sink_log = sink.log();
    let mut gk = Gatekeeper::new(
        caps(false, false),
        FakeSurfacer::new(GateOutcome::Approved),
        sink,
    );

    gk.authorize(
        "req-e4".to_string(),
        GateKind::Message,
        from_agent(),
        targets(),
        "preview".to_string(),
        token.clone(),
        20,
    )
    .expect("prepare should succeed (no audit yet)");

    let err = gk
        .commit(&token, GateDecision::Approve, 21)
        .expect_err("sink failure on commit must propagate");

    match err {
        ServerError::Audit(_) => {}
        other => panic!("expected ServerError::Audit, got {other:?}"),
    }

    assert!(
        gk.audit_entries().is_empty(),
        "durability-first: commit records nothing when the sink rejects"
    );
    assert!(sink_log.borrow().appended.is_empty());
}

/// E5 — `ServerError`: Display is non-empty and distinct per variant; `From<GateError>` works;
/// `std::error::Error` is implemented.
#[test]
fn e5_server_error_traits() {
    let audit = ServerError::Audit("disk full".to_string());
    let gate = ServerError::Gate(GateError::UnknownToken);

    // Display non-empty and distinct per variant.
    let audit_str = audit.to_string();
    let gate_str = gate.to_string();
    assert!(!audit_str.is_empty(), "Audit Display must be non-empty");
    assert!(!gate_str.is_empty(), "Gate Display must be non-empty");
    assert_ne!(
        audit_str, gate_str,
        "the two variants must render distinctly"
    );

    // From<GateError> yields the Gate variant.
    let converted: ServerError = GateError::AlreadyResolved.into();
    assert_eq!(converted, ServerError::Gate(GateError::AlreadyResolved));

    // std::error::Error is implemented — usable as a trait object / source chain.
    let as_err: &dyn std::error::Error = &gate;
    assert!(!as_err.to_string().is_empty());
    fn assert_is_error<E: std::error::Error>(_e: &E) {}
    assert_is_error(&audit);
    assert_is_error(&gate);
}
