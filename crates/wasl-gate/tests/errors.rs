//! Black-box error-path tests for `wasl_gate` per docs/specs/gate.md §Test plan (E1-E4).

use wasl_gate::{build_request, GateError, GateLedger};
use wasl_proto::{AgentId, ApprovalToken, GateDecision, GateKind};

fn agent(id: &str) -> AgentId {
    AgentId(id.to_string())
}

fn targets(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("peer-{i}")).collect()
}

// ---------------------------------------------------------------------
// E1 — commit unknown token → Err(UnknownToken)
// ---------------------------------------------------------------------

#[test]
fn e1_commit_unknown_token_errors() {
    let mut ledger = GateLedger::new();
    let token = ApprovalToken("never-prepared".to_string());

    let result = ledger.commit(&token, GateDecision::Approve);

    assert_eq!(result, Err(GateError::UnknownToken));
}

// ---------------------------------------------------------------------
// E2 — commit a token, then commit it again → Err(AlreadyResolved), not UnknownToken
// ---------------------------------------------------------------------

#[test]
fn e2_recommit_resolved_token_is_already_resolved_not_unknown_token() {
    let mut ledger = GateLedger::new();
    let token = ApprovalToken("tok-resolve-twice".to_string());
    let req = build_request(
        "req-1".to_string(),
        GateKind::Consult,
        agent("liam"),
        &targets(1),
        "preview".to_string(),
    );

    ledger.prepare(token.clone(), req.clone());

    let first = ledger.commit(&token, GateDecision::Approve);
    assert_eq!(first, Ok((req, GateDecision::Approve)));

    let second = ledger.commit(&token, GateDecision::Approve);
    assert_eq!(
        second,
        Err(GateError::AlreadyResolved),
        "re-committing a resolved token must be AlreadyResolved, not UnknownToken"
    );
    assert_ne!(
        second,
        Err(GateError::UnknownToken),
        "must not be mistaken for an unknown token"
    );
}

// ---------------------------------------------------------------------
// E3 — commit with Deny returns (request, Deny{reason}) and clears pending
// ---------------------------------------------------------------------

#[test]
fn e3_commit_deny_returns_request_and_deny_and_clears_pending() {
    let mut ledger = GateLedger::new();
    let token = ApprovalToken("tok-deny".to_string());
    let req = build_request(
        "req-deny".to_string(),
        GateKind::Command,
        agent("mia"),
        &targets(3),
        "preview to deny".to_string(),
    );

    ledger.prepare(token.clone(), req.clone());
    assert!(ledger.is_pending(&token));

    let decision = GateDecision::Deny {
        reason: "not authorized".to_string(),
    };
    let result = ledger.commit(&token, decision.clone());

    assert_eq!(result, Ok((req, decision)));
    assert!(!ledger.is_pending(&token), "denied gate must clear from pending");
    assert_eq!(ledger.pending_count(), 0);
    assert_eq!(ledger.peek(&token), None);
}

// ---------------------------------------------------------------------
// E4 — GateError Display non-empty + distinct per variant; std::error::Error implemented
// ---------------------------------------------------------------------

#[test]
fn e4_gate_error_display_non_empty_and_distinct() {
    let unknown_display = GateError::UnknownToken.to_string();
    let resolved_display = GateError::AlreadyResolved.to_string();

    assert!(!unknown_display.is_empty(), "UnknownToken Display must be non-empty");
    assert!(
        !resolved_display.is_empty(),
        "AlreadyResolved Display must be non-empty"
    );
    assert_ne!(
        unknown_display, resolved_display,
        "Display text must differ per variant"
    );
}

#[test]
fn e4_gate_error_implements_std_error() {
    fn assert_std_error<E: std::error::Error>(_: &E) {}
    assert_std_error(&GateError::UnknownToken);
    assert_std_error(&GateError::AlreadyResolved);

    // Exercise the trait object path too, confirming source()/Display are usable
    // through `dyn std::error::Error` as real call sites would use it.
    let boxed: Box<dyn std::error::Error> = Box::new(GateError::UnknownToken);
    assert!(!boxed.to_string().is_empty());
}
