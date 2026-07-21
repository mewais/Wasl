//! Phase-1 SKELETON smoke test for R3 `wasl-session` (the local control-surface
//! binary).
//!
//! session.md: R3 is a THIN CLI over a versioned control-plane (R4) client. Its
//! Deps line (lines 3-6) says it depends on R1 for "the `TurnEvent`/`TargetStatus`
//! /`TurnReceipt`/`TargetId`/`TurnSource`/`CancelReason`/`ShutdownMode` PUBLIC
//! types + `GateDecision`", and the behavior contract (lines 100-101) says the
//! CLI "only ever observes/expresses the PUBLIC vocabulary … It never sees
//! `ProtocolEvent`".
//!
//! In Phase-1, R3 is essentially the crate/bin skeleton + its dependency edge to
//! R1. The command/arg SEMANTICS and their mapping to R4 requests are unit-tested
//! against a FAKE R4 client (session.md §Testability lines 122-126) — that is
//! Phase-2/behavioral work, NOT Phase-1. session.md does NOT pin a concrete Rust
//! arg-type NAME for Phase-1 (the commands are given as a `wasl session …` CLI
//! grammar, line 23-31), so this file does NOT invent one; it asserts only the
//! Phase-1 obligation: the crate EXISTS, DEPENDS ON R1, and can name exactly the
//! PUBLIC vocabulary the spec says it renders — and CANNOT (must not) reach the
//! internal `ProtocolEvent` (that symbol is intentionally NOT imported here).
//!
//! TEST-FIRST / EXPECTED RED: will not compile until the coder creates the
//! `wasl-runtime` crate (owning the public types) AND the `wasl-session` crate
//! skeleton that depends on it.

// The dep edge + "R3 renders only the PUBLIC vocabulary": these `use`s only
// resolve if `wasl-session`'s Cargo.toml depends on `wasl-runtime` and R1
// exports exactly this public surface (session.md Deps lines 3-6).
use wasl_proto::GateDecision;
use wasl_runtime::{
    CancelReason, EventSeq, ShutdownMode, TargetId, TargetStatus, TurnEvent, TurnId, TurnReceipt,
    TurnSource,
};

#[test]
fn r3_depends_on_r1_and_renders_only_the_public_vocabulary() {
    // TargetId — the handle `list`/`attach`/`send`/… address (session.md).
    let target = TargetId("tgt-1".to_string());
    assert_eq!(target.0, "tgt-1");

    // TurnReceipt — `send` prints this (session.md §send lines 65-66).
    let receipt = TurnReceipt::Started {
        turn_id: TurnId("turn-7".to_string()),
        first_seq: EventSeq(13),
    };
    // The CLI renders the receipt (here: prove it is a matchable public value).
    match receipt {
        TurnReceipt::Started { turn_id, first_seq } => {
            assert_eq!(turn_id, TurnId("turn-7".to_string()));
            assert_eq!(first_seq, EventSeq(13));
        }
        _ => panic!("expected Started"),
    }

    // TargetStatus — `list`/`attach`/`stop` render this (session.md §list/§attach).
    let status = TargetStatus::Idle {
        since: "2026-07-02T12:00:00.000Z".to_string(),
    };
    assert!(matches!(status, TargetStatus::Idle { .. }));

    // TurnEvent — `attach` renders the transcript from these (session.md §attach).
    let ev = TurnEvent::AssistantDelta {
        turn_id: TurnId("turn-7".to_string()),
        text: "PON".to_string(),
    };
    assert!(matches!(ev, TurnEvent::AssistantDelta { .. }));

    // CancelReason::OperatorRequest — `interrupt` maps to this (session.md
    // §interrupt lines 72-73: R4 cancel with CancelReason::OperatorRequest).
    let reason = CancelReason::OperatorRequest;
    assert_eq!(reason, CancelReason::OperatorRequest);

    // ShutdownMode — `stop [--drain]` maps Drain/Immediate (session.md §stop).
    assert_ne!(ShutdownMode::Drain, ShutdownMode::Immediate);

    // TurnSource::LocalHuman — `send` ALWAYS uses this (session.md §send line 63:
    // "source = TurnSource::LocalHuman").
    let source = TurnSource::LocalHuman;
    assert_eq!(source, TurnSource::LocalHuman);

    // GateDecision — `gate approve|deny` expresses this (session.md §gate lines
    // 87-89: approve → Approve; deny → Deny{reason}).
    assert!(GateDecision::Approve.is_approved());
    assert!(!GateDecision::Deny {
        reason: "not allowed".to_string()
    }
    .is_approved());
}

#[test]
fn r3_maps_gate_subcommand_to_public_gate_decision() {
    // session.md §gate lines 87-89: `approve` → GateDecision::Approve;
    // `deny [--reason]` → GateDecision::Deny{reason}. Pin that mapping shape
    // (the value the CLI expresses over R4), without inventing an arg type.
    let approve = GateDecision::Approve;
    let deny = GateDecision::Deny {
        reason: "not allowed".to_string(),
    };
    assert!(approve.is_approved());
    match deny {
        GateDecision::Deny { reason } => assert_eq!(reason, "not allowed"),
        GateDecision::Approve => panic!("expected Deny"),
    }
}
