//! Phase-2A behavioral suite — causation cycle rejection: synchronous, max-hop,
//! AND the queued-path cycle (spec item 10). §8.2.
//!
//! BEHAVIORAL, not type-only: asserts concrete RejectedUnavailable outcomes when
//! the target's own identity would close a cycle, when hop > MAX_HOP, and when a
//! QUEUED turn would close a cycle at activation time (re-evaluated on pop).
//! MUST currently fail (no actor yet).
//!
//! Spec citations (docs/specs/runtime.md §8.2, lines 685-701):
//!   * reject if the target's own agent identity already appears in
//!     causation.path (would-be cycle) — evaluated at admission for a SYNC turn
//!     AND re-evaluated when a QUEUED turn is about to start.
//!   * reject if hop > MAX_HOP (locked default 8, §11).
//!   * a rejected cycle/hop ⇒ RejectedUnavailable (with a distinct RuntimeError
//!     in audit).
//!
//! FROZEN-SURFACE NOTE: cycle rejection needs the actor to know the TARGET'S OWN
//! agent identity to compare against `causation.path`. The frozen `SpawnOptions`
//! hides `target_key`/identity (src/lib.rs:657, fields private). The coder MUST
//! wire the target's own identity so the actor can detect "self already on the
//! path" (runtime.md §8.2 line 690, §6 line 547 "target_key"). These tests use
//! the TargetKey/context id as that identity (the spec binds TargetKey ==
//! context_id and admission checks `from` against the key's owner, §10 line 764);
//! if the coder derives identity differently, only the `path` values below need
//! re-mapping — the reject SEMANTICS are name-independent.

mod support;

use std::time::Duration;

use wasl_adapter::DeliveryKind;
use wasl_proto::{AgentId, RequestId};
use wasl_runtime::{BusyPolicy, Causation, ProtocolEvent, TurnReceipt};

use support::{
    agent_request, consult, key, provider, root_causation, sink_arc, tight_config, ProtocolScript,
    RecordingSink, ScriptedProtocol, TestClock,
};

/// The identity the target is expected to compare against `causation.path`. The
/// spec binds TargetKey == context_id and checks the owner identity; the tests
/// use the context id string as the on-path agent id so "self on path" is
/// detectable by the actor.
const SELF_CTX: &str = "ctx-cycle";

fn busy_forever_script() -> ProtocolScript {
    ProtocolScript::new()
        .ready(Some("ses-1"))
        .emit(ProtocolEvent::TurnAccepted { native_turn: None })
}

// =====================================================================
// Item 10a — SYNCHRONOUS cycle: a turn whose causation.path ALREADY contains the
// target's own identity is rejected at admission ⇒ RejectedUnavailable, no
// injection. §8.2 line 690.
// =====================================================================

#[tokio::test]
async fn sync_cycle_when_self_already_on_path_is_rejected() {
    let proto = ScriptedProtocol::new();
    let handle = proto.install(SELF_CTX, busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov.target_for(&key(SELF_CTX)).await.expect("target");

    // The path already contains this target's own identity → would-be cycle.
    let cyclic = Causation {
        root_request: RequestId("r".into()),
        parent_request: Some(RequestId("r".into())),
        path: vec![AgentId("upstream".into()), AgentId(SELF_CTX.into())],
        hop: 2,
    };
    let req = agent_request(
        SELF_CTX,
        DeliveryKind::Consult,
        "loop back to me",
        None,
        cyclic,
    );

    let receipt = target.submit_turn(req, BusyPolicy::Reject).await;
    assert!(
        matches!(receipt, TurnReceipt::RejectedUnavailable { .. }),
        "a sync turn whose path already contains the target's own identity is a cycle ⇒ \
         RejectedUnavailable (§8.2), got {receipt:?}"
    );
    // A rejected cycle never injects.
    assert_eq!(
        handle.shared.send_turn_count(),
        0,
        "a cycle-rejected turn must never reach the transport (§8.2)"
    );
}

// =====================================================================
// Item 10b — MAX-HOP: a turn with hop > MAX_HOP (locked default 8) is rejected
// ⇒ RejectedUnavailable. §8.2 line 691 / §11.
// =====================================================================

#[tokio::test]
async fn over_max_hop_is_rejected() {
    let proto = ScriptedProtocol::new();
    let handle = proto.install("ctx-hop", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    // tight_config() leaves max_hop at its locked default (8) — do not override,
    // so this exercises the real MAX_HOP bound.
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov.target_for(&key("ctx-hop")).await.expect("target");

    let deep = Causation {
        root_request: RequestId("r".into()),
        parent_request: Some(RequestId("r".into())),
        path: (0..9).map(|i| AgentId(format!("a{i}"))).collect(),
        hop: 9, // > MAX_HOP (8)
    };
    let req = agent_request("ctx-hop", DeliveryKind::Consult, "too deep", None, deep);

    let receipt = target.submit_turn(req, BusyPolicy::Reject).await;
    assert!(
        matches!(receipt, TurnReceipt::RejectedUnavailable { .. }),
        "hop > MAX_HOP (8) ⇒ RejectedUnavailable (§8.2 line 691), got {receipt:?}"
    );
    assert_eq!(
        handle.shared.send_turn_count(),
        0,
        "an over-max-hop turn must never reach the transport"
    );
}

// =====================================================================
// Item 10c — a within-bounds turn (self NOT on path, hop <= MAX_HOP) is admitted
// normally — proving the reject is specific to the cycle/hop condition, not a
// blanket refusal.
// =====================================================================

#[tokio::test]
async fn non_cyclic_within_hop_turn_is_admitted() {
    let proto = ScriptedProtocol::new();
    let handle = proto.install("ctx-ok", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov.target_for(&key("ctx-ok")).await.expect("target");

    // Path does NOT contain "ctx-ok"; hop well under MAX_HOP.
    let ok = Causation {
        root_request: RequestId("r".into()),
        parent_request: Some(RequestId("r".into())),
        path: vec![AgentId("upstream".into())],
        hop: 1,
    };
    let receipt = target
        .submit_turn(
            agent_request("ctx-ok", DeliveryKind::Consult, "fine", None, ok),
            BusyPolicy::Reject,
        )
        .await;
    assert!(
        matches!(receipt, TurnReceipt::Started { .. }),
        "a non-cyclic within-hop turn is admitted (Started), got {receipt:?}"
    );
    assert_eq!(
        handle.shared.send_turn_count(),
        1,
        "the admitted turn injects once"
    );
}

// =====================================================================
// Item 10d — QUEUED-PATH cycle: a queued turn that WOULD close a cycle is caught
// when it is about to START (re-evaluated on pop), not silently activated. §8.2
// line 690 ("re-evaluated when a QUEUED turn is about to start"). This is the
// A-queues-into-B-while-B-queues-into-A shape: the queued turn already carries
// the target's own identity on its path, so when the active turn completes and
// the queued turn is popped, it must be rejected/expired rather than injected.
// =====================================================================

#[tokio::test]
async fn queued_turn_that_would_close_a_cycle_is_not_activated() {
    let proto = ScriptedProtocol::new();
    // The active turn completes on demand (we push completion), so the queued
    // turn is popped and re-evaluated for a cycle at activation.
    let handle = proto.install(
        "ctx-qcycle",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov.target_for(&key("ctx-qcycle")).await.expect("target");

    // Active, non-cyclic turn holds the target busy.
    let active = target
        .submit_turn(consult("ctx-qcycle", "active"), BusyPolicy::Reject)
        .await;
    assert!(matches!(active, TurnReceipt::Started { .. }));

    // A queued turn whose path already contains this target's identity — a cycle
    // that will close when it is popped. Under QueueRelated it may be admitted to
    // the queue now (relatedness is by root), but MUST NOT be injected at
    // activation. Some impls reject it synchronously at admission too; either way
    // it must never reach the transport as a second injection.
    let cyclic = Causation {
        root_request: RequestId("shared-root".into()),
        parent_request: Some(RequestId("shared-root".into())),
        path: vec![AgentId(SELF_CTX.into()), AgentId("ctx-qcycle".into())],
        hop: 2,
    };
    let queued = target
        .submit_turn(
            agent_request("ctx-qcycle", DeliveryKind::Consult, "cycle", None, cyclic),
            BusyPolicy::QueueRelated {
                max_wait: Duration::from_millis(120_000),
            },
        )
        .await;

    // Complete the active turn so the queue is drained and the cyclic queued turn
    // is re-evaluated on pop.
    handle.push_event(ProtocolEvent::TurnCompleted {
        reply: "active done".into(),
    });

    // Drain the stream; the cyclic queued turn must NOT have been injected — the
    // transport was asked to send only the ONE (active) turn.
    let stream = target.events(wasl_runtime::EventSeq(0)).await;
    let _ = support::drain_items(stream, 32).await;

    assert_eq!(
        handle.shared.send_turn_count(),
        1,
        "a queued turn that would close a cycle must be caught on pop and NEVER injected \
         (§8.2 queued-path cycle re-evaluation); only the active turn was injected"
    );

    // Whether the queued cyclic turn was rejected at admission or expired/rejected
    // on pop, it must be a refusal (RejectedUnavailable) or a queue-time terminal
    // (TurnExpired/TurnCancelled) — never a silent successful activation.
    let is_refused_or_terminal = matches!(queued, TurnReceipt::RejectedUnavailable { .. })
        || matches!(queued, TurnReceipt::Queued { .. });
    assert!(
        is_refused_or_terminal,
        "the cyclic queued turn is either rejected up front or admitted-then-caught-on-pop, \
         never a plain Started, got {queued:?}"
    );
}

// Keep root_causation meaningful for readers (used by other suites for the
// non-cyclic baseline shape).
#[allow(dead_code)]
fn _baseline() -> Causation {
    root_causation("r")
}
