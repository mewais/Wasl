//! Phase-2A behavioral suite — duplicate idempotency-key dedup (§4.4) and
//! native-gate admission + resolution (§9) — spec items 8, 9.
//!
//! BEHAVIORAL, not type-only: asserts a duplicate key does NOT re-inject
//! (transport send_turn called once) and returns Duplicate{original}, incl. a
//! Barrier race proving dedup holds OUTSIDE the mailbox; and that AwaitingGate
//! blocks admission like any busy state and gate resolution transitions back
//! out. MUST currently fail (no actor / NativeGateResolver yet).
//!
//! Spec citations (docs/specs/runtime.md):
//!   * idempotency dedup OUTSIDE the mailbox — §4.4 (lines 383-389): a per-key
//!     lock consulted BEFORE the command enters the mailbox, so two concurrent
//!     same-key submits can NEVER both enqueue; a later same-key submit returns
//!     Duplicate{original}, NO second injection.
//!   * receipt Duplicate{original} — §3 (lines 238-240).
//!   * AwaitingGate is BUSY, blocks admission — §4.1 (line 318), §7.1
//!     (Busy/AwaitingGate rows), §9 (lines 709-725): resolvable via
//!     resolve_native_gate(turn_id, decision) through the single-writer actor.

mod support;

use std::sync::Arc;
use std::time::Duration;

use wasl_proto::GateDecision;
use wasl_runtime::{
    BusyPolicy, DeliveryTarget, EventSeq, NativeGateDetail, ProtocolEvent, TurnEvent, TurnReceipt,
};

use support::{
    agent_request, consult, drain_items, events_of, key, provider, resolve_gate, sink_arc,
    tight_config, ProtocolScript, RecordingSink, ScriptedProtocol, TestClock, TransportCall,
};

use support::root_causation;
use wasl_adapter::DeliveryKind;

fn busy_forever_script() -> ProtocolScript {
    ProtocolScript::new()
        .ready(Some("ses-1"))
        .emit(ProtocolEvent::TurnAccepted { native_turn: None })
}

// =====================================================================
// Item 8a — a second submit with the SAME idempotency key does not re-inject
// and returns Duplicate{original} wrapping the first receipt verbatim.
// =====================================================================

#[tokio::test]
async fn duplicate_idempotency_key_returns_duplicate_and_does_not_reinject() {
    let proto = ScriptedProtocol::new();
    let handle = proto.install("ctx-idem", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov.target_for(&key("ctx-idem")).await.expect("target");

    let first = target
        .submit_turn(
            agent_request(
                "ctx-idem",
                DeliveryKind::Command,
                "do it",
                Some("key-1"),
                root_causation("r"),
            ),
            BusyPolicy::Reject,
        )
        .await;
    assert!(
        matches!(first, TurnReceipt::Started { .. }),
        "first submit with a fresh key is admitted (Started), got {first:?}"
    );
    let injections_after_first = handle.shared.send_turn_count();
    assert_eq!(injections_after_first, 1, "the first command injects once");

    // A SECOND submit with the SAME key returns Duplicate{original == first},
    // and does NOT inject again.
    let second = target
        .submit_turn(
            agent_request(
                "ctx-idem",
                DeliveryKind::Command,
                "do it",
                Some("key-1"),
                root_causation("r"),
            ),
            BusyPolicy::Reject,
        )
        .await;
    match second {
        TurnReceipt::Duplicate { original } => {
            assert_eq!(
                *original, first,
                "Duplicate must wrap the ORIGINAL receipt verbatim (§3 lines 238-240)"
            );
        }
        other => panic!("expected Duplicate{{original}}, got {other:?}"),
    }
    assert_eq!(
        handle.shared.send_turn_count(),
        1,
        "a duplicate key must NEVER cause a second injection (§4.4), send_turn count grew"
    );
}

// =====================================================================
// Item 8b (race) — two CONCURRENT same-key submits (Barrier rendezvous, not a
// sleep): exactly one is admitted (Started/Queued), the other is Duplicate, and
// the transport is injected exactly ONCE. This proves dedup holds OUTSIDE the
// mailbox (both could otherwise enqueue). §4.4 lines 383-389.
// =====================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_same_key_submits_dedup_outside_the_mailbox() {
    use tokio::sync::Barrier;

    let proto = ScriptedProtocol::new();
    let handle = proto.install("ctx-idem-race", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov
        .target_for(&key("ctx-idem-race"))
        .await
        .expect("target");

    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();
    for _ in 0..2 {
        let t = target.clone();
        let b = barrier.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            t.submit_turn(
                agent_request(
                    "ctx-idem-race",
                    DeliveryKind::Command,
                    "same",
                    Some("dupe-key"),
                    root_causation("r"),
                ),
                BusyPolicy::Reject,
            )
            .await
        }));
    }
    let mut receipts = Vec::new();
    for h in handles {
        receipts.push(h.await.expect("submit task"));
    }

    let duplicates = receipts
        .iter()
        .filter(|r| matches!(r, TurnReceipt::Duplicate { .. }))
        .count();
    let admitted = receipts
        .iter()
        .filter(|r| matches!(r, TurnReceipt::Started { .. } | TurnReceipt::Queued { .. }))
        .count();
    assert_eq!(
        admitted, 1,
        "exactly ONE concurrent same-key submit may be admitted, got {receipts:?}"
    );
    assert_eq!(
        duplicates, 1,
        "the other concurrent same-key submit must be Duplicate, got {receipts:?}"
    );
    // The decisive proof: exactly ONE physical injection, never two — even
    // though both raced. Dedup happened before the mailbox (§4.4).
    assert_eq!(
        handle.shared.send_turn_count(),
        1,
        "two racing same-key submits must inject exactly once (dedup outside the mailbox, §4.4)"
    );
}

// =====================================================================
// Item 8c — a DIFFERENT idempotency key on the same target is NOT deduped
// (independent keys inject independently, up to admission).
// =====================================================================

#[tokio::test]
async fn different_keys_are_not_deduped() {
    let proto = ScriptedProtocol::new();
    let handle = proto.install("ctx-idem-diff", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov
        .target_for(&key("ctx-idem-diff"))
        .await
        .expect("target");

    let first = target
        .submit_turn(
            agent_request(
                "ctx-idem-diff",
                DeliveryKind::Consult,
                "one",
                Some("k-A"),
                root_causation("r"),
            ),
            BusyPolicy::Queue {
                max_wait: Duration::from_millis(120_000),
            },
        )
        .await;
    let second = target
        .submit_turn(
            agent_request(
                "ctx-idem-diff",
                DeliveryKind::Consult,
                "two",
                Some("k-B"),
                root_causation("r"),
            ),
            BusyPolicy::Queue {
                max_wait: Duration::from_millis(120_000),
            },
        )
        .await;
    assert!(
        !matches!(second, TurnReceipt::Duplicate { .. }),
        "a DIFFERENT key must not be deduped against the first, got {second:?}"
    );
    // The first is active (Started), the second queued behind it (Queued) — both
    // admitted, neither a Duplicate.
    assert!(matches!(first, TurnReceipt::Started { .. }));
    assert!(matches!(second, TurnReceipt::Queued { .. }));
    let _ = handle;
}

// =====================================================================
// Item 9a — AwaitingGate blocks admission exactly like Busy: while the active
// turn is parked on a native gate, a Reject submit ⇒ RejectedBusy (NOT admitted,
// NOT RejectedUnavailable — the session is busy-not-idle, §4.1 line 318).
// =====================================================================

#[tokio::test]
async fn awaiting_gate_blocks_admission_as_busy() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-gate",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .emit(ProtocolEvent::NativeGateRequested {
                detail: NativeGateDetail {
                    summary: "write file?".into(),
                    preview: "echo hi > /tmp/x".into(),
                },
            }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov.target_for(&key("ctx-gate")).await.expect("target");

    let started = target
        .submit_turn(consult("ctx-gate", "go"), BusyPolicy::Reject)
        .await;
    let active_turn = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    // Drain until the gate is surfaced.
    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 16).await;
    let evs = events_of(&items);
    assert!(
        evs.iter().any(|e| matches!(
            e, TurnEvent::NativeGateRequested { turn_id: t, .. } if *t == active_turn
        )),
        "the native gate must surface a NativeGateRequested event (§9), got {evs:?}"
    );

    // A new submit while AwaitingGate ⇒ RejectedBusy (busy, not idle, §7.1).
    let blocked = target
        .submit_turn(consult("ctx-gate", "sneak-in"), BusyPolicy::Reject)
        .await;
    assert_eq!(
        blocked,
        TurnReceipt::RejectedBusy { active_turn },
        "AwaitingGate is BUSY — admission is refused as RejectedBusy (§4.1 line 318 / §7.1)"
    );
}

// =====================================================================
// Item 9b — gate resolution through the single-writer actor: resolve_native_gate
// approves, the actor answers over the transport, emits NativeGateResolved, and
// the turn proceeds to completion. §9 option (i).
// =====================================================================

#[tokio::test]
async fn resolving_native_gate_approve_transitions_out_and_completes() {
    let proto = ScriptedProtocol::new();
    // After the gate, the transport is scripted (post-resolution) to complete
    // the turn. We push the completion after asserting the gate, mirroring how
    // a real driver emits its post-approval events.
    let handle = proto.install(
        "ctx-gate-ok",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .emit(ProtocolEvent::NativeGateRequested {
                detail: NativeGateDetail {
                    summary: "write file?".into(),
                    preview: "echo hi".into(),
                },
            }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    // A larger `journal_capacity` than `tight_config()`'s default (8): this
    // test re-subscribes from `EventSeq(0)` multiple times (full replays),
    // and the total event count (including an optional intervening
    // `StatusChanged{Idle}`/`TurnQueued` per §7.1 row 582's `Starting`-phase
    // handling) can exceed 8 — this test's actual intent (resolving a gate)
    // has nothing to do with exercising journal eviction/truncation, so give
    // it enough room rather than pinning to the tight, truncation-prone
    // default.
    let config = wasl_runtime::ManagedConfig {
        journal_capacity: 32,
        ..tight_config()
    };
    let prov = provider(proto, config, clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-gate-ok")).await.expect("target");

    let started = target
        .submit_turn(consult("ctx-gate-ok", "go"), BusyPolicy::Reject)
        .await;
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    // Let the gate surface.
    let s1 = target.events(EventSeq(0)).await;
    let _ = drain_items(s1, 8).await;

    // Resolve the gate through the actor (option (i), single-writer).
    let res = resolve_gate(&target, turn_id.clone(), GateDecision::Approve).await;
    assert_eq!(
        res,
        Ok(()),
        "resolving an AwaitingGate succeeds (§9 option (i))"
    );

    // After approval the transport completes the turn (driver post-approval).
    handle.push_event(ProtocolEvent::TurnCompleted {
        reply: "wrote it".into(),
    });

    let s2 = target.events(EventSeq(0)).await;
    let items = drain_items(s2, 32).await;
    let evs = events_of(&items);
    // NativeGateResolved carrying the approval, then the turn completes.
    assert!(
        evs.iter().any(|e| matches!(
            e,
            TurnEvent::NativeGateResolved { turn_id: t, decision: GateDecision::Approve }
                if *t == turn_id
        )),
        "resolution emits NativeGateResolved{{Approve}} (§9), got {evs:?}"
    );
    assert!(
        evs.iter().any(|e| matches!(
            e, TurnEvent::TurnCompleted { turn_id: t, .. } if *t == turn_id
        )),
        "after gate approval the turn completes, got {evs:?}"
    );

    // The actor answered the gate over the single-writer transport (a send_turn
    // carrying the resolution, never a raw side-channel — §9 line 718).
    assert!(
        handle
            .shared
            .calls()
            .iter()
            .any(|c| matches!(c, TransportCall::SendTurn { .. })),
        "the actor answers the gate over the transport it solely holds (§9)"
    );
}
