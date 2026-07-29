//! Hardening-plan behavioral suite — proves two CURRENT bugs (test-first, RED).
//!
//! These tests translate the BEHAVIORS in
//! `docs/design/live-runtime-hardening-plan.md` §3.3 and §3.4 into the REAL R1
//! public API (as exercised by `tests/cancel_and_crash.rs`,
//! `tests/gate_and_events.rs`, and `tests/support/mod.rs`). They MUST currently
//! FAIL against the unfixed implementation; every wait is bounded by a
//! `tokio::time::timeout` hang-guard so a regression fails fast, not hangs.
//!
//! ### §3.3 — cancelling an active interruptible turn advances the queue WITHOUT
//! waiting for the cancelled turn's real terminal event.
//! Today: `cancel_turn` calls `transport.interrupt()` then IMMEDIATELY clears the
//! active turn and activates the next queued turn — without waiting for the
//! cancelled turn's own terminal event (interrupt() is best-effort/async). We
//! prove this by GATING turn A's terminal event (never releasing it), queueing
//! turn B behind A, cancelling A, and asserting B has NOT been injected (no
//! second `send_turn`) while A's gated terminal is still unreleased. Then we
//! release A's gate and assert B eventually DOES get injected.
//! Spec (docs/specs/runtime.md §7.2): "active AND interruptible ⇒ → Interrupting;
//! drive transport.interrupt(); ON ACK emit TurnCancelled; → Idle (or next
//! queued)" — the next queued turn activates only after the interrupt is
//! confirmed by the cancelled turn's terminal event, not before.
//!
//! ### §3.4 — resolving a native gate reports success even when the transport
//! write actually failed.
//! Today: the gate-resolution path calls `transport.send_turn(&answer)` and
//! reports the gate resolved regardless of whether that write succeeded. We arm
//! the transport to fail the next send, then resolve the gate, and assert the
//! call returns Err AND no `NativeGateResolved` event is emitted for the gate.
//! Spec (docs/specs/runtime.md §9): on resolution the actor "answers the CLI's
//! prompt over the transport, emits NativeGateResolved" — an answer that never
//! reached the transport must NOT be reported as resolved.

mod support;

use std::sync::Arc;
use std::time::Duration;

use wasl_proto::GateDecision;
use wasl_runtime::{
    BusyPolicy, CancelReason, DeliveryTarget, EventSeq, NativeGateDetail, ProtocolEvent,
    RuntimeError, TurnEvent, TurnReceipt,
};

use support::{
    consult, drain_items, events_of, key, provider, resolve_gate, sink_arc, tight_config,
    ProtocolScript, RecordingSink, ScriptedProtocol, TestClock,
};

// =====================================================================
// §3.3 — a cancelled active interruptible turn must NOT let the next queued turn
// be injected until the cancelled turn's own (gated) terminal event is observed.
// =====================================================================

#[tokio::test]
async fn cancel_does_not_activate_next_queued_turn_before_cancelled_terminal() {
    let proto = ScriptedProtocol::new();
    // Turn A becomes active (TurnAccepted), then its terminal event is GATED:
    // the transport will only emit A's TurnCompleted after the test releases
    // "a_terminal". Until then, A is genuinely still terminating from the
    // transport's point of view (interrupt() is best-effort/async), so the
    // actor must NOT yet activate B.
    let handle = proto.install(
        "ctx-cancel-wait",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .gated(
                "a_terminal",
                ProtocolEvent::TurnCompleted {
                    reply: "A done".into(),
                },
            ),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov
        .target_for(&key("ctx-cancel-wait"))
        .await
        .expect("target");

    // Submit A (active, interruptible).
    let started_a = tokio::time::timeout(
        Duration::from_secs(5),
        target.submit_turn(consult("ctx-cancel-wait", "A"), BusyPolicy::Reject),
    )
    .await
    .expect("submit A did not hang");
    let turn_a = match started_a {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started for A, got {other:?}"),
    };

    // Queue B behind A.
    let queued_b = tokio::time::timeout(
        Duration::from_secs(5),
        target.submit_turn(
            consult("ctx-cancel-wait", "B"),
            BusyPolicy::Queue {
                max_wait: Duration::from_millis(120_000),
            },
        ),
    )
    .await
    .expect("submit B did not hang");
    let turn_b = match queued_b {
        TurnReceipt::Queued { turn_id, .. } => turn_id,
        other => panic!("expected Queued for B, got {other:?}"),
    };
    assert_ne!(turn_a, turn_b, "A and B are distinct turns");

    // Exactly one send_turn so far: A was injected, B is only queued.
    assert_eq!(
        handle.shared.send_turn_count(),
        1,
        "before cancel, only A is injected (B is queued), got {}",
        handle.shared.send_turn_count()
    );

    // Cancel A. `interrupt()` is best-effort/async and A's terminal event is
    // still gated (unreleased), so the actor must NOT yet promote/inject B.
    let cancel_res = tokio::time::timeout(
        Duration::from_secs(5),
        target.cancel_turn(turn_a.clone(), CancelReason::OperatorRequest),
    )
    .await
    .expect("cancel_turn did not hang");
    assert_eq!(
        cancel_res,
        Ok(()),
        "cancel of interruptible active A succeeds"
    );

    // The transport DID see an interrupt (the actor drove it).
    assert!(
        handle
            .shared
            .calls()
            .iter()
            .any(|c| matches!(c, support::TransportCall::Interrupt { .. })),
        "the actor must drive transport.interrupt() on cancel (§7.2)"
    );

    // THE BUG (§3.3): B must NOT have been injected yet — A's real terminal
    // event has not been observed (its gate is unreleased). A second send_turn
    // here means the actor blindly activated B before confirming A terminated.
    // Give any (buggy) premature activation a bounded window to have happened.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        handle.shared.send_turn_count(),
        1,
        "B must NOT be injected before A's cancelled turn emits its real terminal \
         event (§3.3): interrupt() is best-effort/async, so activating the next \
         queued turn immediately misattributes a late A-terminal to B. \
         send_turn_count = {}",
        handle.shared.send_turn_count()
    );

    // Now release A's real terminal event. Only NOW may B be promoted+injected.
    handle.release("a_terminal");

    // B must eventually be injected (a second send_turn) once A really terminated.
    let mut injected_b = false;
    for _ in 0..50 {
        if handle.shared.send_turn_count() >= 2 {
            injected_b = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        injected_b,
        "after A's real terminal event, B must be promoted and injected \
         (a second send_turn), got send_turn_count = {}",
        handle.shared.send_turn_count()
    );
}

// =====================================================================
// §3.4 — resolving a native gate must report the transport write failure, not
// success: on a failed answer it returns Err AND emits no NativeGateResolved.
// =====================================================================

#[tokio::test]
async fn resolve_native_gate_reports_transport_write_failure() {
    let proto = ScriptedProtocol::new();
    // A turn is active and the transport requests a native permission gate.
    let handle = proto.install(
        "ctx-gate-fail",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .emit(ProtocolEvent::NativeGateRequested {
                detail: NativeGateDetail {
                    summary: "delete file?".into(),
                    preview: "rm x".into(),
                },
            }),
    );
    // Give the journal generous room: this test re-subscribes from EventSeq(0)
    // twice (a full replay each time), same as gate_and_events.rs's Deny test.
    let config = wasl_runtime::ManagedConfig {
        journal_capacity: 32,
        ..tight_config()
    };
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, config, clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov
        .target_for(&key("ctx-gate-fail"))
        .await
        .expect("target");

    let started = tokio::time::timeout(
        Duration::from_secs(5),
        target.submit_turn(consult("ctx-gate-fail", "go"), BusyPolicy::Reject),
    )
    .await
    .expect("submit did not hang");
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    // Let the gate surface (AwaitingGate).
    let s0 = target.events(EventSeq(0)).await;
    let _ = drain_items(s0, 8).await;

    // Arm the NEXT send_turn (the gate answer's transport write) to fail —
    // synchronously on the shared handle, exactly as
    // cancel_and_crash.rs::transport_error_before_injection_is_failed_before_injection
    // arms a pre-injection failure.
    handle.shared.arm_fail_next_send(RuntimeError::Transport {
        detail: "broken pipe answering gate".into(),
    });

    // Resolve the gate. The answer's transport write will fail.
    let res = tokio::time::timeout(
        Duration::from_secs(5),
        resolve_gate(&target, turn_id.clone(), GateDecision::Approve),
    )
    .await
    .expect("resolve_native_gate did not hang");

    // THE BUG (§3.4): the gate resolution must SURFACE the transport write
    // failure, not report success. (We assert `.is_err()` generically rather
    // than pinning a specific new RuntimeError variant, since the plan does not
    // pin one; RuntimeError::Transport is the variant every send_turn error is
    // mapped to today, but the contract under test is simply "not Ok".)
    assert!(
        res.is_err(),
        "resolving a gate whose transport answer write FAILED must return Err, \
         not Ok (§3.4): a resolution reported successful despite a failed write \
         silently loses the fact that the CLI's prompt was never answered. got {res:?}"
    );

    // And no NativeGateResolved event may be emitted for this gate — the gate
    // was NOT actually resolved (the answer never reached the transport).
    let s1 = target.events(EventSeq(0)).await;
    let items = drain_items(s1, 16).await;
    let evs = events_of(&items);
    assert!(
        !evs.iter().any(|e| matches!(
            e,
            TurnEvent::NativeGateResolved { turn_id: t, .. } if *t == turn_id
        )),
        "a gate whose answer write FAILED must NOT emit NativeGateResolved (§3.4), \
         got {evs:?}"
    );
}
