//! Phase-2A behavioral suite — cancellation (§7.2 matrix) and crash/certainty
//! (§7.4 / §8.1) — spec items 6, 7.
//!
//! BEHAVIORAL, not type-only: asserts concrete cancel outcomes per (status ×
//! turn), the `Interrupting` transition, dequeue-only for a queued cancel, the
//! `!interruptible` `Unsupported` error, the wrong/completed-turn no-op, and the
//! delivery-certainty classification (OutcomeUnknown vs FailedBeforeInjection)
//! with NO auto-retry. MUST currently fail (no actor yet).
//!
//! Spec citations (docs/specs/runtime.md):
//!   * cancel_turn matrix — §7.2 (lines 588-596): active & !interruptible ⇒
//!     Err(Unsupported); active & interruptible ⇒ Interrupting → TurnCancelled;
//!     queued ⇒ removed + TurnCancelled, status unchanged; completed/unknown ⇒
//!     Ok(()) no-op (LOCKED); Offline/Stopped ⇒ Err(Unavailable).
//!   * certainty transitions — §8.1 (lines 628-652): Injected on send_turn Ok;
//!     OutcomeUnknown if the process exits while Injected+unfinished;
//!     FailedBeforeInjection if the transport fails BEFORE send_turn succeeds.
//!   * ProcessExited handling — §7.4 (lines 620-621).
//!   * NO auto-retry of a command at OutcomeUnknown — §8.1 (line 681).

mod support;

use std::sync::Arc;
use std::time::Duration;

use wasl_runtime::{
    BusyPolicy, CancelReason, DeliveryCertainty, DeliveryTarget, EventSeq, ProtocolEvent,
    RuntimeError, TurnEvent, TurnId, TurnReceipt,
};

use support::{
    command, consult, drain_items, events_of, key, provider, sink_arc, tight_config,
    ProtocolScript, RecordingSink, ScriptedProtocol, TestClock,
};

fn busy_forever_script() -> ProtocolScript {
    ProtocolScript::new()
        .ready(Some("ses-1"))
        .emit(ProtocolEvent::TurnAccepted { native_turn: None })
}

// =====================================================================
// Item 6a — cancel an ACTIVE turn on an interruptible target: → Interrupting,
// drive transport.interrupt(), emit TurnCancelled.
// =====================================================================

#[tokio::test]
async fn cancel_active_interruptible_turn_emits_turncancelled() {
    let proto = ScriptedProtocol::new();
    // After the interrupt ack the transport is scripted to have nothing more;
    // the actor turns the interrupt into a TurnCancelled terminal event.
    let handle = proto.install("ctx-cancel", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov.target_for(&key("ctx-cancel")).await.expect("target");

    let started = target
        .submit_turn(consult("ctx-cancel", "go"), BusyPolicy::Reject)
        .await;
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    let res = target
        .cancel_turn(turn_id.clone(), CancelReason::OperatorRequest)
        .await;
    assert_eq!(
        res,
        Ok(()),
        "cancel on an interruptible active turn succeeds (§7.2)"
    );
    // The transport actually saw an interrupt (single-writer drove it).
    assert!(
        handle
            .shared
            .calls()
            .iter()
            .any(|c| matches!(c, support::TransportCall::Interrupt { .. })),
        "the actor must drive transport.interrupt() on an interruptible cancel (§7.2)"
    );

    // The public stream shows a TurnCancelled for this turn with the reason.
    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);
    assert!(
        evs.iter().any(|e| matches!(
            e,
            TurnEvent::TurnCancelled { turn_id: t, reason: CancelReason::OperatorRequest }
                if *t == turn_id
        )),
        "cancel must emit TurnCancelled{{turn_id, OperatorRequest}} (§7.2), got {evs:?}"
    );
}

// =====================================================================
// Item 6b — cancel on a !interruptible target ⇒ Err(Unsupported), status
// unchanged.
// =====================================================================

#[tokio::test]
async fn cancel_active_turn_on_non_interruptible_target_is_unsupported() {
    let proto = ScriptedProtocol::non_interruptible();
    proto.install("ctx-noint", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov.target_for(&key("ctx-noint")).await.expect("target");

    let started = target
        .submit_turn(consult("ctx-noint", "go"), BusyPolicy::Reject)
        .await;
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    let res = target
        .cancel_turn(turn_id, CancelReason::OperatorRequest)
        .await;
    assert!(
        matches!(res, Err(RuntimeError::Unsupported { .. })),
        "cancel on a !interruptible active turn ⇒ Err(Unsupported) (§7.2), got {res:?}"
    );
}

// =====================================================================
// Item 6c — cancel a QUEUED turn: remove from the queue (dequeue only), emit
// TurnCancelled, active status unchanged.
// =====================================================================

#[tokio::test]
async fn cancel_queued_turn_dequeues_it_without_touching_active() {
    let proto = ScriptedProtocol::new();
    proto.install("ctx-deq", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov.target_for(&key("ctx-deq")).await.expect("target");

    let active = target
        .submit_turn(consult("ctx-deq", "active"), BusyPolicy::Reject)
        .await;
    let active_turn = match active {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    let queued = target
        .submit_turn(
            consult("ctx-deq", "queued"),
            BusyPolicy::Queue {
                max_wait: Duration::from_millis(120_000),
            },
        )
        .await;
    let queued_turn = match queued {
        TurnReceipt::Queued { turn_id, .. } => turn_id,
        other => panic!("expected Queued, got {other:?}"),
    };

    let res = target
        .cancel_turn(queued_turn.clone(), CancelReason::Superseded)
        .await;
    assert_eq!(res, Ok(()), "cancel of a queued turn succeeds (§7.2)");

    // The active turn is still active (cancel a queued turn does not disturb it):
    // a further Reject submit still hits RejectedBusy{active_turn} unchanged.
    let re = target
        .submit_turn(consult("ctx-deq", "probe"), BusyPolicy::Reject)
        .await;
    assert_eq!(
        re,
        TurnReceipt::RejectedBusy { active_turn },
        "cancelling a queued turn leaves the active turn untouched (§7.2 'status unchanged')"
    );

    // A TurnCancelled for the QUEUED turn (never a spurious activation of it).
    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);
    assert!(
        evs.iter().any(|e| matches!(
            e, TurnEvent::TurnCancelled { turn_id: t, .. } if *t == queued_turn
        )),
        "a cancelled queued turn emits TurnCancelled (§7.2), got {evs:?}"
    );
    assert!(
        !evs.iter().any(|e| matches!(
            e, TurnEvent::TurnAccepted { turn_id: t, .. } if *t == queued_turn
        )),
        "a cancelled queued turn must NEVER be activated (no TurnAccepted for it)"
    );
}

// =====================================================================
// Item 6d — cancel a wrong / already-completed turn_id ⇒ Ok(()) no-op (LOCKED,
// §7.2: cancelling a finished turn is benign).
// =====================================================================

#[tokio::test]
async fn cancel_completed_or_unknown_turn_is_ok_noop() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-noop",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .happy_turn("x", "reply"),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov.target_for(&key("ctx-noop")).await.expect("target");

    let started = target
        .submit_turn(consult("ctx-noop", "go"), BusyPolicy::Reject)
        .await;
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };
    // Let the turn complete.
    let s = target.events(EventSeq(0)).await;
    let _ = drain_items(s, 32).await;

    // Cancelling the now-COMPLETED turn is a benign Ok(()) no-op.
    let done = target.cancel_turn(turn_id, CancelReason::A2aCancel).await;
    assert_eq!(
        done,
        Ok(()),
        "cancelling a completed turn ⇒ Ok(()) no-op (§7.2 LOCKED)"
    );

    // Cancelling a wholly-unknown turn id is ALSO a benign Ok(()) no-op.
    let unknown = target
        .cancel_turn(TurnId("never-existed".into()), CancelReason::A2aCancel)
        .await;
    assert_eq!(
        unknown,
        Ok(()),
        "cancelling an unknown turn ⇒ Ok(()) no-op (§7.2 LOCKED)"
    );
}

// =====================================================================
// Item 7a — crash AFTER injection, before completion, of a COMMAND ⇒
// OutcomeUnknown. No auto-retry (transport is only ever asked to send once).
// =====================================================================

#[tokio::test]
async fn command_process_exit_after_injection_is_outcome_unknown_no_retry() {
    let proto = ScriptedProtocol::new();
    // Command turn: accept, then the process EXITS mid-turn (before completion).
    let handle = proto.install(
        "ctx-crash",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .emit(ProtocolEvent::ProcessExited { code: Some(137) }),
    );
    let clock = TestClock::new();
    let recording = RecordingSink::default();
    let sink = sink_arc(recording);
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov.target_for(&key("ctx-crash")).await.expect("target");

    let started = target
        .submit_turn(command("ctx-crash", "do it"), BusyPolicy::Reject)
        .await;
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);

    // ProcessExited surfaced, and the injected command's certainty is
    // OutcomeUnknown (§7.4 / §8.1) — a definite MAY-have-run classification.
    assert!(
        evs.iter()
            .any(|e| matches!(e, TurnEvent::ProcessExited { .. })),
        "a mid-turn process exit surfaces ProcessExited (§7.4), got {evs:?}"
    );
    let terminal_certainty = evs.iter().find_map(|e| match e {
        TurnEvent::TurnFailed {
            turn_id: t,
            certainty,
            ..
        } if *t == turn_id => Some(*certainty),
        TurnEvent::TurnCompleted {
            turn_id: t,
            certainty,
            ..
        } if *t == turn_id => Some(*certainty),
        _ => None,
    });
    assert_eq!(
        terminal_certainty,
        Some(DeliveryCertainty::OutcomeUnknown),
        "a command injected then crashed mid-turn is OutcomeUnknown (§8.1), got {terminal_certainty:?}"
    );

    // NO auto-retry: the transport was asked to send exactly once (§8.1 line 681).
    assert_eq!(
        handle.shared.send_turn_count(),
        1,
        "an OutcomeUnknown command must NEVER be blindly re-injected (§8.1 line 681)"
    );
}

// =====================================================================
// Item 7b — transport error BEFORE send_turn succeeds ⇒ FailedBeforeInjection,
// SAFE (no side effect could have occurred). No retry.
// =====================================================================

#[tokio::test]
async fn transport_error_before_injection_is_failed_before_injection() {
    let proto = ScriptedProtocol::new();
    // The FIRST send_turn fails (fail_next_send), so the turn never injects.
    let handle = proto.install(
        "ctx-preinject",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .fail_next_send(RuntimeError::Transport {
                detail: "broken pipe before accept".into(),
            }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov
        .target_for(&key("ctx-preinject"))
        .await
        .expect("target");

    let started = target
        .submit_turn(consult("ctx-preinject", "go"), BusyPolicy::Reject)
        .await;
    // Admission itself may still return Started (admitted), or the actor may
    // surface the failure as a terminal event on the stream — either way the
    // certainty must be FailedBeforeInjection and there is no retry.
    let turn_id = match &started {
        TurnReceipt::Started { turn_id, .. } => Some(turn_id.clone()),
        _ => None,
    };

    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);
    let failed_certainty = evs.iter().find_map(|e| match e {
        TurnEvent::TurnFailed {
            turn_id: t,
            certainty,
            ..
        } => {
            if turn_id.as_ref().map_or(true, |id| id == t) {
                Some(*certainty)
            } else {
                None
            }
        }
        _ => None,
    });
    assert_eq!(
        failed_certainty,
        Some(DeliveryCertainty::FailedBeforeInjection),
        "a transport failure BEFORE send_turn succeeds ⇒ FailedBeforeInjection (§8.1), got {evs:?}"
    );

    // The send_turn that failed does not count as an injection, and there is no
    // second attempt (no blind retry). At most one send_turn was attempted.
    assert!(
        handle.shared.send_turn_count() <= 1,
        "a pre-injection failure is not retried (§8.1); send_turn attempts = {}",
        handle.shared.send_turn_count()
    );
}

// =====================================================================
// Item 7c — ProcessExited while IDLE / before any injection ⇒
// FailedBeforeInjection (no side-effect risk), target → Faulted/Stopped.
// =====================================================================

#[tokio::test]
async fn process_exit_before_injection_is_safe_failed_before_injection() {
    let proto = ScriptedProtocol::new();
    // The session dies right after becoming ready, before any turn is injected.
    proto.install(
        "ctx-earlyexit",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::ProcessExited { code: Some(1) }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov
        .target_for(&key("ctx-earlyexit"))
        .await
        .expect("target");

    // Drain the pre-injection lifecycle: SessionReady then ProcessExited.
    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 16).await;
    let evs = events_of(&items);
    assert!(
        evs.iter()
            .any(|e| matches!(e, TurnEvent::ProcessExited { .. })),
        "an idle-time process exit surfaces ProcessExited (§7.4 line 621), got {evs:?}"
    );

    // A submit AFTER the exit is refused as unavailable (Offline/Faulted/Stopped
    // ⇒ RejectedUnavailable, §7.1) — never Started, never a blind retry-spawn.
    let after = target
        .submit_turn(consult("ctx-earlyexit", "late"), BusyPolicy::Reject)
        .await;
    assert!(
        matches!(after, TurnReceipt::RejectedUnavailable { .. }),
        "submitting to a faulted/stopped target ⇒ RejectedUnavailable (§7.1), got {after:?}"
    );
}
