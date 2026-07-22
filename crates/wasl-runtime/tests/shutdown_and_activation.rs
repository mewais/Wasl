//! Phase-2A coverage-closing suite — the §7.3 `shutdown` matrix, the successful
//! queue-pop-and-ACTIVATE FIFO advancement path, and cancel-on-terminal-status.
//!
//! These drive spec-required, IMPLEMENTED-but-previously-undriven paths in the
//! per-target actor (`src/actor.rs`): `shutdown`/`stop`, `advance_after_terminal`
//! success arm, and the terminal-status `cancel_turn` guard. They are behavioral
//! and route through the same `support::ScriptedProtocol` scripted transport the
//! rest of the Phase-2A suite uses.
//!
//! Spec citations (docs/specs/runtime.md):
//!   * shutdown matrix — §7.3 (lines 599-607): `Drain` on Idle ⇒ Stopped;
//!     `Drain` on Busy/AwaitingGate ⇒ Draining, finish active, expire queue,
//!     then Stopped; `Immediate` (any) ⇒ Draining(0) then Stopped, active turn
//!     ⇒ TurnCancelled{Shutdown}, queued ⇒ TurnExpired, an injected-but-
//!     unfinished `command` ⇒ OutcomeUnknown.
//!   * queue pop → next turn or → Idle — §7.4 (line 618 `TurnCompleted` row),
//!     §5 ordering guarantee 2 (lines 471-487): a queued turn emits TurnAccepted
//!     only when it becomes ACTIVE, strictly AFTER the prior terminal event.
//!   * cancel on Offline/Stopped ⇒ Err(Unavailable) — §7.2 (line 596).

mod support;

use std::sync::Arc;
use std::time::Duration;

use wasl_runtime::{
    BusyPolicy, CancelReason, DeliveryCertainty, DeliveryTarget, EventSeq, NativeGateDetail,
    ProtocolEvent, RuntimeError, ShutdownMode, TargetStatus, TurnEvent, TurnReceipt,
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

/// Drive an idle-but-ready session: SessionReady, no turn ever accepted, so the
/// target parks Idle until a submit.
fn idle_ready_script() -> ProtocolScript {
    ProtocolScript::new().ready(Some("ses-1"))
}

// =====================================================================
// Item 1a — shutdown(Drain) on an IDLE target ⇒ Stopped immediately, emitting
// ProcessExited. §7.3 (Drain, Idle row).
// =====================================================================

#[tokio::test]
async fn shutdown_drain_on_idle_target_stops_immediately() {
    let proto = ScriptedProtocol::new();
    proto.install("ctx-drain-idle", idle_ready_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov
        .target_for(&key("ctx-drain-idle"))
        .await
        .expect("target");

    // Let the session become ready + idle (drain the SessionReady lifecycle).
    let s0 = target.events(EventSeq(0)).await;
    let _ = drain_items(s0, 8).await;

    let res = target.shutdown(ShutdownMode::Drain).await;
    assert_eq!(res, Ok(()), "Drain on an idle target succeeds (§7.3)");

    // The status is now Stopped, and a ProcessExited surfaced on the stream.
    assert_eq!(
        target.status().await,
        TargetStatus::Stopped,
        "Drain on Idle ⇒ Stopped immediately (§7.3)"
    );
    let s1 = target.events(EventSeq(0)).await;
    let items = drain_items(s1, 16).await;
    let evs = events_of(&items);
    assert!(
        evs.iter()
            .any(|e| matches!(e, TurnEvent::ProcessExited { .. })),
        "Drain on Idle emits ProcessExited (§7.3), got {evs:?}"
    );

    // A submit after a Drain-stop is refused as unavailable (§7.1).
    let after = target
        .submit_turn(consult("ctx-drain-idle", "late"), BusyPolicy::Reject)
        .await;
    assert!(
        matches!(after, TurnReceipt::RejectedUnavailable { .. }),
        "submitting to a Stopped target ⇒ RejectedUnavailable (§7.1), got {after:?}"
    );
}

// =====================================================================
// Item 1b — shutdown(Drain) on a BUSY target ⇒ Draining, finishes the active
// turn (its terminal event) then Stopped, and rejects a new submit while
// draining (§7.3, Drain/Busy row).
// =====================================================================

#[tokio::test]
async fn shutdown_drain_on_busy_target_drains_then_stops() {
    let proto = ScriptedProtocol::new();
    let handle = proto.install("ctx-drain-busy", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov
        .target_for(&key("ctx-drain-busy"))
        .await
        .expect("target");

    let started = target
        .submit_turn(consult("ctx-drain-busy", "go"), BusyPolicy::Reject)
        .await;
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    // Begin draining while the turn is still active.
    let res = target.shutdown(ShutdownMode::Drain).await;
    assert_eq!(res, Ok(()), "Drain on a busy target succeeds (§7.3)");
    assert!(
        matches!(target.status().await, TargetStatus::Draining { .. }),
        "Drain on a busy target ⇒ Draining until the active turn finishes (§7.3), got {:?}",
        target.status().await
    );

    // A new submit while Draining is refused — drain admits nothing new (§7.3).
    let refused = target
        .submit_turn(consult("ctx-drain-busy", "new"), BusyPolicy::Reject)
        .await;
    assert!(
        matches!(refused, TurnReceipt::RejectedBusy { .. }),
        "Draining admits nothing new ⇒ RejectedBusy (§7.3), got {refused:?}"
    );

    // Now let the active turn complete: the actor finishes it, then Stops.
    handle.push_event(ProtocolEvent::TurnCompleted {
        reply: "done".into(),
    });
    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);
    assert!(
        evs.iter().any(|e| matches!(
            e, TurnEvent::TurnCompleted { turn_id: t, .. } if *t == turn_id
        )),
        "the active turn finishes (its terminal TurnCompleted) before stop (§7.3), got {evs:?}"
    );
    assert!(
        evs.iter()
            .any(|e| matches!(e, TurnEvent::ProcessExited { .. })),
        "after draining the active turn the target stops (ProcessExited), got {evs:?}"
    );
    assert_eq!(
        target.status().await,
        TargetStatus::Stopped,
        "a drained busy target ends Stopped (§7.3)"
    );
}

// =====================================================================
// Item 1c — shutdown(Immediate) on a BUSY target with an injected COMMAND ⇒
// the active turn is TurnCancelled{Shutdown} and its certainty becomes
// OutcomeUnknown (write-ahead safety guarantee), then the target Stops.
// §7.3 (Immediate row) / §8.1.
// =====================================================================

#[tokio::test]
async fn shutdown_immediate_cancels_injected_command_as_outcome_unknown() {
    let proto = ScriptedProtocol::new();
    let handle = proto.install("ctx-imm-cmd", busy_forever_script());
    let clock = TestClock::new();
    let recording = RecordingSink::default();
    let sink = sink_arc(recording);
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-imm-cmd")).await.expect("target");

    // A COMMAND turn: write-ahead durable, then injected, never completes.
    let started = target
        .submit_turn(command("ctx-imm-cmd", "rm -rf /danger"), BusyPolicy::Reject)
        .await;
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };
    assert_eq!(
        handle.shared.send_turn_count(),
        1,
        "the command is injected once before shutdown"
    );

    // Immediate shutdown cancels the in-flight command.
    let res = target.shutdown(ShutdownMode::Immediate).await;
    assert_eq!(res, Ok(()), "Immediate shutdown succeeds (§7.3)");

    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);
    assert!(
        evs.iter().any(|e| matches!(
            e,
            TurnEvent::TurnCancelled { turn_id: t, reason: CancelReason::Shutdown }
                if *t == turn_id
        )),
        "Immediate shutdown cancels the active turn ⇒ TurnCancelled{{Shutdown}} (§7.3), got {evs:?}"
    );
    assert!(
        evs.iter()
            .any(|e| matches!(e, TurnEvent::ProcessExited { .. })),
        "Immediate shutdown stops the target (ProcessExited), got {evs:?}"
    );
    assert_eq!(
        target.status().await,
        TargetStatus::Stopped,
        "Immediate shutdown ⇒ Stopped (§7.3)"
    );

    // NO blind re-injection of an OutcomeUnknown command (§8.1 line 681): the
    // transport was asked to send exactly once, and the injected-but-unfinished
    // command's OutcomeUnknown certainty is asserted (durably) in the companion
    // test `shutdown_immediate_injected_command_audits_outcome_unknown`.
    assert_eq!(
        handle.shared.send_turn_count(),
        1,
        "an OutcomeUnknown command must NEVER be blindly re-injected on shutdown (§8.1 line 681)"
    );
}

// =====================================================================
// Item 1c (audit) — shutdown(Immediate) on an injected command records the
// OutcomeUnknown certainty in the durable audit sink (§8.1 / §7.3). Kept
// separate so the sink handle is inspectable.
// =====================================================================

#[tokio::test]
async fn shutdown_immediate_injected_command_audits_outcome_unknown() {
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc as StdArc, Mutex};
    use wasl_audit::{AuditEntry, RuntimeTurnPhase};

    let proto = ScriptedProtocol::new();
    let handle = proto.install("ctx-imm-audit", busy_forever_script());
    let clock = TestClock::new();
    // A recording sink whose entries we can inspect after the shutdown.
    let recording = RecordingSink::with_counter(StdArc::new(AtomicU64::new(0)));
    let recording = StdArc::new(recording);
    let sink: StdArc<Mutex<dyn wasl_audit::Sink + Send>> = StdArc::new(Mutex::new(SinkProxy {
        inner: recording.clone(),
    }));
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov
        .target_for(&key("ctx-imm-audit"))
        .await
        .expect("target");

    let _ = target
        .submit_turn(command("ctx-imm-audit", "danger"), BusyPolicy::Reject)
        .await;
    assert_eq!(handle.shared.send_turn_count(), 1);

    target
        .shutdown(ShutdownMode::Immediate)
        .await
        .expect("immediate shutdown");

    let entries: Vec<AuditEntry> = recording.snapshot();
    assert!(
        entries.iter().any(|e| e.phase == Some(RuntimeTurnPhase::Failed)
            && e.certainty == Some(DeliveryCertainty::OutcomeUnknown)),
        "an injected command cancelled by Immediate shutdown is audited OutcomeUnknown (§7.3/§8.1), \
         got {entries:?}"
    );
}

/// A tiny proxy so a shared `Arc<RecordingSink>` can be handed to the actor as
/// the `Arc<Mutex<dyn Sink>>` while the test keeps the `Arc` to inspect entries.
struct SinkProxy {
    inner: std::sync::Arc<RecordingSink>,
}

impl wasl_audit::Sink for SinkProxy {
    fn append(&mut self, entry: &wasl_audit::AuditEntry) -> Result<(), wasl_audit::AuditError> {
        // RecordingSink::append takes &mut self; go through a cell-free path by
        // pushing directly into its public Mutex fields.
        self.inner.entries.lock().unwrap().push(entry.clone());
        Ok(())
    }
}

// =====================================================================
// Item 1c' — shutdown(Immediate) on an IDLE target (no active turn): the
// `if let Some(active) = self.active.take()` None arm runs (nothing to cancel),
// the empty queue is a no-op, and the target Stops. §7.3 (Immediate, any).
// =====================================================================

#[tokio::test]
async fn shutdown_immediate_on_idle_target_stops_cleanly() {
    let proto = ScriptedProtocol::new();
    proto.install("ctx-imm-idle", idle_ready_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-imm-idle")).await.expect("target");

    // Reach Idle (no active turn), then Immediate-shutdown: no active turn to
    // cancel (the None arm), no queued turns, just stop.
    let s0 = target.events(EventSeq(0)).await;
    let _ = drain_items(s0, 8).await;

    let stream = target.events(EventSeq(0)).await;
    target
        .shutdown(ShutdownMode::Immediate)
        .await
        .expect("immediate shutdown of an idle target");

    let items = drain_items(stream, 16).await;
    let evs = events_of(&items);
    assert!(
        evs.iter()
            .any(|e| matches!(e, TurnEvent::ProcessExited { .. })),
        "Immediate shutdown of an idle target stops it (ProcessExited), got {evs:?}"
    );
    // No spurious TurnCancelled (there was no active turn to cancel).
    assert!(
        !evs.iter()
            .any(|e| matches!(e, TurnEvent::TurnCancelled { .. })),
        "an idle Immediate shutdown cancels nothing, got {evs:?}"
    );
    assert_eq!(target.status().await, TargetStatus::Stopped);
}

// =====================================================================
// Item 1d — shutdown(Immediate) with a NON-EMPTY queue expires the queued
// turns (TurnExpired) and cancels the active turn. §7.3 (Immediate row:
// queued ⇒ TurnExpired).
// =====================================================================

#[tokio::test]
async fn shutdown_immediate_expires_queued_turns() {
    let proto = ScriptedProtocol::new();
    proto.install("ctx-imm-q", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov.target_for(&key("ctx-imm-q")).await.expect("target");

    // Active turn.
    let active = target
        .submit_turn(consult("ctx-imm-q", "active"), BusyPolicy::Reject)
        .await;
    let active_turn = match active {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };
    // Two queued turns behind it.
    let long = BusyPolicy::Queue {
        max_wait: Duration::from_millis(120_000),
    };
    let q0 = target
        .submit_turn(consult("ctx-imm-q", "q0"), long.clone())
        .await;
    let q1 = target.submit_turn(consult("ctx-imm-q", "q1"), long).await;
    let q0_id = match q0 {
        TurnReceipt::Queued { turn_id, .. } => turn_id,
        other => panic!("expected Queued, got {other:?}"),
    };
    let q1_id = match q1 {
        TurnReceipt::Queued { turn_id, .. } => turn_id,
        other => panic!("expected Queued, got {other:?}"),
    };

    // Subscribe BEFORE the shutdown so the live broadcast captures every event
    // (a tiny journal ring would otherwise drop early seqs below the floor). The
    // stream closes when the target stops, so the drain returns fast.
    let stream = target.events(EventSeq(0)).await;
    target
        .shutdown(ShutdownMode::Immediate)
        .await
        .expect("immediate shutdown");

    let items = drain_items(stream, 64).await;
    let evs = events_of(&items);
    // The active turn is cancelled; both queued turns expire.
    assert!(
        evs.iter().any(|e| matches!(
            e, TurnEvent::TurnCancelled { turn_id: t, reason: CancelReason::Shutdown } if *t == active_turn
        )),
        "Immediate shutdown cancels the active turn (§7.3), got {evs:?}"
    );
    for qid in [&q0_id, &q1_id] {
        assert!(
            evs.iter().any(|e| matches!(
                e, TurnEvent::TurnExpired { turn_id: t } if t == qid
            )),
            "Immediate shutdown expires queued turn {qid:?} (§7.3), got {evs:?}"
        );
    }
}

// =====================================================================
// Item 1e — shutdown(Immediate) on an AwaitingGate target cancels the gated
// turn and stops (the Immediate row applies in every phase incl. AwaitingGate).
// =====================================================================

#[tokio::test]
async fn shutdown_immediate_on_awaiting_gate_cancels_and_stops() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-imm-gate",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .emit(ProtocolEvent::NativeGateRequested {
                detail: NativeGateDetail {
                    summary: "write?".into(),
                    preview: "echo hi".into(),
                },
            }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-imm-gate")).await.expect("target");

    let started = target
        .submit_turn(consult("ctx-imm-gate", "go"), BusyPolicy::Reject)
        .await;
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };
    // Let the gate surface.
    let s0 = target.events(EventSeq(0)).await;
    let _ = drain_items(s0, 8).await;
    assert!(
        matches!(target.status().await, TargetStatus::AwaitingGate { .. }),
        "the target is AwaitingGate before shutdown, got {:?}",
        target.status().await
    );

    // Subscribe BEFORE shutdown so the live broadcast captures the cancellation
    // even with a tiny journal ring; the stream closes on stop (fast drain).
    let stream = target.events(EventSeq(0)).await;
    target
        .shutdown(ShutdownMode::Immediate)
        .await
        .expect("immediate shutdown");

    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);
    assert!(
        evs.iter().any(|e| matches!(
            e, TurnEvent::TurnCancelled { turn_id: t, reason: CancelReason::Shutdown } if *t == turn_id
        )),
        "Immediate shutdown cancels the gated active turn (§7.3), got {evs:?}"
    );
    assert_eq!(target.status().await, TargetStatus::Stopped);
}

// =====================================================================
// Item 1f — shutdown is idempotent on an already-terminal target: a second
// shutdown returns Ok(()) (the terminal guard, §7.3 `Drain, Draining` no-op /
// terminal fast-path).
// =====================================================================

#[tokio::test]
async fn shutdown_on_already_terminal_target_is_ok_noop() {
    let proto = ScriptedProtocol::new();
    proto.install("ctx-double-stop", idle_ready_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov
        .target_for(&key("ctx-double-stop"))
        .await
        .expect("target");

    let s0 = target.events(EventSeq(0)).await;
    let _ = drain_items(s0, 8).await;

    target
        .shutdown(ShutdownMode::Drain)
        .await
        .expect("first drain");
    assert_eq!(target.status().await, TargetStatus::Stopped);

    // A second shutdown (either mode) on a terminal target is a benign Ok no-op.
    assert_eq!(
        target.shutdown(ShutdownMode::Immediate).await,
        Ok(()),
        "shutdown on an already-terminal target is Ok(()) (§7.3 terminal guard)"
    );
    assert_eq!(
        target.shutdown(ShutdownMode::Drain).await,
        Ok(()),
        "a second Drain on a terminal target is Ok(()) (§7.3)"
    );
}

// =====================================================================
// Item 2 — SUCCESSFUL queue-pop-and-ACTIVATE: submit A (active), submit B
// (queues), complete A, and assert B automatically activates next — status
// goes Busy(A) → (A completes) → Busy(B), B gets its own TurnAccepted, and B is
// injected. This is the core FIFO advancement success arm (§7.4 pop-Q → next).
// =====================================================================

#[tokio::test]
async fn completing_active_turn_auto_activates_next_queued_turn() {
    let proto = ScriptedProtocol::new();
    // A accepts (busy). We push A's completion after B is queued so the pop
    // path activates B. B's own TurnAccepted is emitted by the actor at
    // activation, then B is injected (a second send_turn).
    let handle = proto.install(
        "ctx-activate",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-activate")).await.expect("target");

    let a = target
        .submit_turn(consult("ctx-activate", "A"), BusyPolicy::Reject)
        .await;
    let a_id = match a {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started for A, got {other:?}"),
    };
    let b = target
        .submit_turn(
            consult("ctx-activate", "B"),
            BusyPolicy::Queue {
                max_wait: Duration::from_millis(120_000),
            },
        )
        .await;
    let b_id = match b {
        TurnReceipt::Queued { turn_id, position } => {
            assert_eq!(position, 0, "B queues at position 0 behind A");
            turn_id
        }
        other => panic!("expected Queued for B, got {other:?}"),
    };
    assert_eq!(
        handle.shared.send_turn_count(),
        1,
        "only A is injected while B waits in the queue"
    );

    // Complete A. The actor pops the queue and ACTIVATES B (not a cyclic/rejected
    // turn — B is a plain sibling), injecting it.
    handle.push_event(ProtocolEvent::TurnCompleted {
        reply: "A-done".into(),
    });

    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 64).await;
    let evs = events_of(&items);

    // A completes.
    let a_completed_idx = evs
        .iter()
        .position(|e| matches!(e, TurnEvent::TurnCompleted { turn_id: t, .. } if *t == a_id));
    assert!(
        a_completed_idx.is_some(),
        "A must complete (its terminal TurnCompleted), got {evs:?}"
    );
    // B is ACCEPTED (activated) strictly AFTER A's terminal event (§5 g2).
    let b_accepted_idx = evs
        .iter()
        .position(|e| matches!(e, TurnEvent::TurnAccepted { turn_id: t, .. } if *t == b_id));
    assert!(
        b_accepted_idx.is_some(),
        "B must be activated (its own TurnAccepted) on pop (§7.4), got {evs:?}"
    );
    assert!(
        b_accepted_idx.unwrap() > a_completed_idx.unwrap(),
        "B activates strictly AFTER A's terminal event (§5 ordering guarantee 2), got {evs:?}"
    );
    // B is NOT expired/cancelled — it is a genuine successful activation.
    assert!(
        !evs.iter().any(|e| matches!(
            e, TurnEvent::TurnExpired { turn_id: t } if *t == b_id
        )),
        "B is a successful activation, never TurnExpired (this is the success arm), got {evs:?}"
    );

    // B is now the ACTIVE turn (Busy on B), and it was injected (a 2nd send_turn).
    match target.status().await {
        TargetStatus::Busy { active_turn, .. } => assert_eq!(
            active_turn, b_id,
            "after A completes, B is the active turn (queue-pop-activate success)"
        ),
        other => panic!("expected Busy(B) after activation, got {other:?}"),
    }
    assert_eq!(
        handle.shared.send_turn_count(),
        2,
        "activating B injects it (a second send_turn): the pop-and-inject success arm"
    );
}

// =====================================================================
// Item 8 — cancel_turn on an already-terminal (Stopped) target ⇒
// Err(Unavailable), never a panic or a wrong transition. §7.2 (line 596).
// =====================================================================

#[tokio::test]
async fn cancel_on_stopped_target_returns_unavailable() {
    let proto = ScriptedProtocol::new();
    proto.install("ctx-cancel-term", idle_ready_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov
        .target_for(&key("ctx-cancel-term"))
        .await
        .expect("target");

    let s0 = target.events(EventSeq(0)).await;
    let _ = drain_items(s0, 8).await;

    // Stop the target.
    target
        .shutdown(ShutdownMode::Drain)
        .await
        .expect("drain to stop");
    assert_eq!(target.status().await, TargetStatus::Stopped);

    // Cancelling any turn on a Stopped target ⇒ Err(Unavailable) (§7.2 line 596).
    let res = target
        .cancel_turn(
            wasl_runtime::TurnId("whatever".into()),
            CancelReason::OperatorRequest,
        )
        .await;
    assert!(
        matches!(res, Err(RuntimeError::Unavailable { .. })),
        "cancel on a Stopped/terminal target ⇒ Err(Unavailable) (§7.2 line 596), got {res:?}"
    );
}
