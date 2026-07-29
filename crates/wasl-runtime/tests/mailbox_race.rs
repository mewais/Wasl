#![allow(clippy::items_after_statements)]
//! Tests for Issue B: race `next_event()` against the mailbox (§2, §0.1 items 1-3).
mod support;

use support::*;

use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::task::yield_now;
use tokio::time::timeout;

use wasl_proto::RequestId;
use wasl_runtime::{
    BusyPhase, BusyPolicy, CancelReason, EventSeq, ManagedConfig, ProtocolEvent, RuntimeError,
    TargetStatus, TurnEvent, TurnId,
};

// A short timeout for expecting responsiveness.
const RESPONSIVENESS_TIMEOUT: Duration = Duration::from_millis(200);

/// A submit racing the transport's `SessionReady` may (per §7.1 row 582 /
/// §7.4 row 612) land either "already ready, started immediately" or
/// "still `Starting`, queued then promoted once ready arrives". The two
/// orderings differ in which/how many extra housekeeping events (a leading
/// `TurnQueued`, an intervening `StatusChanged{Idle}`) appear around
/// `SessionReady`/`TurnAccepted` — both are spec-correct. Assert only the
/// invariant that actually matters: `SessionReady` is observed, `TurnAccepted`
/// is observed, and `SessionReady` precedes it — never pin an exact position.
fn assert_session_ready_precedes_turn_accepted(events: &[TurnEvent]) {
    let ready_idx = events
        .iter()
        .position(|e| matches!(e, TurnEvent::SessionReady { .. }));
    let accepted_idx = events
        .iter()
        .position(|e| matches!(e, TurnEvent::TurnAccepted { .. }));
    assert!(
        ready_idx.is_some(),
        "expected a SessionReady event, got {:?}",
        events
    );
    assert!(
        accepted_idx.is_some(),
        "expected a TurnAccepted event, got {:?}",
        events
    );
    assert!(
        ready_idx < accepted_idx,
        "expected SessionReady to precede TurnAccepted, got {:?}",
        events
    );
}

#[test]
fn issue_b_unfixed_core_bug_timeout_today() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let clock = TestClock::new();
        let sink = sink_arc(RecordingSink::default());

        // Use default config, which has a hang_watchdog_timeout (60s).
        let config = ManagedConfig::default();

        let protocol = ScriptedProtocol::new();
        let _shared_counter = protocol.shared_counter();
        let provider = provider(protocol.clone(), config, clock.clone(), sink.clone());

        let context_id = "test_target";

        // Script a turn that will block next_event indefinitely by gating its terminal event.
        let script = ProtocolScript::new()
            .ready(Some("native-session-id"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .gated(
                "hold_event",
                ProtocolEvent::TurnCompleted {
                    reply: "ok".to_string(),
                },
            );
        let script_handle = protocol.install(context_id, script);

        let target = provider.target_for(&key(context_id)).await.unwrap();

        let stream = target.events(EventSeq(0)).await;

        // Submit turn and get turn_id from receipt
        let turn_receipt = target
            .submit_turn(consult(context_id, "long_running"), BusyPolicy::Reject)
            .await;
        let turn_id = match turn_receipt {
            // §7.1 row 582: a submit racing the transport's `SessionReady` may
            // land either `Started` (session was already ready) or `Queued`
            // (session was still `Starting`, promoted once ready arrives) —
            // both are spec-correct and carry a `turn_id`.
            wasl_runtime::TurnReceipt::Started { turn_id, .. }
            | wasl_runtime::TurnReceipt::Queued { turn_id, .. } => turn_id,
            _ => panic!(
                "Expected TurnReceipt::Started or Queued, got {:?}",
                turn_receipt
            ),
        };

        // Expect SessionReady and TurnAccepted. Drain generously (up to 5:
        // possibly TurnQueued, SessionReady, TurnAccepted, 2x StatusChanged)
        // with a SHORT per-item timeout — the extra housekeeping events are
        // either already buffered or never coming, no reason to wait the full
        // hang-guard for them.
        let events = drain_items_timeout(stream, 5, Duration::from_millis(500)).await;
        let events = events_of(&events);
        assert_session_ready_precedes_turn_accepted(&events);

        // Now, the actor is blocked inside `transport.next_event().await` on the
        // gated terminal event (actor.rs:201 — a standalone await that is NOT
        // raced against the mailbox). We send a cancel_turn command.
        //
        // `cancel_turn` does `mailbox.send(..).await` (which succeeds — the
        // bounded mailbox has room) then `rx.await` for the actor's reply. We wrap
        // the WHOLE call in an explicit timeout so it can never hang the test,
        // regardless of the actor's state.
        //
        // This asserts the CORRECT (fixed) contract — "every mailbox command is
        // serviced promptly, even while a slow event is outstanding": the bounded
        // wait does NOT fire, and cancel returns a real result. Today, with the
        // unfixed actor stuck in `next_event()`, the Cancel is never processed and
        // `rx.await` never resolves, so the timeout FIRES and this assertion fails
        // FAST and CLEANLY (~200ms, no hang, specific message). Once the core fix
        // lands (the actor races `next_event()` against its mailbox), the Cancel is
        // serviced promptly and this passes. Same polarity as
        // `cap_delta_truncation.rs`: red today, green when fixed.
        let cancel_res = timeout(
            RESPONSIVENESS_TIMEOUT,
            target.cancel_turn(turn_id.clone(), CancelReason::Shutdown),
        )
        .await;
        assert!(
            cancel_res.is_ok(),
            "REQUIRED (fixed) contract: cancel_turn must be serviced within {:?} even \
             while the actor has a slow/gated event outstanding — the mailbox command \
             must be raced against next_event(). It timed out instead (Err = bounded \
             wait fired), which is the unfixed Issue-B bug: the actor is blocked in \
             next_event() and never processes the Cancel. Fix the actor to select over \
             next_event() vs. the mailbox.",
            RESPONSIVENESS_TIMEOUT
        );
        // With the fix, the serviced cancel returns Ok(()) (the active turn is
        // interruptible under the full-featured scripted caps) or a benign
        // NotFound/Ok — never a hang.
        let cancel_inner = cancel_res.expect("bounded above");
        assert!(
            cancel_inner.is_ok() || matches!(cancel_inner, Err(RuntimeError::NotFound { .. })),
            "cancel_turn returned an unexpected error: {:?}",
            cancel_inner
        );

        // Clean up: release the event so the actor can eventually process it and exit gracefully.
        script_handle.release("hold_event");
        let new_stream = target.events(EventSeq(0)).await;
        let _ = timeout(Duration::from_secs(5), drain_items(new_stream, 2)).await;
    });
}

#[test]
fn issue_b_simultaneously_ready_neither_lost() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let clock = TestClock::new();
        let sink = sink_arc(RecordingSink::default());
        let config = ManagedConfig::default();
        let protocol = ScriptedProtocol::new();
        let _shared_counter = protocol.shared_counter();
        let provider = provider(protocol.clone(), config, clock.clone(), sink.clone());

        let context_id = "test_target";

        // Script: SessionReady, then an AssistantDelta event ready to emit immediately.
        // The TurnCompleted is gated so the turn remains active for a bit.
        let script = ProtocolScript::new()
            .ready(Some("native-session-id"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .emit(ProtocolEvent::AssistantDelta {
                text: "first delta".to_string(),
            })
            .gated(
                "hold_completion",
                ProtocolEvent::TurnCompleted {
                    reply: "done".to_string(),
                },
            );
        let script_handle = protocol.install(context_id, script);

        let target = provider.target_for(&key(context_id)).await.unwrap();

        let stream = target.events(EventSeq(0)).await;

        // Submit turn to make it active
        let turn_receipt = target
            .submit_turn(consult(context_id, "simultaneous"), BusyPolicy::Reject)
            .await;
        let turn_id = match turn_receipt {
            // §7.1 row 582: a submit racing the transport's `SessionReady` may
            // land either `Started` (session was already ready) or `Queued`
            // (session was still `Starting`, promoted once ready arrives) —
            // both are spec-correct and carry a `turn_id`.
            wasl_runtime::TurnReceipt::Started { turn_id, .. }
            | wasl_runtime::TurnReceipt::Queued { turn_id, .. } => turn_id,
            _ => panic!(
                "Expected TurnReceipt::Started or Queued, got {:?}",
                turn_receipt
            ),
        };

        // Drain initial events: [optional leading TurnQueued], SessionReady,
        // [optional StatusChanged{Idle}], TurnAccepted, 2xStatusChanged{Busy},
        // AssistantDelta (§7.1 row 582 / §7.4 row 612 — see
        // `assert_session_ready_precedes_turn_accepted`). Grab extra slots with
        // a SHORT per-item timeout for the optional events so this doesn't
        // stall when they legitimately never come.
        let events = drain_items_timeout(stream, 6, Duration::from_millis(500)).await;
        let received_events = events_of(&events);
        assert_session_ready_precedes_turn_accepted(&received_events);
        assert!(
            received_events.iter().any(|e| matches!(
                e,
                TurnEvent::StatusChanged {
                    status: TargetStatus::Busy {
                        phase: BusyPhase::Dispatching,
                        ..
                    }
                }
            )),
            "expected a StatusChanged{{Busy,Dispatching}} event, got {:?}",
            received_events
        );
        assert!(
            received_events.iter().any(|e| matches!(
                e,
                TurnEvent::StatusChanged {
                    status: TargetStatus::Busy {
                        phase: BusyPhase::Generating,
                        ..
                    }
                }
            )),
            "expected a StatusChanged{{Busy,Generating}} event, got {:?}",
            received_events
        );
        assert!(
            received_events.iter().any(
                |e| matches!(e, TurnEvent::AssistantDelta { text, .. } if text == "first delta")
            ),
            "expected an AssistantDelta{{\"first delta\"}} event, got {:?}",
            received_events
        );

        // Now, the actor has processed the AssistantDelta and is blocked on the gated TurnCompleted.
        // Send a command. Use a new cancel_turn for a non-existent turn to avoid race on the active turn.
        yield_now().await;

        // Fix: Correct RequestId usage with TurnId
        let cancel_another_future = target.cancel_turn(
            TurnId(RequestId("some-other-turn".to_string()).0),
            CancelReason::Shutdown,
        );
        let cancel_current_future = target.cancel_turn(turn_id.clone(), CancelReason::Shutdown);

        // Wait for both to complete — but BOUNDED. The actor is blocked inside
        // `transport.next_event().await` on the gated `TurnCompleted` (actor.rs:201),
        // and that standalone await is not raced against the mailbox.
        //
        // Each `cancel_turn`'s `mailbox.send(..).await` succeeds (the bounded
        // mailbox has room) but the subsequent `rx.await` for the actor's reply
        // only resolves once the actor processes the Cancel — which it cannot do
        // while stuck in `next_event()`. We wrap the ENTIRE `join!` in an explicit
        // timeout so neither future can hang the test.
        //
        // This asserts the CORRECT (fixed) contract: BOTH simultaneously-ready
        // cancels are serviced within the bound (neither is lost while an event is
        // outstanding), so the join completes. Verified from actor.rs::run +
        // actor.rs::cancel and provider.rs::cancel_turn: today, with the unfixed
        // actor, neither reply arrives, the join never completes, the timeout
        // FIRES, and this assertion fails FAST and CLEANLY (~200ms, specific
        // message). Once the actor races next_event() vs. the mailbox, both
        // replies arrive promptly and this passes.
        let joined = timeout(RESPONSIVENESS_TIMEOUT, async {
            tokio::join!(cancel_another_future, cancel_current_future)
        })
        .await;
        assert!(
            joined.is_ok(),
            "REQUIRED (fixed) contract: two simultaneously-ready cancel_turn commands \
             must both be serviced within {:?} while the actor has a gated event \
             outstanding — neither may be lost. The bounded join timed out instead, \
             which is the unfixed Issue-B bug: the actor is blocked in next_event() \
             and processes no mailbox command. Fix the actor to select over \
             next_event() vs. the mailbox.",
            RESPONSIVENESS_TIMEOUT
        );
        let (cancel_another_res, cancel_current_res) = joined.expect("bounded above");

        // The cancel for the non-existent turn should return NotFound or Ok.
        assert!(
            cancel_another_res.is_ok()
                || matches!(cancel_another_res, Err(RuntimeError::NotFound { .. })),
            "Cancel another turn failed unexpectedly: {:?}",
            cancel_another_res
        );

        // The cancel for the current turn should return NotFound if it was processed
        // after the turn completed, or Ok if it was processed before.
        assert!(
            cancel_current_res.is_ok()
                || matches!(cancel_current_res, Err(RuntimeError::NotFound { .. })),
            "Cancel current turn failed unexpectedly: {:?}",
            cancel_current_res
        );

        // Release the completion event so the turn can finish if it hasn\'t already been cancelled.
        script_handle.release("hold_completion");

        // Get a new stream for draining remaining events.
        let new_stream = target.events(EventSeq(0)).await;
        let _ = timeout(Duration::from_secs(5), drain_items(new_stream, 2)).await;
    });
}

#[test]
fn issue_b_sustained_mailbox_traffic_not_starve_events() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let clock = TestClock::new();
        let sink = sink_arc(RecordingSink::default());
        let config = ManagedConfig::default();
        let protocol = ScriptedProtocol::new();
        let _shared_counter = protocol.shared_counter();
        let provider = provider(protocol.clone(), config, clock.clone(), sink.clone());

        let context_id = "test_target";

        // Script: SessionReady, TurnAccepted, then a gated TurnCompleted.
        let script = ProtocolScript::new()
            .ready(Some("native-session-id"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .gated(
                "hold_completion",
                ProtocolEvent::TurnCompleted {
                    reply: "done".to_string(),
                },
            );
        let script_handle = protocol.install(context_id, script);

        let target = provider.target_for(&key(context_id)).await.unwrap();

        let stream = target.events(EventSeq(0)).await;

        // Submit turn to make it active
        let turn_receipt = target
            .submit_turn(
                consult(context_id, "long_running_event"),
                BusyPolicy::Reject,
            )
            .await;
        let _turn_id = match turn_receipt {
            // §7.1 row 582: a submit racing the transport's `SessionReady` may
            // land either `Started` (session was already ready) or `Queued`
            // (session was still `Starting`, promoted once ready arrives) —
            // both are spec-correct and carry a `turn_id`.
            wasl_runtime::TurnReceipt::Started { turn_id, .. }
            | wasl_runtime::TurnReceipt::Queued { turn_id, .. } => turn_id,
            _ => panic!(
                "Expected TurnReceipt::Started or Queued, got {:?}",
                turn_receipt
            ),
        };

        // Drain initial events: [optional leading TurnQueued], SessionReady,
        // [optional StatusChanged{Idle}], TurnAccepted, StatusChanged
        // {Dispatching}, StatusChanged{Generating} (§7.1 row 582 / §7.4 row
        // 612). Grab extra slots with a SHORT per-item timeout for the
        // optional events.
        let events = drain_items_timeout(stream, 5, Duration::from_millis(500)).await;
        let drained_count = events.len();
        let received_events = events_of(&events);
        assert_session_ready_precedes_turn_accepted(&received_events);
        assert!(
            received_events.iter().any(|e| matches!(
                e,
                TurnEvent::StatusChanged {
                    status: TargetStatus::Busy {
                        phase: BusyPhase::Dispatching,
                        ..
                    }
                }
            )),
            "expected a StatusChanged{{Busy,Dispatching}} event, got {:?}",
            received_events
        );
        assert!(
            received_events.iter().any(|e| matches!(
                e,
                TurnEvent::StatusChanged {
                    status: TargetStatus::Busy {
                        phase: BusyPhase::Generating,
                        ..
                    }
                }
            )),
            "expected a StatusChanged{{Busy,Generating}} event, got {:?}",
            received_events
        );

        // Now, the actor is blocked inside `transport.next_event().await` on the
        // gated `TurnCompleted` (actor.rs:201). Send a sustained stream of new
        // commands (submit_turn for fresh turns that, once serviced, are
        // RejectedBusy since a turn is already active under BusyPolicy::Reject).
        //
        // What actually happens (verified by reading provider.rs::submit_turn →
        // admit_via_mailbox and actor.rs::run): `submit_turn` does
        // `mailbox.send(Command::Submit{..}).await` then `rx.await` for the
        // receipt. The mailbox is a BOUNDED mpsc channel of capacity 256
        // (ManagedConfig::default().mailbox_capacity), so `send().await` for these
        // 5 messages does NOT block on capacity — it lands in the buffer and
        // returns immediately. The reply wait `rx.await` is the real blocker: it
        // only resolves once the actor processes the Submit, which it cannot do
        // while stuck in `next_event()`. So the mailbox `send` is NOT what blocks;
        // the receipt wait is — which is why the FIRST iteration would hang forever
        // if unbounded. We bound EACH submit with an explicit timeout.
        //
        // This asserts the CORRECT (fixed) contract — "sustained mailbox traffic
        // must not starve while an event is outstanding": every submit is serviced
        // within the bound and returns a receipt (RejectedBusy). Today, with the
        // unfixed actor, the first submit's receipt never arrives, its bounded wait
        // FIRES, and this fails FAST and CLEANLY (~200ms, specific message). Once
        // the actor races next_event() vs. the mailbox, every submit returns
        // promptly and this passes.
        let num_commands = 5;
        for i in 0..num_commands {
            yield_now().await;
            let submit_res = timeout(
                RESPONSIVENESS_TIMEOUT,
                target.submit_turn(
                    consult(context_id, &format!("traffic-{}", i)),
                    BusyPolicy::Reject,
                ),
            )
            .await;
            let receipt = submit_res.unwrap_or_else(|_| {
                panic!(
                    "REQUIRED (fixed) contract: submit_turn #{} was not serviced within \
                     {:?} while the actor had a gated event outstanding — sustained \
                     mailbox traffic is being STARVED. This is the unfixed Issue-B bug: \
                     the actor is blocked in next_event() and produces no receipt. Fix \
                     the actor to select over next_event() vs. the mailbox.",
                    i, RESPONSIVENESS_TIMEOUT
                )
            });
            // A fresh turn while one is already active + BusyPolicy::Reject ⇒ the
            // command reaches the mailbox and is answered with RejectedBusy.
            assert!(
                matches!(receipt, wasl_runtime::TurnReceipt::RejectedBusy { .. }),
                "submit_turn #{} should be RejectedBusy (a turn is active), got {:?}",
                i,
                receipt
            );
        }

        // Release the terminal event after sending commands.
        script_handle.release("hold_completion");

        // Now, wait for the TurnCompleted event. This should eventually arrive.
        //
        // `events(after_seq)` replays EXACTLY-ONCE from `after_seq` onward (the
        // journal's own documented, deliberately-tested contract) — re-subscribing
        // at `EventSeq(0)` would replay the WHOLE history again from the start
        // (SessionReady, TurnAccepted, ...), not just the new tail. Resume from
        // however many events were ACTUALLY drained above (`drained_count`,
        // which may legitimately be 4 or 5 — see the optional intervening
        // StatusChanged{Idle} comment above), not a hardcoded count. Search for
        // TurnCompleted by content (not a fixed position/count) so this isn't
        // brittle to exactly how many intermediate events the fix produces.
        let new_stream = target.events(EventSeq(drained_count as u64)).await;
        let final_events = timeout(Duration::from_secs(5), drain_items(new_stream, 10)).await;
        assert!(
            final_events.is_ok(),
            "Expected TurnCompleted event to be delivered, but it timed out."
        );
        let received_final_events = events_of(&final_events.unwrap());
        assert!(
            received_final_events
                .iter()
                .any(|e| matches!(e, TurnEvent::TurnCompleted { .. })),
            "Expected a TurnCompleted event among the new events, got: {:?}",
            received_final_events
        );
    });
}

#[test]
fn issue_b_watchdog_concurrent_with_mailbox_command() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let clock = TestClock::new();
        let sink = sink_arc(RecordingSink::default());

        // Default ManagedConfig does not have hang_watchdog_timeout field. Rely on default.
        let config = ManagedConfig::default();

        let protocol = ScriptedProtocol::new();
        let _shared_counter = protocol.shared_counter();
        let provider = provider(protocol.clone(), config, clock.clone(), sink.clone());

        let context_id = "test_target";

        // Script: SessionReady, TurnAccepted, then a gated TurnCompleted that is NEVER released.
        let script = ProtocolScript::new()
            .ready(Some("native-session-id"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .gated(
                "never_release",
                ProtocolEvent::TurnCompleted {
                    reply: "done".to_string(),
                },
            );
        let _script_handle = protocol.install(context_id, script);

        let target = provider.target_for(&key(context_id)).await.unwrap();

        let stream_initial = target.events(EventSeq(0)).await;

        // Submit turn to make it active. This turn will hang.
        let turn_receipt = target
            .submit_turn(consult(context_id, "hanging_turn"), BusyPolicy::Reject)
            .await;
        let turn_id = match turn_receipt {
            // §7.1 row 582: a submit racing the transport's `SessionReady` may
            // land either `Started` (session was already ready) or `Queued`
            // (session was still `Starting`, promoted once ready arrives) —
            // both are spec-correct and carry a `turn_id`.
            wasl_runtime::TurnReceipt::Started { turn_id, .. }
            | wasl_runtime::TurnReceipt::Queued { turn_id, .. } => turn_id,
            _ => panic!(
                "Expected TurnReceipt::Started or Queued, got {:?}",
                turn_receipt
            ),
        };

        // Drain initial events: SessionReady, [optional StatusChanged{Idle} —
        // §7.1 row 582 / §7.4 row 612, see comment in
        // issue_b_unfixed_core_bug_timeout_today above], TurnAccepted. Grab one
        // extra slot with a SHORT per-item timeout for the optional event.
        let events = drain_items_timeout(stream_initial, 3, Duration::from_millis(500)).await;
        let drained_count = events.len();
        let received_events = events_of(&events);
        assert_session_ready_precedes_turn_accepted(&received_events);

        // Advance clock past default watchdog timeout (assume > 60s for now).
        clock.advance(Duration::from_secs(65));

        // Yield to allow the actor to process the clock advance and fire the watchdog.
        yield_now().await;

        // While the watchdog is potentially terminating the transport, send a mailbox command.
        // This command should not cause a panic or state corruption.
        let cancel_res = timeout(RESPONSIVENESS_TIMEOUT, async {
            target
                .cancel_turn(turn_id.clone(), CancelReason::Shutdown)
                .await
        })
        .await;

        // The command must be SERVICED quickly (the outer `timeout` must not
        // elapse) even while the watchdog may be mid-termination — the actual
        // requirement this test exists to verify (§0.1 item 2b: a command
        // arriving during watchdog escalation must not be silently raced
        // against process teardown). The INNER result may be `Ok(())` or any
        // error (`Unavailable`/`NotFound`/etc. are all fine — the turn may
        // already be gone by the time this is processed); what must NEVER
        // happen is the outer bound firing (`Err(Elapsed)`), which would mean
        // the actor never serviced the command at all.
        assert!(
            cancel_res.is_ok(),
            "cancel_turn must be serviced within {:?} even while the watchdog is \
             (potentially) mid-termination — it must never be silently raced against \
             process teardown (§0.1 item 2b). Timed out instead: {:?}",
            RESPONSIVENESS_TIMEOUT,
            cancel_res
        );

        // Expect the target to eventually be in a Faulted or Stopped state, and not panic.
        // We can check the status stream for this.
        //
        // Same fix as the sustained-traffic test above: resume from however many
        // events were ACTUALLY drained above (`drained_count`), not a hardcoded
        // count, and search generously for the Faulted/Stopped transition
        // rather than assuming a fixed position.
        let stream_final = target.events(EventSeq(drained_count as u64)).await;
        let status_events = timeout(Duration::from_secs(5), drain_items(stream_final, 10)).await;
        assert!(
            status_events.is_ok(),
            "Expected status change events, but timed out."
        );
        let received_status_events = events_of(&status_events.unwrap());

        let has_faulted = received_status_events.iter().any(|e| {
            matches!(
                e,
                TurnEvent::StatusChanged {
                    status: TargetStatus::Faulted { .. }
                }
            )
        });
        let has_stopped = received_status_events.iter().any(|e| {
            matches!(
                e,
                TurnEvent::StatusChanged {
                    status: TargetStatus::Stopped
                }
            )
        });

        assert!(
            has_faulted || has_stopped,
            "Expected actor to transition to Faulted or Stopped status, but found: {:?}",
            received_status_events
        );

        // Confirm no panic by reaching here.
    });
}
