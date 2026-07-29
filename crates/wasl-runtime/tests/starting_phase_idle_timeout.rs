//! Regression test for a real bug found while implementing the actor's
//! `Starting`-phase handling (§7.1 row 582 / §7.4 row 612): a real driver's
//! `next_event()` legitimately returns `None` a few times (its own internal
//! idle-poll timeout — see `claude_stream.rs`/`opencode_acp.rs`'s `pull_one`
//! doc comments: a real `system/init` can land several seconds after that
//! poll interval) BEFORE the actual `SessionReady` arrives. The scripted
//! transport used by every other test in this crate cannot model this
//! (its `next_event()` either has an immediately-ready step, is genuinely
//! gated, or returns `None` forever once its queue is exhausted) — so this
//! needed a small hand-written `InteractiveTransport` double instead.
//!
//! Before the fix, ANY `None` returned by `next_event()` while the actor was
//! still `Starting` set `idle_drained = true`, and `idle_drained` is never
//! reset except by `start_turn` (which cannot run before `SessionReady`) —
//! so `pump_mode()` would park the actor on the mailbox FOREVER, structurally
//! unable to ever observe the real `SessionReady` that arrives moments later.
//! A target whose driver's first event legitimately takes longer than one
//! idle-poll interval would get stuck in `Starting` permanently.
mod support;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::runtime::Runtime;
use tokio::time::{timeout, Duration};

use support::*;

use wasl_runtime::{
    BusyPolicy, ComposedPrompt, EventSeq, InteractiveProtocol, InteractiveTransport, ManagedConfig,
    NativeSessionId, ProtocolEvent, RuntimeCapabilities, RuntimeError, SpawnOptions, TargetStatus,
    TurnEvent, TurnReceipt, WaslManagedRuntimeProvider,
};

/// A transport whose `next_event()` returns `None` a fixed number of times
/// (simulating a real driver's own idle-poll timeout elapsing with nothing
/// buffered yet) before finally producing `SessionReady`, then behaves like a
/// normal happy-path session.
struct DelayedReadyTransport {
    none_calls_remaining: u32,
    emitted_ready: bool,
    emitted_turn_accepted: bool,
    turn_submitted: bool,
}

#[async_trait]
impl InteractiveTransport for DelayedReadyTransport {
    async fn send_turn(&mut self, _prompt: &ComposedPrompt) -> Result<(), RuntimeError> {
        self.turn_submitted = true;
        Ok(())
    }

    async fn next_event(&mut self) -> Option<ProtocolEvent> {
        if self.none_calls_remaining > 0 {
            self.none_calls_remaining -= 1;
            // A REAL driver's `pull_one` would have genuinely awaited its
            // idle-poll duration here (never an instant/busy return); a short
            // real yield keeps this test both fast and non-busy-looping.
            tokio::time::sleep(Duration::from_millis(5)).await;
            return None;
        }
        if !self.emitted_ready {
            self.emitted_ready = true;
            return Some(ProtocolEvent::SessionReady {
                native: Some(NativeSessionId("native-delayed".to_string())),
            });
        }
        if self.turn_submitted && !self.emitted_turn_accepted {
            self.emitted_turn_accepted = true;
            return Some(ProtocolEvent::TurnAccepted { native_turn: None });
        }
        if self.turn_submitted && self.emitted_turn_accepted {
            return Some(ProtocolEvent::TurnCompleted {
                reply: "delayed-ready reply".to_string(),
            });
        }
        // Idle, ready, nothing submitted yet: behave like a normal quiescent
        // real driver (a real bounded idle-poll `None`).
        tokio::time::sleep(Duration::from_millis(5)).await;
        None
    }

    async fn interrupt(&mut self) -> Result<(), RuntimeError> {
        Ok(())
    }
}

struct DelayedReadyProtocol {
    none_calls_before_ready: u32,
    spawn_count: AtomicU32,
}

#[async_trait]
impl InteractiveProtocol for DelayedReadyProtocol {
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            persistent_process: true,
            persistent_conversation: true,
            structured_turns: true,
            streaming_output: false,
            attachable: true,
            interruptible: false,
            structured_provenance: true,
            resumable: false,
        }
    }

    async fn spawn(
        &self,
        _opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        self.spawn_count.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(DelayedReadyTransport {
            none_calls_remaining: self.none_calls_before_ready,
            emitted_ready: false,
            emitted_turn_accepted: false,
            turn_submitted: false,
        }))
    }

    async fn resume(
        &self,
        _native: &NativeSessionId,
        _opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        Err(RuntimeError::Unsupported {
            operation: "resume".to_string(),
        })
    }
}

#[test]
fn session_ready_is_observed_even_after_several_idle_poll_timeouts() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let clock = TestClock::new();
        let sink = sink_arc(RecordingSink::default());
        let protocol = Arc::new(DelayedReadyProtocol {
            // Several idle-poll `None`s before `SessionReady` — modeling a
            // real driver whose first event lands well past one poll
            // interval (documented, observed real behavior for claude).
            none_calls_before_ready: 6,
            spawn_count: AtomicU32::new(0),
        });
        let provider =
            WaslManagedRuntimeProvider::with_seams(protocol, ManagedConfig::default(), clock, sink);

        let target = provider
            .target_for(&key("ctx-delayed-ready"))
            .await
            .expect("target");

        let stream = target.events(EventSeq(0)).await;

        // Before the fix: the actor would park on the mailbox after the
        // FIRST idle `None` and never poll the transport again, so
        // `SessionReady` would never be journaled and this drain would hang
        // until the outer test timeout fired. Bounded here so a regression
        // fails fast and cleanly instead of hanging the suite.
        let events = timeout(Duration::from_secs(5), drain_items(stream, 1))
            .await
            .expect("SessionReady must be observed even after several idle-poll timeouts");
        let events = events_of(&events);
        assert!(
            matches!(events.first(), Some(TurnEvent::SessionReady { .. })),
            "expected SessionReady, got {:?}",
            events
        );

        // A submit AFTER SessionReady has been observed must start
        // immediately (the target is genuinely Idle by then), proving the
        // actor is not just occasionally polling but has correctly reached
        // steady state.
        let receipt = timeout(
            Duration::from_secs(5),
            target.submit_turn(consult("ctx-delayed-ready", "hello"), BusyPolicy::Reject),
        )
        .await
        .expect("submit_turn must not hang");
        assert!(
            matches!(receipt, TurnReceipt::Started { .. }),
            "expected Started once SessionReady has already been observed, got {:?}",
            receipt
        );

        let stream2 = target.events(EventSeq(1)).await;
        let final_events = timeout(Duration::from_secs(5), drain_items(stream2, 10))
            .await
            .expect("turn must complete, not hang");
        let final_events = events_of(&final_events);
        assert!(
            final_events
                .iter()
                .any(|e| matches!(e, TurnEvent::TurnCompleted { .. })),
            "expected TurnCompleted, got {:?}",
            final_events
        );
    });
}

#[test]
fn submit_during_the_delayed_starting_window_is_queued_then_promoted() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let clock = TestClock::new();
        let sink = sink_arc(RecordingSink::default());
        let protocol = Arc::new(DelayedReadyProtocol {
            none_calls_before_ready: 6,
            spawn_count: AtomicU32::new(0),
        });
        let provider =
            WaslManagedRuntimeProvider::with_seams(protocol, ManagedConfig::default(), clock, sink);

        let target = provider
            .target_for(&key("ctx-delayed-ready-2"))
            .await
            .expect("target");

        let stream = target.events(EventSeq(0)).await;

        // Submit immediately, racing the several idle-poll `None`s before
        // `SessionReady`. Per §7.1 row 582 this is queued (or, if the race
        // resolves the other way, started immediately) — either is
        // spec-correct; the key regression assertion is that this NEVER
        // hangs and the turn EVENTUALLY completes.
        let receipt = timeout(
            Duration::from_secs(5),
            target.submit_turn(consult("ctx-delayed-ready-2", "hello"), BusyPolicy::Reject),
        )
        .await
        .expect("submit_turn must not hang even while racing the delayed ready signal");
        assert!(
            matches!(
                receipt,
                TurnReceipt::Started { .. } | TurnReceipt::Queued { .. }
            ),
            "expected Started or Queued, got {:?}",
            receipt
        );

        let status = timeout(Duration::from_secs(5), async {
            loop {
                let status = target.status().await;
                if matches!(
                    status,
                    TargetStatus::Busy { .. } | TargetStatus::Idle { .. }
                ) {
                    return status;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("target must leave Starting status, not hang forever");
        assert!(
            matches!(
                status,
                TargetStatus::Busy { .. } | TargetStatus::Idle { .. }
            ),
            "expected the target to leave Starting, got {:?}",
            status
        );

        let final_events = timeout(Duration::from_secs(5), drain_items(stream, 20))
            .await
            .expect("events must be observed, not hang");
        let final_events = events_of(&final_events);
        assert!(
            final_events
                .iter()
                .any(|e| matches!(e, TurnEvent::TurnCompleted { .. })),
            "expected the turn to eventually complete, got {:?}",
            final_events
        );
    });
}
