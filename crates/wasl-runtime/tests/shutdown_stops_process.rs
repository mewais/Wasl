//! Hardening-2 §3.2 — `shutdown()` MUST actually stop the underlying process /
//! release the live transport handle; it must NOT merely flip the actor to a
//! terminal state while the actor task parks forever in `PumpNext::Wait` still
//! HOLDING `self.transport` (and therefore the live child) alive.
//!
//! Bug being pinned (hardening-plan §3.2, verified directly against the source):
//! `stop()` sets `self.terminal = true`, closes the journal, and emits `Stopped`
//! — but never touches `self.transport`. Because `pump_mode()` returns
//! `PumpNext::Wait` once terminal, the actor task then blocks on
//! `mailbox.recv().await` FOREVER, keeping the transport (the real CLI process)
//! alive until the LAST `ManagedTarget` handle is dropped elsewhere — which may
//! never happen promptly.
//!
//! Spec citations:
//!   * `runtime.md` §7.3 shutdown matrix — `Immediate` (any status): "kill
//!     transport; emit `ProcessExited`, `StatusChanged`"; `Drain` on Busy:
//!     finish the active turn, then "Stopped" — which per §6 (both real drivers'
//!     `Drop` kills+reaps the child) means the transport is RELEASED.
//!   * hardening-plan §3.2/§3.2-fix: make the transport droppable independent of
//!     the actor's own lifetime, `.take()`'d and dropped during `stop()`; an
//!     `Immediate` shutdown should end the actor task, not park forever.
//!
//! These tests use the shared scripted `InteractiveTransport` double. Dropping
//! that double is the in-memory analog of a real driver's `Drop` killing+reaping
//! the child (§6), observed via the `TransportShared::was_dropped()` flag added
//! to `tests/support`. A correct fix drops the transport during `shutdown()`;
//! the CURRENT (unfixed) actor never does, so every assertion below is RED today.
//!
//! Every wait is a bounded `tokio::time::timeout` so a regression (the transport
//! NEVER being released) fails FAST and cleanly rather than hanging the suite.

mod support;

use std::sync::Arc;
use std::time::Duration;

use wasl_runtime::{
    BusyPolicy, DeliveryTarget, EventSeq, ProtocolEvent, ShutdownMode, TargetStatus, TurnReceipt,
};

use support::{
    consult, drain_items, key, provider, sink_arc, tight_config, ProtocolScript, RecordingSink,
    ScriptHandle, ScriptedProtocol, TestClock,
};

/// A ready session that accepts a turn and then never completes it (busy
/// forever) — so the transport is genuinely live and held when we shut down.
fn busy_forever_script() -> ProtocolScript {
    ProtocolScript::new()
        .ready(Some("ses-1"))
        .emit(ProtocolEvent::TurnAccepted { native_turn: None })
}

/// Poll the shared transport handle until it reports the transport was dropped,
/// bounded by `bound`. Returns `true` iff the drop was observed within the
/// bound. This is a bounded hang-GUARD: a correct `shutdown()` releases the
/// transport well within `bound`; the current (unfixed) actor never does, so
/// this returns `false` after the bound elapses (a clean, fast RED — never a
/// hang).
async fn dropped_within(handle: &ScriptHandle, bound: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + bound;
    while tokio::time::Instant::now() < deadline {
        if handle.shared.was_dropped() {
            return true;
        }
        // Yield so the actor task can make progress on a current-thread runtime,
        // then a short sleep so we do not busy-spin the executor.
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    handle.shared.was_dropped()
}

// =====================================================================
// §3.2 (a) — shutdown(Immediate) on a BUSY target actually RELEASES the
// transport (drops the live handle), even while the ManagedTarget Arc + the
// provider are STILL alive. Today the transport is never dropped by `stop()`,
// so this is RED (the flag never flips within the bound).
// =====================================================================

#[tokio::test]
async fn immediate_shutdown_drops_the_underlying_transport() {
    let proto = ScriptedProtocol::new();
    let handle = proto.install("ctx-imm-drop", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-imm-drop")).await.expect("target");

    // Make the session genuinely busy: an accepted, never-completing turn keeps
    // the transport live and held by the actor.
    let started = target
        .submit_turn(consult("ctx-imm-drop", "go"), BusyPolicy::Reject)
        .await;
    assert!(
        matches!(started, TurnReceipt::Started { .. }),
        "the turn must Start (busy), got {started:?}"
    );

    assert!(
        !handle.shared.was_dropped(),
        "sanity: the transport is still held while the turn is active"
    );

    target
        .shutdown(ShutdownMode::Immediate)
        .await
        .expect("immediate shutdown");

    // The core assertion (§3.2 / §7.3 "kill transport"): the transport handle is
    // actually RELEASED by the shutdown, while `target` (a ManagedTarget Arc) and
    // `prov` are still alive in this scope. The CURRENT actor parks in
    // `PumpNext::Wait` still holding it → this stays false → RED.
    assert!(
        dropped_within(&handle, Duration::from_secs(2)).await,
        "shutdown(Immediate) must actually stop the underlying process — the \
         transport handle must be dropped/released (§7.3 kill transport; §3.2), \
         NOT held alive while the actor task parks forever in PumpNext::Wait"
    );

    // And the public status reflects the stop.
    assert_eq!(
        target.status().await,
        TargetStatus::Stopped,
        "Immediate shutdown ⇒ Stopped (§7.3)"
    );
}

// =====================================================================
// §3.2 (b) — the actor task must TERMINATE promptly after an Immediate shutdown
// (not park forever in PumpNext::Wait). Observed from outside: even on an IDLE
// target — where there is no active turn to end — the transport is released
// (the task dropped it) while the target Arc is still held. A task that parks
// forever holds the transport forever; a task that exits drops it. RED today.
// =====================================================================

#[tokio::test]
async fn immediate_shutdown_releases_the_transport_even_on_an_idle_target() {
    let proto = ScriptedProtocol::new();
    // Idle-but-ready: SessionReady, no turn — the target parks Idle.
    let handle = proto.install("ctx-imm-task", ProtocolScript::new().ready(Some("ses-1")));
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-imm-task")).await.expect("target");

    // Reach Idle (drain the SessionReady lifecycle).
    let s0 = target.events(EventSeq(0)).await;
    let _ = drain_items(s0, 8).await;

    target
        .shutdown(ShutdownMode::Immediate)
        .await
        .expect("immediate shutdown of idle target");

    // Keep the target Arc + provider alive (this scope) and assert the actor
    // task released the transport anyway — i.e. it did not merely go terminal
    // and park in PumpNext::Wait forever while still owning the transport (§3.2:
    // "decide explicitly whether the actor task itself should exit promptly
    // after an Immediate shutdown rather than parking forever").
    assert!(
        dropped_within(&handle, Duration::from_secs(2)).await,
        "after Immediate shutdown the actor task must not park forever holding \
         the transport — it must release it promptly (§3.2)"
    );
}

// =====================================================================
// §3.2 / §7.3 — Drain mode on a BUSY target still eventually STOPS the process:
// once the active turn's terminal event arrives the transport must be released.
// Today `stop()` never touches the transport in the Drain path either → RED.
// =====================================================================

#[tokio::test]
async fn drain_shutdown_on_busy_target_releases_transport_after_turn_finishes() {
    let proto = ScriptedProtocol::new();
    let handle = proto.install("ctx-drain-drop", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov
        .target_for(&key("ctx-drain-drop"))
        .await
        .expect("target");

    let started = target
        .submit_turn(consult("ctx-drain-drop", "go"), BusyPolicy::Reject)
        .await;
    assert!(
        matches!(started, TurnReceipt::Started { .. }),
        "the turn must Start (busy), got {started:?}"
    );

    // Begin draining while the turn is still active — Drain must finish the
    // active turn first, so the transport is NOT yet released here.
    target
        .shutdown(ShutdownMode::Drain)
        .await
        .expect("drain shutdown");
    assert!(
        !handle.shared.was_dropped(),
        "Drain must let the active turn finish first — the transport is still \
         live before the turn's terminal event (§7.3 Drain/Busy row)"
    );

    // Now let the active turn complete: the drain finishes and the target Stops,
    // which per §6/§7.3 must RELEASE the transport (stop the process).
    handle.push_event(ProtocolEvent::TurnCompleted {
        reply: "done".into(),
    });

    assert!(
        dropped_within(&handle, Duration::from_secs(2)).await,
        "Drain on a Busy target must eventually STOP the process once the active \
         turn finishes — the transport must be released (§7.3 Drain/Busy → \
         Stopped; §3.2), got status {:?}",
        target.status().await
    );
    assert_eq!(
        target.status().await,
        TargetStatus::Stopped,
        "a drained busy target ends Stopped (§7.3)"
    );
}
