//! Phase-2A coverage-closing suite — the §7.4-line-622 hostile-input hardening:
//! a STRAY / duplicate terminal `ProtocolEvent` (`TurnCompleted` / `TurnFailed`)
//! referencing NO active turn must be silently IGNORED.
//!
//! Behavioral; drives spec-required, IMPLEMENTED-but-previously-undriven actor
//! paths (`complete_active`'s `None => return` guard, `fail_active`'s
//! `is_none() { return }` guard) through the `support::ScriptedProtocol` scripted
//! transport by emitting the terminal event BEFORE `SessionReady` — i.e. before
//! any turn is (or can be) active. At that point `on_pumped_event` does NOT park
//! the event (it only parks non-lifecycle events once `ready`), so the stray
//! reaches `on_protocol_event` → `complete_active` / `fail_active` with
//! `active == None`, exercising the hostile-input drop.
//!
//! Spec citations (docs/specs/runtime.md):
//!   * §7.4 line 622 — "hostile-input hardening, R2 2B scripts": a stray /
//!     duplicate terminal event referencing no active turn is IGNORED (never a
//!     panic, never a spurious public terminal event, no status corruption).
//!
//! ASSERTIONS (both tests):
//!   1. No spurious public `TurnEvent::TurnCompleted` / `TurnEvent::TurnFailed`
//!      is emitted on the live stream for the stray.
//!   2. The target status is unaffected — it becomes / stays `Idle` (or `Ready`
//!      pre-turn), never `Faulted`, and the actor does not panic.
//!   3. The actor continues to function normally afterward: a subsequent REAL
//!      `submit_turn` still starts, injects, and completes.

mod support;

use std::sync::Arc;

use wasl_runtime::{
    BusyPolicy, DeliveryTarget, EventSeq, ProtocolEvent, TargetStatus, TurnEvent, TurnReceipt,
};

use support::{
    consult, drain_items, events_of, key, provider, sink_arc, tight_config, ProtocolScript,
    RecordingSink, ScriptedProtocol, TestClock,
};

// =====================================================================
// Stray TurnCompleted with NO active turn (pre-SessionReady) is IGNORED —
// `complete_active`'s `None => return` guard (§7.4 line 622).
// =====================================================================

#[tokio::test]
async fn stray_turn_completed_with_no_active_turn_is_silently_ignored() {
    let proto = ScriptedProtocol::new();
    // The transport misbehaves: it emits a `TurnCompleted` FIRST, before any
    // `SessionReady` and before any turn is submitted. With `active == None` and
    // `ready == false` this is NOT parked — it reaches `complete_active`, whose
    // `None => return` guard must silently drop it (§7.4 line 622). Only AFTER
    // the stray does the session go ready, so a real turn can then run.
    let handle = proto.install(
        "ctx-stray-completed",
        ProtocolScript::new().emit(ProtocolEvent::TurnCompleted {
            reply: "phantom reply for a turn that never started".into(),
        }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov
        .target_for(&key("ctx-stray-completed"))
        .await
        .expect("target");

    // Drain the live stream: the stray must produce NO public terminal event.
    // (Subscribing sends a `Poke` that re-polls the transport, so the stray is
    // pumped; the drain's hang-guard returns once the transport is quiescent.)
    let s0 = target.events(EventSeq(0)).await;
    let pre = drain_items(s0, 8).await;
    let pre_evs = events_of(&pre);
    assert!(
        !pre_evs
            .iter()
            .any(|e| matches!(e, TurnEvent::TurnCompleted { .. })),
        "a stray pre-ready TurnCompleted must NOT surface a public TurnCompleted \
         (§7.4 line 622 — silently ignored), got {pre_evs:?}"
    );

    // Status is unaffected: the actor did not fault or panic on the stray.
    assert!(
        !matches!(target.status().await, TargetStatus::Faulted { .. }),
        "a stray terminal event must not fault the target (§7.4), got {:?}",
        target.status().await
    );

    // The actor still functions: complete the session's script with a real,
    // properly-lifecycled turn and prove it starts, injects, and completes.
    handle.push_event(ProtocolEvent::SessionReady {
        native: Some(wasl_runtime::NativeSessionId("ses-1".into())),
    });
    handle.push_event(ProtocolEvent::TurnAccepted { native_turn: None });
    handle.push_event(ProtocolEvent::TurnCompleted {
        reply: "real reply".into(),
    });

    let started = target
        .submit_turn(consult("ctx-stray-completed", "go"), BusyPolicy::Reject)
        .await;
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!(
            "after ignoring a stray completion the actor must still admit a real turn, \
             got {other:?}"
        ),
    };

    let s1 = target.events(EventSeq(0)).await;
    let items = drain_items(s1, 32).await;
    let evs = events_of(&items);
    assert!(
        evs.iter().any(|e| matches!(
            e, TurnEvent::TurnCompleted { turn_id: t, reply, .. }
                if *t == turn_id && reply == "real reply"
        )),
        "the REAL turn completes normally after the stray was ignored, got {evs:?}"
    );
    // Exactly ONE TurnCompleted total (the real one) — the stray added none.
    assert_eq!(
        evs.iter()
            .filter(|e| matches!(e, TurnEvent::TurnCompleted { .. }))
            .count(),
        1,
        "exactly one TurnCompleted (the real turn); the stray contributed none, got {evs:?}"
    );
    // Back to Idle after the real turn: a clean, functioning actor.
    assert!(
        matches!(target.status().await, TargetStatus::Idle { .. }),
        "after the real turn completes the target is Idle (§7.4), got {:?}",
        target.status().await
    );
}

// =====================================================================
// Stray TurnFailed with NO active turn (pre-SessionReady) is IGNORED —
// `fail_active`'s `is_none() { return }` guard (§7.4 line 622).
// =====================================================================

#[tokio::test]
async fn stray_turn_failed_with_no_active_turn_is_silently_ignored() {
    let proto = ScriptedProtocol::new();
    // The transport misbehaves symmetrically: a `TurnFailed` FIRST, before
    // `SessionReady` and before any submit. With `active == None` this reaches
    // `fail_active`, whose `if self.active.is_none() { return }` guard must
    // silently drop it (§7.4 line 622) — never emitting a public TurnFailed and
    // never taking the `emit_failed_terminal` path.
    let handle = proto.install(
        "ctx-stray-failed",
        ProtocolScript::new().emit(ProtocolEvent::TurnFailed {
            error: "phantom failure for a turn that never started".into(),
        }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov
        .target_for(&key("ctx-stray-failed"))
        .await
        .expect("target");

    // Drain the live stream: the stray must produce NO public terminal event.
    let s0 = target.events(EventSeq(0)).await;
    let pre = drain_items(s0, 8).await;
    let pre_evs = events_of(&pre);
    assert!(
        !pre_evs
            .iter()
            .any(|e| matches!(e, TurnEvent::TurnFailed { .. })),
        "a stray pre-ready TurnFailed must NOT surface a public TurnFailed \
         (§7.4 line 622 — silently ignored), got {pre_evs:?}"
    );

    // Status is unaffected: the actor did not fault or panic on the stray.
    assert!(
        !matches!(target.status().await, TargetStatus::Faulted { .. }),
        "a stray terminal event must not fault the target (§7.4), got {:?}",
        target.status().await
    );

    // The actor still functions: run a real, properly-lifecycled turn.
    handle.push_event(ProtocolEvent::SessionReady {
        native: Some(wasl_runtime::NativeSessionId("ses-1".into())),
    });
    handle.push_event(ProtocolEvent::TurnAccepted { native_turn: None });
    handle.push_event(ProtocolEvent::TurnCompleted {
        reply: "real reply".into(),
    });

    let started = target
        .submit_turn(consult("ctx-stray-failed", "go"), BusyPolicy::Reject)
        .await;
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!(
            "after ignoring a stray failure the actor must still admit a real turn, \
             got {other:?}"
        ),
    };

    let s1 = target.events(EventSeq(0)).await;
    let items = drain_items(s1, 32).await;
    let evs = events_of(&items);
    assert!(
        evs.iter().any(|e| matches!(
            e, TurnEvent::TurnCompleted { turn_id: t, reply, .. }
                if *t == turn_id && reply == "real reply"
        )),
        "the REAL turn completes normally after the stray was ignored, got {evs:?}"
    );
    // The stray contributed NO public TurnFailed across the whole session.
    assert_eq!(
        evs.iter()
            .filter(|e| matches!(e, TurnEvent::TurnFailed { .. }))
            .count(),
        0,
        "the stray TurnFailed contributed no public TurnFailed, got {evs:?}"
    );
    // Back to Idle after the real turn: a clean, functioning actor.
    assert!(
        matches!(target.status().await, TargetStatus::Idle { .. }),
        "after the real turn completes the target is Idle (§7.4), got {:?}",
        target.status().await
    );
}
