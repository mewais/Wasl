//! Phase-2A coverage-closing suite — assorted spec-required edge arms of the
//! actor + provider that the primary behavioral suites don't drive: drain/exit
//! queue-expiry, a queued turn whose injection FAILS on pop, the `refresh_status`
//! non-Busy phase arm (enqueue while AwaitingGate), stray/duplicate terminal
//! events being IGNORED (§7.4 last row), and the provider's `Debug` +
//! `capabilities()` + concurrent get-or-create forwards (§10).
//!
//! All behavioral, driven through the `support::ScriptedProtocol` transport.
//!
//! Spec citations (docs/specs/runtime.md):
//!   * Drain expires the queue then stops — §7.3 (Drain/Busy row, line 604).
//!   * ProcessExited expires queued turns — §7.4 (line 620).
//!   * a queued turn that fails to inject on pop emits its terminal + advances —
//!     §7.4 + §8.1 (FailedBeforeInjection).
//!   * AwaitingGate counts as busy; a queued turn behind a gate — §4.1/§7.1.
//!   * stray events referencing a completed turn are IGNORED — §7.4 (line 622).
//!   * RuntimeProvider get-or-create idempotency + Debug supertrait — §10, R1
//!     AGENTS.md (Debug required to `{:?}` a `Result<Arc<dyn DeliveryTarget>, _>`).

mod support;

use std::sync::Arc;
use std::time::Duration;

use wasl_runtime::{
    BusyPolicy, DeliveryCertainty, DeliveryTarget, EventSeq, NativeGateDetail, ProtocolEvent,
    RuntimeError, ShutdownMode, TargetStatus, TurnEvent, TurnReceipt,
};

use support::{
    consult, drain_items, events_of, key, provider, sink_arc, tight_config, ProtocolScript,
    RecordingSink, ScriptedProtocol, TestClock,
};

fn busy_forever_script() -> ProtocolScript {
    ProtocolScript::new()
        .ready(Some("ses-1"))
        .emit(ProtocolEvent::TurnAccepted { native_turn: None })
}

// =====================================================================
// actor 662-665 — Drain on a BUSY target WITH a queued turn: when the active
// turn finishes, the drain branch expires the queue (TurnExpired) then stops.
// §7.3 (Drain/Busy: "finish active … then drop queue as TurnExpired, then
// Stopped").
// =====================================================================

#[tokio::test]
async fn drain_with_queued_turn_expires_the_queue_then_stops() {
    let proto = ScriptedProtocol::new();
    let handle = proto.install("ctx-drain-q", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-drain-q")).await.expect("target");

    let active = target
        .submit_turn(consult("ctx-drain-q", "active"), BusyPolicy::Reject)
        .await;
    assert!(matches!(active, TurnReceipt::Started { .. }));
    let queued = target
        .submit_turn(
            consult("ctx-drain-q", "queued"),
            BusyPolicy::Queue {
                max_wait: Duration::from_millis(120_000),
            },
        )
        .await;
    let queued_id = match queued {
        TurnReceipt::Queued { turn_id, .. } => turn_id,
        other => panic!("expected Queued, got {other:?}"),
    };

    // Subscribe (live receiver captures everything from here) BEFORE draining.
    let stream = target.events(EventSeq(0)).await;
    target
        .shutdown(ShutdownMode::Drain)
        .await
        .expect("drain begins");
    // Finish the active turn so the drain branch runs (expire queue, then stop).
    handle.push_event(ProtocolEvent::TurnCompleted {
        reply: "active done".into(),
    });
    // A throwaway subscribe pokes the actor to pump the pushed completion (the
    // first subscribe's poke was already consumed before the push).
    drop(target.events(EventSeq(0)).await);

    let items = drain_items(stream, 64).await;
    let evs = events_of(&items);
    assert!(
        evs.iter().any(|e| matches!(
            e, TurnEvent::TurnExpired { turn_id: t } if *t == queued_id
        )),
        "Drain expires the queued turn once the active one finishes (§7.3), got {evs:?}"
    );
    assert!(
        evs.iter()
            .any(|e| matches!(e, TurnEvent::ProcessExited { .. })),
        "after draining, the target stops (ProcessExited), got {evs:?}"
    );
    assert_eq!(target.status().await, TargetStatus::Stopped);
}

// =====================================================================
// actor 640-644 — ProcessExited while BUSY with a QUEUED turn expires the
// queued turns. §7.4 (line 620 "queued turns → TurnExpired").
// =====================================================================

#[tokio::test]
async fn process_exit_expires_queued_turns() {
    let proto = ScriptedProtocol::new();
    // busy_forever (ready + TurnAccepted), so both submits are admitted while the
    // actor is parked (not blocked in next_event); the crash is PUSHED after both
    // turns are in place, then a poke pumps it.
    let handle = proto.install("ctx-exit-q", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-exit-q")).await.expect("target");

    let _active = target
        .submit_turn(consult("ctx-exit-q", "active"), BusyPolicy::Reject)
        .await;
    let queued = target
        .submit_turn(
            consult("ctx-exit-q", "queued"),
            BusyPolicy::Queue {
                max_wait: Duration::from_millis(120_000),
            },
        )
        .await;
    let queued_id = match queued {
        TurnReceipt::Queued { turn_id, .. } => turn_id,
        other => panic!("expected Queued, got {other:?}"),
    };

    // Subscribe (live) BEFORE the crash so the broadcast captures the expiry,
    // then push the process exit and poke so the actor pumps it. The crash
    // closes the broadcast ⇒ the drain returns fast.
    let stream = target.events(EventSeq(0)).await;
    handle.push_event(ProtocolEvent::ProcessExited { code: Some(9) });
    drop(target.events(EventSeq(0)).await);
    let items = drain_items(stream, 64).await;
    let evs = events_of(&items);
    assert!(
        evs.iter()
            .any(|e| matches!(e, TurnEvent::ProcessExited { .. })),
        "a mid-turn crash surfaces ProcessExited (§7.4), got {evs:?}"
    );
    assert!(
        evs.iter().any(|e| matches!(
            e, TurnEvent::TurnExpired { turn_id: t } if *t == queued_id
        )),
        "a mid-turn ProcessExited expires the queued turns (§7.4), got {evs:?}"
    );
}

// =====================================================================
// actor 691-701 — a QUEUED turn whose injection FAILS on pop emits its terminal
// failure (FailedBeforeInjection) and the actor advances. Here the transport's
// NEXT send_turn (the queued turn's injection) is armed to fail. §7.4 + §8.1.
// =====================================================================

#[tokio::test]
async fn queued_turn_injection_failure_on_pop_emits_terminal_and_advances() {
    let proto = ScriptedProtocol::new();
    // Script: ready, TurnAccepted (active injected ok), then FailNextSend arms
    // the transport so the NEXT send_turn (the popped queued turn) fails.
    let handle = proto.install(
        "ctx-qfail",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .fail_next_send(RuntimeError::Transport {
                detail: "queued injection broke".into(),
            }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov.target_for(&key("ctx-qfail")).await.expect("target");

    let active = target
        .submit_turn(consult("ctx-qfail", "active"), BusyPolicy::Reject)
        .await;
    assert!(matches!(active, TurnReceipt::Started { .. }));
    let queued = target
        .submit_turn(
            consult("ctx-qfail", "queued"),
            BusyPolicy::Queue {
                max_wait: Duration::from_millis(120_000),
            },
        )
        .await;
    let queued_id = match queued {
        TurnReceipt::Queued { turn_id, .. } => turn_id,
        other => panic!("expected Queued, got {other:?}"),
    };

    // Let the FailNextSend step be pumped (it arms the failure) via a drained
    // subscribe, then subscribe the asserting (live) stream, complete the active
    // turn, and poke so the queued turn is popped and its injection fails.
    let s0 = target.events(EventSeq(0)).await;
    let _ = drain_items(s0, 6).await;
    let stream = target.events(EventSeq(0)).await;
    handle.push_event(ProtocolEvent::TurnCompleted {
        reply: "active done".into(),
    });
    drop(target.events(EventSeq(0)).await);
    let items = drain_items(stream, 64).await;
    let evs = events_of(&items);
    // The popped queued turn is activated (TurnAccepted) then fails to inject
    // ⇒ TurnFailed{FailedBeforeInjection}. The actor then advances (→ Idle).
    let queued_failed = evs.iter().find_map(|e| match e {
        TurnEvent::TurnFailed {
            turn_id: t,
            certainty,
            ..
        } if *t == queued_id => Some(*certainty),
        _ => None,
    });
    assert_eq!(
        queued_failed,
        Some(DeliveryCertainty::FailedBeforeInjection),
        "a queued turn whose injection fails on pop ⇒ TurnFailed{{FailedBeforeInjection}} \
         (§7.4/§8.1), got {evs:?}"
    );
    // After the failed pop the queue is empty ⇒ the target returns to Idle.
    assert!(
        matches!(target.status().await, TargetStatus::Idle { .. }),
        "after the queued turn's injection failure the actor advances to Idle, got {:?}",
        target.status().await
    );
}

// =====================================================================
// actor 926-931 — `refresh_status` non-Busy phase arm: enqueue a turn while the
// active turn is AwaitingGate. Enqueue calls refresh_status; the current status
// is AwaitingGate (not Busy), so the `_ => Generating` phase-default arm runs.
// =====================================================================

#[tokio::test]
async fn enqueue_while_awaiting_gate_refreshes_status_via_default_phase_arm() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-gate-enq",
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
        prov.target_for(&key("ctx-gate-enq")).await.expect("target");

    let _started = target
        .submit_turn(consult("ctx-gate-enq", "go"), BusyPolicy::Reject)
        .await;
    // Let the gate surface (status becomes AwaitingGate).
    let s0 = target.events(EventSeq(0)).await;
    let _ = drain_items(s0, 8).await;
    assert!(
        matches!(target.status().await, TargetStatus::AwaitingGate { .. }),
        "the active turn is AwaitingGate, got {:?}",
        target.status().await
    );

    // Enqueue a turn behind the gated active turn (Queue policy). Enqueue calls
    // refresh_status while status is AwaitingGate ⇒ the `_ => Generating` arm.
    let queued = target
        .submit_turn(
            consult("ctx-gate-enq", "queued"),
            BusyPolicy::Queue {
                max_wait: Duration::from_millis(120_000),
            },
        )
        .await;
    assert!(
        matches!(queued, TurnReceipt::Queued { position: 0, .. }),
        "a turn behind an AwaitingGate active turn queues (AwaitingGate is busy), got {queued:?}"
    );
    // `enqueue` calls `refresh_status`, whose phase-match sees the current
    // AwaitingGate status (not Busy) and takes the `_ => Generating` default arm
    // (actor.rs ~930), recomputing a Busy{Generating, queued:1} snapshot. Assert
    // that default-arm outcome (the queued depth is now 1).
    match target.status().await {
        TargetStatus::Busy { queued, .. } => assert_eq!(
            queued, 1,
            "refresh_status on enqueue recomputes the queued depth via the default phase arm"
        ),
        other => panic!("expected a Busy snapshot after enqueue-refresh, got {other:?}"),
    }
}

// =====================================================================
// actor 583-585 / 617-619 — stray/duplicate terminal events referencing an
// already-completed turn are IGNORED (no re-emit, no status change). §7.4 last
// row (line 622). A second TurnCompleted and a post-completion TurnFailed both
// hit the `active.is_none()` guards.
// =====================================================================

#[tokio::test]
async fn duplicate_and_stray_terminal_events_are_ignored() {
    let proto = ScriptedProtocol::new();
    // ready, accept, complete, then a DUPLICATE complete and a stray TurnFailed —
    // both after the turn is already done (active is None).
    proto.install(
        "ctx-stray",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .emit(ProtocolEvent::TurnCompleted {
                reply: "first".into(),
            })
            .emit(ProtocolEvent::TurnCompleted {
                reply: "duplicate".into(),
            })
            .emit(ProtocolEvent::TurnFailed {
                error: "stray failure".into(),
            }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov.target_for(&key("ctx-stray")).await.expect("target");

    let started = target
        .submit_turn(consult("ctx-stray", "go"), BusyPolicy::Reject)
        .await;
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);

    // Exactly ONE terminal event for the turn — the duplicate completion and the
    // stray failure are ignored (§7.4 last row), never re-emitted.
    let terminals = evs
        .iter()
        .filter(|e| {
            matches!(
                e,
                TurnEvent::TurnCompleted { turn_id: t, .. } | TurnEvent::TurnFailed { turn_id: t, .. }
                    if *t == turn_id
            )
        })
        .count();
    assert_eq!(
        terminals, 1,
        "a completed turn must have exactly ONE terminal event; the duplicate completion \
         and stray failure are IGNORED (§7.4 line 622), got {evs:?}"
    );
    // The one terminal is the FIRST completion, not the duplicate's reply.
    assert!(
        evs.iter().any(|e| matches!(
            e, TurnEvent::TurnCompleted { turn_id: t, reply, .. } if *t == turn_id && reply == "first"
        )),
        "the surviving terminal is the first completion (§7.4), got {evs:?}"
    );
    // The session did not fault — after completion it is Idle (stray events did
    // not drive a spurious transition).
    assert!(
        matches!(target.status().await, TargetStatus::Idle { .. }),
        "stray post-completion events leave the target Idle, got {:?}",
        target.status().await
    );
}

// =====================================================================
// actor 269-282 — `drain_pending` terminal-break: a turn-lifecycle event pumped
// while IDLE (before its owning turn is submitted) is PARKED, then replayed into
// the now-active turn on submit; if a replayed event TERMINATES the turn, the
// replay loop breaks (actor.rs ~277-281) rather than replaying into a gone turn.
// Here a lone TurnCompleted is parked, then a submit activates a turn which the
// replayed completion immediately finishes ⇒ the drain_pending break arm runs.
// §7.4 (pre-queued script for the next turn).
// =====================================================================

#[tokio::test]
async fn parked_terminal_event_replayed_on_submit_hits_drain_pending_break() {
    let proto = ScriptedProtocol::new();
    // ready then a lone TurnCompleted (no TurnAccepted): pumped while idle+ready,
    // the completion is parked as the pre-queued script for the next turn.
    proto.install(
        "ctx-park",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnCompleted {
                reply: "parked-done".into(),
            }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov.target_for(&key("ctx-park")).await.expect("target");

    // Poke the actor (via a subscribe) so it pumps `ready` then parks the lone
    // TurnCompleted BEFORE we submit.
    let s0 = target.events(EventSeq(0)).await;
    let _ = drain_items(s0, 4).await;

    // Now submit a turn: start_turn activates it, drain_pending replays the parked
    // TurnCompleted which terminates it ⇒ the `active.is_none()` break runs.
    let started = target
        .submit_turn(consult("ctx-park", "go"), BusyPolicy::Reject)
        .await;
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);
    // The submitted turn is completed by the replayed parked completion.
    assert!(
        evs.iter().any(|e| matches!(
            e, TurnEvent::TurnCompleted { turn_id: t, reply, .. }
                if *t == turn_id && reply == "parked-done"
        )),
        "the parked completion is replayed into the newly-active turn (§7.4), got {evs:?}"
    );
    // After the replayed completion terminated the turn, the actor is Idle.
    assert!(
        matches!(target.status().await, TargetStatus::Idle { .. }),
        "after the parked terminal replay the target is Idle, got {:?}",
        target.status().await
    );
}

// =====================================================================
// provider 62-68 / 84-86 — the `Debug` supertrait forward and `capabilities()`
// on the managed target (§10 / R1 AGENTS.md Debug requirement).
// =====================================================================

#[tokio::test]
async fn managed_target_debug_and_capabilities_forward() {
    let proto = ScriptedProtocol::new();
    proto.install("ctx-dbg", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov.target_for(&key("ctx-dbg")).await.expect("target");

    // Debug forward (the `Debug for ManagedTarget` impl): formatting the trait
    // object must not panic and must mention the target id.
    let dbg = format!("{target:?}");
    assert!(
        dbg.contains("ManagedTarget") && dbg.contains("ctx-dbg"),
        "Debug for the managed target renders its id (§10 handle), got {dbg:?}"
    );

    // capabilities() forward: the scripted driver's report is surfaced verbatim.
    let caps = target.capabilities();
    assert!(
        caps.persistent_process && caps.interruptible && caps.resumable,
        "the managed target forwards the driver's capability report (§10), got {caps:?}"
    );
}

// =====================================================================
// provider 285-292 — concurrent get-or-create for the SAME key: two racing
// target_for calls must return the SAME live target (the re-check-under-lock
// branch), never two. §10 (idempotent get-or-create).
// =====================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_target_for_same_key_returns_one_target() {
    use tokio::sync::Barrier;

    let proto = ScriptedProtocol::new();
    // Install MANY scripts so every racing creator that reaches `create_target`
    // (before the lock re-check discards all but one) has a transport to spawn.
    let racers = 8usize;
    for i in 0..racers {
        proto.install(&format!("ctx-getcreate-race-{i}"), busy_forever_script());
    }
    // Make spawn yield so concurrent creators overlap the provider's initial
    // "not present" check, deterministically forcing the re-check-under-lock arm.
    proto.enable_slow_spawn();
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = Arc::new(provider(proto, tight_config(), clock, sink));

    // Many racers hit `target_for(SAME key)` simultaneously; several pass the
    // initial "not present" check and call `create_target` concurrently, so all
    // but the first hit the re-check-under-lock branch (provider.rs ~288-289).
    let barrier = Arc::new(Barrier::new(racers));
    let mut handles = Vec::new();
    for _ in 0..racers {
        let p = prov.clone();
        let b = barrier.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            p.target_for(&key("ctx-getcreate-race")).await
        }));
    }
    let mut ids = Vec::new();
    for h in handles {
        let t = h.await.expect("create task").expect("target");
        ids.push(t.target_id().clone());
    }
    // Every racer got the SAME live target (idempotent get-or-create §10).
    for id in &ids {
        assert_eq!(
            *id, ids[0],
            "concurrent get-or-create for one key returns the SAME target (§10), got {ids:?}"
        );
    }
    // And the registry holds exactly one target for that key.
    let listed = prov.list_targets().await;
    let count = listed
        .iter()
        .filter(|id| id.0 == "ctx-getcreate-race")
        .count();
    assert_eq!(
        count, 1,
        "an idempotently-created target appears once (§10), got {listed:?}"
    );
}
