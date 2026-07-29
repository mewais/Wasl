//! Phase-2B item 4 — `hold`: a deterministic block-until-released turn, used to
//! test queueing / busy-state WITHOUT timing flakiness (no sleeps for logic).
//!
//! Per `docs/specs/runtime-protocol.md` §1.1, `hold` lets the fake block a turn
//! deterministically until a `release` line arrives — the fake's answer to the
//! scripted `ScriptStep::Gated`/`Notify` mechanism R1's in-memory double used,
//! but here across a REAL process's real stdin.
//!
//! Two layers, per the task's "check the spec" instruction:
//!   * PRIMARY (driver level): drive `hold`/`release` directly on the
//!     `InteractiveTransport`, asserting the turn does NOT complete until
//!     released — proving deterministic block/release with no sleep. This is the
//!     protocol-driver-only property Phase-2B owns.
//!   * GENERICITY (actor integration): run the SAME `FakeLineProtocol` through
//!     the Phase-2A `WaslManagedRuntimeProvider` and assert a second submit
//!     QUEUES behind a held turn (`TurnReceipt::Queued`) and only starts after
//!     `release` — proving the generic single-writer actor drives a REAL process
//!     driver identically to the in-memory double (runtime.md §14 genericity).
//!
//! EXPECTED RED: FakeLineProtocol + the fake's `hold`/`release` modes do not
//! exist; and the actor-integration test additionally needs the SpawnOptions
//! target-key seam (flagged below).

mod support;

use std::time::Duration;

use support::*;

use wasl_runtime::{ComposedPrompt, InteractiveProtocol, ProtocolEvent, SpawnOptions};
use wasl_runtime_protocol::FakeLineProtocol;

fn driver() -> FakeLineProtocol {
    FakeLineProtocol::new(fake_agent_bin())
}

fn prompt(line: &str) -> ComposedPrompt {
    ComposedPrompt(line.to_string())
}

// =====================================================================
// PRIMARY (driver level) — hold blocks the turn deterministically; release
// completes it. No sleep drives the logic: the ONLY bounded wait is a short
// "must NOT have completed yet" negative check.
// =====================================================================

#[tokio::test]
async fn hold_blocks_the_turn_until_release_deterministically() {
    let proto = driver();
    let mut tx = proto
        .spawn(&SpawnOptions::default())
        .await
        .expect("spawn fake agent");
    let _ = next_or_hang(&mut *tx, "ready").await.expect("ready");

    // Start a held turn. The fake may emit `accepted` (busy) but MUST NOT emit a
    // terminal until released.
    tx.send_turn(&prompt(&req_hold())).await.expect("hold");

    // Read any non-terminal events (e.g. accepted) with a SHORT bound, and assert
    // NO terminal arrives while held. A short negative-wait is legitimate here:
    // proving the ABSENCE of an event over a bounded window is the only way to
    // assert "still blocked", and it is kept short (300ms).
    let mut pre = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(50), tx.next_event()).await {
            Ok(Some(ev)) => {
                assert!(
                    !matches!(
                        ev,
                        ProtocolEvent::TurnCompleted { .. } | ProtocolEvent::TurnFailed { .. }
                    ),
                    "a held turn must NOT complete before release (§1.1), got {ev:?}"
                );
                pre.push(ev);
            }
            Ok(None) => panic!("stream ended while turn was held"),
            Err(_) => {} // no event in this slice — expected while held
        }
    }

    // Release — NOW the turn completes deterministically.
    tx.send_turn(&prompt(&req_release("released-reply")))
        .await
        .expect("release");
    let evs = drain_to_terminal(&mut *tx, "after release").await;
    assert_eq!(
        completion_of(&evs),
        Some("released-reply".to_string()),
        "release completes the held turn with its reply (§1.1): {evs:?}"
    );
}

#[tokio::test]
async fn a_held_turn_reports_accepted_then_stays_busy() {
    // The `hold` proves the BUSY window: after send_turn the turn is accepted
    // (busy/generating) but not completed — the deterministic busy state a
    // queueing test needs, with no reliance on wall-clock timing.
    let proto = driver();
    let mut tx = proto
        .spawn(&SpawnOptions::default())
        .await
        .expect("spawn fake agent");
    let _ = next_or_hang(&mut *tx, "ready").await.expect("ready");

    tx.send_turn(&prompt(&req_hold())).await.expect("hold");

    // The first event of a held turn should be the accept (busy), never a
    // terminal.
    let ev = next_or_hang(&mut *tx, "held accept")
        .await
        .expect("an event");
    assert!(
        is_accepted(&ev),
        "a held turn is accepted (busy) first, got {ev:?}"
    );

    // Release to let the process finish cleanly (so the child can be reaped).
    tx.send_turn(&prompt(&req_release("done")))
        .await
        .expect("release");
    let _ = drain_to_terminal(&mut *tx, "cleanup release").await;
}

// =====================================================================
// GENERICITY (actor integration) — the SAME real driver through the Phase-2A
// generic provider. A second submit while a `hold` turn is active must QUEUE
// (TurnReceipt::Queued), proving the generic single-writer actor + FIFO queue
// drive a REAL process driver exactly as the scripted in-memory double did.
//
// CODER MUST CLOSE (Phase-2B, src/**): the R1 `SpawnOptions` fields are
// crate-private and carry no target key today (runtime.md §6 documents a
// `target_key` field the driver needs). For `WaslManagedRuntimeProvider` to
// spawn a per-key `FakeLineProtocol` transport, the driver's `spawn` must be
// reachable via the provider's seam. This test drives the provider's PUBLIC API
// (`with_seams` + `target_for` + `submit_turn`) exactly as R1's own Phase-2A
// suite does; if the SpawnOptions target-key seam is not yet wired for a real
// driver, THIS test is the RED spec that forces it.
// =====================================================================

//
// CODER MUST ALSO ADD (Phase-2B, `Cargo.toml` [dev-dependencies]): this
// actor-integration test builds a `TurnRequest`, which requires the same
// vocabulary crates R1's own support uses — `wasl-adapter` (`Delivery`/
// `DeliveryKind`), `wasl-proto` (`AgentId`/`RequestId`), and `wasl-audit`
// (`Sink`/`AuditEntry`/`AuditError`). These are NOT dependencies of the empty R2
// skeleton; add them as `[dev-dependencies]` (they are workspace-hoisted). The
// driver-level `hold` tests above need NONE of these — only this genericity
// proof does, so if the actor-integration seam is out of 2B scope the driver
// tests still stand alone.
#[tokio::test]
async fn second_submit_queues_behind_a_held_turn_through_the_generic_actor() {
    use std::sync::{Arc, Mutex};

    use wasl_adapter::{Delivery, DeliveryKind};
    use wasl_proto::{AgentId, RequestId};
    use wasl_runtime::{
        BusyPolicy, Causation, IdempotencyKey, ManagedConfig, MonotonicClock, RuntimeProvider,
        TargetKey, TargetStatus, TurnReceipt, TurnRequest, TurnSource, WaslManagedRuntimeProvider,
    };

    // A no-op sink is sufficient here — this test asserts QUEUEING, not audit.
    // The concrete `Sink` type comes from `wasl_audit`; a recording double keeps
    // it self-contained. (Mirrors R1 support's `sink_arc`.)
    struct NullSink;
    impl wasl_audit::Sink for NullSink {
        fn append(&mut self, _e: &wasl_audit::AuditEntry) -> Result<(), wasl_audit::AuditError> {
            Ok(())
        }
    }

    let proto = FakeLineProtocol::new(fake_agent_bin());
    let sink: Arc<Mutex<dyn wasl_audit::Sink + Send>> = Arc::new(Mutex::new(NullSink));
    let provider = WaslManagedRuntimeProvider::with_seams(
        proto,
        ManagedConfig::default(),
        Arc::new(MonotonicClock),
        sink,
    );

    let key = TargetKey("ctx-hold".to_string());
    let target = provider
        .target_for(&key)
        .await
        .expect("get-or-create the managed target backed by the real fake agent");

    let make_req = |text: &str, idem: &str| TurnRequest {
        delivery: Delivery {
            context_id: "ctx-hold".to_string(),
            from: "wasl-b".to_string(),
            kind: DeliveryKind::Consult,
            text: text.to_string(),
            data: vec![],
        },
        source: TurnSource::Agent {
            from: AgentId("wasl-b".to_string()),
        },
        idempotency_key: Some(IdempotencyKey(idem.to_string())),
        causation: Causation {
            root_request: RequestId("root".to_string()),
            parent_request: None,
            path: vec![],
            hop: 0,
        },
    };

    // Turn 1: a `hold` — becomes the active (busy) turn and blocks. A REAL
    // process is spawned here (`FakeLineProtocol`), so this submit races the
    // driver's own `SessionReady` (runtime.md §7.1 row 582): it may come back
    // `Started` (already ready) OR `Queued` (still `Starting`, promoted the
    // moment ready arrives) — both are spec-correct, and unlike the scripted
    // test double, a real spawned process's startup latency makes this a
    // COMMON outcome, not a rare edge case.
    let r1 = target
        .submit_turn(make_req(&req_hold(), "idem-1"), BusyPolicy::Reject)
        .await;
    assert!(
        matches!(r1, TurnReceipt::Started { .. } | TurnReceipt::Queued { .. }),
        "first submit must Start or Queue, got {r1:?}"
    );

    // Regardless of which receipt turn 1 got, wait for the target to
    // actually reach `Busy` (i.e. turn 1 has been promoted and is genuinely
    // active/held) before submitting turn 2 — this is what the test
    // actually needs to be deterministic ("a second submit while a turn is
    // genuinely active must queue"), not the receipt of the FIRST submit.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(target.status().await, TargetStatus::Busy { .. }) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("turn 1 must reach Busy (the fake is genuinely held) before turn 2 is submitted");

    // Turn 2 WHILE held: with `Queue`, it must QUEUE behind the held turn rather
    // than start (the busy state is now confirmed deterministic above).
    let r2 = target
        .submit_turn(
            make_req(&req_say("second"), "idem-2"),
            BusyPolicy::Queue {
                max_wait: Duration::from_secs(5),
            },
        )
        .await;
    assert!(
        matches!(r2, TurnReceipt::Queued { .. }),
        "a second submit while a `hold` turn is active must QUEUE (§4.2), got {r2:?}"
    );

    // Drain the target for shutdown so the real child is reaped.
    let _ = target.shutdown(wasl_runtime::ShutdownMode::Immediate).await;
}

// --- local helper --------------------------------------------------------

fn completion_of(evs: &[ProtocolEvent]) -> Option<String> {
    evs.iter()
        .find_map(|e| completed_reply(e).map(|s| s.to_string()))
}
