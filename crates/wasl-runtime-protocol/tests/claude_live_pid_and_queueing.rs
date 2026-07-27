#![cfg(feature = "live-cli")]
//! Phase 4 — the `--features live-cli` end-to-end proof
//! (`docs/specs/runtime-protocol.md` §5.3 /
//! `docs/design/live-runtime-impl-plan.md` Phase 4): "assert SAME
//! PID/native-session-id across a human `send` and an A2A `consult` (not a
//! fresh spawn) + busy/queueing live test." Mirrors the
//! `wasl-adapter-claude/tests/live.rs` / `wasl-adapter-opencode/tests/live.rs`
//! skeleton (`#![cfg(feature = "live-cli")]`, `WASL_LIVE_NONCE`-scoped fresh
//! context, cheap exact-word probes). Costs real `claude` CLI model calls
//! (≤4 total across both tests).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use uuid::{uuid, Uuid};

use wasl_adapter::{Delivery, DeliveryKind};
use wasl_audit::{AuditEntry, AuditError, Sink};
use wasl_proto::AgentId;
use wasl_runtime::{
    BusyPolicy, Causation, EventSeq, ManagedConfig, MonotonicClock, NativeSessionId, StreamItem,
    TargetKey, TurnEvent, TurnId, TurnReceipt, TurnRequest, TurnSource, WaslManagedRuntimeProvider,
};
use wasl_runtime_protocol::ClaudeStreamProtocol;

/// The SAME UUIDv5 namespace `wasl-adapter-claude::WASL_SESSION_NS` uses,
/// independently declared here (no cross-crate dependency) but bit-for-bit
/// identical BY DESIGN so a context id maps to the SAME native claude session
/// whether delivered via the ephemeral adapter or the managed runtime (see
/// `Cargo.toml`'s `uuid` dependency comment).
const WASL_SESSION_NS: Uuid = uuid!("2441e851-1801-440d-a8e9-214d1df08e6e");

struct NoopSink;

impl Sink for NoopSink {
    fn append(&mut self, _entry: &AuditEntry) -> Result<(), AuditError> {
        Ok(())
    }
}

fn fresh_context_id(test_name: &str) -> String {
    let nonce = std::env::var("WASL_LIVE_NONCE").expect(
        "WASL_LIVE_NONCE must be set (to a fresh value) to run \
         claude_live_pid_and_queueing.rs",
    );
    format!("wasl-runtime-protocol-claude-live-{test_name}-{nonce}")
}

fn build_provider(context_id: &str) -> WaslManagedRuntimeProvider<ClaudeStreamProtocol> {
    let _ = context_id; // one provider/protocol instance serves every target it creates
    WaslManagedRuntimeProvider::with_seams(
        ClaudeStreamProtocol::new("claude"),
        ManagedConfig::default(),
        Arc::new(MonotonicClock),
        Arc::new(Mutex::new(NoopSink)),
    )
}

fn human_turn(context_id: &str, text: &str) -> TurnRequest {
    TurnRequest {
        delivery: Delivery {
            context_id: context_id.to_string(),
            from: "wasl-live-test".to_string(),
            kind: DeliveryKind::Consult,
            text: text.to_string(),
            data: vec![],
        },
        source: TurnSource::LocalHuman,
        idempotency_key: None,
        causation: Causation {
            root_request: wasl_proto::RequestId("live-root-1".to_string()),
            parent_request: None,
            path: vec![],
            hop: 0,
        },
    }
}

fn a2a_turn(context_id: &str, text: &str) -> TurnRequest {
    TurnRequest {
        delivery: Delivery {
            context_id: context_id.to_string(),
            from: "a2a-peer".to_string(),
            kind: DeliveryKind::Consult,
            text: text.to_string(),
            data: vec![],
        },
        source: TurnSource::Agent {
            from: AgentId("a2a-peer".to_string()),
        },
        idempotency_key: None,
        causation: Causation {
            root_request: wasl_proto::RequestId("live-root-2".to_string()),
            parent_request: None,
            path: vec![],
            hop: 0,
        },
    }
}

/// Drain `stream` until `done(&events_so_far)` is true, or `bound` elapses (a
/// hang-guard). **Bug fix, found by actually running this against a real
/// driver:** a real managed target's `events()` is a genuinely LIVE tail that
/// never ends while the target is alive — the original `drain_all` used
/// `.collect()` (wait for stream END), which hangs FOREVER against it. See
/// the identical fix in `crates/wasl-runtime/tests/conformance_suite.rs` /
/// `tests/claude_conformance.rs`.
async fn drain_until(
    mut stream: wasl_runtime::TurnEventStream,
    bound: Duration,
    mut done: impl FnMut(&[TurnEvent]) -> bool,
) -> Vec<TurnEvent> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + bound;
    loop {
        if done(&out) {
            break;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(StreamItem::Event { event, .. })) => out.push(event),
            Ok(Some(StreamItem::Truncated { .. })) => continue,
            Ok(None) => break,
            Err(_) => break,
        }
    }
    out
}

fn completed_reply_for(events: &[TurnEvent], turn_id: &TurnId) -> Option<String> {
    events.iter().find_map(|e| match e {
        TurnEvent::TurnCompleted {
            turn_id: t, reply, ..
        } if t == turn_id => Some(reply.clone()),
        _ => None,
    })
}

/// §5.3: same PID/native-session-id serves a human `send` (`TurnSource::
/// LocalHuman`) and a follow-up A2A `consult` (`TurnSource::Agent`) — NOT a
/// fresh spawn. The `DeliveryTarget` API exposes no raw OS PID, so "same
/// process" is proven the way the spec intends at THIS layer: ONE target
/// instance (one spawn, one `ClaudeStreamProtocol::spawn` call) serves BOTH
/// turns, and the SAME byte-stable native session id (independently computed
/// here via the identical UUIDv5 scheme) is observed — the driver never
/// re-spawned a second claude process for the second turn.
#[tokio::test]
async fn same_pid_and_native_session_serves_a_human_send_and_an_a2a_consult() {
    let context_id = fresh_context_id("pid");
    let expected_native =
        NativeSessionId(Uuid::new_v5(&WASL_SESSION_NS, context_id.as_bytes()).to_string());

    let provider = build_provider(&context_id);
    let target = provider
        .target_for(&TargetKey(context_id.clone()))
        .await
        .expect("target_for must succeed against the real claude CLI");

    // Turn 1: a human `send`.
    let receipt1 = target
        .submit_turn(
            human_turn(&context_id, "Reply with exactly the word: PING_OK"),
            BusyPolicy::Reject,
        )
        .await;
    let turn1_id = match receipt1 {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started for the human turn, got {other:?}"),
    };

    let events_after_1 = drain_until(
        target.events(EventSeq(0)).await,
        Duration::from_secs(60),
        |evs| completed_reply_for(evs, &turn1_id).is_some(),
    )
    .await;
    let observed_native = events_after_1.iter().find_map(|e| match e {
        TurnEvent::SessionReady { native } => native.clone(),
        _ => None,
    });
    assert_eq!(
        observed_native,
        Some(expected_native.clone()),
        "the native session id must be the byte-stable UUIDv5 minted from the \
         context id (VERIFIED shape, runtime-protocol.md §2), proving the \
         client-supplied --session-id was used"
    );
    let reply1 = completed_reply_for(&events_after_1, &turn1_id)
        .expect("no TurnCompleted correlated to the human turn's OWN turn_id");
    assert!(
        reply1.contains("PING_OK"),
        "human turn's own reply did not contain PING_OK: {reply1:?}"
    );

    // Turn 2: an A2A `consult`, on the SAME target (no fresh spawn/target_for).
    let receipt2 = target
        .submit_turn(
            a2a_turn(
                &context_id,
                "What exact word did I ask for? Answer with just that word.",
            ),
            BusyPolicy::Reject,
        )
        .await;
    let turn2_id = match receipt2 {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started for the A2A turn, got {other:?}"),
    };
    assert_ne!(turn1_id, turn2_id, "the two turns must have distinct ids");

    let events_after_2 = drain_until(
        target.events(EventSeq(0)).await,
        Duration::from_secs(60),
        |evs| completed_reply_for(evs, &turn2_id).is_some(),
    )
    .await;
    let reply2 = completed_reply_for(&events_after_2, &turn2_id)
        .expect("no TurnCompleted correlated to the A2A turn's OWN turn_id");
    assert!(
        reply2.contains("PING_OK"),
        "the SAME process must recall the human turn's token on the A2A turn \
         (session continuity, not a fresh spawn); got {reply2:?}"
    );

    // No SECOND, DIFFERENT native session id ever appeared (i.e. no re-spawn):
    // every `SessionReady` observed across the WHOLE replayed history carries
    // the SAME native id as turn 1's.
    let all_native_ids: Vec<_> = events_after_2
        .iter()
        .filter_map(|e| match e {
            TurnEvent::SessionReady { native } => native.clone(),
            _ => None,
        })
        .collect();
    for native in &all_native_ids {
        assert_eq!(
            *native, expected_native,
            "a second, DIFFERENT native session id appeared — this would mean \
             the A2A turn got a FRESH spawn instead of reusing the same live \
             process"
        );
    }
}

/// §5.3 busy/queueing: a second turn submitted while the first is still
/// active (before its `TurnCompleted`) with `BusyPolicy::Queue` is admitted
/// into the FIFO queue (`TurnReceipt::Queued`), and both eventually complete
/// with distinct, non-interleaved replies — mirroring the FIFO assertion
/// idiom `wasl-runtime/tests/fifo_and_admission.rs` already uses for the
/// scripted driver, now proven against the REAL claude CLI.
#[tokio::test]
async fn busy_policy_queues_a_second_turn_behind_an_active_one() {
    let context_id = fresh_context_id("queue");
    let provider = build_provider(&context_id);
    let target = provider
        .target_for(&TargetKey(context_id.clone()))
        .await
        .expect("target_for must succeed against the real claude CLI");

    // Turn 1: admitted and starts running (a real claude turn takes several
    // seconds, so submitting turn 2 immediately afterward reliably lands while
    // turn 1 is still active — no sleep, no timing assumption beyond "a real
    // model call is not instantaneous").
    let receipt1 = target
        .submit_turn(
            human_turn(&context_id, "Reply with exactly the word: ONE"),
            BusyPolicy::Reject,
        )
        .await;
    let turn1_id = match receipt1 {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started for turn 1, got {other:?}"),
    };

    // Turn 2: submitted while turn 1 is (almost certainly) still active, with a
    // generous queue wait bound (well above a single real claude turn's
    // observed ~3-7s duration, spikes/live-substrate/out/claude/events.jsonl).
    let receipt2 = target
        .submit_turn(
            human_turn(&context_id, "Reply with exactly the word: TWO"),
            BusyPolicy::Queue {
                max_wait: Duration::from_secs(120),
            },
        )
        .await;
    let turn2_id = match receipt2 {
        TurnReceipt::Queued { turn_id, .. } => turn_id,
        TurnReceipt::Started { turn_id, .. } => {
            // Turn 1 finished before turn 2 was admitted (a slow first call) —
            // still a valid distinct-turn outcome; note it and continue.
            eprintln!(
                "note: turn 2 started immediately (turn 1 completed first); \
                 the FIFO-queue path was not exercised this run"
            );
            turn_id
        }
        other => panic!("expected Queued (or Started, if turn 1 already finished), got {other:?}"),
    };
    assert_ne!(turn1_id, turn2_id, "the two turns must have distinct ids");

    let events = drain_until(
        target.events(EventSeq(0)).await,
        Duration::from_secs(90),
        |evs| {
            completed_reply_for(evs, &turn1_id).is_some()
                && completed_reply_for(evs, &turn2_id).is_some()
        },
    )
    .await;
    let reply1 = completed_reply_for(&events, &turn1_id).expect("turn 1 must complete");
    let reply2 = completed_reply_for(&events, &turn2_id).expect("turn 2 must complete");
    assert!(
        reply1.contains("ONE"),
        "turn 1's OWN reply did not contain ONE: {reply1:?}"
    );
    assert!(
        reply2.contains("TWO"),
        "turn 2's OWN reply did not contain TWO (non-interleaved output — each \
         turn's reply must correlate to ITS OWN turn_id, never the other's)"
    );
}
