#![cfg(feature = "live-cli")]
//! Phase 5 — the `--features live-cli` end-to-end proof, mirroring Phase 4's
//! `claude_live_pid_and_queueing.rs` structure (`docs/specs/runtime-protocol.md`
//! §5.3 / `docs/design/live-runtime-impl-plan.md` Phase 5) but proving the
//! genericity-DIFFERENTIATING property this driver actually has: REAL
//! streaming deltas (`streaming_output: true`), not just same-pid/queueing.
//! Costs real `opencode` CLI model calls (≤4 total across both tests).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;

use wasl_adapter::{Delivery, DeliveryKind};
use wasl_audit::{AuditEntry, AuditError, Sink};
use wasl_proto::AgentId;
use wasl_runtime::{
    BusyPolicy, Causation, EventSeq, ManagedConfig, MonotonicClock, StreamItem, TargetKey,
    TurnEvent, TurnId, TurnReceipt, TurnRequest, TurnSource, WaslManagedRuntimeProvider,
};
use wasl_runtime_protocol::OpenCodeAcpProtocol;

struct NoopSink;

impl Sink for NoopSink {
    fn append(&mut self, _entry: &AuditEntry) -> Result<(), AuditError> {
        Ok(())
    }
}

fn fresh_context_id(test_name: &str) -> String {
    let nonce = std::env::var("WASL_LIVE_NONCE").expect(
        "WASL_LIVE_NONCE must be set (to a fresh value) to run \
         opencode_acp_live_streaming_and_queueing.rs",
    );
    format!("wasl-runtime-protocol-opencode-live-{test_name}-{nonce}")
}

fn build_provider() -> WaslManagedRuntimeProvider<OpenCodeAcpProtocol> {
    WaslManagedRuntimeProvider::with_seams(
        OpenCodeAcpProtocol::new("opencode"),
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
/// never ends while the target is alive -- the original `drain_all` used
/// `.collect()` (wait for stream END), which hangs FOREVER against it. See
/// the identical fix in `crates/wasl-runtime/tests/conformance_suite.rs` /
/// `tests/claude_live_pid_and_queueing.rs`.
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

/// §5.3: same PID/native-session-id serves a human `send` and a follow-up
/// A2A `consult` -- NOT a fresh spawn. Unlike claude (whose native id is
/// client-minted, byte-stable independent of the CLI), opencode's native id
/// is SERVER-assigned (`session/new`'s `sessionId`) -- so the proof here is
/// "the SAME server-assigned id is observed both times, and it looks like a
/// real opencode session id", not an independently-recomputed value.
#[tokio::test]
async fn same_pid_and_native_session_serves_a_human_send_and_an_a2a_consult() {
    let context_id = fresh_context_id("pid");
    let provider = build_provider();
    let target = provider
        .target_for(&TargetKey(context_id.clone()))
        .await
        .expect("target_for must succeed against the real opencode acp CLI");

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
    let native_1 = events_after_1
        .iter()
        .find_map(|e| match e {
            TurnEvent::SessionReady { native } => native.clone(),
            _ => None,
        })
        .expect("no SessionReady observed after turn 1");
    assert!(
        native_1.0.starts_with("ses_"),
        "opencode-minted session ids are VERIFIED to start with ses_, got {:?}",
        native_1.0
    );
    let reply1 = completed_reply_for(&events_after_1, &turn1_id)
        .expect("no TurnCompleted correlated to the human turn's OWN turn_id");
    assert!(
        reply1.contains("PING_OK"),
        "human turn's own reply did not contain PING_OK: {reply1:?}"
    );

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

    // No second, DIFFERENT native session id ever appeared (no re-spawn).
    let all_native_ids: Vec<_> = events_after_2
        .iter()
        .filter_map(|e| match e {
            TurnEvent::SessionReady { native } => native.clone(),
            _ => None,
        })
        .collect();
    for native in &all_native_ids {
        assert_eq!(
            *native, native_1,
            "a second, DIFFERENT native session id appeared -- this would mean \
             the A2A turn got a FRESH spawn instead of reusing the same live \
             process"
        );
    }
}

/// The genericity-DIFFERENTIATING property vs Phase 4: `TurnCompleted.reply`
/// must be the FULL accumulated text (opencode's own `session/prompt`
/// JSON-RPC response carries NO reply text -- only `stopReason`/`usage`; the
/// text arrives via one-or-more streaming `agent_message_chunk`
/// notifications BEFORE the response), proving the transport's
/// accumulate-then-emit-on-terminal logic is correct, not just that SOME
/// `AssistantDelta` fired.
#[tokio::test]
async fn assistant_delta_streams_and_turn_completed_carries_the_full_accumulated_reply() {
    let context_id = fresh_context_id("stream");
    let provider = build_provider();
    let target = provider
        .target_for(&TargetKey(context_id.clone()))
        .await
        .expect("target_for must succeed against the real opencode acp CLI");

    let receipt = target
        .submit_turn(
            human_turn(&context_id, "Reply with exactly the word: STREAM_OK"),
            BusyPolicy::Reject,
        )
        .await;
    let turn_id = match receipt {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    let events = drain_until(
        target.events(EventSeq(0)).await,
        Duration::from_secs(60),
        |evs| completed_reply_for(evs, &turn_id).is_some(),
    )
    .await;

    let deltas: Vec<&str> = events
        .iter()
        .filter_map(|e| match e {
            TurnEvent::AssistantDelta { turn_id: t, text } if *t == turn_id => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        !deltas.is_empty(),
        "streaming_output claimed but no AssistantDelta was observed for this \
         turn's OWN turn_id"
    );

    let reply = completed_reply_for(&events, &turn_id)
        .expect("no TurnCompleted correlated to this turn's OWN turn_id");
    assert!(
        reply.contains("STREAM_OK"),
        "TurnCompleted.reply must carry the FULL accumulated text even though \
         opencode's own session/prompt JSON-RPC response has no reply field \
         at all -- got {reply:?}"
    );
    // The accumulated final reply must equal the concatenation of the
    // observed deltas (in order) -- proves the accumulator isn't dropping or
    // reordering chunks.
    let concatenated: String = deltas.concat();
    assert_eq!(
        reply, concatenated,
        "TurnCompleted.reply must equal the exact concatenation of the \
         AssistantDelta chunks observed for this turn, in order"
    );
}

/// §5.3 busy/queueing, mirroring Phase 4's `claude_live_pid_and_queueing.rs`.
#[tokio::test]
async fn busy_policy_queues_a_second_turn_behind_an_active_one() {
    let context_id = fresh_context_id("queue");
    let provider = build_provider();
    let target = provider
        .target_for(&TargetKey(context_id.clone()))
        .await
        .expect("target_for must succeed against the real opencode acp CLI");

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
        "turn 2's OWN reply did not contain TWO (non-interleaved output): {reply2:?}"
    );
}
