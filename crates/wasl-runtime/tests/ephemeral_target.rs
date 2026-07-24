//! Phase 3 — `EphemeralTarget: DeliveryTarget` behavioral spec.
//!
//! `EphemeralRuntimeProvider`/`EphemeralTarget` do NOT exist yet in
//! `wasl_runtime`'s src — this file forward-references them and is expected to
//! FAIL TO COMPILE (unresolved import) until the code-agent implements them per
//! `docs/specs/runtime.md`'s description: `submit_turn` maps 1:1 to exactly one
//! `wasl_adapter::AgentAdapter::deliver` call; a synthetic event sequence
//! (`TurnAccepted` -> `AssistantDelta`(final reply) -> `TurnCompleted`, or
//! `TurnAccepted` -> `TurnFailed` on error) gives callers ONE uniform event
//! model regardless of provider; `submit_turn` NEVER returns anything but
//! `Started` (a fresh ephemeral spawn is never "busy"); capabilities report
//! honestly what today's headless adapters can do
//! (`persistent_process: false`, `structured_turns: false`,
//! `persistent_conversation: true` via the adapter's own resume ledger).

use std::sync::{Arc, Mutex};

use futures::StreamExt;

use wasl_adapter::{AdapterError, AgentAdapter, Delivery, DeliveryKind};
use wasl_runtime::{
    BusyPolicy, Causation, DeliveryTarget, EventSeq, RuntimeProvider, StreamItem, TargetKey,
    TurnEvent, TurnReceipt, TurnRequest, TurnSource,
};

// Forward reference: this type is Phase-3's deliverable, not yet implemented.
use wasl_runtime::EphemeralRuntimeProvider;

/// A test-local fake `AgentAdapter`: records every `Delivery` it was called
/// with and returns a single scripted `Ok`/`Err` result.
struct FakeAdapter {
    calls: Arc<Mutex<Vec<Delivery>>>,
    result: Arc<Mutex<Result<String, AdapterError>>>,
}

impl FakeAdapter {
    fn new(result: Result<String, AdapterError>) -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            result: Arc::new(Mutex::new(result)),
        }
    }

    fn calls(&self) -> Vec<Delivery> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl AgentAdapter for FakeAdapter {
    async fn deliver(&self, d: &Delivery) -> Result<String, AdapterError> {
        self.calls.lock().unwrap().push(d.clone());
        self.result.lock().unwrap().clone()
    }
}

fn sample_request() -> TurnRequest {
    TurnRequest {
        delivery: Delivery {
            context_id: "ctx-1".to_string(),
            from: "peer-a".to_string(),
            kind: DeliveryKind::Consult,
            text: "hello".to_string(),
            data: vec![],
        },
        source: TurnSource::Agent {
            from: wasl_proto::AgentId("peer-a".to_string()),
        },
        idempotency_key: None,
        causation: Causation {
            root_request: wasl_proto::RequestId("r1".to_string()),
            parent_request: None,
            path: vec![],
            hop: 0,
        },
    }
}

/// Drain a `TurnEventStream` into the `TurnEvent`s it carries (ignoring any
/// `Truncated` control items, which a fresh ephemeral stream never emits).
async fn drain_events(stream: wasl_runtime::TurnEventStream) -> Vec<TurnEvent> {
    stream
        .filter_map(|item| async move {
            match item {
                StreamItem::Event { event, .. } => Some(event),
                StreamItem::Truncated { .. } => None,
            }
        })
        .collect()
        .await
}

#[tokio::test]
async fn target_for_returns_a_delivery_target() {
    let adapter = FakeAdapter::new(Ok("ok".to_string()));
    let provider = EphemeralRuntimeProvider::new(adapter);

    let target = provider
        .target_for(&TargetKey("ctx-1".to_string()))
        .await
        .expect("target_for must succeed for a fresh key");

    // Type-level proof: `target` is `Arc<dyn DeliveryTarget>`.
    let _: &dyn DeliveryTarget = target.as_ref();
}

#[tokio::test]
async fn submit_turn_calls_deliver_exactly_once_with_correct_delivery() {
    let adapter = FakeAdapter::new(Ok("reply text".to_string()));
    let calls = Arc::clone(&adapter.calls.clone());
    let provider = EphemeralRuntimeProvider::new(adapter);
    let target = provider
        .target_for(&TargetKey("ctx-1".to_string()))
        .await
        .unwrap();

    let req = sample_request();
    let _receipt = target.submit_turn(req.clone(), BusyPolicy::Reject).await;

    let recorded = calls.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1, "deliver must be called exactly once");
    assert_eq!(recorded[0].context_id, req.delivery.context_id);
    assert_eq!(recorded[0].from, req.delivery.from);
    assert_eq!(recorded[0].kind, req.delivery.kind);
    assert_eq!(recorded[0].text, req.delivery.text);
    assert_eq!(recorded[0].data, req.delivery.data);
}

#[tokio::test]
async fn submit_turn_always_returns_started() {
    let adapter = FakeAdapter::new(Ok("ok".to_string()));
    let provider = EphemeralRuntimeProvider::new(adapter);
    let target = provider
        .target_for(&TargetKey("ctx-1".to_string()))
        .await
        .unwrap();

    // Even a Reject policy must never yield RejectedBusy/Queued for a fresh
    // ephemeral spawn -- there is no busy state to reject/queue against.
    let receipt = target
        .submit_turn(sample_request(), BusyPolicy::Reject)
        .await;
    assert!(
        matches!(receipt, TurnReceipt::Started { .. }),
        "expected Started, got {receipt:?}"
    );
}

#[tokio::test]
async fn successful_delivery_emits_accepted_delta_completed() {
    let adapter = FakeAdapter::new(Ok("the reply".to_string()));
    let provider = EphemeralRuntimeProvider::new(adapter);
    let target = provider
        .target_for(&TargetKey("ctx-1".to_string()))
        .await
        .unwrap();

    let receipt = target
        .submit_turn(sample_request(), BusyPolicy::Reject)
        .await;
    let turn_id = match receipt {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    let events = drain_events(target.events(EventSeq(0)).await).await;

    assert_eq!(events.len(), 3, "expected exactly 3 events, got {events:?}");
    match &events[0] {
        TurnEvent::TurnAccepted { turn_id: t, .. } => assert_eq!(*t, turn_id),
        other => panic!("event 0: expected TurnAccepted, got {other:?}"),
    }
    match &events[1] {
        TurnEvent::AssistantDelta { turn_id: t, text } => {
            assert_eq!(*t, turn_id);
            assert_eq!(text, "the reply");
        }
        other => panic!("event 1: expected AssistantDelta, got {other:?}"),
    }
    match &events[2] {
        TurnEvent::TurnCompleted {
            turn_id: t, reply, ..
        } => {
            assert_eq!(*t, turn_id);
            assert_eq!(reply, "the reply");
        }
        other => panic!("event 2: expected TurnCompleted, got {other:?}"),
    }
}

#[tokio::test]
async fn failed_delivery_emits_accepted_then_failed() {
    let adapter = FakeAdapter::new(Err(AdapterError {
        message: "boom".to_string(),
    }));
    let provider = EphemeralRuntimeProvider::new(adapter);
    let target = provider
        .target_for(&TargetKey("ctx-1".to_string()))
        .await
        .unwrap();

    let receipt = target
        .submit_turn(sample_request(), BusyPolicy::Reject)
        .await;
    let turn_id = match receipt {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    let events = drain_events(target.events(EventSeq(0)).await).await;

    assert_eq!(events.len(), 2, "expected exactly 2 events, got {events:?}");
    match &events[0] {
        TurnEvent::TurnAccepted { turn_id: t, .. } => assert_eq!(*t, turn_id),
        other => panic!("event 0: expected TurnAccepted, got {other:?}"),
    }
    match &events[1] {
        TurnEvent::TurnFailed {
            turn_id: t, error, ..
        } => {
            assert_eq!(*t, turn_id);
            assert!(
                error.contains("boom"),
                "expected the failure detail to surface, got {error:?}"
            );
        }
        other => panic!("event 1: expected TurnFailed (not TurnCompleted), got {other:?}"),
    }
}

#[tokio::test]
async fn capabilities_reports_ephemeral_truthfully() {
    let adapter = FakeAdapter::new(Ok("ok".to_string()));
    let provider = EphemeralRuntimeProvider::new(adapter);
    let target = provider
        .target_for(&TargetKey("ctx-1".to_string()))
        .await
        .unwrap();

    let caps = target.capabilities();

    // The properties today's spawn-per-turn adapters genuinely have vs. lack:
    // no persistent process, no reliable structured turn boundaries (a single
    // synthetic accept/delta/complete triple, not a real driver's turn
    // lifecycle), no streaming (one final chunk), not attachable/interruptible
    // (the OS process is gone before anyone could attach or interrupt it), no
    // structured provenance (no native ids), not resumable within a node's
    // lifetime (each turn is a fresh process) -- but the underlying adapter's
    // OWN resume ledger (session-id/thread-id, already proven in the adapter
    // crates) DOES give conversational continuity across turns.
    assert!(
        !caps.persistent_process,
        "ephemeral has no persistent process"
    );
    assert!(
        !caps.structured_turns,
        "ephemeral has no real turn-boundary protocol"
    );
    assert!(
        !caps.streaming_output,
        "ephemeral emits one final chunk, not deltas"
    );
    assert!(!caps.attachable, "there is no live process to attach to");
    assert!(
        !caps.interruptible,
        "a completed/spawned-then-gone process cannot be interrupted"
    );
    assert!(
        !caps.structured_provenance,
        "no native session/turn ids are available"
    );
    assert!(
        !caps.resumable,
        "ephemeral does not survive a node restart as a live target"
    );
    assert!(
        caps.persistent_conversation,
        "the wrapped adapter's own resume ledger gives conversational continuity"
    );
}
