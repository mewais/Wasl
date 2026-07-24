//! Phase 3 — the "zero behavior change" proof: an `EphemeralTarget` wrapping
//! an `AgentAdapter` must produce EXACTLY the same reply as calling
//! `AgentAdapter::deliver` directly. This is what lets `runtime = "ephemeral"`
//! (the default) be byte-identical to today's `AdapterExecutor` path.
//!
//! Expected to fail to compile until `wasl_runtime::EphemeralRuntimeProvider`
//! exists (the test-first baseline, same as ephemeral_target.rs).

use std::sync::{Arc, Mutex};

use futures::StreamExt;

use wasl_adapter::{AdapterError, AgentAdapter, Delivery, DeliveryKind};
use wasl_runtime::{
    BusyPolicy, Causation, DeliveryTarget, EphemeralRuntimeProvider, EventSeq, RuntimeProvider,
    StreamItem, TargetKey, TurnEvent, TurnReceipt, TurnRequest, TurnSource,
};

struct FakeAdapter {
    calls: Arc<Mutex<Vec<Delivery>>>,
    result: Result<String, AdapterError>,
}

impl FakeAdapter {
    fn new(result: Result<String, AdapterError>) -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            result,
        }
    }
}

#[async_trait::async_trait]
impl AgentAdapter for FakeAdapter {
    async fn deliver(&self, d: &Delivery) -> Result<String, AdapterError> {
        self.calls.lock().unwrap().push(d.clone());
        self.result.clone()
    }
}

fn sample_delivery() -> Delivery {
    Delivery {
        context_id: "ctx-diff".to_string(),
        from: "peer-a".to_string(),
        kind: DeliveryKind::Command,
        text: "do the thing".to_string(),
        data: vec![serde_json::json!({"k": "v"})],
    }
}

fn sample_request(delivery: Delivery) -> TurnRequest {
    TurnRequest {
        delivery,
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

async fn final_reply_from(stream: wasl_runtime::TurnEventStream) -> Option<String> {
    let events: Vec<TurnEvent> = stream
        .filter_map(|item| async move {
            match item {
                StreamItem::Event { event, .. } => Some(event),
                StreamItem::Truncated { .. } => None,
            }
        })
        .collect()
        .await;
    events.into_iter().find_map(|e| match e {
        TurnEvent::TurnCompleted { reply, .. } => Some(reply),
        _ => None,
    })
}

#[tokio::test]
async fn successful_reply_is_byte_identical_direct_vs_ephemeral_target() {
    let delivery = sample_delivery();

    // Path A: call AgentAdapter::deliver directly (today's existing path).
    let direct_adapter = FakeAdapter::new(Ok("the exact same reply text".to_string()));
    let direct_reply = direct_adapter
        .deliver(&delivery)
        .await
        .expect("direct deliver must succeed");

    // Path B: the SAME logical adapter behavior, wrapped as an EphemeralTarget.
    let wrapped_adapter = FakeAdapter::new(Ok("the exact same reply text".to_string()));
    let provider = EphemeralRuntimeProvider::new(wrapped_adapter);
    let target = provider
        .target_for(&TargetKey(delivery.context_id.clone()))
        .await
        .expect("target_for must succeed");
    let receipt = target
        .submit_turn(sample_request(delivery.clone()), BusyPolicy::Reject)
        .await;
    assert!(matches!(receipt, TurnReceipt::Started { .. }));
    let via_target_reply = final_reply_from(target.events(EventSeq(0)).await)
        .await
        .expect("EphemeralTarget must surface a TurnCompleted reply");

    assert_eq!(
        direct_reply, via_target_reply,
        "wrapping an adapter as an EphemeralTarget must not change the reply text \
         AT ALL -- this is the zero-behavior-change guarantee for the default \
         runtime=\"ephemeral\" path"
    );
}

#[tokio::test]
async fn failure_detail_is_identical_direct_vs_ephemeral_target() {
    let delivery = sample_delivery();

    let direct_adapter = FakeAdapter::new(Err(AdapterError {
        message: "the exact same failure detail".to_string(),
    }));
    let direct_err = direct_adapter
        .deliver(&delivery)
        .await
        .expect_err("direct deliver must fail");

    let wrapped_adapter = FakeAdapter::new(Err(AdapterError {
        message: "the exact same failure detail".to_string(),
    }));
    let provider = EphemeralRuntimeProvider::new(wrapped_adapter);
    let target = provider
        .target_for(&TargetKey(delivery.context_id.clone()))
        .await
        .expect("target_for must succeed");
    let _receipt = target
        .submit_turn(sample_request(delivery.clone()), BusyPolicy::Reject)
        .await;

    let events: Vec<TurnEvent> = target
        .events(EventSeq(0))
        .await
        .filter_map(|item| async move {
            match item {
                StreamItem::Event { event, .. } => Some(event),
                StreamItem::Truncated { .. } => None,
            }
        })
        .collect()
        .await;
    let via_target_error = events
        .into_iter()
        .find_map(|e| match e {
            TurnEvent::TurnFailed { error, .. } => Some(error),
            _ => None,
        })
        .expect("EphemeralTarget must surface a TurnFailed error");

    assert_eq!(
        direct_err.message, via_target_error,
        "the failure detail surfaced through EphemeralTarget must match the \
         AdapterError's message exactly -- no information is lost or altered \
         by the wrapping layer"
    );
}
