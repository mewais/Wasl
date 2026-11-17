//! D1 (live-runtime-plan §3) — `RuntimeExecutor` drives an inbound A2A task into
//! a persistent managed `DeliveryTarget` as a `TurnSource::Agent` turn, then
//! returns that turn's reply. Uses a scripted fake `DeliveryTarget` (no real CLI).

use std::sync::{Arc, Mutex};

use futures::stream;
use wasl_a2a_server::TaskExecutor;
use wasl_a2a_types::{Message, Part};
use wasl_node::RuntimeExecutor;
use wasl_proto::{AgentId, DeliveryCertainty};
use wasl_runtime::{
    BusyPolicy, CancelReason, DeliveryTarget, EventSeq, NativeGateResolver, RuntimeCapabilities,
    RuntimeError, RuntimeProvider, ShutdownMode, StreamItem, TargetId, TargetKey, TargetStatus,
    ToolCallAnswerer, TurnEvent, TurnEventStream, TurnId, TurnReceipt, TurnRequest, TurnSource,
};

fn caps() -> RuntimeCapabilities {
    RuntimeCapabilities {
        persistent_process: true,
        persistent_conversation: true,
        structured_turns: true,
        streaming_output: true,
        attachable: true,
        interruptible: true,
        structured_provenance: true,
        resumable: true,
    }
}

/// A scripted `DeliveryTarget`: returns a fixed admission receipt, yields a fixed
/// list of events from `events()`, and records every `submit_turn` request.
struct FakeTarget {
    id: TargetId,
    receipt: TurnReceipt,
    events: Vec<TurnEvent>,
    captured: Arc<Mutex<Vec<TurnRequest>>>,
}

impl std::fmt::Debug for FakeTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FakeTarget")
    }
}

#[async_trait::async_trait]
impl DeliveryTarget for FakeTarget {
    fn target_id(&self) -> &TargetId {
        &self.id
    }
    fn capabilities(&self) -> RuntimeCapabilities {
        caps()
    }
    async fn status(&self) -> TargetStatus {
        TargetStatus::Stopped
    }
    async fn submit_turn(&self, req: TurnRequest, _busy_policy: BusyPolicy) -> TurnReceipt {
        self.captured.lock().unwrap().push(req);
        self.receipt.clone()
    }
    async fn events(&self, _after_seq: EventSeq) -> TurnEventStream {
        let items: Vec<StreamItem> = self
            .events
            .iter()
            .enumerate()
            .map(|(i, e)| StreamItem::Event {
                seq: EventSeq(i as u64),
                event: e.clone(),
            })
            .collect();
        Box::pin(stream::iter(items))
    }
    async fn cancel_turn(
        &self,
        _turn_id: TurnId,
        _reason: CancelReason,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }
    async fn shutdown(&self, _mode: ShutdownMode) -> Result<(), RuntimeError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl NativeGateResolver for FakeTarget {
    async fn resolve_native_gate(
        &self,
        _turn_id: TurnId,
        _decision: wasl_proto::GateDecision,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl ToolCallAnswerer for FakeTarget {
    async fn answer_tool_call(&self, _turn_id: TurnId, _value: String) -> Result<(), RuntimeError> {
        Ok(())
    }
}

/// A provider that always hands back the one scripted target (get-or-create noop).
struct FakeProvider {
    target: Arc<FakeTarget>,
}

#[async_trait::async_trait]
impl RuntimeProvider for FakeProvider {
    async fn target_for(&self, _key: &TargetKey) -> Result<Arc<dyn DeliveryTarget>, RuntimeError> {
        Ok(Arc::clone(&self.target) as Arc<dyn DeliveryTarget>)
    }
    async fn list_targets(&self) -> Vec<TargetId> {
        vec![self.target.id.clone()]
    }
}

fn tid() -> TurnId {
    TurnId("t1".to_string())
}

/// An inbound A2A message carrying Wasl provenance (`kind` + `from`) in metadata.
fn inbound(from: &str, kind: &str, text: &str) -> Message {
    let mut m = Message::user_text(text, "msg-1");
    m.metadata = Some(serde_json::json!({ "wasl": { "kind": kind, "from": from } }));
    m
}

fn fake(
    receipt: TurnReceipt,
    events: Vec<TurnEvent>,
) -> (RuntimeExecutor, Arc<Mutex<Vec<TurnRequest>>>) {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let target = Arc::new(FakeTarget {
        id: TargetId("fake".to_string()),
        receipt,
        events,
        captured: Arc::clone(&captured),
    });
    let provider = Arc::new(FakeProvider { target });
    (RuntimeExecutor::new(provider), captured)
}

// ── Tests ───────────────────────────────────────────────────────────────────

/// The core hookup: an inbound consult is submitted as a `TurnSource::Agent`
/// turn carrying the delivery, and the turn's `TurnCompleted.reply` is returned.
#[tokio::test]
async fn submits_agent_turn_and_returns_the_turn_reply() {
    let (exec, captured) = fake(
        TurnReceipt::Started {
            turn_id: tid(),
            first_seq: EventSeq(0),
        },
        vec![
            TurnEvent::TurnAccepted {
                turn_id: tid(),
                source: TurnSource::Agent {
                    from: AgentId("wasl-b".to_string()),
                },
            },
            TurnEvent::AssistantDelta {
                turn_id: tid(),
                text: "po".to_string(),
            },
            TurnEvent::TurnCompleted {
                turn_id: tid(),
                reply: "pong".to_string(),
                certainty: DeliveryCertainty::Completed,
            },
        ],
    );

    let out = exec
        .execute("ctx-1".to_string(), inbound("wasl-b", "consult", "ping"))
        .await
        .expect("execute ok");
    assert_eq!(
        out,
        vec![Part::Text {
            text: "pong".to_string()
        }]
    );

    // The submitted turn carried the peer as the source and the delivery verbatim.
    let reqs = captured.lock().unwrap();
    assert_eq!(reqs.len(), 1, "exactly one turn submitted");
    let req = &reqs[0];
    assert_eq!(
        req.source,
        TurnSource::Agent {
            from: AgentId("wasl-b".to_string())
        },
        "inbound A2A turn is sourced from the peer"
    );
    assert_eq!(req.delivery.text, "ping");
    assert_eq!(req.delivery.from, "wasl-b");
}

/// A `TurnFailed` maps to an `ExecError` carrying the failure detail.
#[tokio::test]
async fn turn_failed_maps_to_exec_error() {
    let (exec, _c) = fake(
        TurnReceipt::Started {
            turn_id: tid(),
            first_seq: EventSeq(0),
        },
        vec![TurnEvent::TurnFailed {
            turn_id: tid(),
            error: "boom".to_string(),
            certainty: DeliveryCertainty::Completed,
        }],
    );
    let err = exec
        .execute("ctx-1".to_string(), inbound("wasl-b", "command", "do it"))
        .await
        .expect_err("must surface the in-session failure");
    assert_eq!(err.message, "boom");
}

/// A refused admission (unavailable target) maps to an `ExecError`, not a hang.
#[tokio::test]
async fn rejected_admission_maps_to_exec_error() {
    let (exec, _c) = fake(
        TurnReceipt::RejectedUnavailable {
            status: TargetStatus::Stopped,
        },
        vec![],
    );
    let err = exec
        .execute("ctx-1".to_string(), inbound("wasl-b", "consult", "ping"))
        .await
        .expect_err("a refused turn must error");
    assert!(
        err.message.contains("unavailable"),
        "error explains the refusal: {}",
        err.message
    );
}

/// Events for OTHER turns interleaved on the shared stream are ignored; only the
/// submitted turn's terminal event resolves the call.
#[tokio::test]
async fn ignores_events_for_other_turns() {
    let other = TurnId("other".to_string());
    let (exec, _c) = fake(
        TurnReceipt::Started {
            turn_id: tid(),
            first_seq: EventSeq(0),
        },
        vec![
            // A different turn completing must NOT resolve our call.
            TurnEvent::TurnCompleted {
                turn_id: other.clone(),
                reply: "not-ours".to_string(),
                certainty: DeliveryCertainty::Completed,
            },
            TurnEvent::TurnCompleted {
                turn_id: tid(),
                reply: "ours".to_string(),
                certainty: DeliveryCertainty::Completed,
            },
        ],
    );
    let out = exec
        .execute("ctx-1".to_string(), inbound("wasl-b", "consult", "ping"))
        .await
        .expect("execute ok");
    assert_eq!(
        out,
        vec![Part::Text {
            text: "ours".to_string()
        }]
    );
}
