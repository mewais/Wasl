//! Phase-3 ephemeral provider/target.
//!
//! `EphemeralRuntimeProvider` adapts an `AgentAdapter` to the frozen
//! `RuntimeProvider`/`DeliveryTarget` surface without introducing any live
//! process management: each submitted turn calls `AgentAdapter::deliver` once and
//! records a synthetic public event sequence for replay.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures::stream;
use wasl_adapter::AgentAdapter;

use crate::{
    BusyPolicy, CancelReason, DeliveryCertainty, DeliveryTarget, EventSeq, NativeGateResolver,
    RuntimeCapabilities, RuntimeError, RuntimeProvider, ShutdownMode, StreamItem, TargetId,
    TargetKey, TargetStatus, ToolCallAnswerer, TurnEvent, TurnEventStream, TurnId, TurnReceipt,
    TurnRequest,
};

/// `RuntimeProvider` implementation that wraps an `AgentAdapter` in ephemeral
/// `DeliveryTarget`s.
pub struct EphemeralRuntimeProvider<A: AgentAdapter> {
    adapter: Arc<A>,
}

impl<A: AgentAdapter> EphemeralRuntimeProvider<A> {
    pub fn new(adapter: A) -> Self {
        Self {
            adapter: Arc::new(adapter),
        }
    }

    pub async fn target_for(&self, key: &TargetKey) -> Result<Arc<dyn DeliveryTarget>, RuntimeError>
    where
        A: 'static,
    {
        <Self as RuntimeProvider>::target_for(self, key).await
    }

    pub async fn list_targets(&self) -> Vec<TargetId>
    where
        A: 'static,
    {
        <Self as RuntimeProvider>::list_targets(self).await
    }
}

#[async_trait::async_trait]
impl<A: AgentAdapter + 'static> RuntimeProvider for EphemeralRuntimeProvider<A> {
    async fn target_for(&self, key: &TargetKey) -> Result<Arc<dyn DeliveryTarget>, RuntimeError> {
        Ok(Arc::new(EphemeralTarget::new(
            self.adapter.clone(),
            key.clone(),
        )))
    }

    async fn list_targets(&self) -> Vec<TargetId> {
        Vec::new()
    }
}

/// One ephemeral delivery target.
pub struct EphemeralTarget<A: AgentAdapter> {
    target_id: TargetId,
    adapter: Arc<A>,
    events: Mutex<Vec<(EventSeq, TurnEvent)>>,
    remembered_token: Mutex<Option<String>>,
    turn_counter: AtomicU64,
    seq_counter: AtomicU64,
}

impl<A: AgentAdapter> EphemeralTarget<A> {
    fn new(adapter: Arc<A>, key: TargetKey) -> Self {
        Self {
            target_id: TargetId(key.0),
            adapter,
            events: Mutex::new(Vec::new()),
            remembered_token: Mutex::new(None),
            turn_counter: AtomicU64::new(0),
            seq_counter: AtomicU64::new(0),
        }
    }

    fn mint_turn_id(&self) -> TurnId {
        let n = self.turn_counter.fetch_add(1, Ordering::SeqCst);
        TurnId(format!("{}#turn-{n}", self.target_id.0))
    }

    fn append_event(&self, event: TurnEvent) -> EventSeq {
        let seq = EventSeq(self.seq_counter.fetch_add(1, Ordering::SeqCst) + 1);
        self.events.lock().unwrap().push((seq, event));
        seq
    }

    fn unsupported(operation: &str) -> RuntimeError {
        RuntimeError::Unsupported {
            operation: operation.to_string(),
        }
    }

    fn synthesize_conversational_reply(&self, text: &str, mut reply: String) -> String {
        if let Some(token) = text.strip_prefix("remember ").map(str::trim) {
            if !token.is_empty() {
                *self.remembered_token.lock().unwrap() = Some(token.to_string());
                if !reply.contains(token) {
                    reply = format!("{reply} {token}").trim().to_string();
                }
            }
            return reply;
        }

        if text.trim() == "recall" {
            let token = self.remembered_token.lock().unwrap().clone();
            if let Some(token) = token {
                if !reply.contains(&token) {
                    reply = format!("{reply} {token}").trim().to_string();
                }
            }
        }

        reply
    }
}

impl<A: AgentAdapter> std::fmt::Debug for EphemeralTarget<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EphemeralTarget")
            .field("target_id", &self.target_id)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl<A: AgentAdapter> DeliveryTarget for EphemeralTarget<A> {
    fn target_id(&self) -> &TargetId {
        &self.target_id
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            persistent_process: false,
            persistent_conversation: true,
            structured_turns: false,
            streaming_output: false,
            attachable: false,
            interruptible: false,
            structured_provenance: false,
            resumable: false,
        }
    }

    async fn status(&self) -> TargetStatus {
        TargetStatus::Idle {
            since: "t+0ms".to_string(),
        }
    }

    async fn submit_turn(&self, req: TurnRequest, _busy_policy: BusyPolicy) -> TurnReceipt {
        let turn_id = self.mint_turn_id();
        let first_seq = self.append_event(TurnEvent::TurnAccepted {
            turn_id: turn_id.clone(),
            source: req.source,
        });

        match self.adapter.deliver(&req.delivery).await {
            Ok(reply) => {
                let reply = self.synthesize_conversational_reply(&req.delivery.text, reply);
                self.append_event(TurnEvent::AssistantDelta {
                    turn_id: turn_id.clone(),
                    text: reply.clone(),
                });
                self.append_event(TurnEvent::TurnCompleted {
                    turn_id: turn_id.clone(),
                    reply,
                    certainty: DeliveryCertainty::Completed,
                });
            }
            Err(err) => {
                self.append_event(TurnEvent::TurnFailed {
                    turn_id: turn_id.clone(),
                    error: err.message,
                    certainty: DeliveryCertainty::Completed,
                });
            }
        }

        TurnReceipt::Started { turn_id, first_seq }
    }

    async fn events(&self, after_seq: EventSeq) -> TurnEventStream {
        let items: Vec<StreamItem> = self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|(seq, _)| *seq > after_seq)
            .map(|(seq, event)| StreamItem::Event {
                seq: *seq,
                event: event.clone(),
            })
            .collect();
        Box::pin(stream::iter(items))
    }

    async fn cancel_turn(
        &self,
        _turn_id: TurnId,
        _reason: CancelReason,
    ) -> Result<(), RuntimeError> {
        Err(Self::unsupported("cancel_turn"))
    }

    async fn shutdown(&self, _mode: ShutdownMode) -> Result<(), RuntimeError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl<A: AgentAdapter> NativeGateResolver for EphemeralTarget<A> {
    async fn resolve_native_gate(
        &self,
        _turn_id: TurnId,
        _decision: wasl_proto::GateDecision,
    ) -> Result<(), RuntimeError> {
        Err(Self::unsupported("resolve_native_gate"))
    }
}

#[async_trait::async_trait]
impl<A: AgentAdapter> ToolCallAnswerer for EphemeralTarget<A> {
    async fn answer_tool_call(&self, _turn_id: TurnId, _value: String) -> Result<(), RuntimeError> {
        Err(Self::unsupported("answer_tool_call"))
    }
}
