//! `RuntimeExecutor` — the D1 hookup (live-runtime-plan §3): drive inbound A2A
//! tasks into a PERSISTENT managed runtime session (`DeliveryTarget`) instead of
//! a one-shot adapter turn.
//!
//! `wasl_node::receiver` wraps a `wasl_adapter::AdapterExecutor`, which spins a
//! throwaway `AgentAdapter` turn per inbound task. `RuntimeExecutor` is the
//! `TaskExecutor` that instead calls `DeliveryTarget::submit_turn` with
//! `TurnSource::Agent { from }` and then follows that turn's `events()` stream,
//! returning its final reply as the A2A task result — so a peer's
//! consult/command/message lands in the node's live session (its full history,
//! model, gates), and dispatch talks only to the `DeliveryTarget` seam.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use wasl_a2a_server::{ExecError, TaskExecutor};
use wasl_a2a_types::{Message, Part};
use wasl_adapter::delivery_from_message;
use wasl_proto::{AgentId, RequestId};
use wasl_runtime::{
    BusyPolicy, Causation, DeliveryTarget, EventSeq, StreamItem, TurnEvent, TurnId, TurnReceipt,
    TurnRequest, TurnSource,
};

/// Default bounded wait for a queued A2A turn before it expires, so a peer's task
/// isn't dropped merely because the session is mid-turn.
const DEFAULT_QUEUE_WAIT: Duration = Duration::from_secs(300);

/// A [`TaskExecutor`] backed by a persistent managed [`DeliveryTarget`].
pub struct RuntimeExecutor {
    target: Arc<dyn DeliveryTarget>,
    busy_policy: BusyPolicy,
}

impl RuntimeExecutor {
    /// Wrap `target` with the default busy policy (queue behind the active turn
    /// for a bounded wait).
    pub fn new(target: Arc<dyn DeliveryTarget>) -> Self {
        Self {
            target,
            busy_policy: BusyPolicy::Queue {
                max_wait: DEFAULT_QUEUE_WAIT,
            },
        }
    }

    /// Wrap `target` with an explicit busy policy.
    pub fn with_busy_policy(target: Arc<dyn DeliveryTarget>, busy_policy: BusyPolicy) -> Self {
        Self {
            target,
            busy_policy,
        }
    }

    /// Resolve an admission receipt to the `(turn_id, subscribe-from seq)` to
    /// follow, or an `ExecError` for a refusal. `Duplicate` unwraps to its
    /// recorded original receipt (no second injection occurred).
    fn resolve_receipt(receipt: TurnReceipt) -> Result<(TurnId, EventSeq), ExecError> {
        match receipt {
            // Atomic subscribe point: `first_seq` is this turn's first journalled
            // event, so `events(first_seq)` follows it with no gap or dup.
            TurnReceipt::Started { turn_id, first_seq } => Ok((turn_id, first_seq)),
            // Queued: its events arrive once it starts. Follow from genesis and
            // filter by `turn_id` — a live forward subscription still yields them.
            TurnReceipt::Queued { turn_id, .. } => Ok((turn_id, EventSeq(0))),
            TurnReceipt::RejectedBusy { active_turn } => Err(ExecError {
                message: format!("runtime target busy with turn {}", active_turn.0),
            }),
            TurnReceipt::RejectedUnavailable { status } => Err(ExecError {
                message: format!("runtime target unavailable: {status:?}"),
            }),
            TurnReceipt::Duplicate { original } => Self::resolve_receipt(*original),
        }
    }
}

#[async_trait::async_trait]
impl TaskExecutor for RuntimeExecutor {
    async fn execute(&self, context_id: String, message: Message) -> Result<Vec<Part>, ExecError> {
        // Same message→Delivery extraction the one-shot path uses.
        let delivery = delivery_from_message(context_id, &message);
        let from = AgentId(delivery.from.clone());

        // This inbound A2A task begins a fresh call path (root, hop 0). Threading
        // the peer's own causation from metadata (for cross-node cycle rejection,
        // §8) is a later refinement — the peer does not forward it today.
        let causation = Causation {
            root_request: RequestId(format!("a2a-{}", delivery.context_id)),
            parent_request: None,
            path: vec![from.clone()],
            hop: 0,
        };

        let req = TurnRequest {
            delivery,
            source: TurnSource::Agent { from },
            idempotency_key: None,
            causation,
        };

        let receipt = self.target.submit_turn(req, self.busy_policy.clone()).await;
        let (turn_id, from_seq) = Self::resolve_receipt(receipt)?;

        // Follow the turn to its terminal event and surface the reply.
        let mut stream = self.target.events(from_seq).await;
        while let Some(item) = stream.next().await {
            match item {
                StreamItem::Event { event, .. } => match event {
                    TurnEvent::TurnCompleted {
                        turn_id: tid,
                        reply,
                        ..
                    } if tid == turn_id => {
                        return Ok(vec![Part::Text { text: reply }]);
                    }
                    TurnEvent::TurnFailed {
                        turn_id: tid,
                        error,
                        ..
                    } if tid == turn_id => {
                        return Err(ExecError { message: error });
                    }
                    TurnEvent::TurnCancelled {
                        turn_id: tid,
                        reason,
                    } if tid == turn_id => {
                        return Err(ExecError {
                            message: format!("runtime turn cancelled: {reason:?}"),
                        });
                    }
                    TurnEvent::TurnExpired { turn_id: tid } if tid == turn_id => {
                        return Err(ExecError {
                            message: "runtime turn expired in queue before starting".to_string(),
                        });
                    }
                    _ => {}
                },
                // The retained window advanced past our subscription point before we
                // read the terminal event — report rather than hang; the peer retries.
                StreamItem::Truncated { .. } => {
                    return Err(ExecError {
                        message: "runtime event stream truncated before turn completion"
                            .to_string(),
                    });
                }
            }
        }

        Err(ExecError {
            message: "runtime event stream ended before the turn completed".to_string(),
        })
    }
}
