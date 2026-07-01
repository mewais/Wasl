//! wasl-adapter — the last inch: bridge an incoming A2A Task to the real local agent.
//!
//! Defines the `AgentAdapter` trait (deliver a message to a live agent session,
//! get its reply), the `Delivery` extraction from an A2A `Message` (Wasl verb +
//! provenance travel in `metadata.wasl`), and `AdapterExecutor` which implements
//! `wasl_a2a_server::TaskExecutor` by calling the adapter. The trait + fake +
//! extraction are fully unit-tested; the CONCRETE CLI-driving adapters are a
//! separate block (UNTESTED HERE). See `docs/specs/adapter.md`.

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use wasl_a2a_types::{part_text, Message, Part};

/// The Wasl verb the receiver should act on. Serializes lowercase
/// (`consult`/`command`/`message`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryKind {
    /// Ask the agent a question and wait for its answer.
    Consult,
    /// Instruct the agent to perform an action and return its result.
    Command,
    /// Inject a plain message; the agent isn't expected to reply.
    Message,
}

/// A resolved unit of work handed to an [`AgentAdapter`]: the verb + provenance +
/// body extracted from an incoming A2A message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    /// Stable conversation id → the receiver maps this to a persistent session.
    pub context_id: String,
    /// Sender agent id (for provenance in the injected turn).
    pub from: String,
    /// The Wasl verb the receiver should act on.
    pub kind: DeliveryKind,
    /// The prompt/instruction/message body (from the message's first TextPart).
    pub text: String,
    /// Any DataParts (structured data shared alongside the text), in order.
    pub data: Vec<Value>,
}

/// An error surfaced by an [`AgentAdapter`] while delivering to the local agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterError {
    /// Human-readable failure detail.
    pub message: String,
}

impl fmt::Display for AdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for AdapterError {}

/// The last inch: deliver `d` to the local agent and return its reply text.
///
/// For Consult/Command the reply is the agent's answer/result; for Message it is
/// an ack. The same `context_id` across calls MUST map to the same live session.
#[async_trait::async_trait]
pub trait AgentAdapter: Send + Sync {
    /// Deliver `d` to the local agent and return its reply text (or an error).
    async fn deliver(&self, d: &Delivery) -> Result<String, AdapterError>;
}

/// Pull the Wasl verb + provenance out of `metadata.wasl`, failing safe to
/// `(Message, "unknown")` when the metadata is absent or malformed.
fn extract_verb(metadata: &Option<Value>) -> (DeliveryKind, String) {
    let wasl = metadata.as_ref().and_then(|m| m.get("wasl"));

    let kind = wasl
        .and_then(|w| w.get("kind"))
        .and_then(|k| serde_json::from_value::<DeliveryKind>(k.clone()).ok())
        .unwrap_or(DeliveryKind::Message);

    let from = wasl
        .and_then(|w| w.get("from"))
        .and_then(|f| f.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "unknown".to_string());

    (kind, from)
}

/// Pure extraction (unit-tested): pull a [`Delivery`] out of an A2A message + its
/// resolved context id.
pub fn delivery_from_message(context_id: String, msg: &Message) -> Delivery {
    let (kind, from) = extract_verb(&msg.metadata);

    let text = msg
        .parts
        .iter()
        .find_map(part_text)
        .unwrap_or("")
        .to_string();

    let data = msg
        .parts
        .iter()
        .filter_map(|p| match p {
            Part::Data { data } => Some(data.clone()),
            _ => None,
        })
        .collect();

    Delivery {
        context_id,
        from,
        kind,
        text,
        data,
    }
}

/// Plugs an [`AgentAdapter`] into the B1 server by implementing
/// [`wasl_a2a_server::TaskExecutor`].
pub struct AdapterExecutor<A: AgentAdapter> {
    adapter: A,
}

impl<A: AgentAdapter> AdapterExecutor<A> {
    /// Wrap `adapter` in an executor.
    pub fn new(adapter: A) -> Self {
        AdapterExecutor { adapter }
    }
}

#[async_trait::async_trait]
impl<A: AgentAdapter> wasl_a2a_server::TaskExecutor for AdapterExecutor<A> {
    async fn execute(
        &self,
        context_id: String,
        message: Message,
    ) -> Result<Vec<Part>, wasl_a2a_server::ExecError> {
        let d = delivery_from_message(context_id, &message);
        match self.adapter.deliver(&d).await {
            Ok(reply) => Ok(vec![Part::Text { text: reply }]),
            Err(e) => Err(wasl_a2a_server::ExecError { message: e.message }),
        }
    }
}
