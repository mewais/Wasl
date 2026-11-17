# C1 — `wasl-adapter` — spec (authoritative)

**Tier:** L1 (trait + fake fully testable). **Deps:** `wasl-a2a-types`, `wasl-a2a-server` (for the
`TaskExecutor` trait it implements), `serde`, `serde_json`, `async-trait`. Dev: `tokio` (macros/rt). No
`unsafe`. This is the **last inch**: the seam between an incoming A2A Task and the real local agent, plus the
`TaskExecutor` that plugs it into the A2A server (B1).

**Testability (honesty):** the `AgentAdapter` trait, the `AdapterExecutor` mapping, and metadata/part
extraction are unit-tested to **100%** with a FAKE adapter. The CONCRETE adapters that drive a real
claude/codex CLI are a SEPARATE block (`docs/specs/real-adapters.md`, block E3) and are
**UNTESTED HERE** — this crate contains only the trait + fake + executor + extraction logic.

## Types
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryKind { Consult, Command, Message }   // the Wasl verb the receiver should act on

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub context_id: String,     // stable conversation → the receiver maps this to a persistent session
    pub from: String,           // sender agent id (for provenance in the injected turn)
    pub kind: DeliveryKind,
    pub text: String,           // the prompt/instruction/message body (from the message's TextPart)
    pub data: Vec<serde_json::Value>, // any DataParts (structured data shared alongside the text)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterError { pub message: String }   // Display + std::error::Error

/// The last inch: deliver `d` to the local agent and return its reply text. For Consult/Command the reply is
/// the agent's answer/result; for Message it is an ack (delivery confirmation) — the agent isn't expected to
/// reply, but the adapter still returns once the message is injected. Same `context_id` across calls MUST map
/// to the same live session (that is how B's context persists across messages).
#[async_trait::async_trait]
pub trait AgentAdapter: Send + Sync {
    async fn deliver(&self, d: &Delivery) -> Result<String, AdapterError>;
}
```

## Wire convention (how the verb + provenance travel in an A2A Message)
The sender puts the Wasl verb + sender id in `Message.metadata`:
`{"wasl": {"kind": "consult"|"command"|"message", "from": "<agent-id>"}}`. The text is the first `Part::Text`;
any `Part::Data` values are collected into `Delivery.data`. Missing/%malformed `wasl` metadata → default
`kind = Message`, `from = "unknown"` (a message with no verb is treated as a plain message, never a command —
fail safe).
```rust
/// Pure extraction (unit-tested): pull a `Delivery` out of an A2A message + its resolved context id.
pub fn delivery_from_message(context_id: String, msg: &Message) -> Delivery;
```

## AdapterExecutor — plugs an adapter into the B1 server
```rust
pub struct AdapterExecutor<A: AgentAdapter> { /* adapter */ }
impl<A: AgentAdapter> AdapterExecutor<A> { pub fn new(adapter: A) -> Self; }

#[async_trait::async_trait]
impl<A: AgentAdapter> wasl_a2a_server::TaskExecutor for AdapterExecutor<A> {
    /// Extract the Delivery, call `adapter.deliver`, and wrap the reply as `vec![Part::text(reply)]`.
    /// An `AdapterError` → `Err(ExecError{ message })` (→ the Task ends `failed`).
    async fn execute(&self, context_id: String, message: Message) -> Result<Vec<Part>, wasl_a2a_server::ExecError>;
}
```

## Test plan (`tests/adapter.rs`; 100% line+region). A **fake `AgentAdapter`** that records the `Delivery` it
received and is scriptable to return a reply string or an `AdapterError`.
- A1 `delivery_from_message`: a message with metadata `{"wasl":{"kind":"consult","from":"A"}}` + a TextPart
  "hi" + a DataPart `{"x":1}` → `Delivery{ kind:Consult, from:"A", text:"hi", data:[{"x":1}], context_id }`.
- A2 `delivery_from_message` fail-safe: no metadata → `kind:Message, from:"unknown"`; malformed `wasl` blob →
  same defaults; no TextPart → `text:""`.
- A3 each `DeliveryKind` serde round-trips lowercase (`consult`/`command`/`message`).
- A4 `AdapterExecutor::execute` success: fake returns `"answer"` → `Ok(vec![Part::Text{"answer"}])`; assert
  the fake saw the exact `Delivery` (kind/from/text/data) derived from the message.
- A5 `AdapterExecutor::execute` error: fake returns `AdapterError` → `Err(ExecError)` carrying the message.
- A6 a Command-kind message → the fake sees `kind:Command` (proves the verb reaches the adapter, so a
  receiver can treat a command differently from a consult).
- A7 `AdapterError` Display non-empty + `std::error::Error`.

No `#[ignore]`/`todo!`/tautology. The fake proves the wiring; the REAL agent drivers (E3) are elsewhere and
are UNTESTED HERE.
