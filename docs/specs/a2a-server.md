# B1 — `wasl-a2a-server` — spec (authoritative)

**Tier:** L2 (HTTP IO; loopback-testable). **Deps:** `wasl-a2a-types`, `axum` 0.7, `tokio`, `serde`,
`serde_json`, `async-trait`, `futures`. Dev: `reqwest` 0.12 (json+stream), `tokio` (macros/rt). No `unsafe`.
This is the RECEIVE side of a node: an A2A (JSON-RPC 2.0 over HTTP) server that accepts incoming messages,
runs each through a `TaskExecutor` (the seam that drives the local agent), manages the Task lifecycle, and
returns/streams results.

**Testability (honesty):** the request-handling CORE (`handle_send`/`handle_get`/`handle_cancel`/
`stream_events` + id gen + lifecycle) is pure-ish and unit-tested to **100% line+region** with a fake
executor. The axum router + real HTTP bind is the IO shell, verified by a **loopback integration test**
(127.0.0.1, real `reqwest`), NOT held to 100% coverage. Driving a REAL agent is a different crate
(`wasl-adapter` / the real adapters) and is out of scope here.

## The executor seam
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecError { pub message: String }   // Display + std::error::Error

#[async_trait::async_trait]
pub trait TaskExecutor: Send + Sync {
    /// Service one incoming message for `context_id`: produce the agent's reply parts, or an execution
    /// error (→ the Task ends `failed`). Same `context_id` across calls = the same conversation
    /// (persistence is the executor's concern — see the adapter block).
    async fn execute(&self, context_id: String, message: Message) -> Result<Vec<Part>, ExecError>;
}
```

## Id generation (deterministic + injectable, so tests are stable)
```rust
pub trait IdGen: Send + Sync { fn next(&self, prefix: &str) -> String; }
/// Default: an atomic counter → `"{prefix}-{n}"` starting at 1.
pub struct CounterIds { /* AtomicU64 */ }   // impl Default + IdGen
```

## Server
```rust
pub struct A2aServer { /* Arc<dyn TaskExecutor>, Arc<Mutex<HashMap<String,Task>>>, AgentCard, Arc<dyn IdGen> */ }

impl A2aServer {
    pub fn new(agent_card: AgentCard, executor: Arc<dyn TaskExecutor>) -> Self;  // default CounterIds
    pub fn with_ids(agent_card, executor, ids: Arc<dyn IdGen>) -> Self;

    // ── request-handling core (unit-tested to 100%) ───────────────────────────
    /// message/send: assign contextId (message.context_id or a fresh one) + a fresh taskId; run the
    /// executor; on Ok(parts) → Task status `completed` + one Artifact carrying `parts`; on Err(e) →
    /// Task status `failed` with a status message of the error text. Store the Task; return it.
    pub async fn handle_send(&self, params: MessageSendParams) -> Task;
    /// tasks/get: look up by id → the Task, or Err(RpcError::task_not_found(id)).
    pub fn handle_get(&self, id: &str) -> Result<Task, RpcError>;
    /// tasks/cancel: if present and non-terminal → set state `canceled`, return it; if terminal →
    /// Err(RpcError::not_cancelable); if absent → Err(RpcError::task_not_found).
    pub fn handle_cancel(&self, id: &str) -> Result<Task, RpcError>;
    /// message/stream: run the executor once and produce the ordered SSE events:
    /// [StatusUpdate(working, final=false), ArtifactUpdate(artifact), StatusUpdate(completed, final=true)]
    /// on success; [StatusUpdate(working,false), StatusUpdate(failed, final=true)] on executor error.
    pub async fn stream_events(&self, params: MessageSendParams) -> Vec<StreamEvent>;
    pub fn agent_card(&self) -> &AgentCard;

    // ── HTTP shell (loopback-tested, not coverage-gated) ──────────────────────
    /// Build the axum Router: POST `/` = JSON-RPC dispatch (method → the handlers above; message/stream
    /// returns `text/event-stream` SSE); GET `/.well-known/agent-card.json` = the AgentCard.
    pub fn router(self: Arc<Self>) -> axum::Router;
    /// Bind `addr` and serve until shutdown (the thin main loop).
    pub async fn serve(self: Arc<Self>, addr: std::net::SocketAddr) -> std::io::Result<()>;
}

/// One SSE event (serialized as its A2A wire form on the `data:` line).
pub enum StreamEvent { Status(TaskStatusUpdateEvent), Artifact(TaskArtifactUpdateEvent) }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcError { pub code: i64, pub message: String }
impl RpcError { pub fn task_not_found(id:&str)->Self; pub fn not_cancelable(id:&str)->Self;
                pub fn method_not_found(m:&str)->Self; pub fn invalid_params(d:&str)->Self; }
// codes: A2A TaskNotFound = -32001; JSON-RPC MethodNotFound = -32601; InvalidParams = -32602.
```
JSON-RPC dispatch: parse the top-level `{jsonrpc,id,method,params}`; route `message/send`,
`message/stream`, `tasks/get`, `tasks/cancel`; unknown method → MethodNotFound; params that don't
deserialize → InvalidParams; wrap results as `JsonRpcResponse::ok`, errors as `::err`.

## Test plan
**`tests/core.rs` (unit — no HTTP — 100% line+region on the handling core). Fake `TaskExecutor` (scriptable
to return parts or ExecError) + the default `CounterIds` (so taskId is `task-1`, contextId `ctx-1`, …).**
- C1 `handle_send` success: executor returns 2 text parts → Task state `completed`, one Artifact with those
  parts, taskId `task-1`; the message's `context_id` is honored when present, else `ctx-1` assigned.
- C2 `handle_send` executor error → Task state `failed`, status.message carries the error text; still stored.
- C3 `handle_get`: after a send, get by the returned id → the same Task; unknown id → `task_not_found` (code -32001).
- C4 `handle_cancel`: a stored (completed) task → `not_cancelable`; a freshly-`submitted`/`working` task →
  `canceled`; unknown id → `task_not_found`. (Construct a working task via a slow/paused fake if needed, or
  test the terminal + missing branches deterministically.)
- C5 `stream_events` success → exactly [Status(working,final=false), Artifact(parts), Status(completed,final=true)]
  in order; executor error → [Status(working,false), Status(failed,final=true)]. Assert the events' taskId/
  contextId and states.
- C6 JSON-RPC dispatch helper: unknown method → MethodNotFound (-32601); bad params → InvalidParams (-32602).
- C7 `RpcError` constructors carry the right codes; `ExecError` Display + std::error::Error.

**`tests/loopback.rs` (integration — real HTTP on 127.0.0.1:0 with `reqwest`; proves the axum shell + SSE).
Bind `router()` to an ephemeral port in a spawned task; use reqwest.**
- L1 POST a `message/send` JSON-RPC → 200, response is `JsonRpcResponse::ok` with a completed Task + artifact
  matching the fake executor's parts.
- L2 GET `/.well-known/agent-card.json` → the AgentCard JSON (name/url/capabilities).
- L3 `tasks/get` for the returned id → the Task; a bogus id → a JSON-RPC error with code -32001.
- L4 `message/stream` → `text/event-stream`; collect the SSE `data:` lines and assert the ordered
  status/artifact/status(final) events (parse each back into the A2A event types).

No `#[ignore]`/`todo!`/tautology. Keep the loopback test hermetic (ephemeral port, spawned server, short
timeouts, await readiness). The `serve()` bind loop itself is the only thing that may be uncovered — that is
the IO shell; everything reachable via `router()` is exercised by the loopback tests.
