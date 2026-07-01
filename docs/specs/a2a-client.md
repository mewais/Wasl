# B2 — `wasl-a2a-client` — spec (authoritative)

**Tier:** L2 (HTTP IO; loopback-testable to 100%). **Deps:** `wasl-a2a-types`, `reqwest` 0.12 (json+stream),
`tokio`, `serde`, `serde_json`, `futures`. Dev: `wasl-a2a-server` + `async-trait` (spin a real loopback
server with a fake executor), `tokio` (macros/rt). No `unsafe`. This is the SEND side of a node: an A2A
client that POSTs JSON-RPC to a peer's A2A endpoint and parses the results.

**Testability (honesty):** every client method is a request/response with no un-coverable shell, so this
crate is unit-tested to **100% line+region** by driving a REAL loopback `wasl-a2a-server` (with a fake
executor) — a genuine client↔server interop test on 127.0.0.1. (Different ports here stand in for different
hosts; the SSH/WAN case is the same code path and is UNTESTED HERE only in the sense of a second physical
machine.)

## API
```rust
pub struct A2aClient { /* reqwest::Client + base_url + request-id counter */ }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcErr { pub code: i64, pub message: String }

#[derive(Debug)]
pub enum ClientError { Http(String), Rpc(RpcErr), Decode(String) }   // Display + std::error::Error

pub enum A2aEvent { Status(TaskStatusUpdateEvent), Artifact(TaskArtifactUpdateEvent) }

impl A2aClient {
    pub fn new(base_url: impl Into<String>) -> Self;                       // default reqwest::Client
    pub fn with_client(base_url: impl Into<String>, client: reqwest::Client) -> Self;

    /// POST `message/send`. Ok(Task) on a JSON-RPC result; Err(Rpc) on a JSON-RPC error; Err(Http) on a
    /// transport failure; Err(Decode) if the body isn't a valid JsonRpcResponse<Task>.
    pub async fn send_message(&self, message: Message) -> Result<Task, ClientError>;
    /// POST `tasks/get`.
    pub async fn get_task(&self, id: &str) -> Result<Task, ClientError>;
    /// POST `tasks/cancel`.
    pub async fn cancel_task(&self, id: &str) -> Result<Task, ClientError>;
    /// GET `/.well-known/agent-card.json`.
    pub async fn fetch_agent_card(&self) -> Result<AgentCard, ClientError>;
    /// POST `message/stream`; parse the SSE `data:` frames into ordered `A2aEvent`s and collect them
    /// (stop after the final status event or when the stream ends).
    pub async fn send_message_streaming(&self, message: Message) -> Result<Vec<A2aEvent>, ClientError>;
}
```
Each call builds a `JsonRpcRequest` (jsonrpc "2.0", an incrementing id, the method, typed params), POSTs to
`base_url`, and parses a `JsonRpcResponse<R>`: the `result` arm → `Ok`, the `error` arm → `Err(Rpc{code,
message})`. `send_message`/`get_task`/`cancel_task` parse `R = Task`; `fetch_agent_card` is a plain GET →
`AgentCard`. SSE parsing: read the byte stream, split on blank lines, take each `data:` payload, deserialize
into the A2A event type by its `kind` (`status-update`/`artifact-update`).

## Test plan (`tests/loopback.rs`; 100% line+region). Spin a real `wasl-a2a-server` on `127.0.0.1:0` with a
**fake `TaskExecutor`** (scriptable: returns e.g. one text part `"pong:{received text}"`, or an `ExecError`).
Await server readiness; short timeouts; hermetic ephemeral port. Real `#[test]`s (tokio), no ignore/tautology.
- CL1 `send_message` (text "ping") → `Ok(Task)` state `completed`, artifact part text `"pong:ping"`.
- CL2 `get_task(returned_id)` → the same Task; `get_task("nope")` → `Err(ClientError::Rpc{code:-32001,..})`.
- CL3 `cancel_task` on the completed task's id → `Err(Rpc{not-cancelable})`; on `"nope"` → `Err(Rpc{-32001})`.
- CL4 `fetch_agent_card` → the served `AgentCard` (assert name/url/capabilities).
- CL5 `send_message_streaming` → `Ok(vec)` = ordered `[Status(working,final=false), Artifact(pong),
  Status(completed,final=true)]`; assert states + the artifact text.
- CL6 executor error path: fake set to return `ExecError` → `send_message` → `Ok(Task)` with state `failed`
  (execution failures are a failed Task, NOT an Rpc error) — asserts the client surfaces failed tasks.
- CL7 transport error: point the client at an unused port (or shut the server first) → `Err(ClientError::Http)`.
- CL8 `ClientError` Display distinct per variant + `std::error::Error`; a malformed body → `Err(Decode)`
  (drive via a tiny hand-rolled axum route returning junk, or assert the Decode arm through a crafted case).

Coverage: hit Ok / Rpc-error / Http-error / Decode for the request methods, the streaming happy path, the
failed-Task path, and the agent-card GET. Delete unreachable branches rather than padding.
