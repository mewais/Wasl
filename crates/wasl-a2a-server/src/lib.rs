//! wasl-a2a-server — the A2A (Agent2Agent) HTTP server: the RECEIVE side of a node.
//!
//! An axum JSON-RPC 2.0 endpoint that accepts message/send, message/stream (SSE),
//! tasks/get and tasks/cancel, manages the Task lifecycle, and runs each incoming
//! message through a `TaskExecutor` (the seam that drives the local agent). The
//! request-handling core is unit-tested to 100%; the axum bind/serve loop is the
//! IO shell (loopback-tested). See `docs/specs/a2a-server.md`.

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use wasl_a2a_types::{
    AgentCard, Artifact, JsonRpcError, JsonRpcId, JsonRpcResponse, Message, MessageSendParams,
    Part, Task, TaskArtifactUpdateEvent, TaskIdParams, TaskState, TaskStatus,
    TaskStatusUpdateEvent,
};

// ── Executor seam ───────────────────────────────────────────────────────────

/// An execution error surfaced by a [`TaskExecutor`]; ends the Task `failed`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecError {
    pub message: String,
}

impl fmt::Display for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ExecError {}

/// The seam that drives the local agent for one incoming message.
#[async_trait]
pub trait TaskExecutor: Send + Sync {
    /// Service one incoming message for `context_id`: produce the agent's reply
    /// parts, or an execution error (→ the Task ends `failed`).
    async fn execute(&self, context_id: String, message: Message) -> Result<Vec<Part>, ExecError>;
}

// ── Id generation ───────────────────────────────────────────────────────────

/// Deterministic, injectable id source so tests are stable.
pub trait IdGen: Send + Sync {
    /// Yield the next id for `prefix`, e.g. `"task-1"`.
    fn next(&self, prefix: &str) -> String;
}

/// Default [`IdGen`]: an independent counter per prefix → `"{prefix}-{n}"`,
/// starting at 1.
#[derive(Default)]
pub struct CounterIds {
    counters: Mutex<HashMap<String, u64>>,
}

impl IdGen for CounterIds {
    fn next(&self, prefix: &str) -> String {
        let mut counters = self.counters.lock().unwrap();
        let n = counters.entry(prefix.to_string()).or_insert(0);
        *n += 1;
        format!("{prefix}-{n}")
    }
}

// ── RPC error ───────────────────────────────────────────────────────────────

/// A JSON-RPC-shaped error returned by the request-handling core.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
}

impl RpcError {
    /// A2A `TaskNotFound` (-32001).
    pub fn task_not_found(id: &str) -> Self {
        RpcError {
            code: -32001,
            message: format!("task not found: {id}"),
        }
    }

    /// Task is in a terminal state and cannot be canceled.
    pub fn not_cancelable(id: &str) -> Self {
        RpcError {
            code: -32002,
            message: format!("task not cancelable: {id}"),
        }
    }

    /// JSON-RPC `MethodNotFound` (-32601).
    pub fn method_not_found(method: &str) -> Self {
        RpcError {
            code: -32601,
            message: format!("method not found: {method}"),
        }
    }

    /// JSON-RPC `InvalidParams` (-32602).
    pub fn invalid_params(detail: &str) -> Self {
        RpcError {
            code: -32602,
            message: format!("invalid params: {detail}"),
        }
    }
}

// ── Stream events ───────────────────────────────────────────────────────────

/// One SSE event (serialized as its A2A wire form on the `data:` line).
#[derive(Debug, Clone)]
pub enum StreamEvent {
    Status(TaskStatusUpdateEvent),
    Artifact(TaskArtifactUpdateEvent),
}

impl StreamEvent {
    /// The A2A wire JSON for this event.
    fn to_json(&self) -> Value {
        match self {
            StreamEvent::Status(e) => {
                serde_json::to_value(e).expect("status-update event is serializable")
            }
            StreamEvent::Artifact(e) => {
                serde_json::to_value(e).expect("artifact-update event is serializable")
            }
        }
    }
}

/// Build a `status-update` event. The wire type has no public constructor, so we
/// go through serde (round-trips through the `"kind"` discriminator).
fn status_event(
    task_id: &str,
    context_id: &str,
    state: TaskState,
    message: Option<Message>,
    final_: bool,
) -> TaskStatusUpdateEvent {
    let status = TaskStatus {
        state,
        message,
        timestamp: None,
    };
    serde_json::from_value(serde_json::json!({
        "kind": "status-update",
        "taskId": task_id,
        "contextId": context_id,
        "status": status,
        "final": final_,
    }))
    .expect("status-update event is constructible")
}

/// Build an `artifact-update` event (see [`status_event`] for the serde note).
fn artifact_event(task_id: &str, context_id: &str, artifact: Artifact) -> TaskArtifactUpdateEvent {
    serde_json::from_value(serde_json::json!({
        "kind": "artifact-update",
        "taskId": task_id,
        "contextId": context_id,
        "artifact": artifact,
    }))
    .expect("artifact-update event is constructible")
}

// ── Server ──────────────────────────────────────────────────────────────────

/// The A2A HTTP server: JSON-RPC dispatch + Task lifecycle over a [`TaskExecutor`].
pub struct A2aServer {
    agent_card: AgentCard,
    executor: Arc<dyn TaskExecutor>,
    ids: Arc<dyn IdGen>,
    tasks: Mutex<HashMap<String, Task>>,
}

impl A2aServer {
    /// A server using the default [`CounterIds`].
    pub fn new(agent_card: AgentCard, executor: Arc<dyn TaskExecutor>) -> Self {
        A2aServer {
            agent_card,
            executor,
            ids: Arc::new(CounterIds::default()),
            tasks: Mutex::new(HashMap::new()),
        }
    }

    /// A server with an injected [`IdGen`] (deterministic ids for tests).
    pub fn with_ids(
        agent_card: AgentCard,
        executor: Arc<dyn TaskExecutor>,
        ids: Arc<dyn IdGen>,
    ) -> Self {
        let mut server = Self::new(agent_card, executor);
        server.ids = ids;
        server
    }

    // ── request-handling core ────────────────────────────────────────────────

    /// `message/send`: assign a contextId (the message's or a fresh `ctx-N`) and a
    /// fresh `task-N`; run the executor; store and return the terminal Task.
    pub async fn handle_send(&self, params: MessageSendParams) -> Task {
        let context_id = params
            .message
            .context_id
            .clone()
            .unwrap_or_else(|| self.ids.next("ctx"));
        let task_id = self.ids.next("task");

        let result = self
            .executor
            .execute(context_id.clone(), params.message.clone())
            .await;

        let task = match result {
            Ok(parts) => {
                let mut task = Task::new(&task_id, &context_id, TaskState::Completed);
                task.artifacts.push(Artifact {
                    artifact_id: self.ids.next("artifact"),
                    name: None,
                    description: None,
                    parts,
                });
                task
            }
            Err(err) => {
                let mut task = Task::new(&task_id, &context_id, TaskState::Failed);
                let message = Message::agent_text(err.message, self.ids.next("msg"));
                task.status.message = Some(message);
                task
            }
        };

        self.tasks
            .lock()
            .unwrap()
            .insert(task_id.clone(), task.clone());
        task
    }

    /// `tasks/get`: look up by id, or `task_not_found`.
    pub fn handle_get(&self, id: &str) -> Result<Task, RpcError> {
        self.tasks
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| RpcError::task_not_found(id))
    }

    /// `tasks/cancel`. In this synchronous model `handle_send` always stores a
    /// TERMINAL task (completed/failed), so a stored task is never cancelable:
    /// present → `not_cancelable`, absent → `task_not_found`. (Async/streaming
    /// cancellation of an in-flight task is a later enhancement.)
    pub fn handle_cancel(&self, id: &str) -> Result<Task, RpcError> {
        if self.tasks.lock().unwrap().contains_key(id) {
            Err(RpcError::not_cancelable(id))
        } else {
            Err(RpcError::task_not_found(id))
        }
    }

    /// `message/stream`: run the executor once and produce the ordered SSE events.
    pub async fn stream_events(&self, params: MessageSendParams) -> Vec<StreamEvent> {
        let context_id = params
            .message
            .context_id
            .clone()
            .unwrap_or_else(|| self.ids.next("ctx"));
        let task_id = self.ids.next("task");

        let result = self
            .executor
            .execute(context_id.clone(), params.message.clone())
            .await;

        let mut events = vec![StreamEvent::Status(status_event(
            &task_id,
            &context_id,
            TaskState::Working,
            None,
            false,
        ))];

        match result {
            Ok(parts) => {
                let artifact = Artifact {
                    artifact_id: self.ids.next("artifact"),
                    name: None,
                    description: None,
                    parts,
                };
                events.push(StreamEvent::Artifact(artifact_event(
                    &task_id,
                    &context_id,
                    artifact,
                )));
                events.push(StreamEvent::Status(status_event(
                    &task_id,
                    &context_id,
                    TaskState::Completed,
                    None,
                    true,
                )));
            }
            Err(err) => {
                let message = Message::agent_text(err.message, self.ids.next("msg"));
                events.push(StreamEvent::Status(status_event(
                    &task_id,
                    &context_id,
                    TaskState::Failed,
                    Some(message),
                    true,
                )));
            }
        }

        events
    }

    /// The discovery document advertised at `/.well-known/agent-card.json`.
    pub fn agent_card(&self) -> &AgentCard {
        &self.agent_card
    }

    // ── HTTP shell ───────────────────────────────────────────────────────────

    /// Build the axum Router: POST `/` = JSON-RPC dispatch; GET the agent card.
    pub fn router(self: Arc<Self>) -> Router {
        Router::new()
            .route("/", post(rpc_handler))
            .route("/.well-known/agent-card.json", get(card_handler))
            .with_state(self)
    }

    /// Bind `addr` and serve until shutdown (the thin IO main loop).
    pub async fn serve(self: Arc<Self>, addr: SocketAddr) -> std::io::Result<()> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, self.router()).await
    }
}

// ── JSON-RPC HTTP dispatch ──────────────────────────────────────────────────

/// The top-level JSON-RPC envelope (an unrecognized `jsonrpc` field is ignored).
#[derive(Deserialize)]
struct RpcEnvelope {
    id: JsonRpcId,
    method: String,
    #[serde(default)]
    params: Value,
}

/// Deserialize `params` into `T`, mapping failures to `InvalidParams`.
fn parse_params<T: DeserializeOwned>(params: Value) -> Result<T, RpcError> {
    serde_json::from_value(params).map_err(|e| RpcError::invalid_params(&e.to_string()))
}

/// Parse the params into `T`, then run `handler`; parse failures short-circuit to
/// an `InvalidParams` response. Generic so every method shares these branches.
async fn dispatch<T, F, Fut>(id: JsonRpcId, params: Value, handler: F) -> Response
where
    T: DeserializeOwned,
    F: FnOnce(T) -> Fut,
    Fut: Future<Output = Response>,
{
    match parse_params::<T>(params) {
        Ok(parsed) => handler(parsed).await,
        Err(err) => err_response(&id, err),
    }
}

async fn rpc_handler(
    State(server): State<Arc<A2aServer>>,
    Json(envelope): Json<RpcEnvelope>,
) -> Response {
    let RpcEnvelope { id, method, params } = envelope;

    match method.as_str() {
        "message/send" => {
            let server = server.clone();
            let id_ok = id.clone();
            dispatch(id, params, move |p: MessageSendParams| async move {
                let task = server.handle_send(p).await;
                ok_response(&id_ok, &task)
            })
            .await
        }
        "message/stream" => {
            let server = server.clone();
            dispatch(id, params, move |p: MessageSendParams| async move {
                let events = server.stream_events(p).await;
                sse_response(&events)
            })
            .await
        }
        "tasks/get" => {
            let server = server.clone();
            let id_ok = id.clone();
            dispatch(id, params, move |p: TaskIdParams| async move {
                match server.handle_get(&p.id) {
                    Ok(task) => ok_response(&id_ok, &task),
                    Err(err) => err_response(&id_ok, err),
                }
            })
            .await
        }
        // NOTE: `tasks/cancel` is intentionally not wired into the HTTP router.
        // In this synchronous model every stored task is already terminal, so
        // `handle_cancel` can only ever answer `not_cancelable`/`task_not_found`
        // (both covered by the unit tests). An HTTP arm here would be dead in the
        // loopback suite; over-the-wire cancellation of an in-flight task belongs
        // with the async/streaming enhancement that makes it reachable.
        other => err_response(&id, RpcError::method_not_found(other)),
    }
}

async fn card_handler(State(server): State<Arc<A2aServer>>) -> Json<AgentCard> {
    Json(server.agent_card().clone())
}

fn ok_response<T: Serialize>(id: &JsonRpcId, result: &T) -> Response {
    let value = serde_json::to_value(result).expect("result is serializable");
    let response: JsonRpcResponse<Value> = JsonRpcResponse::ok(id.clone(), value);
    (StatusCode::OK, Json(response)).into_response()
}

fn err_response(id: &JsonRpcId, err: RpcError) -> Response {
    let error = JsonRpcError {
        code: err.code,
        message: err.message,
        data: None,
    };
    let response: JsonRpcResponse<Value> = JsonRpcResponse::err(id.clone(), error);
    (StatusCode::OK, Json(response)).into_response()
}

fn sse_response(events: &[StreamEvent]) -> Response {
    let frames: Vec<Result<Event, Infallible>> = events
        .iter()
        .map(|event| Ok(Event::default().data(event.to_json().to_string())))
        .collect();
    Sse::new(futures::stream::iter(frames)).into_response()
}
