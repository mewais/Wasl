//! wasl-a2a-client — the A2A client: the SEND side of a node.
//!
//! A reqwest-backed client that POSTs A2A JSON-RPC (message/send, message/stream
//! SSE, tasks/get, tasks/cancel) to a peer node's A2A endpoint and parses the
//! Task / AgentCard / stream events. Every method is a request/response with no
//! un-coverable shell, so this crate is unit-tested to 100% against a real
//! loopback `wasl-a2a-server`. See `docs/specs/a2a-client.md`.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::StreamExt;
use serde::Serialize;

use wasl_a2a_types::{
    AgentCard, JsonRpcId, JsonRpcRequest, JsonRpcResponse, Message, MessageSendParams, Task,
    TaskArtifactUpdateEvent, TaskIdParams, TaskStatusUpdateEvent,
};

// ── Errors ────────────────────────────────────────────────────────────────

/// A JSON-RPC error object surfaced by a peer (the `error` arm of a response).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcErr {
    pub code: i64,
    pub message: String,
}

/// Everything that can go wrong on a client call.
#[derive(Debug)]
pub enum ClientError {
    /// A transport-level failure (connect/send/read).
    Http(String),
    /// The peer answered with a JSON-RPC `error`.
    Rpc(RpcErr),
    /// The body was not the expected JSON shape.
    Decode(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Http(detail) => write!(f, "http transport error: {detail}"),
            ClientError::Rpc(err) => write!(f, "rpc error {}: {}", err.code, err.message),
            ClientError::Decode(detail) => write!(f, "decode error: {detail}"),
        }
    }
}

impl std::error::Error for ClientError {}

fn http_err(err: reqwest::Error) -> ClientError {
    ClientError::Http(err.to_string())
}

fn decode_err(err: reqwest::Error) -> ClientError {
    ClientError::Decode(err.to_string())
}

// ── Stream events ───────────────────────────────────────────────────────────

/// One parsed SSE update from a `message/stream` call, in wire order.
///
/// `untagged` so a `data:` frame decodes in one non-generic `from_str` call: serde
/// tries `Status` (needs `kind:"status-update"` + its fields) then `Artifact`; a
/// frame matching neither fails to deserialize → the caller maps it to `Decode`.
#[derive(serde::Deserialize)]
#[serde(untagged)]
pub enum A2aEvent {
    Status(TaskStatusUpdateEvent),
    Artifact(TaskArtifactUpdateEvent),
}

// ── Client ──────────────────────────────────────────────────────────────────

/// An A2A client pointed at one peer's endpoint base URL.
pub struct A2aClient {
    client: reqwest::Client,
    base_url: String,
    next_id: AtomicU64,
}

impl A2aClient {
    /// A client over a fresh default [`reqwest::Client`].
    pub fn new(base_url: impl Into<String>) -> Self {
        Self::with_client(base_url, reqwest::Client::new())
    }

    /// A client over a caller-supplied [`reqwest::Client`] (timeouts, proxies…).
    pub fn with_client(base_url: impl Into<String>, client: reqwest::Client) -> Self {
        A2aClient {
            client,
            base_url: base_url.into(),
            next_id: AtomicU64::new(1),
        }
    }

    fn next_id(&self) -> JsonRpcId {
        JsonRpcId::Num(self.next_id.fetch_add(1, Ordering::Relaxed) as i64)
    }

    /// Send `builder`, mapping a transport failure to [`ClientError::Http`].
    /// Non-generic on purpose: the transport-error path lives in ONE place (this
    /// fn) rather than per generic instantiation, so it is covered once.
    async fn send(
        &self,
        builder: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, ClientError> {
        builder.send().await.map_err(http_err)
    }

    /// POST a JSON-RPC request whose result is a [`Task`].
    async fn rpc<P: Serialize>(&self, method: &str, params: P) -> Result<Task, ClientError> {
        let request = JsonRpcRequest::new(self.next_id(), method, params);
        let builder = self.client.post(&self.base_url).json(&request);
        let response = self.send(builder).await?;
        parse_task(response).await
    }

    /// POST `message/send`.
    pub async fn send_message(&self, message: Message) -> Result<Task, ClientError> {
        self.rpc(
            "message/send",
            MessageSendParams {
                message,
                metadata: None,
            },
        )
        .await
    }

    /// POST `tasks/get`.
    pub async fn get_task(&self, id: &str) -> Result<Task, ClientError> {
        self.rpc("tasks/get", TaskIdParams { id: id.to_string() })
            .await
    }

    /// POST `tasks/cancel`.
    pub async fn cancel_task(&self, id: &str) -> Result<Task, ClientError> {
        self.rpc("tasks/cancel", TaskIdParams { id: id.to_string() })
            .await
    }

    /// GET `/.well-known/agent-card.json` (base URL is expected to end in `/`).
    pub async fn fetch_agent_card(&self) -> Result<AgentCard, ClientError> {
        let url = format!("{}.well-known/agent-card.json", self.base_url);
        let builder = self.client.get(url);
        let response = self.send(builder).await?;
        parse_card(response).await
    }

    /// POST `message/stream` and collect the SSE `data:` frames in order.
    pub async fn send_message_streaming(
        &self,
        message: Message,
    ) -> Result<Vec<A2aEvent>, ClientError> {
        let params = MessageSendParams {
            message,
            metadata: None,
        };
        let request = JsonRpcRequest::new(self.next_id(), "message/stream", params);
        let builder = self.client.post(&self.base_url).json(&request);
        let response = self.send(builder).await?;
        parse_stream(response).await
    }
}

/// Parse a JSON-RPC response body into `Ok(Task)` or `Err(Rpc)`.
async fn parse_task(response: reqwest::Response) -> Result<Task, ClientError> {
    match response
        .json::<JsonRpcResponse<Task>>()
        .await
        .map_err(decode_err)?
    {
        JsonRpcResponse::Ok { result, .. } => Ok(result),
        JsonRpcResponse::Err { error, .. } => Err(ClientError::Rpc(RpcErr {
            code: error.code,
            message: error.message,
        })),
    }
}

/// Parse the agent-card GET body.
async fn parse_card(response: reqwest::Response) -> Result<AgentCard, ClientError> {
    response.json::<AgentCard>().await.map_err(decode_err)
}

/// Drain the SSE byte stream, split on blank lines, and decode each `data:` frame.
async fn parse_stream(response: reqwest::Response) -> Result<Vec<A2aEvent>, ClientError> {
    let mut stream = response.bytes_stream();
    let mut buffer: Vec<u8> = Vec::new();
    while let Some(Ok(chunk)) = stream.next().await {
        buffer.extend_from_slice(&chunk);
    }

    let body = String::from_utf8_lossy(&buffer);
    let mut events = Vec::new();
    for frame in body.split("\n\n") {
        let frame = frame.trim();
        if frame.is_empty() {
            continue;
        }
        let payload = frame
            .strip_prefix("data:")
            .ok_or_else(|| ClientError::Decode(format!("SSE frame has no data field: {frame}")))?
            .trim_start();
        let event: A2aEvent =
            serde_json::from_str(payload).map_err(|e| ClientError::Decode(e.to_string()))?;
        events.push(event);
    }
    Ok(events)
}
