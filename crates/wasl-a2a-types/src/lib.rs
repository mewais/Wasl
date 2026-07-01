//! wasl-a2a-types — faithful A2A (Agent2Agent) wire types (pure serde).
//!
//! A2A is JSON-RPC 2.0 over HTTP with camelCase fields. This crate models the
//! subset Wasl uses: Agent Card, Message + Part (text/file/data), Task + status
//! lifecycle, Artifact, the JSON-RPC params/results for message/send,
//! message/stream, tasks/get, tasks/cancel, and the SSE update events. No IO.
//! See `docs/specs/a2a-types.md`.

use serde::{Deserialize, Serialize};

/// Lifecycle state of a [`Task`]. Serializes kebab-case (`"input-required"`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskState {
    Submitted,
    Working,
    InputRequired,
    Completed,
    Canceled,
    Failed,
}

/// Author of a [`Message`]. Serializes lowercase (`"user"` / `"agent"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Agent,
}

/// A single content part, tagged by its lowercase `kind` discriminator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Part {
    Text { text: String },
    File { file: FileContent },
    Data { data: serde_json::Value },
}

/// File payload for a [`Part::File`]; exactly one of `bytes`/`uri` is set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileContent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    /// base64-encoded content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
}

/// A2A message. The wire form always carries `"kind":"message"`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    kind: String,
    pub role: Role,
    pub parts: Vec<Part>,
    pub message_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl Message {
    /// A user message consisting of a single text part.
    pub fn user_text(text: impl Into<String>, message_id: impl Into<String>) -> Message {
        Message {
            kind: "message".to_string(),
            role: Role::User,
            parts: vec![Part::Text { text: text.into() }],
            message_id: message_id.into(),
            task_id: None,
            context_id: None,
            metadata: None,
        }
    }

    /// An agent message consisting of a single text part.
    pub fn agent_text(text: impl Into<String>, message_id: impl Into<String>) -> Message {
        Message {
            kind: "message".to_string(),
            role: Role::Agent,
            parts: vec![Part::Text { text: text.into() }],
            message_id: message_id.into(),
            task_id: None,
            context_id: None,
            metadata: None,
        }
    }
}

/// Current status of a [`Task`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStatus {
    pub state: TaskState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<Message>,
    /// RFC3339 timestamp, caller-supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

/// An output artifact produced by a task.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Artifact {
    pub artifact_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub parts: Vec<Part>,
}

impl Artifact {
    /// An artifact consisting of a single text part.
    pub fn text(artifact_id: impl Into<String>, text: impl Into<String>) -> Artifact {
        Artifact {
            artifact_id: artifact_id.into(),
            name: None,
            description: None,
            parts: vec![Part::Text { text: text.into() }],
        }
    }
}

/// A2A task. The wire form always carries `"kind":"task"`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    kind: String,
    pub id: String,
    pub context_id: String,
    pub status: TaskStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<Artifact>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub history: Vec<Message>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl Task {
    /// A fresh task in `state` with empty artifacts/history.
    pub fn new(id: impl Into<String>, context_id: impl Into<String>, state: TaskState) -> Task {
        Task {
            kind: "task".to_string(),
            id: id.into(),
            context_id: context_id.into(),
            status: TaskStatus {
                state,
                message: None,
                timestamp: None,
            },
            artifacts: Vec::new(),
            history: Vec::new(),
            metadata: None,
        }
    }
}

/// The A2A discovery document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCard {
    pub name: String,
    pub description: String,
    /// The A2A endpoint base URL.
    pub url: String,
    pub version: String,
    pub capabilities: AgentCapabilities,
    #[serde(default)]
    pub default_input_modes: Vec<String>,
    #[serde(default)]
    pub default_output_modes: Vec<String>,
    #[serde(default)]
    pub skills: Vec<AgentSkill>,
}

/// Capabilities advertised in an [`AgentCard`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCapabilities {
    #[serde(default)]
    pub streaming: bool,
    #[serde(default)]
    pub push_notifications: bool,
}

/// A skill entry in an [`AgentCard`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentSkill {
    pub id: String,
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub examples: Vec<String>,
}

/// A JSON-RPC 2.0 request envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonRpcRequest<P> {
    pub jsonrpc: String,
    pub id: JsonRpcId,
    pub method: String,
    pub params: P,
}

impl<P> JsonRpcRequest<P> {
    /// Build a request with `jsonrpc` set to `"2.0"`.
    pub fn new(id: JsonRpcId, method: impl Into<String>, params: P) -> JsonRpcRequest<P> {
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id,
            method: method.into(),
            params,
        }
    }
}

/// A JSON-RPC request/response id: either a number or a string.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JsonRpcId {
    Num(i64),
    Str(String),
}

/// A JSON-RPC error object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// A JSON-RPC 2.0 response: `result` XOR `error`, never both.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JsonRpcResponse<R> {
    Ok {
        jsonrpc: String,
        id: JsonRpcId,
        result: R,
    },
    Err {
        jsonrpc: String,
        id: JsonRpcId,
        error: JsonRpcError,
    },
}

impl<R> JsonRpcResponse<R> {
    /// A success response with `jsonrpc` set to `"2.0"`.
    pub fn ok(id: JsonRpcId, result: R) -> JsonRpcResponse<R> {
        JsonRpcResponse::Ok {
            jsonrpc: "2.0".to_string(),
            id,
            result,
        }
    }

    /// An error response with `jsonrpc` set to `"2.0"`.
    pub fn err(id: JsonRpcId, error: JsonRpcError) -> JsonRpcResponse<R> {
        JsonRpcResponse::Err {
            jsonrpc: "2.0".to_string(),
            id,
            error,
        }
    }
}

/// Params for `message/send` (and `message/stream`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageSendParams {
    pub message: Message,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Params for `tasks/get` and `tasks/cancel`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskIdParams {
    pub id: String,
}

/// SSE event: a task status change. Wire form carries `"kind":"status-update"`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStatusUpdateEvent {
    kind: String,
    pub task_id: String,
    pub context_id: String,
    pub status: TaskStatus,
    /// A2A spells this wire key `final`.
    #[serde(rename = "final", default)]
    pub final_: bool,
}

/// SSE event: an artifact update. Wire form carries `"kind":"artifact-update"`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskArtifactUpdateEvent {
    kind: String,
    pub task_id: String,
    pub context_id: String,
    pub artifact: Artifact,
    #[serde(default)]
    pub append: bool,
    #[serde(default)]
    pub last_chunk: bool,
}

/// Extract the text of a [`Part::Text`], or `None` for other variants.
pub fn part_text(part: &Part) -> Option<&str> {
    match part {
        Part::Text { text } => Some(text),
        _ => None,
    }
}
