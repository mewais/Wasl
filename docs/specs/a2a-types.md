# A1 — `wasl-a2a-types` — spec (authoritative)

**Tier:** L0 pure serde. **Deps:** `serde`, `serde_json` (both non-dev — round-trip helpers may live in src).
No `unsafe`, no IO. Faithful to the real **A2A** JSON shapes so this can talk to other A2A agents later.
A2A is JSON-RPC 2.0 over HTTP; JSON fields are **camelCase**; several types are tagged by a `kind` field.
This is a faithful CORE SUBSET (message/send + stream, tasks/get, tasks/cancel, the Task lifecycle, Parts,
Artifacts, Agent Card). Out of scope for this block (later): push-notification config, auth schemes,
extensions, gRPC/REST bindings.

## Enums
```rust
#[serde(rename_all = "kebab-case")]  // "submitted","working","input-required","completed","canceled","failed"
pub enum TaskState { Submitted, Working, InputRequired, Completed, Canceled, Failed }

#[serde(rename_all = "lowercase")]   // "user","agent"
pub enum Role { User, Agent }
```

## Parts (tagged by `kind`, lowercase: "text" | "file" | "data")
```rust
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Part {
    Text { text: String },
    File { file: FileContent },
    Data { data: serde_json::Value },
}

// Exactly one of bytes (base64) or uri is set; both optional fields, camelCase mimeType.
#[serde(rename_all = "camelCase")]
pub struct FileContent {
    #[serde(default, skip_serializing_if = "Option::is_none")] pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub bytes: Option<String>, // base64
    #[serde(default, skip_serializing_if = "Option::is_none")] pub uri: Option<String>,
}
```

## Message, Task, Artifact (all camelCase; a literal `kind` discriminator where A2A has one)
```rust
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub role: Role,
    pub parts: Vec<Part>,
    pub message_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub context_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub metadata: Option<serde_json::Value>,
    // A2A carries kind:"message"; model it as a defaulted constant field so round-trips preserve it.
    // (Implement via a `kind` field defaulting to "message", or a #[serde(default)] const — code-agent's choice,
    //  but the serialized JSON MUST contain "kind":"message".)
}

#[serde(rename_all = "camelCase")]
pub struct TaskStatus {
    pub state: TaskState,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub message: Option<Message>,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub timestamp: Option<String>, // RFC3339, caller-supplied
}

#[serde(rename_all = "camelCase")]
pub struct Artifact {
    pub artifact_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub description: Option<String>,
    pub parts: Vec<Part>,
}

#[serde(rename_all = "camelCase")]
pub struct Task {
    pub id: String,
    pub context_id: String,
    pub status: TaskStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")] pub artifacts: Vec<Artifact>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")] pub history: Vec<Message>,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub metadata: Option<serde_json::Value>,
    // serialized JSON MUST contain "kind":"task".
}
```

## Agent Card (the discovery document; camelCase)
```rust
#[serde(rename_all = "camelCase")]
pub struct AgentCard {
    pub name: String,
    pub description: String,
    pub url: String,          // the A2A endpoint base URL
    pub version: String,
    pub capabilities: AgentCapabilities,
    #[serde(default)] pub default_input_modes: Vec<String>,   // e.g. ["text/plain"]
    #[serde(default)] pub default_output_modes: Vec<String>,
    #[serde(default)] pub skills: Vec<AgentSkill>,
}
#[serde(rename_all = "camelCase")]
pub struct AgentCapabilities {
    #[serde(default)] pub streaming: bool,
    #[serde(default)] pub push_notifications: bool,
}
#[serde(rename_all = "camelCase")]
pub struct AgentSkill {
    pub id: String, pub name: String, pub description: String,
    #[serde(default)] pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")] pub examples: Vec<String>,
}
```

## JSON-RPC envelopes (generic)
```rust
#[serde(rename_all = "camelCase")]
pub struct JsonRpcRequest<P> { pub jsonrpc: String /* "2.0" */, pub id: JsonRpcId, pub method: String, pub params: P }
#[serde(untagged)] pub enum JsonRpcId { Num(i64), Str(String) }

pub struct JsonRpcError { pub code: i64, pub message: String,
    #[serde(default, skip_serializing_if="Option::is_none")] pub data: Option<serde_json::Value> }

// A response is either result or error (never both). Model as an enum that serializes to
// {jsonrpc,id,result} or {jsonrpc,id,error}.
pub enum JsonRpcResponse<R> { Ok { jsonrpc:String, id:JsonRpcId, result:R }, Err { jsonrpc:String, id:JsonRpcId, error:JsonRpcError } }
```
Provide constructors: `JsonRpcRequest::new(id, method, params)` (sets jsonrpc="2.0"),
`JsonRpcResponse::ok(id,result)` / `::err(id,error)`, and helpers to read them back.

## Method params/results
```rust
#[serde(rename_all = "camelCase")]
pub struct MessageSendParams {
    pub message: Message,
    #[serde(default, skip_serializing_if="Option::is_none")] pub metadata: Option<serde_json::Value>,
}
// message/send result is a Task (our nodes always create a Task). tasks/get + tasks/cancel:
#[serde(rename_all = "camelCase")]
pub struct TaskIdParams { pub id: String }
```

## SSE streaming events (for message/stream; each is one SSE `data:` JSON)
```rust
#[serde(rename_all = "camelCase")]
pub struct TaskStatusUpdateEvent { pub task_id:String, pub context_id:String, pub status:TaskStatus, #[serde(default)] pub final_: bool }
// NOTE: A2A's field is `final`; since `final` is a Rust keyword, use `final_` with #[serde(rename="final")].
// serialized JSON MUST contain "kind":"status-update".
#[serde(rename_all = "camelCase")]
pub struct TaskArtifactUpdateEvent { pub task_id:String, pub context_id:String, pub artifact:Artifact,
    #[serde(default)] pub append:bool, #[serde(default)] pub last_chunk:bool }
// serialized JSON MUST contain "kind":"artifact-update".
```

## Small constructors / helpers (pure, testable)
- `Message::user_text(text, message_id) -> Message` and `Message::agent_text(text, message_id)` (role +
  a single TextPart, kind message).
- `Task::new(id, context_id, state) -> Task` (empty artifacts/history).
- `Artifact::text(artifact_id, text) -> Artifact`.
- `part_text(&Part) -> Option<&str>` (extract text from a Text part).

## Test plan (`tests/`; 100% line+region; real `#[test]`s, no ignore/todo/tautology)
- T1 `Part` round-trips each variant AND the JSON carries the right `"kind"` ("text"/"file"/"data");
  `FileContent` omits `bytes`/`uri`/`mimeType` when None (assert absent keys), keeps camelCase `mimeType`.
- T2 `TaskState` serializes kebab-case (assert `"input-required"` exactly) and round-trips all variants.
- T3 `Message` round-trips incl. optional taskId/contextId absent vs present; JSON contains `"kind":"message"`
  and camelCase `messageId`/`contextId`. `Message::user_text`/`agent_text` produce the right role+part.
- T4 `Task` round-trips (with + without artifacts/history); JSON contains `"kind":"task"`, camelCase
  `contextId`; empty artifacts/history are omitted. `Task::new` + `Artifact::text` constructors.
- T5 `AgentCard` round-trips (capabilities camelCase `pushNotifications`; skills with/without examples).
- T6 JSON-RPC: `JsonRpcRequest::new` sets `jsonrpc:"2.0"`; round-trip a `message/send` request
  (params = MessageSendParams). `JsonRpcResponse::ok`/`err` serialize to the `result` XOR `error` shape and
  round-trip; `JsonRpcId` accepts a number AND a string.
- T7 SSE events: `TaskStatusUpdateEvent` serializes `"kind":"status-update"`, the `final` key (not `final_`),
  camelCase `taskId`/`contextId`; `TaskArtifactUpdateEvent` `"kind":"artifact-update"`. Round-trip both.
- T8 `part_text` returns the text for a Text part and None for File/Data.

Cross-check the exact JSON against the A2A spec field names (camelCase, the `kind` discriminators, kebab
`input-required`). Where this crate deviates from full A2A, it is a documented subset — do NOT invent fields
the spec doesn't have.
