# WP — `wasl-proto` — spec (authoritative)

**Tier:** L0 (pure, serde-only). **Deps:** `serde` (+ `serde_json` dev only). No IO, no `unsafe`.

Wire + domain types for the Wasl MCP layer. All types `#[derive(Debug, Clone, PartialEq, Eq, Serialize,
Deserialize)]` unless noted. Field/variant serde forms are EXACT — tests assert them.

## 1. Identity
```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HostId(pub String);
```
- Both: `pub fn as_str(&self) -> &str`. `serde(transparent)` → an `AgentId("a")` serializes as the bare
  string `"a"` (not an object).

### AgentCard — peer identity / discovery
```rust
pub struct AgentCard {
    pub id: AgentId,
    pub host: HostId,
    #[serde(default)] pub endpoint: Option<String>,    // how the mesh reaches it; None = local daemon
}
```
`#[serde(default)]` fields omit-and-default when absent.

## 2. Tool params + results (each tool takes one-OR-many peers → multicast/broadcast)
serde: snake_case field names.
```rust
pub struct ConsultParams  { pub peers: Vec<AgentId>, pub question: String }
pub struct CommandParams  { pub peers: Vec<AgentId>, pub task: String }
pub struct MessageParams  { pub peers: Vec<AgentId>, pub payload: String }
pub struct TransferParams { pub src_host: HostId, pub dst_hosts: Vec<HostId>, pub path: String }

pub struct Reply { pub from: AgentId, pub body: String }

pub struct ConsultResult  { pub replies: Vec<Reply> }
pub struct CommandResult  { pub replies: Vec<Reply> }
pub struct MessageResult  { pub delivered: Vec<AgentId> }
pub struct TransferResult { pub delivered: Vec<HostId> }
```
Helpers:
- `ConsultParams::is_broadcast(&self) -> bool` → `peers.len() > 1` (also true for command/message via the
  same-named method on each — or a free `fn is_multicast(peers: &[AgentId]) -> bool`). Provide
  `pub fn is_multicast<T>(targets: &[T]) -> bool` (generic over `AgentId`/`HostId`): `targets.len() > 1`.

## 3. Envelope (a routed inter-agent item)
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EnvelopeKind { Consult, Command, Message }   // "consult" | "command" | "message"

pub struct Envelope {
    pub id: String,
    pub kind: EnvelopeKind,
    pub from: AgentId,
    pub to: Vec<AgentId>,
    pub body: String,
}
```

## 4. Gate (permission-before-send)
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GateKind { Consult, Command, Message, Transfer }

pub struct GateRequest {
    pub id: String,
    pub kind: GateKind,
    pub from: AgentId,
    pub summary: String,   // one-line human description ("send message to a,b")
    pub preview: String,   // the content/payload preview shown to the human (NEVER secrets)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "lowercase")]
pub enum GateDecision {
    Approve,
    Deny { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ApprovalToken(pub String);   // opaque token for the Tier-2 prepare->commit handshake
```
- `GateDecision::is_approved(&self) -> bool`.

## 5. Elicitation (MCP `elicitation/create`, spec rev 2025-06-18)
Wasl uses **message-only** elicitation (empty schema) for a yes/no/cancel approval — valid per spec and
the native shape Codex/Claude render for approvals. NEVER carries secrets.
```rust
pub struct ElicitationCreate { pub message: String }   // requestedSchema is an empty object at the MCP layer

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ElicitationAction { Accept, Decline, Cancel }  // "accept" | "decline" | "cancel"

pub struct ElicitationResponse { pub action: ElicitationAction }
```
- `ElicitationResponse::to_decision(&self) -> Option<GateDecision>`:
  `Accept → Some(GateDecision::Approve)`; `Decline → Some(GateDecision::Deny { reason: "declined" })`;
  `Cancel → None` (dismissed — neither approve nor deny; caller retries/aborts).

## 6. Error
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtoError { Malformed(String) }   // impl Display + std::error::Error
```
- `pub fn parse_consult(json: &str) -> Result<ConsultParams, ProtoError>` (and `parse_command`,
  `parse_message`, `parse_transfer`) — deserialize tool-call params, mapping serde errors to
  `ProtoError::Malformed`. (Thin wrappers proving the parse/error path.)

---

## Test plan (test-agent: `tests/happy.rs`, `tests/errors.rs`; 100% line+region). Every case a real
asserting `#[test]`; no `#[ignore]`/stub/tautology.

**happy.rs**
- H1  `AgentId`/`HostId` serde transparent → bare string `"a"`; round-trip; `as_str`.
- H2  `AgentCard` round-trips; optional fields omitted-and-default to `None`; a full card round-trips.
- H3  `ConsultParams`/`CommandParams`/`MessageParams` round-trip with 1 and many peers.
- H4  `TransferParams` round-trips (src_host + multiple dst_hosts + path).
- H5  results (`ConsultResult`/`CommandResult`/`MessageResult`/`TransferResult` + `Reply`) round-trip.
- H6  `is_multicast`: `&[x]` → false; `&[x,y]` → true; empty → false. (For both AgentId and HostId.)
- H7  `EnvelopeKind`/`GateKind` serde ↔ lowercase strings, each variant.
- H8  `Envelope` round-trips.
- H9  `GateDecision` serde: `Approve` → `{"decision":"approve"}`; `Deny{reason}` →
      `{"decision":"deny","reason":"…"}`; `is_approved` for each.
- H10 `ApprovalToken` transparent string round-trip.
- H11 `ElicitationCreate` round-trip; `ElicitationAction` ↔ "accept"/"decline"/"cancel".
- H12 `ElicitationResponse::to_decision`: Accept→Approve; Decline→Deny("declined"); Cancel→None.

**errors.rs**
- E1  `parse_consult` on valid JSON → Ok with fields; on malformed JSON → `Err(ProtoError::Malformed(_))`.
- E2  same for `parse_command` / `parse_message` / `parse_transfer` (valid + malformed each).
- E3  `ProtoError` `Display` non-empty; `std::error::Error` implemented (source() is None).

Coverage: exercise every serde variant + every helper branch. Delete unreachable branches rather than
leaving them uncovered.
