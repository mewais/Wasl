//! wasl-proto — Wasl wire + domain types (WP).
//!
//! Pure, serde-only types for the Wasl agent-to-agent MCP layer: agent cards,
//! the four tool params/results (consult / command / message / transfer, each
//! supporting one-or-many peers for multicast), reply envelopes, the gate
//! request/decision/token types, and the MCP `elicitation/create` wire shapes.
//!
//! See `docs/specs/proto.md` for the authoritative contract.

use std::fmt;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------
// 1. Identity
// ---------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HostId(pub String);

impl AgentId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl HostId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCard {
    pub id: AgentId,
    pub host: HostId,
    #[serde(default)]
    pub endpoint: Option<String>,
}

// ---------------------------------------------------------------------
// 2. Tool params + results
// ---------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsultParams {
    pub peers: Vec<AgentId>,
    pub question: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandParams {
    pub peers: Vec<AgentId>,
    pub task: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageParams {
    pub peers: Vec<AgentId>,
    pub payload: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferParams {
    pub src_host: HostId,
    pub dst_hosts: Vec<HostId>,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reply {
    pub from: AgentId,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsultResult {
    pub replies: Vec<Reply>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandResult {
    pub replies: Vec<Reply>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageResult {
    pub delivered: Vec<AgentId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferResult {
    pub delivered: Vec<HostId>,
}

/// `true` when more than one target is addressed (broadcast/multicast).
pub fn is_multicast<T>(targets: &[T]) -> bool {
    targets.len() > 1
}

// ---------------------------------------------------------------------
// 3. Envelope
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EnvelopeKind {
    Consult,
    Command,
    Message,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub id: String,
    pub kind: EnvelopeKind,
    pub from: AgentId,
    pub to: Vec<AgentId>,
    pub body: String,
}

// ---------------------------------------------------------------------
// 4. Gate
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GateKind {
    Consult,
    Command,
    Message,
    Transfer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateRequest {
    pub id: String,
    pub kind: GateKind,
    pub from: AgentId,
    pub summary: String,
    pub preview: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "lowercase")]
pub enum GateDecision {
    Approve,
    Deny { reason: String },
}

impl GateDecision {
    pub fn is_approved(&self) -> bool {
        matches!(self, GateDecision::Approve)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ApprovalToken(pub String);

// ---------------------------------------------------------------------
// 5. Elicitation
// ---------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ElicitationCreate {
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ElicitationAction {
    Accept,
    Decline,
    Cancel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ElicitationResponse {
    pub action: ElicitationAction,
}

impl ElicitationResponse {
    pub fn to_decision(&self) -> Option<GateDecision> {
        match self.action {
            ElicitationAction::Accept => Some(GateDecision::Approve),
            ElicitationAction::Decline => Some(GateDecision::Deny {
                reason: "declined".to_string(),
            }),
            ElicitationAction::Cancel => None,
        }
    }
}

// ---------------------------------------------------------------------
// 6. Error
// ---------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtoError {
    Malformed(String),
}

impl fmt::Display for ProtoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtoError::Malformed(msg) => write!(f, "malformed proto payload: {msg}"),
        }
    }
}

impl std::error::Error for ProtoError {}

pub fn parse_consult(payload: &str) -> Result<ConsultParams, ProtoError> {
    serde_json::from_str(payload).map_err(|e| ProtoError::Malformed(e.to_string()))
}

pub fn parse_command(payload: &str) -> Result<CommandParams, ProtoError> {
    serde_json::from_str(payload).map_err(|e| ProtoError::Malformed(e.to_string()))
}

pub fn parse_message(payload: &str) -> Result<MessageParams, ProtoError> {
    serde_json::from_str(payload).map_err(|e| ProtoError::Malformed(e.to_string()))
}

pub fn parse_transfer(payload: &str) -> Result<TransferParams, ProtoError> {
    serde_json::from_str(payload).map_err(|e| ProtoError::Malformed(e.to_string()))
}
