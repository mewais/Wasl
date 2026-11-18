//! wasl-proto — every type that crosses Wasl's control surface (ARCHITECTURE §5 of the Wasit
//! architecture document): agent addresses, peer cards, the live topology a supervisor pushes,
//! tasks and their states, structured results, handoff decisions, and the `wasl.*` method /
//! topic names. Defined once; the node, the MCP front-end and supervisors all use these.
//!
//! # Methods (`Request.method` → params → result)
//!
//! | method | params | result | caller |
//! |---|---|---|---|
//! | [`methods::TOPOLOGY_SET`] | [`Topology`] | [`TopologyApplied`] | supervisor |
//! | [`methods::TOPOLOGY_GET`] | — | [`TopologyList`] | supervisor |
//! | [`methods::PEERS_LIST`] | [`PeersRequest`] | [`PeerList`] | MCP front-end |
//! | [`methods::DELEGATE`] | [`DelegateRequest`] | [`DelegateReply`] | MCP front-end |
//! | [`methods::MESSAGE`] | [`MessageRequest`] | [`DelegateReply`] | MCP front-end |
//! | [`methods::TASK_STATUS`] | [`TaskIds`] | [`TaskStates`] | MCP front-end, supervisor |
//! | [`methods::TASK_WAIT`] | [`WaitRequest`] | [`TaskStates`] | MCP front-end |
//! | [`methods::TASK_CANCEL`] | [`CancelRequest`] | [`TaskBrief`] | MCP front-end, supervisor |
//! | [`methods::TASK_DECIDE`] | [`DecideRequest`] | [`TaskBrief`] | MCP front-end (standalone elicitation) |
//! | [`methods::TASKS_LIST`] | — | [`TaskList`] | supervisor |
//! | [`methods::NODE_DELIVER`] | [`DeliverRequest`] | [`Ack`] | node → node |
//! | [`methods::NODE_UPDATE`] | [`UpdateRequest`] | [`Ack`] | node → node |
//! | [`methods::NODE_CANCEL`] | [`CancelRemoteRequest`] | [`Ack`] | node → node |
//! | [`methods::NODE_PEER_STATUS`] | [`PeerStatusRequest`] | [`PeerStatusReply`] | node → node |
//!
//! Errors are `RpcError`s; Wasl-specific codes are in [`codes`].
//!
//! # Topics (`Subscribe.topic` → payload; cursor = the node journal's sequence number)
//!
//! | topic | payload |
//! |---|---|
//! | [`topics::TASKS`] | [`TaskEvent`] — replay from a cursor yields the latest state of every task changed after it, then live changes |
//! | [`topics::EDGES`] | [`EdgeEvent`] — edge policy changes (e.g. "always allow" chosen in a handoff gate) for the supervisor to persist |
//!
//! # Handoff gates
//!
//! A delegation over an `ask` edge raises a [`wasit_core::GateRequest::Handoff`] on the
//! *sending* session (request id = [`handoff_request_id`]). It is answered like any gate, with
//! `session.command` / `answer_gate`; [`HandoffDecision::from_gate`] defines the mapping: a
//! denial denies, an approval with any `permission_updates` means "always allow on this edge",
//! any other approval allows once.

#![forbid(unsafe_code)]

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use wasit_core::{GateDecision, Usage};

/// Method names.
pub mod methods {
    pub const TOPOLOGY_SET: &str = "wasl.topology.set";
    pub const TOPOLOGY_GET: &str = "wasl.topology.get";
    pub const PEERS_LIST: &str = "wasl.peers.list";
    pub const DELEGATE: &str = "wasl.delegate";
    pub const MESSAGE: &str = "wasl.message";
    pub const TASK_STATUS: &str = "wasl.task.status";
    pub const TASK_WAIT: &str = "wasl.task.wait";
    pub const TASK_CANCEL: &str = "wasl.task.cancel";
    pub const TASK_DECIDE: &str = "wasl.task.decide";
    pub const TASKS_LIST: &str = "wasl.tasks.list";
    pub const NODE_DELIVER: &str = "wasl.node.deliver";
    pub const NODE_UPDATE: &str = "wasl.node.update";
    pub const NODE_CANCEL: &str = "wasl.node.cancel";
    pub const NODE_PEER_STATUS: &str = "wasl.node.peer_status";
}

/// Topic names.
pub mod topics {
    pub const TASKS: &str = "wasl.tasks";
    pub const EDGES: &str = "wasl.edges";
}

/// The route prefix the node serves (register it in a `ServiceMux`).
pub const ROUTES: &[&str] = &["wasl."];

/// Wasl error codes (in addition to the control protocol's).
pub mod codes {
    /// No outgoing edge from the caller to that peer.
    pub const NO_EDGE: &str = "no_edge";
    /// The peer reference matches no agent of the caller's topology.
    pub const UNKNOWN_PEER: &str = "unknown_peer";
    pub const UNKNOWN_TASK: &str = "unknown_task";
    /// The caller is not a session of this host.
    pub const UNKNOWN_SESSION: &str = "unknown_session";
    /// The delegation would re-enter an agent already in the causation chain.
    pub const CYCLE: &str = "cycle";
    /// The delegation would exceed the workstream's maximum hop depth.
    pub const MAX_DEPTH: &str = "max_depth";
    /// The workstream's budget is used up.
    pub const BUDGET: &str = "budget_exhausted";
    /// The edge policy is `deny`.
    pub const DENIED: &str = "denied";
    /// The request is not valid in the task's current state.
    pub const INVALID_STATE: &str = "invalid_state";
    /// A topology with an older version than the one in force.
    pub const STALE_TOPOLOGY: &str = "stale_topology";
    pub const IO: &str = "io";
}

// ── Addresses ───────────────────────────────────────────────────────────────

/// A peer is a *session* on a host: `AgentId` ↔ `(host_id, session_id)`. Serialized as the
/// string `"<host>/<session>"` (split at the last `/`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AgentId {
    pub host: String,
    pub session: String,
}

impl AgentId {
    pub fn new(host: impl Into<String>, session: impl Into<String>) -> Self {
        AgentId {
            host: host.into(),
            session: session.into(),
        }
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.host, self.session)
    }
}

/// Why a string is not an [`AgentId`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BadAgentId(pub String);

impl fmt::Display for BadAgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "not an agent id (expected <host>/<session>): {:?}",
            self.0
        )
    }
}

impl std::error::Error for BadAgentId {}

impl FromStr for AgentId {
    type Err = BadAgentId;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.rsplit_once('/') {
            Some((h, sess)) if !h.is_empty() && !sess.is_empty() => Ok(AgentId::new(h, sess)),
            _ => Err(BadAgentId(s.to_string())),
        }
    }
}

impl Serialize for AgentId {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for AgentId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

// ── Peers and topology ──────────────────────────────────────────────────────

/// Live status of a peer, derived from its session's phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PeerStatus {
    Starting,
    Idle,
    Working,
    NeedsYou,
    /// The process ended (it is continued when a task arrives).
    Stopped,
    Failed,
    /// Its host cannot be reached right now.
    Unreachable,
    #[default]
    Unknown,
}

/// What a supervisor tells the nodes about one agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerCard {
    pub agent: AgentId,
    /// Display name (unique within the workstream; agents may address peers by it).
    pub name: String,
    /// Driver id (`claude`, `opencode`, …), informational.
    #[serde(default)]
    pub driver: String,
    /// Role in the workstream (e.g. `reviewer`).
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub description: String,
    /// Filled in live by [`methods::PEERS_LIST`]; ignored in a pushed topology.
    #[serde(default)]
    pub status: PeerStatus,
}

/// Policy of one directed edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EdgePolicy {
    /// Every delegation raises a handoff gate on the sending session.
    #[default]
    Ask,
    AlwaysAllow,
    Deny,
}

/// A directed edge `from → to`: `from` may delegate to `to`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edge {
    pub from: AgentId,
    pub to: AgentId,
    #[serde(default)]
    pub policy: EdgePolicy,
}

/// Per-workstream safety limits.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    /// Maximum delegation depth (a task delegated by an agent that is itself working on a task
    /// of hop `n` has hop `n + 1`; the first delegation has hop 1).
    #[serde(default = "default_max_depth")]
    pub max_depth: u32,
    /// Deadline of a task whose delegation names none.
    #[serde(default = "default_deadline_secs")]
    pub default_deadline_secs: u64,
    /// Optional budget: once the reported cost of the workstream's tasks (as seen by each node)
    /// reaches it, further delegations are rejected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_usd: Option<f64>,
}

fn default_max_depth() -> u32 {
    4
}

fn default_deadline_secs() -> u64 {
    1800
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_depth: default_max_depth(),
            default_deadline_secs: default_deadline_secs(),
            budget_usd: None,
        }
    }
}

/// The graph of one workstream, pushed by a supervisor (`wasl.topology.set`). Idempotent and
/// versioned: a push with an older version than the one in force is refused, the same version
/// is a no-op. Empty `peers` and `edges` remove the workstream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Topology {
    pub workstream: String,
    pub version: u64,
    #[serde(default)]
    pub peers: Vec<PeerCard>,
    #[serde(default)]
    pub edges: Vec<Edge>,
    #[serde(default)]
    pub limits: Limits,
}

/// Result of [`methods::TOPOLOGY_SET`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologyApplied {
    pub workstream: String,
    pub version: u64,
    /// `false` when the same version was already in force.
    pub changed: bool,
}

/// Result of [`methods::TOPOLOGY_GET`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopologyList {
    pub workstreams: Vec<Topology>,
}

// ── Tasks ───────────────────────────────────────────────────────────────────

/// A task id (`t` + 16 lowercase base32 characters).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(pub String);

impl TaskId {
    pub fn random() -> Self {
        const A: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
        let s: String = (0..16).map(|_| A[fastrand::usize(..32)] as char).collect();
        TaskId(format!("t{s}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// What the sender expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskMode {
    /// A question; the receiver should answer and not change anything.
    Consult,
    /// Work to do; the receiver acts and reports what it did.
    Command,
    /// Information only; no reply is expected (fire-and-forget).
    Message,
}

impl TaskMode {
    pub fn as_str(self) -> &'static str {
        match self {
            TaskMode::Consult => "consult",
            TaskMode::Command => "command",
            TaskMode::Message => "message",
        }
    }
}

/// One delegation. Immutable once created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub workstream: String,
    pub from: AgentId,
    pub to: AgentId,
    pub mode: TaskMode,
    pub instruction: String,
    /// References (paths, URLs) the receiver should look at.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<String>,
    /// Absolute deadline (ms since the Unix epoch).
    pub deadline_ms: u64,
    pub created_ms: u64,
    /// Causation chain: the agents upstream of this task, oldest first, ending with `from`.
    pub chain: Vec<AgentId>,
    /// Hop count (`chain.len()`).
    pub hop: u32,
    /// The task `from` was working on when it delegated this one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<TaskId>,
}

/// How a pending task awaits approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalVia {
    /// A handoff gate on the sending session (answered by the supervisor / human).
    Gate,
    /// Standalone mode: the MCP front-end asks its client (MCP elicitation) and reports the
    /// answer with [`methods::TASK_DECIDE`].
    Elicitation,
}

/// What the receiver is doing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Progress {
    pub tool_calls: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_tool: Option<String>,
    /// The latest assistant text (truncated).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_text: Option<String>,
}

/// How the receiver's turn ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultStatus {
    Success,
    /// The turn completed but the CLI reported an error (e.g. max turns).
    Error,
}

/// A reference the receiver produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artifact {
    pub name: String,
    pub uri: String,
}

/// Structured result of a completed task.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskResult {
    pub status: ResultStatus,
    /// The receiver's final assistant message.
    pub summary: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<Artifact>,
    /// Usage of the receiver's turn (difference of its cumulative counters when reported).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
}

/// Task lifecycle. Terminal states are final.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TaskState {
    /// Waiting for a handoff decision.
    PendingApproval {
        via: ApprovalVia,
    },
    /// Approved; waiting for delivery or for the receiver to be free.
    Queued,
    Running {
        #[serde(default)]
        progress: Progress,
    },
    Completed {
        result: TaskResult,
    },
    Failed {
        error: String,
    },
    Cancelled {
        reason: String,
    },
    Denied {
        reason: String,
    },
    Expired,
}

impl TaskState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskState::Completed { .. }
                | TaskState::Failed { .. }
                | TaskState::Cancelled { .. }
                | TaskState::Denied { .. }
                | TaskState::Expired
        )
    }

    pub fn name(&self) -> &'static str {
        match self {
            TaskState::PendingApproval { .. } => "pending_approval",
            TaskState::Queued => "queued",
            TaskState::Running { .. } => "running",
            TaskState::Completed { .. } => "completed",
            TaskState::Failed { .. } => "failed",
            TaskState::Cancelled { .. } => "cancelled",
            TaskState::Denied { .. } => "denied",
            TaskState::Expired => "expired",
        }
    }
}

/// A task with its current state (the payload of [`topics::TASKS`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskView {
    pub task: Task,
    pub state: TaskState,
    pub updated_ms: u64,
}

/// Which side of a task this node is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    /// This node's session delegated it.
    Outbound,
    /// This node's session works on it.
    Inbound,
}

/// Payload of [`topics::TASKS`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskEvent {
    pub side: Side,
    #[serde(flatten)]
    pub view: TaskView,
}

/// A compact task state for agents.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskBrief {
    pub id: TaskId,
    pub to: AgentId,
    /// The peer's display name, when known.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub to_name: String,
    pub mode: TaskMode,
    #[serde(flatten)]
    pub state: TaskState,
}

// ── Handoff decisions ───────────────────────────────────────────────────────

/// A decision on a handoff.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum HandoffDecision {
    AllowOnce,
    /// Allow and set the edge's policy to `always_allow`.
    AlwaysAllow,
    Deny {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

impl HandoffDecision {
    /// Read a gate answer (see the crate docs).
    pub fn from_gate(d: &GateDecision) -> Self {
        match d {
            GateDecision::Deny { reason } => HandoffDecision::Deny {
                reason: reason.clone(),
            },
            GateDecision::Approve {
                permission_updates: Some(_),
                ..
            } => HandoffDecision::AlwaysAllow,
            GateDecision::Approve { .. } => HandoffDecision::AllowOnce,
        }
    }

    /// The gate answer that means this decision.
    pub fn to_gate(&self) -> GateDecision {
        match self {
            HandoffDecision::AllowOnce => GateDecision::approve(),
            HandoffDecision::AlwaysAllow => GateDecision::Approve {
                updated_input: None,
                permission_updates: Some(serde_json::json!({"edge": "always_allow"})),
                elicitation_content: None,
            },
            HandoffDecision::Deny { reason } => GateDecision::Deny {
                reason: reason.clone(),
            },
        }
    }
}

/// Request id of the handoff gate of a task.
pub fn handoff_request_id(task: &TaskId) -> String {
    format!("wasl-handoff-{task}")
}

// ── Requests and replies ────────────────────────────────────────────────────

/// Empty success.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Ack {}

/// [`methods::PEERS_LIST`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeersRequest {
    /// The calling session (on this host).
    pub from: String,
}

/// One peer reachable from the caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerView {
    #[serde(flatten)]
    pub card: PeerCard,
    pub policy: EdgePolicy,
    pub workstream: String,
}

/// Result of [`methods::PEERS_LIST`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerList {
    pub peers: Vec<PeerView>,
}

/// [`methods::DELEGATE`]. Returns at once with one entry per target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegateRequest {
    /// The calling session (on this host).
    pub from: String,
    /// Peers by agent id (`host/session`) or display name.
    pub to: Vec<String>,
    pub mode: TaskMode,
    pub instruction: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<String>,
}

/// One target of a delegation: the task, or why none was created.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DelegateOutcome {
    /// The reference as given.
    pub to: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskBrief>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<DelegateError>,
}

/// Why a delegation to one target was rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegateError {
    /// One of [`codes`].
    pub code: String,
    pub message: String,
}

/// Result of [`methods::DELEGATE`] and [`methods::MESSAGE`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DelegateReply {
    pub results: Vec<DelegateOutcome>,
}

/// [`methods::MESSAGE`]: fire-and-forget text to peers (a task of mode `message`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageRequest {
    pub from: String,
    pub to: Vec<String>,
    pub text: String,
}

/// [`methods::TASK_STATUS`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskIds {
    pub ids: Vec<TaskId>,
}

/// [`methods::TASK_WAIT`]: long-poll until every task (or, with `any`, at least one) is
/// terminal, or `timeout_ms` passes; then the current states.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitRequest {
    pub ids: Vec<TaskId>,
    pub timeout_ms: u64,
    #[serde(default)]
    pub any: bool,
}

/// Result of [`methods::TASK_STATUS`] / [`methods::TASK_WAIT`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskStates {
    pub tasks: Vec<TaskBrief>,
    /// Ids this node does not know.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unknown: Vec<TaskId>,
    /// The wait ended by its timeout.
    #[serde(default)]
    pub timed_out: bool,
}

/// [`methods::TASK_CANCEL`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRequest {
    pub id: TaskId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// [`methods::TASK_DECIDE`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecideRequest {
    pub id: TaskId,
    pub decision: HandoffDecision,
}

/// Result of [`methods::TASKS_LIST`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskList {
    pub outbound: Vec<TaskView>,
    pub inbound: Vec<TaskView>,
}

/// [`methods::NODE_DELIVER`]: the receiving host takes custody of a task (idempotent).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliverRequest {
    pub task: Task,
}

/// [`methods::NODE_UPDATE`]: the receiving host reports a state change (idempotent; terminal
/// states are final).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdateRequest {
    pub id: TaskId,
    pub state: TaskState,
}

/// [`methods::NODE_CANCEL`]: the sending host withdrew a task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRemoteRequest {
    pub id: TaskId,
    pub reason: String,
    /// The task expired at the sender rather than being cancelled.
    #[serde(default)]
    pub expired: bool,
}

/// [`methods::NODE_PEER_STATUS`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerStatusRequest {
    pub sessions: Vec<String>,
}

/// Result of [`methods::NODE_PEER_STATUS`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerStatusReply {
    pub statuses: Vec<(String, PeerStatus)>,
}

/// What changed an edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeChangeCause {
    /// A supervisor pushed a topology.
    Topology,
    /// "Always allow" was chosen in a handoff decision.
    Decision,
}

/// Payload of [`topics::EDGES`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeEvent {
    pub workstream: String,
    /// Topology version the change applies to.
    pub version: u64,
    pub from: AgentId,
    pub to: AgentId,
    pub policy: EdgePolicy,
    pub cause: EdgeChangeCause,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_ids_round_trip_as_strings() {
        let a = AgentId::new("box-1", "abcdefghijkl");
        let v = serde_json::to_value(&a).unwrap();
        assert_eq!(v, serde_json::json!("box-1/abcdefghijkl"));
        assert_eq!(serde_json::from_value::<AgentId>(v).unwrap(), a);
        assert_eq!(
            "a/b/c".parse::<AgentId>().unwrap(),
            AgentId::new("a/b", "c")
        );
        assert!("nohost".parse::<AgentId>().is_err());
        assert!("/x".parse::<AgentId>().is_err());
    }

    #[test]
    fn handoff_decisions_map_to_gate_answers_and_back() {
        for d in [
            HandoffDecision::AllowOnce,
            HandoffDecision::AlwaysAllow,
            HandoffDecision::Deny {
                reason: Some("no".into()),
            },
        ] {
            assert_eq!(HandoffDecision::from_gate(&d.to_gate()), d);
        }
    }

    #[test]
    fn states_are_tagged_and_terminality_is_known() {
        let s = TaskState::Completed {
            result: TaskResult {
                status: ResultStatus::Success,
                summary: "4".into(),
                artifacts: vec![],
                usage: None,
            },
        };
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["state"], "completed");
        assert!(s.is_terminal());
        assert!(!TaskState::Queued.is_terminal());
        let b = TaskBrief {
            id: TaskId("t1".into()),
            to: AgentId::new("h", "s"),
            to_name: String::new(),
            mode: TaskMode::Consult,
            state: TaskState::Queued,
        };
        let v = serde_json::to_value(&b).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"id":"t1","to":"h/s","mode":"consult","state":"queued"})
        );
        assert_eq!(serde_json::from_value::<TaskBrief>(v).unwrap(), b);
        let l: Limits = serde_json::from_str("{}").unwrap();
        assert_eq!(l, Limits::default());
    }
}
