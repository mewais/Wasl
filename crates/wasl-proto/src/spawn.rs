//! Agents spawning workers on demand (ARCHITECTURE §5 `spawn_agent`).
//!
//! An agent asks its node to start a new agent ([`SpawnRequest`], `wasl.spawn`). Wasl never
//! launches processes itself: the node asks the *spawn authority* — the supervisor — in two
//! short calls. [`methods::SUPERVISOR_SPAWN_PLAN`](crate::methods::SUPERVISOR_SPAWN_PLAN)
//! checks the request against the supervisor's policy and limits and resolves it into a
//! [`SpawnPlan`] (where, which CLI and model, which workstream, and whether a human must
//! approve). If approval is needed the node raises a [`wasit_core::GateRequest::Spawn`] on the
//! calling session (request id [`spawn_request_id`], choices [`spawn_choices`]) and waits for
//! the answer, exactly like a handoff gate. On approval
//! [`methods::SUPERVISOR_SPAWN_LAUNCH`](crate::methods::SUPERVISOR_SPAWN_LAUNCH) launches the
//! agent, records it in the workstream (with a connection from the caller) and returns the new
//! topology. Both supervisor calls are idempotent by [`SpawnRequest::request_id`], which the
//! caller reuses when it retries, so a restart anywhere never starts the agent twice.
//!
//! Without a supervisor (standalone Wasl) the node answers [`codes::SPAWN_UNAVAILABLE`].

use serde::{Deserialize, Serialize};
use wasit_core::{ChoiceText, GateChoice, GateChoiceKind, GateDecision};

use crate::{codes, AgentId, DelegateError, TaskBrief, Topology};

/// What an agent asks for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SpawnSpec {
    /// Name of the new agent (unique in the workstream; peers address it by name).
    pub name: String,
    /// What the new agent is for (shown to the human and to its peers).
    pub role: String,
    /// Agent CLI (driver id); one of the drivers installed on the target host. `None` = the
    /// caller's own driver.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub driver: Option<String>,
    /// Host id or name. `None` = the caller's host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Working directory. `None` = the caller's directory (same host only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// The new agent's mode (as the CLI names it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// A first task, delegated to the new agent as soon as it exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction: Option<String>,
    /// Seconds from the request until the first task's deadline (default: the workstream's).
    /// Never later than the deadline of the task the caller is working on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_secs: Option<u64>,
}

/// `wasl.spawn` (MCP front-end → node). Waits for the human's decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SpawnRequest {
    /// The calling session (on this host).
    pub from: String,
    /// Chosen by the caller and reused on retries (idempotency key).
    pub request_id: String,
    pub spec: SpawnSpec,
}

/// `wasl.spawn.cancel` (MCP front-end → node): the caller stopped waiting for a request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SpawnCancel {
    /// The calling session (on this host).
    pub from: String,
    pub request_id: String,
}

/// Result of `wasl.spawn`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SpawnReply {
    pub agent: AgentId,
    pub name: String,
    /// The workstream it joined (id).
    pub workstream: String,
    /// The first task, when an instruction was given and could be delegated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskBrief>,
    /// Why the first task could not be delegated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_error: Option<DelegateError>,
}

/// `wasl.supervisor.spawn_plan` (node → supervisor).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SpawnAsk {
    pub request_id: String,
    pub parent: AgentId,
    pub spec: SpawnSpec,
}

/// Whether the human must approve.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum SpawnApproval {
    /// Raise a spawn gate on the calling session.
    Ask,
    /// The human chose "always allow" for this workstream.
    Allowed,
}

/// The request resolved by the supervisor (display form for the gate; ids in `details`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SpawnPlan {
    pub approval: SpawnApproval,
    /// Name of the calling agent.
    pub from: String,
    pub name: String,
    pub role: String,
    /// Host display name.
    pub host: String,
    /// Driver display name.
    pub driver: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub cwd: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// Workstream display name.
    pub workstream: String,
    /// Approving creates `workstream` (the caller is in none).
    #[serde(default)]
    pub new_workstream: bool,
    /// Ids and limits (host id, driver id, workstream id, depth, …).
    #[serde(default)]
    pub details: serde_json::Value,
}

/// How a spawn was approved (sent with the launch).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum SpawnDecision {
    AllowOnce,
    /// Always allow spawning in this workstream (connections to spawned agents still ask).
    AlwaysAllow,
    /// Always allow spawning, and handoffs to spawned agents.
    AlwaysAllowWithHandoffs,
    Deny {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    /// No gate: the workstream's policy allowed it.
    Policy,
}

/// `wasl.supervisor.spawn_launch` (node → supervisor).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SpawnLaunch {
    pub request_id: String,
    pub parent: AgentId,
    pub spec: SpawnSpec,
    pub decision: SpawnDecision,
}

/// Result of `wasl.supervisor.spawn_launch`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SpawnLaunched {
    pub agent: AgentId,
    pub name: String,
    pub workstream: String,
    /// The workstream's topology with the new agent (the node applies it before delegating).
    pub topology: Topology,
}

/// `wasl.dismiss` (MCP front-end → node).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct DismissRequest {
    pub from: String,
    /// Name or id of an agent the caller spawned.
    pub peer: String,
}

/// `wasl.supervisor.dismiss` (node → supervisor).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct DismissAsk {
    pub parent: AgentId,
    pub peer: String,
}

/// Result of a dismissal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct DismissReply {
    pub agent: AgentId,
    pub name: String,
}

/// `wasl.supervisor.spawnable` (node → supervisor).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SpawnableAsk {
    pub parent: AgentId,
}

/// One driver an agent may spawn on a host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SpawnDriver {
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<String>,
}

/// One host an agent may spawn on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SpawnHost {
    pub id: String,
    pub name: String,
    pub drivers: Vec<SpawnDriver>,
}

/// What the caller may spawn (in `list_peers`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct Spawnable {
    pub available: bool,
    /// Why not, or what applies (e.g. "every spawn asks the human").
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub note: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hosts: Vec<SpawnHost>,
    /// How many more agents may be spawned in the caller's workstream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining: Option<u32>,
}

impl Spawnable {
    pub fn unavailable(note: impl Into<String>) -> Self {
        Spawnable {
            available: false,
            note: note.into(),
            hosts: Vec::new(),
            remaining: None,
        }
    }
}

/// Choice ids of a spawn gate.
pub mod spawn_choice {
    pub const ALLOW_ONCE: &str = "allow_once";
    pub const ALWAYS_ALLOW: &str = "always_allow_spawning";
    pub const ALWAYS_ALLOW_WITH_HANDOFFS: &str = "always_allow_spawning_and_handoffs";
    pub const DENY: &str = "deny";
}

/// The fixed choices of a spawn gate.
pub fn spawn_choices() -> Vec<GateChoice> {
    vec![
        GateChoice::agent(spawn_choice::ALLOW_ONCE, "Allow once", GateChoiceKind::AllowOnce),
        GateChoice::agent(
            spawn_choice::ALWAYS_ALLOW,
            "Always allow spawning in this workstream",
            GateChoiceKind::AllowAlways,
        )
        .with_description(
            "sets the workstream's spawn policy to always allow; handoffs to spawned agents still ask",
        ),
        GateChoice::agent(
            spawn_choice::ALWAYS_ALLOW_WITH_HANDOFFS,
            "Always allow spawning and handoffs to spawned agents",
            GateChoiceKind::AllowAlways,
        )
        .with_description(
            "also lets this workstream's agents hand work to the agents they spawn without asking",
        ),
        GateChoice::agent(spawn_choice::DENY, "Deny", GateChoiceKind::RejectOnce)
            .with_text(ChoiceText::Optional),
    ]
}

impl SpawnDecision {
    /// The decision a spawn gate answer stands for (`None` for an unknown choice).
    pub fn from_gate(d: &GateDecision) -> Option<Self> {
        match d.choice.as_str() {
            spawn_choice::ALLOW_ONCE => Some(SpawnDecision::AllowOnce),
            spawn_choice::ALWAYS_ALLOW => Some(SpawnDecision::AlwaysAllow),
            spawn_choice::ALWAYS_ALLOW_WITH_HANDOFFS => {
                Some(SpawnDecision::AlwaysAllowWithHandoffs)
            }
            spawn_choice::DENY => Some(SpawnDecision::Deny {
                reason: d.text.clone().filter(|t| !t.trim().is_empty()),
            }),
            _ => None,
        }
    }

    pub fn to_gate(&self) -> GateDecision {
        match self {
            SpawnDecision::AllowOnce | SpawnDecision::Policy => {
                GateDecision::choose(spawn_choice::ALLOW_ONCE)
            }
            SpawnDecision::AlwaysAllow => GateDecision::choose(spawn_choice::ALWAYS_ALLOW),
            SpawnDecision::AlwaysAllowWithHandoffs => {
                GateDecision::choose(spawn_choice::ALWAYS_ALLOW_WITH_HANDOFFS)
            }
            SpawnDecision::Deny { reason } => {
                GateDecision::choose_with(spawn_choice::DENY, reason.as_deref())
            }
        }
    }
}

/// The gate request id of a spawn request.
pub fn spawn_request_id(request_id: &str) -> String {
    format!("wasl-spawn-{request_id}")
}

/// Build the gate for a plan.
pub fn spawn_gate(
    request_id: &str,
    plan: &SpawnPlan,
    instruction: Option<String>,
) -> wasit_core::GateRequest {
    wasit_core::GateRequest::Spawn {
        request_id: spawn_request_id(request_id),
        spawn: Box::new(wasit_core::SpawnGate {
            from: plan.from.clone(),
            name: plan.name.clone(),
            role: plan.role.clone(),
            host: plan.host.clone(),
            driver: plan.driver.clone(),
            model: plan.model.clone(),
            cwd: plan.cwd.clone(),
            mode: plan.mode.clone(),
            instruction,
            workstream: plan.workstream.clone(),
            new_workstream: plan.new_workstream,
            details: plan.details.clone(),
        }),
        choices: spawn_choices(),
    }
}

/// `true` for the error codes a spawn request ends with without anything being started.
pub fn is_spawn_refusal(code: &str) -> bool {
    matches!(
        code,
        codes::SPAWN_UNAVAILABLE | codes::SPAWN_REFUSED | codes::SPAWN_LIMIT | codes::SPAWN_DENIED
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spawn_decisions_map_to_gate_answers_and_back() {
        for d in [
            SpawnDecision::AllowOnce,
            SpawnDecision::AlwaysAllow,
            SpawnDecision::AlwaysAllowWithHandoffs,
            SpawnDecision::Deny {
                reason: Some("not now".into()),
            },
            SpawnDecision::Deny { reason: None },
        ] {
            assert_eq!(SpawnDecision::from_gate(&d.to_gate()), Some(d));
        }
        assert!(spawn_choices()
            .iter()
            .all(|c| SpawnDecision::from_gate(&GateDecision::choose(&c.id)).is_some()));
    }
}
