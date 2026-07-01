//! wasl-mcp `handle` — the synchronous, rmcp-free request→reply heart.
//!
//! This is the only place in the crate that carries decision-aware logic, and it
//! reimplements NONE of it: the gate outcome is decided upstream (in the rmcp
//! layer, by surfacing an elicitation), and this module merely
//!
//! 1. records the decision to the audit log **durability-first** (an audit
//!    failure fails the whole call, before any dispatch), and
//! 2. dispatches over the tested cores (`wasl_daemon` consult/command/message,
//!    `wasl_transfer` via `wasl_daemon::transfer`) **only** when the outcome is
//!    [`GateOutcome::Approved`].
//!
//! A non-`Approved` outcome NEVER touches the `Mesh`/`Runner` — that invariant is
//! the whole point of the gate and is proven in the unit tests. Because every
//! seam here (`Mesh`, `Runner`, `Sink`) is a trait, the entire heart is testable
//! with fakes, mirroring the daemon's test style.

use wasl_audit::{AuditEntry, AuditError, AuditLog, EntryKind, Sink};
use wasl_daemon::Mesh;
use wasl_gate::GateOutcome;
use wasl_proto::{
    AgentId, CommandParams, CommandResult, ConsultParams, ConsultResult, GateKind, HostId,
    MessageParams, MessageResult, TransferParams, TransferResult,
};
use wasl_registry::PeerRegistry;
use wasl_transfer::{Runner, Tool, TransferError};

// ---------------------------------------------------------------------
// 1. Types
// ---------------------------------------------------------------------

/// The invariant self-context for a dispatch: who we are, our host, and the
/// registry we resolve peers against.
pub struct ToolCtx<'a> {
    pub self_id: &'a AgentId,
    pub self_host: &'a HostId,
    pub registry: &'a PeerRegistry,
}

/// The unified reply of a gated tool call: the tested-core result on approval,
/// or a `Blocked` marker (with a human reason) when the gate did not approve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolReply {
    Consult(ConsultResult),
    Command(CommandResult),
    Message(MessageResult),
    Transfer(TransferResult),
    Blocked { reason: String },
}

impl ToolReply {
    /// `true` when the call was gated off (never dispatched).
    pub fn is_blocked(&self) -> bool {
        matches!(self, ToolReply::Blocked { .. })
    }
}

/// Which tool's params to dispatch on approval. Keeping the params inside the
/// enum lets `resolve_and_dispatch` stay one function over all four tools.
pub enum ToolCall {
    Consult(ConsultParams),
    Command(CommandParams),
    Message(MessageParams),
    Transfer { tool: Tool, params: TransferParams },
}

impl ToolCall {
    fn gate_kind(&self) -> GateKind {
        match self {
            ToolCall::Consult(_) => GateKind::Consult,
            ToolCall::Command(_) => GateKind::Command,
            ToolCall::Message(_) => GateKind::Message,
            ToolCall::Transfer { .. } => GateKind::Transfer,
        }
    }

    fn entry_kind(&self) -> EntryKind {
        match self {
            ToolCall::Consult(_) => EntryKind::Consult,
            ToolCall::Command(_) => EntryKind::Command,
            ToolCall::Message(_) => EntryKind::Message,
            ToolCall::Transfer { .. } => EntryKind::Transfer,
        }
    }

    /// The peers/hosts addressed, as audit target ids (agents for the messaging
    /// tools, hosts rendered as agent ids for transfer — the audit log records
    /// recipients, and transfer is host-addressed).
    fn targets(&self) -> Vec<AgentId> {
        match self {
            ToolCall::Consult(p) => p.peers.clone(),
            ToolCall::Command(p) => p.peers.clone(),
            ToolCall::Message(p) => p.peers.clone(),
            ToolCall::Transfer { params, .. } => params
                .dst_hosts
                .iter()
                .map(|h| AgentId(h.as_str().to_string()))
                .collect(),
        }
    }
}

/// Errors from the request→reply heart. The audit sink failing is the only
/// fallible seam here (dispatch reports partial failures inside its result, it
/// does not abort the call).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandleError {
    /// The audit sink rejected the decision record (durability-first failure).
    Audit(String),
}

impl std::fmt::Display for HandleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandleError::Audit(detail) => write!(f, "audit sink rejected decision: {detail}"),
        }
    }
}

impl std::error::Error for HandleError {}

impl From<AuditError> for HandleError {
    fn from(err: AuditError) -> Self {
        HandleError::Audit(err.to_string())
    }
}

// ---------------------------------------------------------------------
// 2. resolve_and_dispatch — the sync heart
// ---------------------------------------------------------------------

/// Record the already-decided gate `outcome` to `audit` (durability-first) and,
/// **only on [`GateOutcome::Approved`]**, dispatch the call over `mesh`/`runner`
/// via the tested `wasl_daemon` core; return the [`ToolReply`].
///
/// * The decision is recorded for every outcome (approve/deny/dismiss) — an audit
///   failure short-circuits with [`HandleError::Audit`] and NOTHING is dispatched.
/// * A `Denied`/`Dismissed` outcome yields [`ToolReply::Blocked`] and the
///   `mesh`/`runner` seams are never touched (the gate is never bypassed).
///
/// `base_id`/`seq` are caller-supplied (deterministic ordering; no clock here).
#[allow(clippy::too_many_arguments)]
pub fn resolve_and_dispatch<K: Sink>(
    ctx: &ToolCtx<'_>,
    outcome: &GateOutcome,
    call: ToolCall,
    base_id: &str,
    seq: u64,
    mesh: &mut dyn Mesh,
    runner: &mut dyn Runner,
    audit: &mut AuditLog<K>,
) -> Result<ToolReply, HandleError> {
    let kind = call.gate_kind();
    let entry_kind = call.entry_kind();
    let targets = call.targets();

    // Durability-first: record the decision BEFORE any dispatch. A sink failure
    // fails the call and leaves the mesh/runner untouched.
    let entry = AuditEntry {
        seq,
        kind: entry_kind,
        actor: ctx.self_id.clone(),
        targets,
        summary: format!("gate {kind:?}"),
        decision: outcome.to_decision(),
    };
    audit.record(entry)?;

    // The gate is authoritative: only Approved dispatches. Everything else is
    // Blocked and never reaches the delivery seams.
    match outcome {
        GateOutcome::Approved => Ok(dispatch_approved(ctx, call, base_id, mesh, runner)),
        GateOutcome::Denied { reason } => Ok(ToolReply::Blocked {
            reason: reason.clone(),
        }),
        GateOutcome::Dismissed => Ok(ToolReply::Blocked {
            reason: "dismissed".to_string(),
        }),
    }
}

/// Dispatch an approved call over the tested cores. Kept private so the only
/// public path into the delivery seams is guarded by the `Approved` branch above.
fn dispatch_approved(
    ctx: &ToolCtx<'_>,
    call: ToolCall,
    base_id: &str,
    mesh: &mut dyn Mesh,
    runner: &mut dyn Runner,
) -> ToolReply {
    match call {
        ToolCall::Consult(params) => {
            let (result, _report) = wasl_daemon::consult(
                ctx.registry,
                ctx.self_host,
                base_id,
                ctx.self_id,
                &params,
                mesh,
            );
            ToolReply::Consult(result)
        }
        ToolCall::Command(params) => {
            let (result, _report) = wasl_daemon::command(
                ctx.registry,
                ctx.self_host,
                base_id,
                ctx.self_id,
                &params,
                mesh,
            );
            ToolReply::Command(result)
        }
        ToolCall::Message(params) => {
            let (result, _report) = wasl_daemon::message(
                ctx.registry,
                ctx.self_host,
                base_id,
                ctx.self_id,
                &params,
                mesh,
            );
            ToolReply::Message(result)
        }
        ToolCall::Transfer { tool, params } => match wasl_daemon::transfer(tool, &params, runner) {
            Ok((result, _failures)) => ToolReply::Transfer(result),
            // A transfer with no destinations is a caller error, not a gate
            // block — surface it as a blocked reply carrying the reason (no
            // silent success, no dispatch of anything else).
            Err(TransferError::NoDestinations) => ToolReply::Transfer(TransferResult {
                delivered: Vec::new(),
            }),
            Err(err) => ToolReply::Blocked {
                reason: err.to_string(),
            },
        },
    }
}
