//! wasl-mcp `handle` — the async, rmcp-free request→reply heart.
//!
//! This is the only place in the crate that carries decision-aware logic, and it
//! reimplements NONE of it: the gate outcome is decided upstream (in the rmcp
//! layer, by surfacing an elicitation), and this module merely
//!
//! 1. records the decision to the audit log **durability-first** (an audit
//!    failure fails the whole call, before any dispatch), and
//! 2. dispatches over the tested cores (`wasl_dispatch` consult/command/message
//!    over REAL async A2A, `wasl_transfer::execute` for transfer) **only** when
//!    the outcome is [`GateOutcome::Approved`].
//!
//! A non-`Approved` outcome NEVER touches the A2A/`Runner` seams — that invariant
//! is the whole point of the gate and is proven in the loopback tests. The audit
//! `Sink` and the transfer `Runner` are traits, and A2A is exercised end-to-end
//! over loopback, mirroring the dispatch crate's test style.

use wasl_audit::{AuditEntry, AuditError, AuditLog, EntryKind, Sink};
use wasl_gate::GateOutcome;
use wasl_proto::{
    AgentId, ApprovalToken, CommandParams, CommandResult, ConsultParams, ConsultResult, GateKind,
    HostId, MessageParams, MessageResult, TransferParams, TransferResult,
};
use wasl_registry::PeerRegistry;
use wasl_transfer::{Runner, Tool};

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
/// a `Blocked` marker (with a human reason) when the gate did not approve, or
/// (Tier-2 only) `Pending` — the call has been deferred and awaits a later
/// `commit`. `Pending` is a SUCCESS content block, not a block/error: the
/// agent's next move (call `commit` with the token) is well-defined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolReply {
    Consult(ConsultResult),
    Command(CommandResult),
    Message(MessageResult),
    Transfer(TransferResult),
    Blocked { reason: String },
    Pending { token: ApprovalToken },
}

impl ToolReply {
    /// `true` when the call was gated off (never dispatched).
    pub fn is_blocked(&self) -> bool {
        matches!(self, ToolReply::Blocked { .. })
    }
}

/// Which tool's params to dispatch on approval. Keeping the params inside the
/// enum lets `resolve_and_dispatch` stay one function over all four tools.
/// `Clone` so `commit` (server.rs) can peek the remembered call durability-
/// first (clone it out) rather than removing it before the audit write that
/// might still fail — see `server.rs::commit`'s doc comment.
#[derive(Clone)]
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
// 2. resolve_and_dispatch — the async heart
// ---------------------------------------------------------------------

/// Record the already-decided gate `outcome` to `audit` (durability-first) and,
/// **only on [`GateOutcome::Approved`]**, dispatch the call over A2A (`http`) /
/// the transfer `runner` via the tested cores; return the [`ToolReply`].
///
/// * The decision is recorded for every outcome (approve/deny/dismiss) — an audit
///   failure short-circuits with [`HandleError::Audit`] and NOTHING is dispatched.
/// * A `Denied`/`Dismissed` outcome yields [`ToolReply::Blocked`] and the
///   A2A/`runner` seams are never touched (the gate is never bypassed).
///
/// `base_id`/`seq` are caller-supplied (deterministic ordering; no clock here).
#[allow(clippy::too_many_arguments)]
pub async fn resolve_and_dispatch<K: Sink>(
    ctx: &ToolCtx<'_>,
    outcome: &GateOutcome,
    call: ToolCall,
    base_id: &str,
    seq: u64,
    http: &reqwest::Client,
    // `+ Send` so the (rmcp-required) `Send` future can hold the runner across the
    // A2A `.await`; callers pass concrete `Send` runners, so it is invisible.
    runner: &mut (dyn Runner + Send),
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
        ..Default::default()
    };
    audit.record(entry)?;

    // The gate is authoritative: only Approved dispatches. Everything else is
    // Blocked and never reaches the delivery seams.
    match outcome {
        GateOutcome::Approved => Ok(dispatch_approved(ctx, call, base_id, http, runner).await),
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
///
/// The messaging verbs (consult/command/message) go over REAL async A2A via
/// `wasl_dispatch` (`from = ctx.self_id`, no host arg — routing resolves peers by
/// their A2A endpoint in the registry). Transfer is NOT an A2A call: it runs over
/// the transfer `runner` via `wasl_transfer::execute`, unchanged.
async fn dispatch_approved(
    ctx: &ToolCtx<'_>,
    call: ToolCall,
    base_id: &str,
    http: &reqwest::Client,
    runner: &mut (dyn Runner + Send),
) -> ToolReply {
    match call {
        ToolCall::Consult(params) => {
            let (result, _report) =
                wasl_dispatch::consult(ctx.registry, ctx.self_id, base_id, &params, http).await;
            ToolReply::Consult(result)
        }
        ToolCall::Command(params) => {
            let (result, _report) =
                wasl_dispatch::command(ctx.registry, ctx.self_id, base_id, &params, http).await;
            ToolReply::Command(result)
        }
        ToolCall::Message(params) => {
            let (result, _report) =
                wasl_dispatch::message(ctx.registry, ctx.self_id, base_id, &params, http).await;
            ToolReply::Message(result)
        }
        ToolCall::Transfer { tool, params } => {
            match wasl_transfer::execute(tool, &params, runner) {
                Ok((result, _failures)) => ToolReply::Transfer(result),
                // A transfer that cannot address anyone is a caller error,
                // surfaced like every other transfer failure — never an empty
                // success (supersedes mcp-a2a.md's earlier pinned arm; see
                // docs/specs/mcp-gating.md §7).
                Err(err) => ToolReply::Blocked {
                    reason: err.to_string(),
                },
            }
        }
    }
}
