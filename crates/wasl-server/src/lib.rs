//! wasl-server — the server request spine (WS).
//!
//! Runs the three-tier permission-before-send gate for a tool call and records
//! every decision to the audit log. Tier-1 (elicitation) and Tier-3 (external) are
//! surfaced synchronously through the `Surfacer` trait; Tier-2 (prepare/commit)
//! defers via the gate ledger and resolves on a later `commit`. Pure
//! orchestration over the `Surfacer` seam + `wasl_audit::Sink` — routing and
//! transport lives in the dispatch/node layer over A2A. See `docs/specs/node.md`.

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use wasl_audit::{AuditEntry, AuditError, AuditLog, EntryKind, Sink};
use wasl_gate::{
    build_request, choose_mode, ClientCaps, GateError, GateLedger, GateMode, GateOutcome,
};
use wasl_proto::{AgentId, ApprovalToken, GateDecision, GateKind, GateRequest};

// ---------------------------------------------------------------------
// 1. Types
// ---------------------------------------------------------------------

/// Surface a gate request to the human in the chosen synchronous mode and return
/// the outcome. Only called for Tier-1 (Elicitation) and Tier-3 (External); Tier-2
/// (PrepareCommit) never calls this — it defers via the ledger.
pub trait Surfacer {
    fn surface(&mut self, mode: GateMode, req: &GateRequest) -> GateOutcome;
}

/// The result of authorizing (or committing) a gated tool call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "verdict")]
pub enum Verdict {
    /// Approved — the caller may send.
    Proceed,
    /// The human denied the request.
    Denied { reason: String },
    /// The human dismissed/cancelled (no decision).
    Dismissed,
    /// Tier-2: awaiting a later `commit()`.
    Pending { token: ApprovalToken },
}

/// Errors from the gate+audit spine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerError {
    /// The audit sink rejected the decision record.
    Audit(String),
    /// Ledger error on commit (unknown / already-resolved token).
    Gate(GateError),
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerError::Audit(detail) => write!(f, "audit sink rejected decision: {detail}"),
            ServerError::Gate(err) => write!(f, "gate ledger error: {err}"),
        }
    }
}

impl std::error::Error for ServerError {}

impl From<GateError> for ServerError {
    fn from(err: GateError) -> Self {
        ServerError::Gate(err)
    }
}

// ---------------------------------------------------------------------
// 2. Gatekeeper
// ---------------------------------------------------------------------

/// The request spine: choose a gate mode, surface/defer the gate, and record the
/// decision to the audit log.
pub struct Gatekeeper<S: Surfacer, K: Sink> {
    caps: ClientCaps,
    surfacer: S,
    ledger: GateLedger,
    audit: AuditLog<K>,
    pending: HashMap<ApprovalToken, (GateKind, AgentId, Vec<AgentId>)>,
}

impl<S: Surfacer, K: Sink> Gatekeeper<S, K> {
    pub fn new(caps: ClientCaps, surfacer: S, sink: K) -> Self {
        Self {
            caps,
            surfacer,
            ledger: GateLedger::new(),
            audit: AuditLog::new(sink),
            pending: HashMap::new(),
        }
    }

    /// The gate mode selected for this client's capabilities.
    pub fn mode(&self) -> GateMode {
        choose_mode(&self.caps)
    }

    /// Run the permission-before-send gate for an outgoing tool call.
    ///
    /// Synchronous tiers (Elicitation/External) surface via the `Surfacer`, record
    /// the decision, and return a terminal verdict. Tier-2 (PrepareCommit) defers
    /// through the ledger and returns `Pending` — nothing is recorded until
    /// `commit`.
    #[allow(clippy::too_many_arguments)]
    pub fn authorize(
        &mut self,
        id: String,
        kind: GateKind,
        from: AgentId,
        targets: Vec<AgentId>,
        preview: String,
        token: ApprovalToken,
        seq: u64,
    ) -> Result<Verdict, ServerError> {
        // Thin generic wrapper: hand the `&mut dyn` seams to the non-generic core
        // so all branches live in a single instantiation (no per-`S`/`K` gap).
        authorize_impl(
            self.mode(),
            &mut self.surfacer,
            &mut self.ledger,
            &mut self.audit,
            &mut self.pending,
            id,
            kind,
            from,
            targets,
            preview,
            token,
            seq,
        )
    }

    /// Resolve a deferred (Tier-2) gate. The ledger runs first (so unknown /
    /// already-resolved tokens error before any audit), then the remembered
    /// request context is recorded and the terminal verdict returned.
    pub fn commit(
        &mut self,
        token: &ApprovalToken,
        decision: GateDecision,
        seq: u64,
    ) -> Result<Verdict, ServerError> {
        commit_impl(
            &mut self.ledger,
            &mut self.audit,
            &mut self.pending,
            token,
            decision,
            seq,
        )
    }

    /// The number of deferred (Tier-2) gates awaiting a commit.
    pub fn pending_count(&self) -> usize {
        self.ledger.pending_count()
    }

    /// The recorded decision log.
    pub fn audit_entries(&self) -> &[AuditEntry] {
        self.audit.entries()
    }
}

// ---------------------------------------------------------------------
// 3. record_decision — the sink-touching seam.
// ---------------------------------------------------------------------

/// A minimal, object-safe durability sink: record one already-built entry
/// durably (append-first) or fail. Blanket-implemented for any `AuditLog<K>` so
/// the fallible recording logic can be factored over a single non-generic
/// `&mut dyn` seam — this keeps its branches out of per-`K` monomorphization
/// (the coverage pattern the sibling connector crate uses).
trait DecisionRecorder {
    fn record_entry(&mut self, entry: AuditEntry) -> Result<(), AuditError>;
}

impl<K: Sink> DecisionRecorder for AuditLog<K> {
    fn record_entry(&mut self, entry: AuditEntry) -> Result<(), AuditError> {
        self.record(entry)
    }
}

/// Build the `Decision` entry and record it durability-first through the
/// non-generic `&mut dyn DecisionRecorder` seam. On sink failure the entry is not
/// recorded and the error propagates (durability-first).
fn record_decision(
    recorder: &mut dyn DecisionRecorder,
    seq: u64,
    kind: GateKind,
    from: AgentId,
    targets: Vec<AgentId>,
    decision: Option<GateDecision>,
) -> Result<(), ServerError> {
    let entry = AuditEntry {
        seq,
        kind: EntryKind::Decision,
        actor: from,
        targets,
        summary: format!("gate {kind:?}"),
        decision,
        ..Default::default()
    };
    recorder
        .record_entry(entry)
        .map_err(|e| ServerError::Audit(e.to_string()))
}

/// The non-generic core of `authorize`. All the branching (mode dispatch,
/// outcome→verdict mapping, the durability-first `?`) lives here over `&mut dyn`
/// seams, so it is instantiated exactly once regardless of the concrete
/// `Surfacer`/`Sink` — every branch is exercised in a single copy.
#[allow(clippy::too_many_arguments)]
fn authorize_impl(
    mode: GateMode,
    surfacer: &mut dyn Surfacer,
    ledger: &mut GateLedger,
    recorder: &mut dyn DecisionRecorder,
    pending: &mut HashMap<ApprovalToken, (GateKind, AgentId, Vec<AgentId>)>,
    id: String,
    kind: GateKind,
    from: AgentId,
    targets: Vec<AgentId>,
    preview: String,
    token: ApprovalToken,
    seq: u64,
) -> Result<Verdict, ServerError> {
    let target_strs: Vec<String> = targets.iter().map(|a| a.as_str().to_string()).collect();
    let req = build_request(id, kind, from.clone(), &target_strs, preview);

    match mode {
        GateMode::Elicitation | GateMode::External => {
            let outcome = surfacer.surface(mode, &req);
            record_decision(recorder, seq, kind, from, targets, outcome.to_decision())?;
            Ok(match outcome {
                GateOutcome::Approved => Verdict::Proceed,
                GateOutcome::Denied { reason } => Verdict::Denied { reason },
                GateOutcome::Dismissed => Verdict::Dismissed,
            })
        }
        GateMode::PrepareCommit => {
            ledger.prepare(token.clone(), req);
            pending.insert(token.clone(), (kind, from, targets));
            Ok(Verdict::Pending { token })
        }
    }
}

/// The non-generic core of `commit`. Look up the remembered context first:
/// `pending` holds the token iff the ledger still has it pending (they are
/// inserted and cleared together), so a miss (`None`) means the token is unknown
/// or already resolved — the ledger commit then yields exactly that error before
/// any audit. On a hit the ledger commit succeeds and the decision is recorded.
fn commit_impl(
    ledger: &mut GateLedger,
    recorder: &mut dyn DecisionRecorder,
    pending: &mut HashMap<ApprovalToken, (GateKind, AgentId, Vec<AgentId>)>,
    token: &ApprovalToken,
    decision: GateDecision,
    seq: u64,
) -> Result<Verdict, ServerError> {
    // The ledger owns token validity and errors (before any audit) on an
    // unknown / already-resolved token; `?` propagates that. `pending` holds the
    // token's context in lockstep with the ledger's pending set, so on a
    // successful commit the `remove` always matches — the miss branch is only
    // reachable together with a ledger error, which has already returned above.
    let (_req, decision) = ledger.commit(token, decision)?;
    let (kind, from, targets) = pending
        .remove(token)
        .unwrap_or((_req.kind, _req.from, Vec::new()));
    record_decision(recorder, seq, kind, from, targets, Some(decision.clone()))?;
    Ok(match decision {
        GateDecision::Approve => Verdict::Proceed,
        GateDecision::Deny { reason } => Verdict::Denied { reason },
    })
}
