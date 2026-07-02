//! wasl-gate — the three-tier permission-before-send gate (WG).
//!
//! Pure decision logic: choose the gate mode per client capabilities
//! (elicitation / prepare-commit / external), build the human-facing gate request
//! and elicitation message (never secrets), map an elicitation response to an
//! outcome, and track pending prepare-commit approvals in a ledger. The actual
//! IO (sending the elicitation, calling an external authority) lives in the server block.
//!
//! See `docs/specs/gate.md` for the authoritative contract.

use std::collections::{HashMap, HashSet};
use std::fmt;

use serde::{Deserialize, Serialize};

use wasl_proto::{
    AgentId, ApprovalToken, ElicitationAction, ElicitationCreate, ElicitationResponse,
    GateDecision, GateKind, GateRequest,
};

// ---------------------------------------------------------------------
// 1. Client capabilities + mode
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientCaps {
    pub elicitation: bool,
    pub external: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateMode {
    Elicitation,
    External,
    PrepareCommit,
}

/// Priority: elicitation > external > prepare_commit.
pub fn choose_mode(caps: &ClientCaps) -> GateMode {
    if caps.elicitation {
        GateMode::Elicitation
    } else if caps.external {
        GateMode::External
    } else {
        GateMode::PrepareCommit
    }
}

// ---------------------------------------------------------------------
// 2. Building the human-facing gate + elicitation
// ---------------------------------------------------------------------

fn verb(kind: GateKind) -> &'static str {
    match kind {
        GateKind::Consult => "consult",
        GateKind::Command => "command",
        GateKind::Message => "message to",
        GateKind::Transfer => "transfer to",
    }
}

/// Build a secret-free gate request. `preview` is the caller's already-sanitized content preview.
pub fn build_request(
    id: String,
    kind: GateKind,
    from: AgentId,
    targets: &[String],
    preview: String,
) -> GateRequest {
    let summary = format!("{} {} peer(s)", verb(kind), targets.len());
    GateRequest {
        id,
        kind,
        from,
        summary,
        preview,
    }
}

/// The message-only elicitation to send for this gate (empty schema at the MCP layer).
pub fn elicitation_for(req: &GateRequest) -> ElicitationCreate {
    ElicitationCreate {
        message: format!("{}\n{}\n\nApprove?", req.summary, req.preview),
    }
}

// ---------------------------------------------------------------------
// 3. Elicitation → outcome
// ---------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateOutcome {
    Approved,
    Denied { reason: String },
    Dismissed,
}

impl GateOutcome {
    pub fn to_decision(&self) -> Option<GateDecision> {
        match self {
            GateOutcome::Approved => Some(GateDecision::Approve),
            GateOutcome::Denied { reason } => Some(GateDecision::Deny {
                reason: reason.clone(),
            }),
            GateOutcome::Dismissed => None,
        }
    }
}

pub fn outcome_of(resp: &ElicitationResponse) -> GateOutcome {
    match resp.action {
        ElicitationAction::Accept => GateOutcome::Approved,
        ElicitationAction::Decline => GateOutcome::Denied {
            reason: "declined".to_string(),
        },
        ElicitationAction::Cancel => GateOutcome::Dismissed,
    }
}

// ---------------------------------------------------------------------
// 4. Prepare→commit ledger
// ---------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateError {
    UnknownToken,
    AlreadyResolved,
}

impl fmt::Display for GateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GateError::UnknownToken => write!(f, "unknown approval token"),
            GateError::AlreadyResolved => write!(f, "approval token already resolved"),
        }
    }
}

impl std::error::Error for GateError {}

#[derive(Debug, Clone, Default)]
pub struct GateLedger {
    pending: HashMap<ApprovalToken, GateRequest>,
    resolved: HashSet<ApprovalToken>,
}

impl GateLedger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a pending gate under `token`. A duplicate token overwrites (re-prepare) — no error.
    pub fn prepare(&mut self, token: ApprovalToken, req: GateRequest) {
        self.pending.insert(token, req);
    }

    pub fn is_pending(&self, token: &ApprovalToken) -> bool {
        self.pending.contains_key(token)
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub fn peek(&self, token: &ApprovalToken) -> Option<&GateRequest> {
        self.pending.get(token)
    }

    pub fn commit(
        &mut self,
        token: &ApprovalToken,
        decision: GateDecision,
    ) -> Result<(GateRequest, GateDecision), GateError> {
        match self.pending.remove(token) {
            Some(req) => {
                self.resolved.insert(token.clone());
                Ok((req, decision))
            }
            None if self.resolved.contains(token) => Err(GateError::AlreadyResolved),
            None => Err(GateError::UnknownToken),
        }
    }
}
