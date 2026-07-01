//! wasl-marsad — the optional Tier-3 Marsad connector (WC).
//!
//! Surface HITL gates to Marsad and get a human decision back, notify Marsad of
//! auto-approved interactions, and emit an observation feed. Pure logic over a
//! mockable `MarsadLink` trait — the only IO is behind that trait (unit-tested
//! with a fake; the real HTTP/IPC link is the daemon/server's concern).
//! Durability-first, like `wasl-audit`: a feed item is cached locally ONLY after
//! the link accepts it. Observations carry a caller-provided `seq` — no clock
//! in this crate.
//!
//! See `docs/specs/marsad-connector.md` for the authoritative contract.

use std::fmt;

use serde::{Deserialize, Serialize};
use wasl_gate::GateOutcome;
use wasl_proto::{AgentId, GateDecision, GateRequest};

// ---------------------------------------------------------------------
// 1. Types
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObsKind {
    Consult,
    Command,
    Message,
    Transfer,
    Gate,
    AutoApproved,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observation {
    /// Caller-provided monotonic sequence (deterministic ordering).
    pub seq: u64,
    pub kind: ObsKind,
    /// Who initiated.
    pub from: AgentId,
    /// Recipients (one, or many for multicast).
    pub to: Vec<AgentId>,
    /// Human-readable, secret-free.
    pub summary: String,
    /// `Some(_)` when this obs records a gate/auto-approve decision.
    #[serde(default)]
    pub decision: Option<GateDecision>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnError {
    Transport(String),
}

impl fmt::Display for ConnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnError::Transport(detail) => write!(f, "marsad transport failure: {detail}"),
        }
    }
}

impl std::error::Error for ConnError {}

// ---------------------------------------------------------------------
// 2. MarsadLink trait (the mockable seam to Marsad)
// ---------------------------------------------------------------------

pub trait MarsadLink {
    /// Surface a HITL gate to Marsad and BLOCK for the human decision.
    fn request_decision(&mut self, req: &GateRequest) -> Result<GateDecision, ConnError>;
    /// Emit an observation onto Marsad's feed.
    fn emit(&mut self, obs: &Observation) -> Result<(), ConnError>;
}

// ---------------------------------------------------------------------
// 3. Connector
// ---------------------------------------------------------------------

pub struct Connector<L: MarsadLink> {
    link: L,
    cache: Vec<Observation>,
}

impl<L: MarsadLink> Connector<L> {
    pub fn new(link: L) -> Self {
        Self {
            link,
            cache: Vec::new(),
        }
    }

    /// Tier-3 gate: request the human decision, emit the resulting `Gate`
    /// observation, cache it durability-first, and return the outcome. If
    /// `request_decision` errs → propagate, emit/cache nothing. If `emit` errs →
    /// propagate, cache nothing (the decision was made but not durably observed).
    pub fn gate(&mut self, req: &GateRequest, seq: u64) -> Result<GateOutcome, ConnError> {
        gate_impl(&mut self.link, &mut self.cache, req, seq)
    }

    /// Auto-approved fallback: build an `AutoApproved` `Observation` carrying
    /// `Some(GateDecision::Approve)`, emit + cache durability-first, propagate +
    /// don't cache on link error. `_kind` is the underlying interaction kind
    /// (Consult/Command/Message/Transfer) — the recorded observation is always
    /// `AutoApproved`.
    pub fn notify_auto_approved(
        &mut self,
        seq: u64,
        _kind: ObsKind,
        from: AgentId,
        to: Vec<AgentId>,
        summary: String,
    ) -> Result<(), ConnError> {
        let obs = Observation {
            seq,
            kind: ObsKind::AutoApproved,
            from,
            to,
            summary,
            decision: Some(GateDecision::Approve),
        };
        self.emit_and_cache(obs)
    }

    /// Emit a plain (non-decision) observation + cache on `Ok`; propagate +
    /// don't cache on error.
    pub fn observe(&mut self, obs: Observation) -> Result<(), ConnError> {
        self.emit_and_cache(obs)
    }

    /// Emit the observation FIRST; on `Ok`, push to the cache; on `Err`,
    /// propagate and do NOT cache (durability-first).
    fn emit_and_cache(&mut self, obs: Observation) -> Result<(), ConnError> {
        emit_and_cache(&mut self.link, &mut self.cache, obs)
    }

    /// The cache, in emit order.
    pub fn feed(&self) -> &[Observation] {
        &self.cache
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Observations matching `kind`, in order.
    pub fn by_kind(&self, kind: ObsKind) -> Vec<&Observation> {
        self.cache.iter().filter(|o| o.kind == kind).collect()
    }

    /// Observations where `decision.is_some()`, in order.
    pub fn decisions(&self) -> Vec<&Observation> {
        self.cache.iter().filter(|o| o.decision.is_some()).collect()
    }
}

// ---------------------------------------------------------------------
// 3b. Link-touching core (non-generic — one instantiation over `dyn`, so the
//     durability-first branches are exercised once regardless of link type).
// ---------------------------------------------------------------------

/// Request the decision, emit the resulting `Gate` observation, cache it
/// durability-first, and return the outcome. Propagate on decision/emit error
/// without caching.
fn gate_impl(
    link: &mut dyn MarsadLink,
    cache: &mut Vec<Observation>,
    req: &GateRequest,
    seq: u64,
) -> Result<GateOutcome, ConnError> {
    let decision = link.request_decision(req)?;
    let obs = Observation {
        seq,
        kind: ObsKind::Gate,
        from: req.from.clone(),
        to: vec![],
        summary: req.summary.clone(),
        decision: Some(decision.clone()),
    };
    emit_and_cache(link, cache, obs)?;
    Ok(decision_to_outcome(&decision))
}

/// Emit the observation FIRST; on `Ok`, push to the cache; on `Err`, propagate
/// and do NOT cache (durability-first).
fn emit_and_cache(
    link: &mut dyn MarsadLink,
    cache: &mut Vec<Observation>,
    obs: Observation,
) -> Result<(), ConnError> {
    link.emit(&obs)?;
    cache.push(obs);
    Ok(())
}

// ---------------------------------------------------------------------
// 4. Decision → outcome (WC-local helper)
// ---------------------------------------------------------------------

/// Map Marsad's concrete decision to a gate outcome (Marsad never "dismisses").
pub fn decision_to_outcome(d: &GateDecision) -> GateOutcome {
    match d {
        GateDecision::Approve => GateOutcome::Approved,
        GateDecision::Deny { reason } => GateOutcome::Denied {
            reason: reason.clone(),
        },
    }
}
