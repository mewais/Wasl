//! wasl-audit — the audit log (WA): a durable, human-auditable interaction +
//! decision record. Pure query over an append-only `Sink` trait; the only IO is
//! behind that trait (unit-tested with a fake). Entries carry a caller-provided
//! `seq` for deterministic ordering — no clock in this crate.
//!
//! See `docs/specs/audit.md` for the authoritative contract.

use std::fmt;

use serde::{Deserialize, Serialize};
use wasl_proto::{AgentId, GateDecision};

/// Re-export of the L0-owned delivery-certainty vocabulary so callers query it
/// as `wasl_audit::DeliveryCertainty`. Owned in `wasl-proto` to avoid a cycle
/// (`wasl-runtime` → `wasl-audit`), and stored first-class on `AuditEntry`.
pub use wasl_proto::DeliveryCertainty;

// ---------------------------------------------------------------------
// 1. Types
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryKind {
    Consult,
    Command,
    Message,
    Transfer,
    Decision,
    /// Managed-session turn-lifecycle event (live-runtime; additive).
    RuntimeTurn,
}

/// The lifecycle phase of a managed-session turn, carried structurally on a
/// `RuntimeTurn` entry (never in `summary`). Follows `EntryKind`'s lowercase
/// serde convention so each phase has a distinct machine-readable tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeTurnPhase {
    Started,
    Injected,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Caller-provided monotonic sequence (deterministic ordering).
    pub seq: u64,
    pub kind: EntryKind,
    /// Who initiated.
    pub actor: AgentId,
    /// Recipients (one, or many for multicast).
    pub targets: Vec<AgentId>,
    /// Human-readable, secret-free.
    pub summary: String,
    /// `Some(_)` when this entry records a gate decision.
    #[serde(default)]
    pub decision: Option<GateDecision>,
    /// `Some(_)` when this entry records a command's delivery certainty — a
    /// first-class, machine-queryable safety field (a crash must not erase the
    /// evidence needed to prevent an unsafe retry). Absent from the wire when
    /// unset, so legacy entries/JSON stay byte-compatible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certainty: Option<DeliveryCertainty>,
    /// `Some(_)` for a `RuntimeTurn` entry: the managed-turn lifecycle phase.
    /// Structural (not in `summary`); absent from the wire when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<RuntimeTurnPhase>,
}

impl Default for AuditEntry {
    fn default() -> Self {
        Self {
            seq: 0,
            kind: EntryKind::Message,
            actor: AgentId(String::new()),
            targets: Vec::new(),
            summary: String::new(),
            decision: None,
            certainty: None,
            phase: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditError {
    Sink(String),
}

impl fmt::Display for AuditError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuditError::Sink(detail) => write!(f, "audit sink failure: {detail}"),
        }
    }
}

impl std::error::Error for AuditError {}

// ---------------------------------------------------------------------
// 2. Sink trait (append-only durability)
// ---------------------------------------------------------------------

pub trait Sink {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError>;
}

// ---------------------------------------------------------------------
// 3. AuditLog
// ---------------------------------------------------------------------

pub struct AuditLog<S: Sink> {
    sink: S,
    cache: Vec<AuditEntry>,
}

impl<S: Sink> AuditLog<S> {
    pub fn new(sink: S) -> Self {
        Self {
            sink,
            cache: Vec::new(),
        }
    }

    /// Append to the sink FIRST; on `Ok`, push to the in-memory cache; on `Err`,
    /// propagate and do NOT cache (the entry isn't durable).
    pub fn record(&mut self, entry: AuditEntry) -> Result<(), AuditError> {
        self.sink.append(&entry)?;
        self.cache.push(entry);
        Ok(())
    }

    pub fn entries(&self) -> &[AuditEntry] {
        &self.cache
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    pub fn by_actor(&self, actor: &AgentId) -> Vec<&AuditEntry> {
        self.cache.iter().filter(|e| &e.actor == actor).collect()
    }

    pub fn by_kind(&self, kind: EntryKind) -> Vec<&AuditEntry> {
        self.cache.iter().filter(|e| e.kind == kind).collect()
    }

    pub fn decisions(&self) -> Vec<&AuditEntry> {
        self.cache.iter().filter(|e| e.decision.is_some()).collect()
    }
}
