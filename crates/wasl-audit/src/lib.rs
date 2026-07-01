//! wasl-audit — the audit log (WA): a durable, human-auditable interaction +
//! decision record. Pure query over an append-only `Sink` trait; the only IO is
//! behind that trait (unit-tested with a fake). Entries carry a caller-provided
//! `seq` for deterministic ordering — no clock in this crate.
//!
//! See `docs/specs/audit.md` for the authoritative contract.

use std::fmt;

use serde::{Deserialize, Serialize};
use wasl_proto::{AgentId, GateDecision};

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
