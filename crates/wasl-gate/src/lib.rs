//! wasl-gate — the three-tier permission-before-send gate (WG).
//!
//! Pure decision logic: choose the gate mode per client capabilities
//! (elicitation / prepare-commit / marsad), build the human-facing gate request
//! + elicitation message (never secrets), map an elicitation response to an
//! outcome, and track pending prepare→commit approvals in a ledger. The actual
//! IO (sending the elicitation, calling Marsad) lives in the server block.
//!
//! See `docs/specs/gate.md` for the authoritative contract. Placeholder.
