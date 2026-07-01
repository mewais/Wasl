//! wasl-server — the server request spine (WS).
//!
//! Runs the three-tier permission-before-send gate for a tool call and records
//! every decision to the audit log. Tier-1 (elicitation) and Tier-3 (marsad) are
//! surfaced synchronously through the `Surfacer` trait; Tier-2 (prepare/commit)
//! defers via the gate ledger and resolves on a later `commit`. Pure
//! orchestration over the `Surfacer` seam + `wasl_audit::Sink` — routing and
//! transport live in the daemon (WD). See `docs/specs/server.md`.
