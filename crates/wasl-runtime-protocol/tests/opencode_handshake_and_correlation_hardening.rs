//! GUTTED — Wasit migration (Phase D increment).
//!
//! This file previously tested `map_acp_line_correlated` (the opencode
//! JSON-RPC id-correlated event mapper) and `classify_handshake_line`
//! (the shared handshake line classifier).
//!
//! `map_acp_line_correlated` is now a no-op shim — all ACP line→AgentEvent
//! mapping lives in `wasit_opencode` (tested in
//! `Wasit/crates/wasit-opencode/src/lib.rs`).
//!
//! `classify_handshake_line` remains live in `json_rpc_handshake.rs` and is
//! tested by its inline `#[cfg(test)]` module there — those tests continue to
//! pass and are NOT removed.
//!
//! Coverage moved:
//! - `map_acp_line_correlated` → `Wasit/crates/wasit-opencode/src/lib.rs`
//! - `classify_handshake_line` → inline tests in
//!   `src/json_rpc_handshake.rs` (still live).
