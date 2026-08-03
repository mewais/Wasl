//! GUTTED — Wasit migration (Phase D increment).
//!
//! This file previously tested `map_codex_line`, `CodexToolTracker`, and
//! the handshake classifier (`classify_codex_handshake_line`) — the pure
//! line→ProtocolEvent mapping functions from Wasl's old hand-rolled codex
//! app-server parser.
//!
//! Those functions are now no-op shims. All line→AgentEvent mapping lives in
//! `wasit_codex::MessageParser` (tested in `Wasit/crates/wasit-codex/src/lib.rs`).
//! `classify_handshake_line` from `json_rpc_handshake.rs` remains live in this
//! crate and is tested in the `json_rpc_handshake` inline tests.
//!
//! Coverage moved:
//! - `map_codex_line` / `CodexToolTracker` → `Wasit/crates/wasit-codex/src/lib.rs`
//! - `classify_handshake_line` → `crates/wasl-runtime-protocol/src/json_rpc_handshake.rs`
//!   inline tests (still live; not removed).
