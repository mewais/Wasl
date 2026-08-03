//! GUTTED — Wasit migration (Phase D increment).
//!
//! `opencode_session_params` in this crate is now a no-op shim. The real
//! session/new and session/load param construction lives in
//! `wasit_opencode::OpenCodeDriver::spawn` (inside wasit-opencode). All
//! contract tests for this function have moved to
//! `Wasit/crates/wasit-opencode/src/lib.rs`.
//!
//! Nothing to test in Wasl's integration suite for this surface any more.
