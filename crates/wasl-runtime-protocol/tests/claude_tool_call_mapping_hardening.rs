//! GUTTED — Wasit migration (Phase D increment).
//!
//! This file previously tested `map_claude_line` and `ClaudeToolTracker`
//! — the pure stateless/stateful line→ProtocolEvent mapping functions from
//! Wasl's old hand-rolled claude stream-json parser.
//!
//! Those functions are now no-op shims. All line→AgentEvent mapping lives in
//! `wasit_claude::LineParser` (tested in `Wasit/crates/wasit-claude/src/lib.rs`)
//! and the AgentEvent→ProtocolEvent translation happens in the adapter's
//! `ClaudeStreamTransport::next_event` (no separate pure-parse surface to test).
//!
//! Coverage moved: `Wasit/crates/wasit-claude/src/lib.rs` tests cover every
//! claude wire line → AgentEvent case that was previously exercised here.
