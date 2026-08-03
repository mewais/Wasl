//! GUTTED — Wasit migration (Phase D increment).
//!
//! The `claude_argv` function in this crate is now a thin shim over
//! `wasit_claude::claude_argv`. All contract tests for the argv builder
//! have moved to `Wasit/crates/wasit-claude/src/lib.rs` (the `tests`
//! module). The old contract tested here (using `-p` / `--session-id`)
//! was the pre-Wasit Wasl-specific shape; the new shape (using `--print`
//! / `--resume` / `--permission-prompt-tool stdio`) lives in Wasit and is
//! tested there.
//!
//! Nothing to test in Wasl's integration suite for this surface any more.
