//! wasl-procrunner — a real `wasl_transfer::Runner` (E2).
//!
//! Executes a transfer's argv as a child process and reports the outcome. Adds
//! NO transfer logic — `wasl-transfer` plans the argv and drives `execute`; this
//! only spawns/waits. See `docs/specs/procrunner.md` for the authoritative
//! contract.
//!
//! Testability: the process plumbing is fully tested here via stand-in commands.
//! A REAL scp/rsync between two hosts is UNTESTED HERE (no 2nd host / SSH) — the
//! argv it runs is exactly what `wasl_transfer::{scp_argv,rsync_argv}` build.

use wasl_transfer::Runner;

/// A `Runner` that executes each transfer argv as a child process and reports the outcome.
#[derive(Debug, Default, Clone)]
pub struct ProcessRunner;

impl ProcessRunner {
    pub fn new() -> Self {
        Self
    }
}

impl Runner for ProcessRunner {
    fn run(&mut self, argv: &[String]) -> Result<(), String> {
        let (prog, rest) = argv.split_first().ok_or_else(|| "empty argv".to_string())?;
        let output = std::process::Command::new(prog)
            .args(rest)
            .output()
            .map_err(|e| format!("failed to spawn {prog}: {e}"))?;
        if output.status.success() {
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(format!("{prog} {}: {}", output.status, stderr.trim()))
        }
    }
}
