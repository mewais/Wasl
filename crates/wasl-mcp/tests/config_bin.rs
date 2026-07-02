//! Spawned-binary fail-loud config path tests for `wasl-mcpd`, per
//! `docs/specs/node-config.md`'s `config_bin.rs` plan: an invalid/unreadable
//! config file must abort startup with exit code 2 and a
//! `wasl-mcpd: config error: …` line on stderr — BEFORE anything else starts
//! (in particular, before the stdio MCP server would ever block on stdin).
//!
//! Only the error path is spawn-tested here: the happy path blocks serving
//! stdio and stays covered by the existing `tests/smoke.rs` in-memory-transport
//! suite (untouched by this block). Env is set on the CHILD process only
//! (`Command::env`) — this file never mutates the test process's own env.

use std::process::{Command, Stdio};

fn wasl_mcpd() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_wasl-mcpd"));
    // Never let a mis-wired test hang on stdin waiting for the (never-started)
    // stdio MCP transport; capture stdout/stderr for assertion.
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    cmd
}

#[test]
fn missing_explicit_config_file_exits_2_with_config_error_prefix() {
    let dir = tempfile::tempdir().expect("tempdir");
    let missing = dir.path().join("does-not-exist.toml");

    let output = wasl_mcpd()
        .env("WASL_CONFIG", &missing)
        .output()
        .expect("spawn wasl-mcpd");

    assert_eq!(
        output.status.code(),
        Some(2),
        "an unreadable explicitly-named config file must abort with exit code 2"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("wasl-mcpd: config error:"),
        "stderr was: {stderr}"
    );
}

#[test]
fn invalid_toml_config_file_exits_2_with_toml_detail() {
    let dir = tempfile::tempdir().expect("tempdir");
    let bad = dir.path().join("bad.toml");
    std::fs::write(&bad, "this is not toml").expect("write bad config");

    let output = wasl_mcpd()
        .env("WASL_CONFIG", &bad)
        .output()
        .expect("spawn wasl-mcpd");

    assert_eq!(
        output.status.code(),
        Some(2),
        "invalid TOML at an explicit config path must abort with exit code 2"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("wasl-mcpd: config error:"),
        "stderr was: {stderr}"
    );
    assert!(
        stderr.contains("invalid config TOML"),
        "stderr was: {stderr}"
    );
}
