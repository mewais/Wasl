//! Black-box integration tests for `wasl_procrunner::ProcessRunner`.
//!
//! These exercise the process plumbing per `docs/specs/procrunner.md` Test plan
//! using only stand-in commands (`sh -c ...`) and a nonexistent program — no
//! network, no real scp/ssh. Tests compile ONLY against the public API
//! (`ProcessRunner`) + the `wasl_transfer::Runner` trait.

use wasl_procrunner::ProcessRunner;
use wasl_transfer::Runner;

/// success: `run(&["sh","-c","exit 0"])` → `Ok(())`.
#[test]
fn success_exit_zero_is_ok() {
    let mut runner = ProcessRunner::new();
    let argv = vec!["sh".to_string(), "-c".to_string(), "exit 0".to_string()];
    let result = runner.run(&argv);
    assert!(result.is_ok(), "expected Ok(()), got {result:?}");
    assert_eq!(result, Ok(()));
}

/// non-zero exit + stderr capture: `run(&["sh","-c","echo boom 1>&2; exit 7"])`
/// → `Err(detail)` where `detail` contains `"boom"` (stderr captured), is
/// non-empty, and mentions the program.
#[test]
fn nonzero_exit_captures_stderr() {
    let mut runner = ProcessRunner::new();
    let argv = vec![
        "sh".to_string(),
        "-c".to_string(),
        "echo boom 1>&2; exit 7".to_string(),
    ];
    let result = runner.run(&argv);
    let detail = result.expect_err("expected Err on non-zero exit");
    assert!(
        detail.contains("boom"),
        "detail should contain captured stderr 'boom', got: {detail:?}"
    );
    assert!(!detail.is_empty(), "detail should be non-empty");
    assert!(
        detail.contains("sh"),
        "detail should mention the program 'sh', got: {detail:?}"
    );
}

/// spawn failure (program not found):
/// `run(&["this-program-does-not-exist-wasl-xyz"])` → `Err(detail)` containing
/// `"failed to spawn"`.
#[test]
fn spawn_failure_reports_failed_to_spawn() {
    let mut runner = ProcessRunner::new();
    let argv = vec!["this-program-does-not-exist-wasl-xyz".to_string()];
    let result = runner.run(&argv);
    let detail = result.expect_err("expected Err on spawn failure");
    assert!(
        detail.contains("failed to spawn"),
        "detail should contain 'failed to spawn', got: {detail:?}"
    );
}

/// empty argv: `run(&[])` → `Err("empty argv")`.
#[test]
fn empty_argv_is_err() {
    let mut runner = ProcessRunner::new();
    let argv: Vec<String> = Vec::new();
    let result = runner.run(&argv);
    let detail = result.expect_err("expected Err on empty argv");
    assert_eq!(detail, "empty argv");
}
