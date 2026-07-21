//! `ProcessCliInvoker` process plumbing, exercised against real child processes
//! using stand-in commands (`cat`, `sh -c`). These prove spawn / stdin-feed /
//! stdout-capture / exit-status mapping — NOT anything about the real `claude`.

use wasl_adapter_claude::{CliInvoker, ProcessCliInvoker};

fn argv(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|s| s.to_string()).collect()
}

#[tokio::test]
async fn cat_echoes_stdin_to_stdout() {
    let inv = ProcessCliInvoker;
    let out = inv.invoke(&argv(&["cat"]), "hello").await;
    assert_eq!(out, Ok("hello".to_string()));
}

#[tokio::test]
async fn sh_printf_captures_stdout() {
    let inv = ProcessCliInvoker::default();
    let out = inv.invoke(&argv(&["sh", "-c", "printf %s abc"]), "").await;
    assert_eq!(out, Ok("abc".to_string()));
}

#[tokio::test]
async fn nonzero_exit_yields_error_with_stderr() {
    let inv = ProcessCliInvoker;
    let err = inv
        .invoke(&argv(&["sh", "-c", "echo oops 1>&2; exit 4"]), "")
        .await
        .unwrap_err();
    assert!(err.message.contains("oops"), "message was: {}", err.message);
}

#[tokio::test]
async fn spawn_failure_yields_spawn_error() {
    let inv = ProcessCliInvoker;
    let err = inv
        .invoke(&argv(&["this-program-does-not-exist-wasl-xyz"]), "")
        .await
        .unwrap_err();
    assert!(
        err.message.contains("spawn"),
        "message was: {}",
        err.message
    );
}

#[tokio::test]
async fn empty_argv_yields_empty_argv_error() {
    let inv = ProcessCliInvoker;
    let err = inv.invoke(&[], "").await.unwrap_err();
    assert_eq!(err.message, "empty argv");
}

/// Regression test for M2: BrokenPipe / EPIPE on stdin write must be benign.
/// A child that writes stdout but never reads stdin should still succeed — the
/// write_all on stdin will hit BrokenPipe (the child closed its end), but this
/// error must be swallowed, not propagated.
#[tokio::test]
async fn stdin_broken_pipe_on_ignoring_child_is_ignored() {
    let inv = ProcessCliInvoker;
    let out = inv
        .invoke(
            &argv(&["sh", "-c", "printf %s READY; exit 0"]),
            "this input is never read by the child process",
        )
        .await;
    // With the bug present, this returns Err containing "BrokenPipe";
    // after the fix it must return Ok("READY").
    assert_eq!(out, Ok("READY".to_string()));
}
