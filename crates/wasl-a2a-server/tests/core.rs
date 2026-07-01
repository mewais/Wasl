//! `tests/core.rs` — UNIT tests for the `wasl_a2a_server` request-handling core.
//!
//! No HTTP: these drive `handle_send` / `handle_get` / `handle_cancel` /
//! `stream_events` directly on an [`A2aServer`] built with `CounterIds`, using a
//! scriptable fake [`TaskExecutor`]. Covers the spec's §Test plan C1–C7.
//!
//! Test-first: this fails to compile until `src/` defines the public API.

mod common;

use std::sync::Arc;

use common::{params_no_context, params_with_context, sample_card, two_parts, FakeExecutor};

use wasl_a2a_server::{A2aServer, CounterIds, ExecError, RpcError, StreamEvent};
use wasl_a2a_types::{part_text, TaskState};

/// Build a server over the given fake executor with deterministic `CounterIds`.
fn server_with(exec: Arc<FakeExecutor>) -> A2aServer {
    A2aServer::with_ids(sample_card(), exec, Arc::new(CounterIds::default()))
}

// ── C1: handle_send success ────────────────────────────────────────────────

#[tokio::test]
async fn c1_send_success_assigns_default_context_and_task_ids() {
    let exec = Arc::new(FakeExecutor::ok(two_parts()));
    let server = server_with(exec.clone());

    let task = server
        .handle_send(params_no_context("hi", "m1"))
        .await;

    // Fresh deterministic ids from CounterIds.
    assert_eq!(task.id, "task-1");
    assert_eq!(task.context_id, "ctx-1");
    // Terminal success state.
    assert_eq!(task.status.state, TaskState::Completed);
    // Exactly one artifact carrying the executor's parts.
    assert_eq!(task.artifacts.len(), 1);
    let got: Vec<&str> = task.artifacts[0]
        .parts
        .iter()
        .filter_map(part_text)
        .collect();
    assert_eq!(got, vec!["hello", "world"]);

    // The executor saw the assigned context id and the incoming message.
    let seen = exec.seen();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].0, "ctx-1");
    assert_eq!(seen[0].1.message_id, "m1");
}

#[tokio::test]
async fn c1_send_success_honors_supplied_context_id() {
    let exec = Arc::new(FakeExecutor::ok(two_parts()));
    let server = server_with(exec.clone());

    let task = server
        .handle_send(params_with_context("hi", "m1", "conv-42"))
        .await;

    assert_eq!(task.context_id, "conv-42");
    assert_eq!(task.id, "task-1");
    assert_eq!(task.status.state, TaskState::Completed);
    assert_eq!(exec.seen()[0].0, "conv-42");
}

// ── C2: handle_send executor error ─────────────────────────────────────────

#[tokio::test]
async fn c2_send_executor_error_is_failed_and_stored() {
    let exec = Arc::new(FakeExecutor::err("kaboom failure"));
    let server = server_with(exec);

    let task = server
        .handle_send(params_no_context("hi", "m1"))
        .await;

    assert_eq!(task.status.state, TaskState::Failed);

    // status.message carries the error text.
    let status_msg = task
        .status
        .message
        .as_ref()
        .expect("failed task must carry a status message");
    let text: String = status_msg.parts.iter().filter_map(part_text).collect();
    assert!(
        text.contains("kaboom failure"),
        "status message should carry the error text, got: {text:?}"
    );

    // Still retrievable afterwards.
    let fetched = server.handle_get(&task.id).expect("failed task is stored");
    assert_eq!(fetched.status.state, TaskState::Failed);
    assert_eq!(fetched.id, task.id);
}

// ── C3: handle_get ─────────────────────────────────────────────────────────

#[tokio::test]
async fn c3_get_returns_same_task_then_not_found() {
    let exec = Arc::new(FakeExecutor::ok(two_parts()));
    let server = server_with(exec);

    let sent = server.handle_send(params_no_context("hi", "m1")).await;
    let got = server.handle_get(&sent.id).expect("stored task");
    assert_eq!(got, sent);

    let err = server
        .handle_get("does-not-exist")
        .expect_err("unknown id must be an error");
    assert_eq!(err.code, -32001);
    assert!(err.message.contains("does-not-exist"));
}

// ── C4: handle_cancel ──────────────────────────────────────────────────────
//
// A `handle_send` always yields a TERMINAL task (completed/failed) before it is
// stored, so the non-terminal `working → canceled` transition is not reachable
// deterministically through the public core API (it would require observing an
// implementation-defined mid-flight insertion). Per the spec's C4 fallback we
// cover the two deterministic branches: terminal → not_cancelable, missing →
// task_not_found.

#[tokio::test]
async fn c4_cancel_completed_task_is_not_cancelable() {
    let exec = Arc::new(FakeExecutor::ok(two_parts()));
    let server = server_with(exec);

    let task = server.handle_send(params_no_context("hi", "m1")).await;
    assert_eq!(task.status.state, TaskState::Completed);

    let err = server
        .handle_cancel(&task.id)
        .expect_err("a completed task cannot be canceled");
    // JSON-RPC error codes are negative; not_cancelable's exact code is not
    // pinned by the spec, so only assert the invariant.
    assert!(err.code < 0);
    assert!(err.message.contains(&task.id));
}

#[tokio::test]
async fn c4_cancel_unknown_task_is_not_found() {
    let exec = Arc::new(FakeExecutor::ok(two_parts()));
    let server = server_with(exec);

    let err = server
        .handle_cancel("missing-task")
        .expect_err("cancel of unknown id must be an error");
    assert_eq!(err.code, -32001);
    assert!(err.message.contains("missing-task"));
}

// ── C5: stream_events ──────────────────────────────────────────────────────

#[tokio::test]
async fn c5_stream_success_emits_working_artifact_completed() {
    let exec = Arc::new(FakeExecutor::ok(two_parts()));
    let server = server_with(exec);

    let events = server
        .stream_events(params_no_context("hi", "m1"))
        .await;

    assert_eq!(events.len(), 3, "success stream has exactly 3 events");

    match &events[0] {
        StreamEvent::Status(s) => {
            assert_eq!(s.status.state, TaskState::Working);
            assert!(!s.final_);
            assert_eq!(s.task_id, "task-1");
            assert_eq!(s.context_id, "ctx-1");
        }
        other => panic!("event[0] must be a working Status, got {other:?}"),
    }

    match &events[1] {
        StreamEvent::Artifact(a) => {
            assert_eq!(a.task_id, "task-1");
            assert_eq!(a.context_id, "ctx-1");
            let got: Vec<&str> = a.artifact.parts.iter().filter_map(part_text).collect();
            assert_eq!(got, vec!["hello", "world"]);
        }
        other => panic!("event[1] must be an Artifact, got {other:?}"),
    }

    match &events[2] {
        StreamEvent::Status(s) => {
            assert_eq!(s.status.state, TaskState::Completed);
            assert!(s.final_);
            assert_eq!(s.task_id, "task-1");
            assert_eq!(s.context_id, "ctx-1");
        }
        other => panic!("event[2] must be a final completed Status, got {other:?}"),
    }
}

#[tokio::test]
async fn c5_stream_error_emits_working_then_failed() {
    let exec = Arc::new(FakeExecutor::err("stream boom"));
    let server = server_with(exec);

    let events = server
        .stream_events(params_no_context("hi", "m1"))
        .await;

    assert_eq!(events.len(), 2, "error stream has exactly 2 events");

    match &events[0] {
        StreamEvent::Status(s) => {
            assert_eq!(s.status.state, TaskState::Working);
            assert!(!s.final_);
            assert_eq!(s.task_id, "task-1");
            assert_eq!(s.context_id, "ctx-1");
        }
        other => panic!("event[0] must be a working Status, got {other:?}"),
    }

    match &events[1] {
        StreamEvent::Status(s) => {
            assert_eq!(s.status.state, TaskState::Failed);
            assert!(s.final_);
            assert_eq!(s.task_id, "task-1");
            assert_eq!(s.context_id, "ctx-1");
        }
        other => panic!("event[1] must be a final failed Status, got {other:?}"),
    }
}

// ── C6: dispatch-level error constructors ──────────────────────────────────
//
// The spec exposes no public dispatch helper (only `router()`), so the actual
// method routing is asserted end-to-end in `loopback.rs`. Here we pin the two
// dispatch-level RpcError constructors that dispatch relies on.

#[test]
fn c6_method_not_found_and_invalid_params_codes() {
    let m = RpcError::method_not_found("message/bogus");
    assert_eq!(m.code, -32601);
    assert!(m.message.contains("message/bogus"));

    let p = RpcError::invalid_params("missing field `message`");
    assert_eq!(p.code, -32602);
    assert!(p.message.contains("missing field `message`"));
}

// ── C7: RpcError constructors + ExecError traits ───────────────────────────

#[test]
fn c7_rpcerror_constructors_carry_expected_codes() {
    let nf = RpcError::task_not_found("task-9");
    assert_eq!(nf.code, -32001);
    assert!(nf.message.contains("task-9"));

    let nc = RpcError::not_cancelable("task-9");
    assert!(nc.code < 0);
    assert!(nc.message.contains("task-9"));

    let mnf = RpcError::method_not_found("foo/bar");
    assert_eq!(mnf.code, -32601);

    let ip = RpcError::invalid_params("nope");
    assert_eq!(ip.code, -32602);
}

#[test]
fn c7_exec_error_display_and_error_trait() {
    let e = ExecError {
        message: "explosion".to_string(),
    };
    assert!(format!("{e}").contains("explosion"));

    // ExecError must be usable as a std::error::Error trait object.
    let as_err: &dyn std::error::Error = &e;
    assert!(as_err.to_string().contains("explosion"));

    // Explicitly documented derives: Clone + PartialEq.
    assert_eq!(e.clone(), e);
    assert_ne!(
        e,
        ExecError {
            message: "other".to_string()
        }
    );
}
