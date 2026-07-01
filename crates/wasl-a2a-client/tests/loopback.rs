//! `tests/loopback.rs` — black-box interop tests for `wasl_a2a_client`.
//!
//! Each test spins up a REAL `wasl_a2a_server` on an ephemeral `127.0.0.1:0`
//! port (bound before `axum::serve` is spawned — no readiness race) driven by a
//! scriptable fake `TaskExecutor`, then points a real `A2aClient` at it over
//! HTTP. Covers the spec's §Test plan CL1–CL8 exactly.
//!
//! Test-first: this MUST fail to compile until `src/` defines the public API
//! (`A2aClient`, `ClientError`, `RpcErr`, `A2aEvent`).

mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use common::{reqwest_client, sample_card, spawn_server, ErrExecutor, PongExecutor};

use axum::http::StatusCode;
use axum::routing::post;
use axum::Router;

use wasl_a2a_types::{part_text, Message, TaskState};

use wasl_a2a_client::{A2aClient, A2aEvent, ClientError, RpcErr};

fn ping() -> Message {
    Message::user_text("ping", "m1")
}

// ── CL1: send_message → completed Task with the pong artifact ──────────────

#[tokio::test]
async fn cl1_send_message_returns_completed_task_with_pong_artifact() {
    let addr = spawn_server(Arc::new(PongExecutor)).await;
    let client = A2aClient::new(format!("http://{addr}/"));

    let task = client.send_message(ping()).await.expect("send_message ok");

    assert_eq!(task.status.state, TaskState::Completed);
    assert_eq!(task.artifacts.len(), 1, "expected one artifact");
    let text = part_text(&task.artifacts[0].parts[0]).expect("artifact text part");
    assert_eq!(text, "pong:ping");
}

// ── CL2: get_task round-trip + missing id → Rpc(-32001) ────────────────────

#[tokio::test]
async fn cl2_get_task_round_trip_and_missing() {
    let addr = spawn_server(Arc::new(PongExecutor)).await;
    let client = A2aClient::new(format!("http://{addr}/"));

    let sent = client.send_message(ping()).await.expect("send_message ok");
    let got = client.get_task(&sent.id).await.expect("get_task ok");
    assert_eq!(got, sent, "get_task should return the same Task");

    let err = client
        .get_task("nope")
        .await
        .expect_err("missing id must error");
    match err {
        ClientError::Rpc(RpcErr { code, .. }) => assert_eq!(code, -32001),
        _ => panic!("expected ClientError::Rpc for a missing task"),
    }
}

// ── CL3: cancel_task on a completed id and on a missing id both → Rpc(neg) ──

#[tokio::test]
async fn cl3_cancel_task_errors_with_negative_rpc_code() {
    let addr = spawn_server(Arc::new(PongExecutor)).await;
    let client = A2aClient::new(format!("http://{addr}/"));

    let sent = client.send_message(ping()).await.expect("send_message ok");

    // Completed (terminal) task → not cancelable; the server does not wire
    // tasks/cancel over HTTP, so this surfaces as a JSON-RPC error either way.
    let completed_err = client
        .cancel_task(&sent.id)
        .await
        .expect_err("cancel of a completed task must error");
    match completed_err {
        ClientError::Rpc(RpcErr { code, .. }) => {
            assert!(code < 0, "expected a negative JSON-RPC code, got {code}")
        }
        _ => panic!("expected ClientError::Rpc for cancel of a completed task"),
    }

    // Unknown id → also a JSON-RPC error.
    let missing_err = client
        .cancel_task("nope")
        .await
        .expect_err("cancel of a missing task must error");
    match missing_err {
        ClientError::Rpc(RpcErr { code, .. }) => {
            assert!(code < 0, "expected a negative JSON-RPC code, got {code}")
        }
        _ => panic!("expected ClientError::Rpc for cancel of a missing task"),
    }
}

// ── CL4: fetch_agent_card → the served AgentCard ───────────────────────────

#[tokio::test]
async fn cl4_fetch_agent_card_matches_served_card() {
    let addr = spawn_server(Arc::new(PongExecutor)).await;
    let client = A2aClient::new(format!("http://{addr}/"));

    let card = client
        .fetch_agent_card()
        .await
        .expect("fetch_agent_card ok");
    let expected = sample_card();

    assert_eq!(card.name, expected.name);
    assert_eq!(card.url, expected.url);
    assert_eq!(card.capabilities, expected.capabilities);
}

// ── CL5: send_message_streaming → ordered working / artifact / completed ───

#[tokio::test]
async fn cl5_streaming_returns_ordered_events() {
    let addr = spawn_server(Arc::new(PongExecutor)).await;
    let client = A2aClient::new(format!("http://{addr}/"));

    let events = client
        .send_message_streaming(ping())
        .await
        .expect("send_message_streaming ok");

    assert_eq!(events.len(), 3, "expected 3 streamed events");

    match &events[0] {
        A2aEvent::Status(s) => {
            assert_eq!(s.status.state, TaskState::Working);
            assert!(!s.final_, "first status must not be final");
            assert_eq!(s.task_id, "task-1");
            assert_eq!(s.context_id, "ctx-1");
        }
        A2aEvent::Artifact(_) => panic!("expected a working Status event first"),
    }

    match &events[1] {
        A2aEvent::Artifact(a) => {
            assert_eq!(a.task_id, "task-1");
            assert_eq!(a.context_id, "ctx-1");
            let text = part_text(&a.artifact.parts[0]).expect("artifact text part");
            assert_eq!(text, "pong:ping");
        }
        A2aEvent::Status(_) => panic!("expected an Artifact event second"),
    }

    match &events[2] {
        A2aEvent::Status(s) => {
            assert_eq!(s.status.state, TaskState::Completed);
            assert!(s.final_, "final status must be final");
            assert_eq!(s.task_id, "task-1");
            assert_eq!(s.context_id, "ctx-1");
        }
        A2aEvent::Artifact(_) => panic!("expected a completed Status event last"),
    }
}

// ── CL6: executor error → Ok(Task) in the failed state (not an Rpc error) ──

#[tokio::test]
async fn cl6_executor_error_yields_failed_task() {
    let addr = spawn_server(Arc::new(ErrExecutor {
        message: "boom".to_string(),
    }))
    .await;
    let client = A2aClient::new(format!("http://{addr}/"));

    let task = client
        .send_message(ping())
        .await
        .expect("execution failure is a failed Task, not an error");
    assert_eq!(task.status.state, TaskState::Failed);
}

// ── CL7: transport failure (dead port) → ClientError::Http ─────────────────

#[tokio::test]
async fn cl7_transport_failure_is_http_error() {
    // Bind then drop to obtain an address with nothing listening.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr: SocketAddr = listener.local_addr().expect("local addr");
    drop(listener);

    let client = A2aClient::with_client(format!("http://{addr}/"), reqwest_client());
    let err = client
        .send_message(ping())
        .await
        .expect_err("a dead port must fail transport");
    assert!(
        matches!(err, ClientError::Http(_)),
        "expected ClientError::Http, got {err:?}"
    );
}

// ── CL8: ClientError Display/Error + malformed body → Decode ───────────────

async fn spawn_garbage_server() -> SocketAddr {
    let app = Router::new().route("/", post(|| async { (StatusCode::OK, "{not json rpc}") }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve garbage");
    });
    addr
}

fn assert_is_error<E: std::error::Error>(_e: &E) {}

#[tokio::test]
async fn cl8_client_error_display_and_decode() {
    // Same inner payload across variants: Display must still distinguish them.
    let http = ClientError::Http("x".to_string());
    let rpc = ClientError::Rpc(RpcErr {
        code: -32001,
        message: "x".to_string(),
    });
    let decode = ClientError::Decode("x".to_string());

    let (dh, dr, dd) = (http.to_string(), rpc.to_string(), decode.to_string());
    assert_ne!(dh, dr, "Http and Rpc Display must differ");
    assert_ne!(dr, dd, "Rpc and Decode Display must differ");
    assert_ne!(dh, dd, "Http and Decode Display must differ");

    // std::error::Error is implemented for every variant.
    assert_is_error(&http);
    assert_is_error(&rpc);
    assert_is_error(&decode);
    let _boxed: Box<dyn std::error::Error> = Box::new(ClientError::Decode("y".to_string()));

    // RpcErr carries code + message.
    if let ClientError::Rpc(inner) = &rpc {
        assert_eq!(inner.code, -32001);
        assert_eq!(inner.message, "x");
    } else {
        panic!("expected the Rpc variant");
    }

    // A 200 body that is not a JsonRpcResponse<Task> → ClientError::Decode.
    let addr = spawn_garbage_server().await;
    let client = A2aClient::new(format!("http://{addr}/"));
    let err = client
        .send_message(ping())
        .await
        .expect_err("garbage body must fail to decode");
    assert!(
        matches!(err, ClientError::Decode(_)),
        "expected ClientError::Decode, got {err:?}"
    );
}
