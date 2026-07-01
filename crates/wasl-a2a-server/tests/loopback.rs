//! `tests/loopback.rs` — INTEGRATION tests over real HTTP for `wasl_a2a_server`.
//!
//! Binds `A2aServer::router()` to an ephemeral `127.0.0.1:0` port in a spawned
//! task and drives it with `reqwest`. Covers the spec's §Test plan L1–L4 plus
//! the dispatch-level error routing (C6) that has no unit seam.
//!
//! Hermetic: the listener is bound synchronously before `axum::serve` is spawned
//! (so connections queue immediately — no readiness race), and every request
//! carries a short client timeout. Test-first: fails to compile until `src/`
//! defines the public API.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::{sample_card, two_parts, FakeExecutor};

use wasl_a2a_server::{A2aServer, CounterIds};
use wasl_a2a_types::{Message, MessageSendParams};

use serde_json::{json, Value};

/// Bind an ephemeral port, spawn the router, and return its address.
async fn spawn_server(exec: Arc<FakeExecutor>) -> SocketAddr {
    let server = Arc::new(A2aServer::with_ids(
        sample_card(),
        exec,
        Arc::new(CounterIds::default()),
    ));
    let app = server.router();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");

    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    addr
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client")
}

/// A `message/send`-shaped JSON-RPC request body with the given method/id.
fn rpc_body(id: i64, method: &str, params: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    })
}

fn send_params(text: &str, message_id: &str) -> Value {
    let params = MessageSendParams {
        message: Message::user_text(text, message_id),
        metadata: None,
    };
    serde_json::to_value(&params).expect("serialize MessageSendParams")
}

// ── L1: message/send → completed Task + artifact ───────────────────────────

#[tokio::test]
async fn l1_message_send_returns_completed_task_with_artifact() {
    let addr = spawn_server(Arc::new(FakeExecutor::ok(two_parts()))).await;
    let url = format!("http://{addr}/");

    let resp = client()
        .post(&url)
        .json(&rpc_body(1, "message/send", send_params("hi", "m1")))
        .send()
        .await
        .expect("POST message/send");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let body: Value = resp.json().await.expect("json body");
    assert_eq!(body["jsonrpc"], "2.0");
    assert_eq!(body["id"], 1);
    assert!(body.get("error").is_none(), "expected ok, got {body:?}");

    let result = &body["result"];
    assert_eq!(result["kind"], "task");
    assert_eq!(result["id"], "task-1");
    assert_eq!(result["contextId"], "ctx-1");
    assert_eq!(result["status"]["state"], "completed");

    let artifacts = result["artifacts"].as_array().expect("artifacts array");
    assert_eq!(artifacts.len(), 1);
    let parts = artifacts[0]["parts"].as_array().expect("parts array");
    let texts: Vec<&str> = parts.iter().map(|p| p["text"].as_str().unwrap()).collect();
    assert_eq!(texts, vec!["hello", "world"]);
}

// ── L2: GET /.well-known/agent-card.json → AgentCard ───────────────────────

#[tokio::test]
async fn l2_agent_card_endpoint_returns_card() {
    let addr = spawn_server(Arc::new(FakeExecutor::ok(two_parts()))).await;
    let url = format!("http://{addr}/.well-known/agent-card.json");

    let resp = client().get(&url).send().await.expect("GET agent card");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let card: Value = resp.json().await.expect("card json");
    let expected = sample_card();
    assert_eq!(card["name"], expected.name);
    assert_eq!(card["url"], expected.url);
    assert_eq!(card["version"], expected.version);
    assert_eq!(card["capabilities"]["streaming"], true);
}

// ── L3: tasks/get valid + bogus ────────────────────────────────────────────

#[tokio::test]
async fn l3_tasks_get_valid_and_bogus() {
    let addr = spawn_server(Arc::new(FakeExecutor::ok(two_parts()))).await;
    let url = format!("http://{addr}/");
    let http = client();

    // First create a task via message/send.
    let sent: Value = http
        .post(&url)
        .json(&rpc_body(1, "message/send", send_params("hi", "m1")))
        .send()
        .await
        .expect("send")
        .json()
        .await
        .expect("send json");
    let task_id = sent["result"]["id"].as_str().expect("task id").to_string();

    // Valid tasks/get → the same Task.
    let got: Value = http
        .post(&url)
        .json(&rpc_body(2, "tasks/get", json!({ "id": task_id })))
        .send()
        .await
        .expect("tasks/get")
        .json()
        .await
        .expect("get json");
    assert!(got.get("error").is_none(), "expected ok, got {got:?}");
    assert_eq!(got["result"]["id"], task_id);
    assert_eq!(got["result"]["status"]["state"], "completed");

    // Bogus id → JSON-RPC error with code -32001.
    let miss: Value = http
        .post(&url)
        .json(&rpc_body(3, "tasks/get", json!({ "id": "nope" })))
        .send()
        .await
        .expect("tasks/get bogus")
        .json()
        .await
        .expect("miss json");
    assert!(miss.get("result").is_none(), "expected error, got {miss:?}");
    assert_eq!(miss["error"]["code"], -32001);
}

// ── L4: message/stream → SSE ordered events ────────────────────────────────

#[tokio::test]
async fn l4_message_stream_emits_ordered_sse_events() {
    let addr = spawn_server(Arc::new(FakeExecutor::ok(two_parts()))).await;
    let url = format!("http://{addr}/");

    let resp = client()
        .post(&url)
        .json(&rpc_body(7, "message/stream", send_params("hi", "m1")))
        .send()
        .await
        .expect("POST message/stream");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .expect("content-type header")
        .to_string();
    assert!(
        content_type.starts_with("text/event-stream"),
        "expected SSE content-type, got {content_type:?}"
    );

    // The stream is finite; collect the full body then parse `data:` frames.
    let body = resp.text().await.expect("sse body");
    let frames: Vec<Value> = body
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(|d| serde_json::from_str::<Value>(d.trim()).expect("parse SSE data as JSON"))
        .collect();

    assert_eq!(frames.len(), 3, "expected 3 SSE frames, got {frames:?}");

    // Frame 0: status-update working, final=false.
    assert_eq!(frames[0]["kind"], "status-update");
    assert_eq!(frames[0]["taskId"], "task-1");
    assert_eq!(frames[0]["contextId"], "ctx-1");
    assert_eq!(frames[0]["status"]["state"], "working");
    assert_eq!(frames[0]["final"], false);

    // Frame 1: artifact-update carrying the executor's parts.
    assert_eq!(frames[1]["kind"], "artifact-update");
    assert_eq!(frames[1]["taskId"], "task-1");
    assert_eq!(frames[1]["contextId"], "ctx-1");
    let parts = frames[1]["artifact"]["parts"]
        .as_array()
        .expect("artifact parts");
    let texts: Vec<&str> = parts.iter().map(|p| p["text"].as_str().unwrap()).collect();
    assert_eq!(texts, vec!["hello", "world"]);

    // Frame 2: status-update completed, final=true.
    assert_eq!(frames[2]["kind"], "status-update");
    assert_eq!(frames[2]["status"]["state"], "completed");
    assert_eq!(frames[2]["final"], true);
}

// ── C6 (over HTTP): dispatch-level errors ──────────────────────────────────

#[tokio::test]
async fn c6_unknown_method_and_bad_params_over_http() {
    let addr = spawn_server(Arc::new(FakeExecutor::ok(two_parts()))).await;
    let url = format!("http://{addr}/");
    let http = client();

    // Unknown method → MethodNotFound (-32601).
    let unknown: Value = http
        .post(&url)
        .json(&rpc_body(1, "message/does-not-exist", json!({})))
        .send()
        .await
        .expect("unknown method")
        .json()
        .await
        .expect("unknown json");
    assert!(
        unknown.get("result").is_none(),
        "expected error, got {unknown:?}"
    );
    assert_eq!(unknown["error"]["code"], -32601);

    // Params that don't deserialize into MessageSendParams → InvalidParams (-32602).
    let bad: Value = http
        .post(&url)
        .json(&rpc_body(2, "message/send", json!({ "not": "a message" })))
        .send()
        .await
        .expect("bad params")
        .json()
        .await
        .expect("bad json");
    assert!(bad.get("result").is_none(), "expected error, got {bad:?}");
    assert_eq!(bad["error"]["code"], -32602);
}
