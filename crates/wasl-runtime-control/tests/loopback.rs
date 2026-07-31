//! INTEGRATION tests over real HTTP+SSE for `wasl-runtime-control`, mirroring
//! `wasl-operator/tests/loopback.rs`'s pattern exactly (the explicit precedent
//! `docs/specs/runtime-control-api.md` names): binds `router()` to an ephemeral
//! `127.0.0.1:0` port, drives it with real `reqwest` + a hand-rolled SSE reader.
//!
//! This is the loopback layer the spec explicitly requires (`Testability
//! honesty`: "The HTTP/UDS router + SSE writer are the IO shell, verified by
//! loopback integration tests ... NOT held to 100%"), proving the actual
//! serving path (not just the wire-type serde layer `tests/fixtures.rs`
//! already pins) works end to end over a real socket.

mod common;

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use common::{
    assistant_delta_event, client, make_hub, make_recording_provider, session_ready_event,
    spawn_router, ScriptedProvider, ScriptedTarget, SseReader,
};

use wasl_runtime::{EventSeq, StreamItem};

const SSE_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn info_endpoint_returns_expected_body() {
    let target = ScriptedTarget::new("tgt-1", vec![]);
    let provider = ScriptedProvider::new(target, false);
    let hub = make_hub(provider);
    let addr = spawn_router(hub).await;

    let resp = client()
        .get(format!("http://{addr}/v1/info"))
        .send()
        .await
        .expect("GET /v1/info");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(
        body,
        json!({ "agent_id": "wasl-a", "host_id": "host-1", "version": "0.1.0", "runtime": "managed" })
    );
}

#[tokio::test]
async fn create_then_snapshot_then_list_round_trip() {
    let target = ScriptedTarget::new("tgt-1", vec![session_ready_event(1)]);
    let provider = ScriptedProvider::new(target, false);
    let hub = make_hub(provider);
    let addr = spawn_router(hub).await;
    let c = client();

    let create_resp = c
        .post(format!("http://{addr}/v1/targets"))
        .json(&json!({ "key": "ctx-42", "protocol": "opencode-acp" }))
        .send()
        .await
        .expect("POST /v1/targets");
    assert_eq!(create_resp.status(), reqwest::StatusCode::OK);
    let created: serde_json::Value = create_resp.json().await.expect("json body");
    assert_eq!(created["target_id"], json!("tgt-1"));
    assert_eq!(created["key"], json!("ctx-42"));

    let snap_resp = c
        .get(format!("http://{addr}/v1/targets/tgt-1"))
        .send()
        .await
        .expect("GET /v1/targets/tgt-1");
    assert_eq!(snap_resp.status(), reqwest::StatusCode::OK);
    let snapshot: serde_json::Value = snap_resp.json().await.expect("json body");
    assert_eq!(snapshot["target_id"], json!("tgt-1"));

    let list_resp = c
        .get(format!("http://{addr}/v1/targets"))
        .send()
        .await
        .expect("GET /v1/targets");
    assert_eq!(list_resp.status(), reqwest::StatusCode::OK);
    let listed: serde_json::Value = list_resp.json().await.expect("json body");
    assert_eq!(listed["targets"][0]["target_id"], json!("tgt-1"));
}

#[tokio::test]
async fn get_unknown_target_is_404() {
    let target = ScriptedTarget::new("tgt-1", vec![]);
    let provider = ScriptedProvider::new(target, false);
    let hub = make_hub(provider);
    let addr = spawn_router(hub).await;

    let resp = client()
        .get(format!("http://{addr}/v1/targets/does-not-exist"))
        .send()
        .await
        .expect("GET /v1/targets/does-not-exist");
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn create_target_admission_refused_is_429() {
    let target = ScriptedTarget::new("tgt-1", vec![]);
    let provider = ScriptedProvider::new(target, true);
    let hub = make_hub(provider);
    let addr = spawn_router(hub).await;

    let resp = client()
        .post(format!("http://{addr}/v1/targets"))
        .json(&json!({ "key": "ctx-42" }))
        .send()
        .await
        .expect("POST /v1/targets");
    assert_eq!(resp.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(body["error"], json!("admission_refused"));
}

#[tokio::test]
async fn submit_turn_returns_started_receipt() {
    let target = ScriptedTarget::new("tgt-1", vec![]);
    let provider = ScriptedProvider::new(target, false);
    let hub = make_hub(provider);
    let addr = spawn_router(hub).await;
    let c = client();

    c.post(format!("http://{addr}/v1/targets"))
        .json(&json!({ "key": "ctx-42" }))
        .send()
        .await
        .expect("create target");

    let resp = c
        .post(format!("http://{addr}/v1/targets/tgt-1/turns"))
        .json(&json!({
            "delivery": { "context_id": "ctx-42", "from": "human", "kind": "message",
                          "text": "hello", "data": [] },
            "source": { "type": "local_human" },
            "idempotency_key": null,
            "busy_policy": { "type": "reject" },
            "causation": { "root_request": "req-1", "parent_request": null, "path": [], "hop": 0 }
        }))
        .send()
        .await
        .expect("POST .../turns");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(body["type"], json!("started"));
}

#[tokio::test]
async fn cancel_unsupported_target_is_409() {
    let target = ScriptedTarget::new("tgt-1", vec![]);
    let provider = ScriptedProvider::new(target, false);
    let hub = make_hub(provider);
    let addr = spawn_router(hub).await;
    let c = client();

    c.post(format!("http://{addr}/v1/targets"))
        .json(&json!({ "key": "ctx-42" }))
        .send()
        .await
        .expect("create target");

    let resp = c
        .post(format!("http://{addr}/v1/targets/tgt-1/cancel"))
        .json(&json!({ "turn_id": "turn-loopback", "reason": "operator_request" }))
        .send()
        .await
        .expect("POST .../cancel");
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(body["error"], json!("unsupported"));
}

#[tokio::test]
async fn gate_unresolvable_is_409() {
    let target = ScriptedTarget::new("tgt-1", vec![]);
    let provider = ScriptedProvider::new(target, false);
    let hub = make_hub(provider);
    let addr = spawn_router(hub).await;
    let c = client();

    c.post(format!("http://{addr}/v1/targets"))
        .json(&json!({ "key": "ctx-42" }))
        .send()
        .await
        .expect("create target");

    let resp = c
        .post(format!("http://{addr}/v1/targets/tgt-1/gate"))
        .json(&json!({ "turn_id": "turn-loopback", "decision": { "decision": "approve" } }))
        .send()
        .await
        .expect("POST .../gate");
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(body["error"], json!("gate_unresolvable"));
}

#[tokio::test]
async fn shutdown_returns_204() {
    let target = ScriptedTarget::new("tgt-1", vec![]);
    let provider = ScriptedProvider::new(target, false);
    let hub = make_hub(provider);
    let addr = spawn_router(hub).await;
    let c = client();

    c.post(format!("http://{addr}/v1/targets"))
        .json(&json!({ "key": "ctx-42" }))
        .send()
        .await
        .expect("create target");

    let resp = c
        .post(format!("http://{addr}/v1/targets/tgt-1/shutdown"))
        .json(&json!({ "mode": "drain" }))
        .send()
        .await
        .expect("POST .../shutdown");
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
}

// ---------------------------------------------------------------------
// SSE: hello frame, then turn_event frames with the per-target EventSeq id
// ---------------------------------------------------------------------

#[tokio::test]
async fn events_sse_hello_then_turn_events_with_seq_ids() {
    let target = ScriptedTarget::new(
        "tgt-1",
        vec![
            session_ready_event(1),
            assistant_delta_event(2, "PON"),
            assistant_delta_event(3, "G"),
        ],
    );
    let provider = ScriptedProvider::new(target, false);
    let hub = make_hub(provider);
    let addr = spawn_router(hub).await;
    let c = client();

    c.post(format!("http://{addr}/v1/targets"))
        .json(&json!({ "key": "ctx-42" }))
        .send()
        .await
        .expect("create target");

    let resp = c
        .get(format!("http://{addr}/v1/targets/tgt-1/events"))
        .send()
        .await
        .expect("GET .../events");
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

    let mut sse = SseReader::new(resp);

    let hello = sse.next_event(SSE_TIMEOUT).await.expect("hello frame");
    assert_eq!(hello.event, "hello");
    assert!(hello.id.is_none(), "hello must carry no id:");
    assert_eq!(hello.data["api_version"], json!(1));
    assert_eq!(hello.data["target_id"], json!("tgt-1"));
    assert_eq!(hello.data["resume"], json!("snapshot"));

    let ev1 = sse
        .next_event(SSE_TIMEOUT)
        .await
        .expect("first turn_event frame");
    assert_eq!(ev1.event, "turn_event");
    assert_eq!(ev1.id, Some(1));
    assert_eq!(ev1.data["type"], json!("session_ready"));

    let ev2 = sse
        .next_event(SSE_TIMEOUT)
        .await
        .expect("second turn_event frame");
    assert_eq!(ev2.event, "turn_event");
    assert_eq!(ev2.id, Some(2));
    assert_eq!(ev2.data["type"], json!("assistant_delta"));
    assert_eq!(ev2.data["text"], json!("PON"));

    let ev3 = sse
        .next_event(SSE_TIMEOUT)
        .await
        .expect("third turn_event frame");
    assert_eq!(ev3.id, Some(3));
    assert_eq!(ev3.data["text"], json!("G"));
}

#[tokio::test]
async fn events_sse_after_seq_resumes_correctly() {
    let target = ScriptedTarget::new(
        "tgt-1",
        vec![
            session_ready_event(1),
            assistant_delta_event(2, "skip-me"),
            assistant_delta_event(3, "keep-me"),
        ],
    );
    let provider = ScriptedProvider::new(target, false);
    let hub = make_hub(provider);
    let addr = spawn_router(hub).await;
    let c = client();

    c.post(format!("http://{addr}/v1/targets"))
        .json(&json!({ "key": "ctx-42" }))
        .send()
        .await
        .expect("create target");

    let resp = c
        .get(format!("http://{addr}/v1/targets/tgt-1/events?after_seq=2"))
        .send()
        .await
        .expect("GET .../events?after_seq=2");
    let mut sse = SseReader::new(resp);

    let hello = sse.next_event(SSE_TIMEOUT).await.expect("hello frame");
    assert_eq!(hello.event, "hello");

    let ev = sse
        .next_event(SSE_TIMEOUT)
        .await
        .expect("the one event after seq 2");
    assert_eq!(ev.id, Some(3));
    assert_eq!(ev.data["text"], json!("keep-me"));
}

#[tokio::test]
async fn events_sse_truncated_frame_carries_earliest_latest() {
    let target = ScriptedTarget::new(
        "tgt-1",
        vec![StreamItem::Truncated {
            earliest: EventSeq(8),
            latest: EventSeq(20),
        }],
    );
    let provider = ScriptedProvider::new(target, false);
    let hub = make_hub(provider);
    let addr = spawn_router(hub).await;
    let c = client();

    c.post(format!("http://{addr}/v1/targets"))
        .json(&json!({ "key": "ctx-42" }))
        .send()
        .await
        .expect("create target");

    let resp = c
        .get(format!("http://{addr}/v1/targets/tgt-1/events"))
        .send()
        .await
        .expect("GET .../events");
    let mut sse = SseReader::new(resp);

    let _hello = sse.next_event(SSE_TIMEOUT).await.expect("hello frame");
    let truncated = sse.next_event(SSE_TIMEOUT).await.expect("truncated frame");
    assert_eq!(truncated.event, "truncated");
    assert!(
        truncated.id.is_none(),
        "truncated frames carry no per-event id"
    );
    assert_eq!(truncated.data["earliest"], json!(8));
    assert_eq!(truncated.data["latest"], json!(20));
}

// =====================================================================
// Context round-trip + protocol-precedence loopback tests
//
// Each test POSTs to /v1/targets, then interrogates the recorded
// SpawnContext that the ScriptedProvider's target_for_with_context()
// captured.  This is the key proof that the context payload crosses
// the HTTP wire all the way into the provider.
// =====================================================================

/// Helper: POST /v1/targets with the given JSON body and expect HTTP 200.
/// Returns the response JSON.
async fn post_create_target(
    addr: std::net::SocketAddr,
    body: serde_json::Value,
) -> serde_json::Value {
    client()
        .post(format!("http://{addr}/v1/targets"))
        .json(&body)
        .send()
        .await
        .expect("POST /v1/targets")
        .json()
        .await
        .expect("response body as JSON")
}

/// Helper: POST /v1/targets with the given JSON body and expect non-200.
/// Returns the (status, body) pair.
async fn post_create_target_expect_error(
    addr: std::net::SocketAddr,
    body: serde_json::Value,
) -> (reqwest::StatusCode, serde_json::Value) {
    let resp = client()
        .post(format!("http://{addr}/v1/targets"))
        .json(&body)
        .send()
        .await
        .expect("POST /v1/targets");
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.expect("error body as JSON");
    (status, body)
}

/// POST with a full context body (protocol + cwd + model) and verify
/// the recorded SpawnContext matches those fields exactly.
#[tokio::test]
async fn create_target_with_full_context_recorded_by_provider() {
    let target = ScriptedTarget::new("tgt-1", vec![]);
    let provider = make_recording_provider(target);
    let hub = make_hub(provider.clone());
    let addr = spawn_router(hub).await;

    post_create_target(
        addr,
        json!({
            "key": "ctx-42",
            "context": {
                "protocol": "claude",
                "cwd": "/workspace/project",
                "model": "claude-opus-4-5"
            }
        }),
    )
    .await;

    let ctx = provider
        .recorded_context()
        .await
        .expect("provider must have received a context");

    // Hub folds context.protocol as the effective protocol.
    assert_eq!(
        ctx.protocol.as_deref(),
        Some("claude"),
        "context.protocol must survive the wire"
    );
    assert_eq!(
        ctx.cwd.as_deref(),
        Some("/workspace/project"),
        "cwd must survive the wire"
    );
    assert_eq!(
        ctx.model.as_deref(),
        Some("claude-opus-4-5"),
        "model must survive the wire"
    );
}

// --- Protocol-precedence tests ---

/// context.protocol wins over top-level protocol (context wins per spec).
#[tokio::test]
async fn protocol_precedence_context_wins_over_top_level() {
    let target = ScriptedTarget::new("tgt-1", vec![]);
    let provider = make_recording_provider(target);
    let hub = make_hub(provider.clone());
    let addr = spawn_router(hub).await;

    post_create_target(
        addr,
        json!({
            "key": "ctx-42",
            "protocol": "opencode-acp",
            "context": { "protocol": "claude" }
        }),
    )
    .await;

    let ctx = provider
        .recorded_context()
        .await
        .expect("provider must have received a context");

    assert_eq!(
        ctx.protocol.as_deref(),
        Some("claude"),
        "context.protocol='claude' must win over top-level 'opencode-acp'"
    );
}

/// Only top-level protocol (no context.protocol) → effective protocol
/// propagated into context.protocol by the hub fold.
#[tokio::test]
async fn protocol_precedence_top_level_only_propagated_to_context() {
    let target = ScriptedTarget::new("tgt-1", vec![]);
    let provider = make_recording_provider(target);
    let hub = make_hub(provider.clone());
    let addr = spawn_router(hub).await;

    post_create_target(
        addr,
        json!({
            "key": "ctx-42",
            "protocol": "claude"
        }),
    )
    .await;

    let ctx = provider
        .recorded_context()
        .await
        .expect("provider must have received a context");

    assert_eq!(
        ctx.protocol.as_deref(),
        Some("claude"),
        "top-level protocol must be folded into context.protocol"
    );
}

/// Neither top-level nor context.protocol → context.protocol remains None.
#[tokio::test]
async fn protocol_precedence_neither_yields_none() {
    let target = ScriptedTarget::new("tgt-1", vec![]);
    let provider = make_recording_provider(target);
    let hub = make_hub(provider.clone());
    let addr = spawn_router(hub).await;

    post_create_target(
        addr,
        json!({ "key": "ctx-42" }),
    )
    .await;

    let ctx = provider
        .recorded_context()
        .await
        .expect("provider must have received a context");

    assert_eq!(
        ctx.protocol,
        None,
        "with no protocol in body, context.protocol must be None"
    );
}

/// An unknown protocol value → the hub's validate_protocol rejects it
/// with HTTP 400 and an "malformed" error body; provider is never called.
#[tokio::test]
async fn protocol_unknown_rejected_by_hub_before_provider() {
    let target = ScriptedTarget::new("tgt-1", vec![]);
    let provider = make_recording_provider(target);
    let hub = make_hub(provider.clone());
    let addr = spawn_router(hub).await;

    let (status, body) = post_create_target_expect_error(
        addr,
        json!({ "key": "ctx-42", "protocol": "not-a-real-protocol" }),
    )
    .await;

    assert_eq!(
        status,
        reqwest::StatusCode::BAD_REQUEST,
        "unknown protocol must be rejected with 400"
    );
    assert_eq!(
        body["error"], json!("malformed"),
        "error body must carry 'malformed' error code"
    );

    // Provider must NOT have been called.
    assert!(
        provider.recorded_context().await.is_none(),
        "provider must not be invoked when protocol validation fails"
    );
}

/// Same precedence test as above but with context.protocol being the unknown
/// value: hub validates the effective protocol (context wins), so the bad
/// value in context still gets rejected.
#[tokio::test]
async fn protocol_unknown_in_context_also_rejected() {
    let target = ScriptedTarget::new("tgt-1", vec![]);
    let provider = make_recording_provider(target);
    let hub = make_hub(provider.clone());
    let addr = spawn_router(hub).await;

    let (status, body) = post_create_target_expect_error(
        addr,
        json!({
            "key": "ctx-42",
            "protocol": "claude",
            "context": { "protocol": "not-a-real-protocol" }
        }),
    )
    .await;

    assert_eq!(
        status,
        reqwest::StatusCode::BAD_REQUEST,
        "unknown protocol in context must be rejected with 400"
    );
    assert_eq!(
        body["error"], json!("malformed"),
        "error body must carry 'malformed' error code"
    );

    assert!(
        provider.recorded_context().await.is_none(),
        "provider must not be invoked when protocol validation fails"
    );
}
