//! `tests/serve.rs` — exercises the crate's own `A2aServer::serve()` entrypoint
//! (the bind + `axum::serve` accept loop), which the `loopback.rs` tests bypass
//! by binding the listener themselves. Covers both the successful bind path and
//! the bind-error (`?`) propagation, so `serve()` is genuinely covered rather
//! than carved out as an untested IO shell.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::{sample_card, two_parts, FakeExecutor};

use serde_json::{json, Value};
use wasl_a2a_server::{A2aServer, CounterIds};

fn build(exec: Arc<FakeExecutor>) -> Arc<A2aServer> {
    Arc::new(A2aServer::with_ids(
        sample_card(),
        exec,
        Arc::new(CounterIds::default()),
    ))
}

/// Grab an ephemeral port, then release it so `serve()` can bind it itself.
async fn free_addr() -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral");
    let addr = l.local_addr().expect("local addr");
    drop(l);
    addr
}

#[tokio::test]
async fn serve_binds_and_answers_message_send() {
    let addr = free_addr().await;
    let server = build(Arc::new(FakeExecutor::ok(two_parts())));

    // Drive the real `serve()` (not the test's own axum::serve).
    let handle = tokio::spawn(async move { server.serve(addr).await });

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let url = format!("http://{addr}/");
    let req = json!({
        "jsonrpc": "2.0", "id": 1, "method": "message/send",
        "params": { "message": { "role": "user", "kind": "message",
            "messageId": "m1", "parts": [{ "kind": "text", "text": "ping" }] } }
    });

    // Retry-connect until serve() has bound + is accepting.
    let mut body: Option<Value> = None;
    for _ in 0..50 {
        match client.post(&url).json(&req).send().await {
            Ok(resp) => {
                body = Some(resp.json().await.expect("json body"));
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    let body = body.expect("serve() never became reachable");

    // The A2A result is a completed Task with the executor's artifact.
    let result = &body["result"];
    assert_eq!(result["kind"], json!("task"));
    assert_eq!(result["status"]["state"], json!("completed"));
    let parts = result["artifacts"][0]["parts"].as_array().expect("parts");
    assert_eq!(parts[0]["text"], json!("hello"));
    assert_eq!(parts[1]["text"], json!("world"));

    handle.abort();
}

#[tokio::test]
async fn serve_bind_error_propagates() {
    // Hold a listener on a port, then ask serve() to bind the SAME address:
    // the bind fails and serve() returns the io::Error via `?`.
    let held = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind held");
    let addr = held.local_addr().expect("local addr");

    let server = build(Arc::new(FakeExecutor::ok(two_parts())));
    let result = server.serve(addr).await;

    assert!(
        result.is_err(),
        "serve() must surface the bind error for an in-use address"
    );
    drop(held);
}
