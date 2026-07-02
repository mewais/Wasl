//! `tests/serve.rs` — exercises `wasl_operator::serve()` (the bind +
//! `axum::serve` accept loop), which `loopback.rs` bypasses by binding its
//! own listener via `router()`. Covers both the successful bind path and
//! bind-error propagation, mirroring `wasl-a2a-server`'s `tests/serve.rs`.
//! Per the crate spec's coverage bar, `serve()`'s bind loop is the one
//! router-adjacent piece allowed to stay otherwise uncovered — this test
//! exists so it isn't *entirely* uncovered.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use common::{at, node_info, FakeClock, T0_MS};

use wasl_operator::{CounterGateIds, HubConfig, OperatorHub};

/// Grab an ephemeral port, then release it so `serve()` can bind it itself.
async fn free_addr() -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral");
    let addr = l.local_addr().expect("local addr");
    drop(l);
    addr
}

fn hub() -> OperatorHub {
    OperatorHub::with_seams(
        node_info(),
        HubConfig::default(),
        Arc::new(FakeClock::new(at(T0_MS))),
        Arc::new(CounterGateIds::default()),
    )
}

#[tokio::test]
async fn serve_binds_and_answers_info() {
    let addr = free_addr().await;
    let h = hub();

    let serve_handle = tokio::spawn(async move { wasl_operator::serve(h, addr).await });

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let url = format!("http://{addr}/v1/info");

    // Retry-connect until serve() has bound + is accepting.
    let mut body: Option<Value> = None;
    for _ in 0..50 {
        match client.get(&url).send().await {
            Ok(resp) => {
                body = Some(resp.json().await.expect("json body"));
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    let body = body.expect("serve() never became reachable");
    assert_eq!(
        body,
        json!({ "agent_id": "wasl-a", "host_id": "host-1", "version": "0.1.0" })
    );

    serve_handle.abort();
}

#[tokio::test]
async fn serve_bind_error_propagates() {
    // Hold a listener on a port, then ask serve() to bind the SAME address:
    // the bind fails and serve() surfaces the io::Error via `?`.
    let held = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind held");
    let addr = held.local_addr().expect("local addr");

    let result = wasl_operator::serve(hub(), addr).await;
    assert!(
        result.is_err(),
        "serve() must surface the bind error for an in-use address"
    );
    drop(held);
}
