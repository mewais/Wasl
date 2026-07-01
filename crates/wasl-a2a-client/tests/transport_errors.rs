//! `tests/transport_errors.rs` — every client method must surface a transport
//! failure as `ClientError::Http`. CL7 covers `send_message`; these cover the
//! `fetch_agent_card` and `send_message_streaming` paths (distinct `execute`
//! instantiations), so the shared transport-error handling is exercised for all.

use std::net::SocketAddr;

use wasl_a2a_client::{A2aClient, ClientError};
use wasl_a2a_types::Message;

/// A definitely-closed loopback address: bind an ephemeral port, then drop it.
async fn dead_addr() -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral");
    let addr = l.local_addr().expect("local addr");
    drop(l);
    addr
}

#[tokio::test]
async fn fetch_agent_card_transport_failure_is_http() {
    let addr = dead_addr().await;
    let client = A2aClient::new(format!("http://{addr}/"));
    let err = client
        .fetch_agent_card()
        .await
        .expect_err("dead address must fail");
    assert!(
        matches!(err, ClientError::Http(_)),
        "expected ClientError::Http, got {err:?}"
    );
}

#[tokio::test]
async fn get_task_transport_failure_is_http() {
    let addr = dead_addr().await;
    let client = A2aClient::new(format!("http://{addr}/"));
    let err = client
        .get_task("t1")
        .await
        .expect_err("dead address must fail");
    assert!(
        matches!(err, ClientError::Http(_)),
        "expected ClientError::Http, got {err:?}"
    );
}

#[tokio::test]
async fn send_message_streaming_transport_failure_is_http() {
    let addr = dead_addr().await;
    let client = A2aClient::new(format!("http://{addr}/"));
    let result = client
        .send_message_streaming(Message::user_text("ping", "m1"))
        .await;
    match result {
        Err(ClientError::Http(_)) => {}
        Err(other) => panic!("expected ClientError::Http, got {other:?}"),
        Ok(_) => panic!("expected ClientError::Http, got Ok(events)"),
    }
}
