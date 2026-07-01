//! `tests/stream_decode.rs` — the SSE parser must surface malformed stream
//! frames as `ClientError::Decode` (per the spec), NOT panic. Each case stands
//! up a tiny axum server that returns a crafted `text/event-stream` body and
//! asserts `send_message_streaming` returns `Err(Decode)`.

use std::net::SocketAddr;

use axum::http::header;
use axum::routing::post;
use axum::Router;

use wasl_a2a_client::{A2aClient, ClientError};
use wasl_a2a_types::Message;

/// Serve `body` as `text/event-stream` on POST `/`.
async fn spawn_sse(body: &'static str) -> SocketAddr {
    let app = Router::new().route(
        "/",
        post(move || async move { ([(header::CONTENT_TYPE, "text/event-stream")], body) }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve sse");
    });
    addr
}

async fn expect_decode(body: &'static str, case: &str) {
    let addr = spawn_sse(body).await;
    let client = A2aClient::new(format!("http://{addr}/"));
    let result = client
        .send_message_streaming(Message::user_text("ping", "m1"))
        .await;
    // Don't format the Ok payload (A2aEvent has no Debug) — match instead.
    match result {
        Err(ClientError::Decode(_)) => {}
        Err(other) => panic!("{case}: expected ClientError::Decode, got {other:?}"),
        Ok(_) => panic!("{case}: expected ClientError::Decode, got Ok(events)"),
    }
}

#[tokio::test]
async fn frame_without_data_prefix_is_decode_error() {
    // A frame that is non-empty but carries no `data:` line.
    expect_decode("event: ping\n\n", "no data prefix").await;
}

#[tokio::test]
async fn frame_with_malformed_json_is_decode_error() {
    expect_decode("data: {not valid json}\n\n", "malformed json").await;
}

#[tokio::test]
async fn frame_with_unknown_kind_is_decode_error() {
    expect_decode("data: {\"kind\":\"mystery-update\"}\n\n", "unknown kind").await;
}

#[tokio::test]
async fn status_update_frame_with_bad_shape_is_decode_error() {
    // Right `kind`, but missing the required taskId/contextId/status fields.
    expect_decode(
        "data: {\"kind\":\"status-update\"}\n\n",
        "bad status-update shape",
    )
    .await;
}

#[tokio::test]
async fn artifact_update_frame_with_bad_shape_is_decode_error() {
    expect_decode(
        "data: {\"kind\":\"artifact-update\"}\n\n",
        "bad artifact-update shape",
    )
    .await;
}
