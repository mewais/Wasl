//! In-memory smoke test: wire a real rmcp client to the `WaslMcp` server over a
//! `tokio::io::duplex` pipe (no OS process, no stdio) and assert that the four
//! Wasl tools are advertised. This covers the async rmcp handler surface end to
//! end — tool registration + `list_tools` over the wire — which the pure-core
//! unit tests cannot.

use rmcp::ServiceExt;
use tokio::io::duplex;

use wasl_audit::AuditLog;
use wasl_mcp::{LocalMesh, MemSink, NoopRunner, WaslMcp, WaslState};
use wasl_proto::{AgentId, HostId};
use wasl_registry::PeerRegistry;

fn handler() -> WaslMcp<MemSink, LocalMesh, NoopRunner> {
    let audit = AuditLog::new(MemSink::new());
    let state = WaslState::new(audit, LocalMesh, NoopRunner);
    WaslMcp::new(
        AgentId("me".to_string()),
        HostId("h1".to_string()),
        PeerRegistry::new(),
        state,
    )
}

#[tokio::test]
async fn lists_the_four_tools() {
    // Two in-memory pipes: client->server and server->client.
    let (client_tx, server_rx) = duplex(4096);
    let (server_tx, client_rx) = duplex(4096);

    // Serve the Wasl handler on the server ends of the pipes. The server's
    // `serve` blocks until it receives the client's `initialize`, so it must run
    // concurrently with the client handshake below — spawn it.
    let server_task = tokio::spawn(async move { handler().serve((server_rx, server_tx)).await });

    // Drive a bare client (`()` is a valid client service) on the other ends.
    let client = ().serve((client_rx, client_tx)).await.expect("client must initialize");

    let server = server_task
        .await
        .expect("server task join")
        .expect("server must initialize");

    let tools = client
        .list_all_tools()
        .await
        .expect("list_tools must succeed");

    let mut names: Vec<String> = tools.iter().map(|t| t.name.to_string()).collect();
    names.sort();
    assert_eq!(
        names,
        vec![
            "command".to_string(),
            "consult".to_string(),
            "message".to_string(),
            "transfer".to_string(),
        ],
        "the four Wasl tools must be advertised"
    );

    client.cancel().await.expect("client shutdown");
    server.cancel().await.expect("server shutdown");
}
