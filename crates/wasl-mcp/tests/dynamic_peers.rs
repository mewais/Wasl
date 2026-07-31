//! D3: a peer registered at runtime via the shared registry handle is visible
//! to the send path. `WaslMcp` builds every dispatch's `ToolCtx` from
//! `registry_snapshot()`, and `registry_handle()` returns the SAME
//! `Arc<RwLock<PeerRegistry>>` the runtime-control `/v1/peers` endpoint mutates
//! — so a peer Marsad adds/removes at runtime reaches `consult`/`command`/
//! `message` dispatch. This proves that linkage in-process (the live 2-node
//! path is D5).

use wasl_audit::AuditLog;
use wasl_mcp::{MemSink, WaslMcp, WaslState};
use wasl_proto::{AgentCard, AgentId, HostId};
use wasl_registry::PeerRegistry;
use wasl_transfer::Runner;

/// A tiny inline transfer runner (mirrors `smoke.rs`); this test never runs a
/// transfer, it only inspects the peer registry.
struct OkRunner;
impl Runner for OkRunner {
    fn run(&mut self, _argv: &[String]) -> Result<(), String> {
        Ok(())
    }
}

fn handler() -> WaslMcp<MemSink, OkRunner> {
    let audit = AuditLog::new(MemSink::new());
    let state = WaslState::new(audit, reqwest::Client::new(), OkRunner);
    WaslMcp::new(
        AgentId("me".to_string()),
        HostId("h1".to_string()),
        PeerRegistry::new(),
        state,
    )
}

#[test]
fn registered_peer_is_visible_to_send_path_snapshot() {
    let mcp = handler();

    // Empty to start — the snapshot the send path reads has no peers.
    assert!(
        mcp.registry_snapshot().list().is_empty(),
        "a fresh handler must have no peers"
    );

    // Register a peer at runtime through the shared handle — exactly what the
    // runtime-control `POST /v1/peers` handler does.
    mcp.registry_handle().write().unwrap().register(AgentCard {
        id: AgentId("peer-b".to_string()),
        host: HostId("h2".to_string()),
        endpoint: Some("http://10.0.0.2:8080".to_string()),
    });

    // The send-path snapshot (the exact value each dispatch builds its ToolCtx
    // from) now sees it.
    let snap = mcp.registry_snapshot();
    let listed = snap.list();
    assert_eq!(listed.len(), 1, "the runtime-registered peer must appear");
    assert_eq!(listed[0].id.as_str(), "peer-b");
    assert_eq!(listed[0].endpoint.as_deref(), Some("http://10.0.0.2:8080"));

    // Unregister → gone from the send-path snapshot too.
    assert!(
        mcp.registry_handle()
            .write()
            .unwrap()
            .unregister(&AgentId("peer-b".to_string())),
        "unregister must report the peer was present"
    );
    assert!(
        mcp.registry_snapshot().list().is_empty(),
        "after unregister the send path must see no peers"
    );
}
