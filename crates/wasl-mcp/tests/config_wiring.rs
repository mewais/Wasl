//! Registry population + adapter factory tests, per
//! `docs/specs/node-config.md`'s `config_wiring.rs` plan — "the point of the
//! block": prove a config file's peers really populate a `PeerRegistry` that
//! routes real A2A dispatch, and that `build_adapter` wires the right binary.
//!
//! The loopback proof mirrors `tests/handle.rs`'s style (a real
//! `wasl_a2a_server::A2aServer` over a real `AdapterExecutor` wrapping a fake,
//! recording `AgentAdapter`) but cannot literally import from `handle.rs`
//! (each `tests/*.rs` file is its own separate crate), so the small harness is
//! duplicated here rather than shared.

use std::os::unix::fs::PermissionsExt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use wasl_a2a_server::A2aServer;
use wasl_a2a_types::{AgentCapabilities, AgentCard as A2aCard};
use wasl_adapter::{AdapterError, AdapterExecutor, AgentAdapter, Delivery, DeliveryKind};
use wasl_audit::AuditLog;
use wasl_gate::GateOutcome;
use wasl_mcp::handle::{resolve_and_dispatch, ToolCall, ToolCtx, ToolReply};
use wasl_mcp::{
    build_adapter, registry_from_peers, AdapterConfig, AdapterKind, Config, MemSink, PeerEntry,
};
use wasl_proto::{AgentCard, AgentId, ConsultParams, HostId};

fn agent(id: &str) -> AgentId {
    AgentId(id.to_string())
}

fn host(id: &str) -> HostId {
    HostId(id.to_string())
}

// ---------------------------------------------------------------------
// registry_from_peers
// ---------------------------------------------------------------------

/// The spec's two-peer example, expressed directly as `PeerEntry`s (this suite
/// exercises `registry_from_peers` itself, independent of TOML parsing, which
/// `config_pure.rs` already covers).
fn two_peer_entries() -> Vec<PeerEntry> {
    vec![
        PeerEntry {
            agent_id: "reviewer".to_string(),
            host_id: "hostB".to_string(),
            url: "http://10.0.0.7:4242/".to_string(),
            name: String::new(),
            host_name: String::new(),
        },
        PeerEntry {
            agent_id: "builder".to_string(),
            host_id: "hostA".to_string(),
            url: "http://127.0.0.1:4343/".to_string(),
            name: String::new(),
            host_name: String::new(),
        },
    ]
}

#[test]
fn registry_from_peers_populates_every_entry() {
    let registry = registry_from_peers(&two_peer_entries());
    assert_eq!(registry.len(), 2);

    let card = registry
        .get(&agent("reviewer"))
        .expect("reviewer must be registered");
    assert_eq!(
        card,
        &AgentCard {
            id: agent("reviewer"),
            host: host("hostB"),
            endpoint: Some("http://10.0.0.7:4242/".to_string()),
        },
        "the post-W0 3-field card shape, populated from the peer entry"
    );

    let other = registry
        .get(&agent("builder"))
        .expect("builder must be registered");
    assert_eq!(other.host, host("hostA"));
    assert_eq!(other.endpoint, Some("http://127.0.0.1:4343/".to_string()));
}

#[test]
fn registry_from_peers_empty_list_is_empty_registry() {
    let registry = registry_from_peers(&[]);
    assert!(registry.is_empty());
}

// ---------------------------------------------------------------------
// A loopback fake AgentAdapter (the only fake in the send path) — mirrors
// tests/handle.rs's FakeAdapter/Peer harness.
// ---------------------------------------------------------------------

struct FakeAdapter {
    recorded: Arc<Mutex<Vec<Delivery>>>,
}

#[async_trait]
impl AgentAdapter for FakeAdapter {
    async fn deliver(&self, d: &Delivery) -> Result<String, AdapterError> {
        self.recorded.lock().expect("recorded lock").push(d.clone());
        Ok(format!("reply:{}", d.text))
    }
}

struct Peer {
    url: String,
    recorded: Arc<Mutex<Vec<Delivery>>>,
    handle: tokio::task::JoinHandle<()>,
}

impl Peer {
    fn deliveries(&self) -> Vec<Delivery> {
        self.recorded.lock().expect("recorded lock").clone()
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

fn a2a_card() -> A2aCard {
    A2aCard {
        name: "loopback-peer".to_string(),
        description: "wasl-mcp config-wiring loopback peer".to_string(),
        url: "http://127.0.0.1/".to_string(),
        version: "0.1.0".to_string(),
        capabilities: AgentCapabilities {
            streaming: false,
            push_notifications: false,
        },
        default_input_modes: vec!["text/plain".to_string()],
        default_output_modes: vec!["text/plain".to_string()],
        skills: vec![],
    }
}

/// Spin a REAL `A2aServer` over a REAL `AdapterExecutor` wrapping a
/// `FakeAdapter`, bound before `axum::serve` spawns (no readiness race).
async fn spawn_echo() -> Peer {
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let adapter = FakeAdapter {
        recorded: recorded.clone(),
    };
    let executor = Arc::new(AdapterExecutor::new(adapter));
    let server = Arc::new(A2aServer::new(a2a_card(), executor));
    let app = server.router();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    Peer {
        url: format!("http://127.0.0.1:{port}/"),
        recorded,
        handle,
    }
}

/// A `Runner` that must never be invoked — this suite only drives `Consult`,
/// which never reaches the transfer seam.
struct UnusedRunner;

impl wasl_transfer::Runner for UnusedRunner {
    fn run(&mut self, _argv: &[String]) -> Result<(), String> {
        panic!("a Consult call must never touch the transfer runner");
    }
}

#[tokio::test]
async fn config_declared_peer_is_dispatchable_over_real_loopback_a2a() {
    let peer = spawn_echo().await;

    // Build the config FROM TOML text whose single peer's url is the bound
    // address of the just-spawned loopback server — proving file → registry →
    // real A2A route end to end.
    let toml = format!(
        "[[peers]]\nagent_id = \"peerA\"\nhost_id = \"loopback\"\nurl = \"{}\"\n",
        peer.url
    );
    let cfg = Config::from_toml_str(&toml).expect("a single valid peer entry must parse");
    let registry = registry_from_peers(&cfg.peers);

    let self_id = agent("me");
    let self_host = host("h1");
    let ctx = ToolCtx {
        self_id: &self_id,
        self_host: &self_host,
        registry: &registry,
    };
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build reqwest client");
    let mut runner = UnusedRunner;
    let mut audit = AuditLog::new(MemSink::new());

    let call = ToolCall::Consult(ConsultParams {
        peers: vec![agent("peerA")],
        question: "status?".to_string(),
    });

    let reply = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Approved,
        call,
        "config-wiring-consult",
        0,
        &http,
        &mut runner,
        &mut audit,
    )
    .await
    .expect("an approved consult over the config-declared peer must succeed");

    match reply {
        ToolReply::Consult(res) => {
            assert_eq!(res.replies.len(), 1);
            assert_eq!(res.replies[0].from, agent("peerA"));
            assert_eq!(res.replies[0].body, "reply:status?");
        }
        other => panic!("expected Consult reply, got {other:?}"),
    }

    let deliveries = peer.deliveries();
    assert_eq!(
        deliveries.len(),
        1,
        "the fake adapter must have SEEN the delivery"
    );
    assert_eq!(deliveries[0].kind, DeliveryKind::Consult);
    assert_eq!(deliveries[0].from, "me");
    assert_eq!(deliveries[0].text, "status?");
}

// ---------------------------------------------------------------------
// build_adapter
// ---------------------------------------------------------------------

#[test]
fn adapter_factory_default_bin_arm_constructs() {
    // The spec's documented negative probe ("no `claude` binary in the test
    // env, so `deliver` errs with a message mentioning `claude`") does not
    // hold in THIS sandbox: a real `claude` (2.1.198) is genuinely on PATH
    // (see crates/wasl-adapter-claude/src/lib.rs's module doc). Actually
    // invoking `deliver` here would shell out to the real CLI — nondeterministic,
    // possibly network/credential-dependent, exactly what tests must never do.
    // Per the spec's own documented alternative ("skip this negative probe and
    // rely on the stub test below for both arms"), this test only exercises the
    // `bin: None` match arm of `build_adapter` (constructing the adapter, which
    // is where the "use the default `claude` bin" decision actually happens);
    // the stub test below is the real behavioural proof, covering the
    // `bin: Some(_)` arm with a real child process end to end.
    let cfg = AdapterConfig {
        kind: AdapterKind::Claude,
        bin: None,
    };
    let _adapter = build_adapter(&cfg);
}

#[tokio::test]
async fn adapter_factory_custom_bin_used_via_with_bin_real_child_process() {
    let dir = tempfile::tempdir().expect("tempdir");
    let script_path = dir.path().join("claude-stub.sh");
    std::fs::write(
        &script_path,
        "#!/bin/sh\nprintf '%s' '{\"result\":\"stub-ok\"}'\n",
    )
    .expect("write stub script");
    let mut perms = std::fs::metadata(&script_path)
        .expect("stat stub")
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script_path, perms).expect("chmod +x stub");

    let cfg = AdapterConfig {
        kind: AdapterKind::Claude,
        bin: Some(script_path.to_string_lossy().into_owned()),
    };
    let adapter = build_adapter(&cfg);

    let delivery = Delivery {
        context_id: "ctx-1".to_string(),
        from: "peer".to_string(),
        kind: DeliveryKind::Consult,
        text: "hi".to_string(),
        data: vec![],
    };

    let reply = adapter
        .deliver(&delivery)
        .await
        .expect("the stub CLI must succeed, proving with_bin plumbing");
    assert_eq!(reply, "stub-ok");
}
