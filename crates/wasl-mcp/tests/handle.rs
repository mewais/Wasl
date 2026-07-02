//! Loopback A2A integration tests for the async request→reply heart
//! (`resolve_and_dispatch`), per `docs/specs/mcp-a2a.md`.
//!
//! These drive the gate→audit→A2A→receive→reply→back path END-TO-END over
//! 127.0.0.1 against a REAL `wasl_a2a_server::A2aServer` running a REAL
//! `wasl_adapter::AdapterExecutor` over a FAKE innermost `AgentAdapter` (the only
//! fake in the send path) — mirroring the `wasl-dispatch` loopback style. The
//! fake adapter records every `Delivery` (so a test can assert the verb + sender
//! + body crossed the wire) and echoes `"reply:{text}"`. Peers are resolved via a
//! real `PeerRegistry` by their A2A `endpoint`. Audit uses `MemSink` (or a
//! failing sink) to exercise the durability-first invariant.
//!
//! The suite MUST fail to compile until src is migrated to the new async A2A API
//! (`resolve_and_dispatch` async + `http: &reqwest::Client`) — test-first.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use wasl_a2a_server::A2aServer;
use wasl_a2a_types::{AgentCapabilities, AgentCard as A2aCard};
use wasl_adapter::{AdapterError, AdapterExecutor, AgentAdapter, Delivery, DeliveryKind};
use wasl_audit::{AuditEntry, AuditError, AuditLog, EntryKind, Sink};
use wasl_gate::GateOutcome;
use wasl_mcp::handle::{resolve_and_dispatch, HandleError, ToolCall, ToolCtx, ToolReply};
use wasl_mcp::MemSink;
use wasl_proto::{
    AgentCard, AgentId, CommandParams, ConsultParams, GateDecision, HostId, MessageParams,
    TransferParams,
};
use wasl_registry::PeerRegistry;
use wasl_transfer::{Runner, Tool};

// ---------------------------------------------------------------------
// Construction helpers
// ---------------------------------------------------------------------

fn agent(id: &str) -> AgentId {
    AgentId(id.to_string())
}

fn host(id: &str) -> HostId {
    HostId(id.to_string())
}

fn ids() -> (AgentId, HostId) {
    (agent("me"), host("h1"))
}

/// A short-timeout client so a dead port fails fast instead of hanging.
fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build reqwest client")
}

/// The A2A discovery card each loopback server advertises (content irrelevant —
/// dispatch routes via the `PeerRegistry`, not this card).
fn a2a_card() -> A2aCard {
    A2aCard {
        name: "loopback-peer".to_string(),
        description: "wasl-mcp loopback peer".to_string(),
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

/// Register `id` in `reg` with the given A2A `endpoint`. Host is a constant —
/// dispatch resolves by endpoint, not host.
fn register_peer(reg: &mut PeerRegistry, id: &AgentId, endpoint: Option<String>) {
    reg.register(AgentCard {
        id: id.clone(),
        host: host("loopback"),
        driver: None,
        workstream: None,
        endpoint,
    });
}

// ---------------------------------------------------------------------
// FakeAdapter — the ONLY fake in the send path
// ---------------------------------------------------------------------

/// A recording, scriptable innermost `AgentAdapter`. Every `Delivery` is pushed
/// onto `recorded`; when `fail` is `Some(msg)` it returns an `AdapterError`
/// (→ a `Failed` Task); otherwise it echoes `"reply:{delivery.text}"`.
struct FakeAdapter {
    recorded: Arc<Mutex<Vec<Delivery>>>,
    fail: Option<String>,
}

#[async_trait]
impl AgentAdapter for FakeAdapter {
    async fn deliver(&self, d: &Delivery) -> Result<String, AdapterError> {
        self.recorded.lock().expect("recorded lock").push(d.clone());
        match &self.fail {
            Some(message) => Err(AdapterError {
                message: message.clone(),
            }),
            None => Ok(format!("reply:{}", d.text)),
        }
    }
}

/// A running loopback peer: its A2A base URL, a handle to the deliveries its fake
/// adapter observed, and the serve task (aborted on drop so tests never hang).
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

/// Spin a REAL `A2aServer` over a REAL `AdapterExecutor` wrapping a `FakeAdapter`.
/// Binds the listener BEFORE spawning `axum::serve` (no readiness race).
async fn spawn(fail: Option<String>) -> Peer {
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let adapter = FakeAdapter {
        recorded: recorded.clone(),
        fail,
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

/// A healthy peer whose adapter echoes `"reply:{text}"`.
async fn spawn_echo() -> Peer {
    spawn(None).await
}

// ---------------------------------------------------------------------
// FakeRunner + failing audit sink
// ---------------------------------------------------------------------

/// A recording `Runner`. Any job whose destination host (the `dst:path` token) is
/// in `fail_dst` returns an error; every other job succeeds.
struct FakeRunner {
    fail_dst: Vec<String>,
    argvs: Vec<Vec<String>>,
}

impl FakeRunner {
    fn new() -> Self {
        Self {
            fail_dst: Vec::new(),
            argvs: Vec::new(),
        }
    }

    fn failing(dst: &str) -> Self {
        Self {
            fail_dst: vec![dst.to_string()],
            argvs: Vec::new(),
        }
    }
}

impl Runner for FakeRunner {
    fn run(&mut self, argv: &[String]) -> Result<(), String> {
        self.argvs.push(argv.to_vec());
        let dst_token = argv.last().cloned().unwrap_or_default();
        let dst_host = dst_token.split(':').next().unwrap_or("").to_string();
        if self.fail_dst.iter().any(|d| d == &dst_host) {
            return Err(format!("boom:{dst_host}"));
        }
        Ok(())
    }
}

/// An audit sink that always fails — to prove durability-first behaviour.
struct FailingSink;

impl Sink for FailingSink {
    fn append(&mut self, _entry: &AuditEntry) -> Result<(), AuditError> {
        Err(AuditError::Sink("disk full".to_string()))
    }
}

// ---------------------------------------------------------------------
// Approved paths dispatch over REAL loopback A2A
// ---------------------------------------------------------------------

#[tokio::test]
async fn approved_consult_dispatches_over_a2a_and_collects_reply() {
    let (self_id, self_host) = ids();
    let peer_id = agent("peerA");
    let peer = spawn_echo().await;

    let mut registry = PeerRegistry::new();
    register_peer(&mut registry, &peer_id, Some(peer.url.clone()));

    let ctx = ToolCtx {
        self_id: &self_id,
        self_host: &self_host,
        registry: &registry,
    };
    let http = http_client();
    let mut runner = FakeRunner::new();
    let mut audit = AuditLog::new(MemSink::new());

    let call = ToolCall::Consult(ConsultParams {
        peers: vec![peer_id.clone()],
        question: "status?".to_string(),
    });

    let reply = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Approved,
        call,
        "base-consult",
        0,
        &http,
        &mut runner,
        &mut audit,
    )
    .await
    .expect("approved consult must succeed");

    match reply {
        ToolReply::Consult(res) => {
            assert_eq!(res.replies.len(), 1, "one reply per delivered peer");
            assert_eq!(res.replies[0].from, peer_id);
            assert_eq!(res.replies[0].body, "reply:status?");
        }
        other => panic!("expected Consult reply, got {other:?}"),
    }

    // The decision was recorded once as an approve.
    let entries = audit.entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].kind, EntryKind::Consult);
    assert_eq!(entries[0].decision, Some(GateDecision::Approve));
    assert_eq!(entries[0].actor, self_id);

    // The verb + sender + body really crossed the A2A wire.
    let deliveries = peer.deliveries();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].kind, DeliveryKind::Consult);
    assert_eq!(deliveries[0].from, "me");
    assert_eq!(deliveries[0].text, "status?");
    assert_eq!(deliveries[0].context_id, "base-consult");
}

#[tokio::test]
async fn approved_command_carries_command_verb_over_a2a() {
    let (self_id, self_host) = ids();
    let peer_id = agent("worker");
    let peer = spawn_echo().await;

    let mut registry = PeerRegistry::new();
    register_peer(&mut registry, &peer_id, Some(peer.url.clone()));

    let ctx = ToolCtx {
        self_id: &self_id,
        self_host: &self_host,
        registry: &registry,
    };
    let http = http_client();
    let mut runner = FakeRunner::new();
    let mut audit = AuditLog::new(MemSink::new());

    let call = ToolCall::Command(CommandParams {
        peers: vec![peer_id.clone()],
        task: "build it".to_string(),
    });

    let reply = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Approved,
        call,
        "base-command",
        0,
        &http,
        &mut runner,
        &mut audit,
    )
    .await
    .expect("approved command must succeed");

    match reply {
        ToolReply::Command(res) => {
            assert_eq!(res.replies.len(), 1);
            assert_eq!(res.replies[0].from, peer_id);
            assert_eq!(res.replies[0].body, "reply:build it");
        }
        other => panic!("expected Command reply, got {other:?}"),
    }
    assert_eq!(audit.entries()[0].kind, EntryKind::Command);

    let deliveries = peer.deliveries();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].kind, DeliveryKind::Command);
    assert_eq!(deliveries[0].from, "me");
    assert_eq!(deliveries[0].text, "build it");
}

#[tokio::test]
async fn approved_message_reports_delivered_peer_over_a2a() {
    let (self_id, self_host) = ids();
    let peer_id = agent("peerA");
    let peer = spawn_echo().await;

    let mut registry = PeerRegistry::new();
    register_peer(&mut registry, &peer_id, Some(peer.url.clone()));

    let ctx = ToolCtx {
        self_id: &self_id,
        self_host: &self_host,
        registry: &registry,
    };
    let http = http_client();
    let mut runner = FakeRunner::new();
    let mut audit = AuditLog::new(MemSink::new());

    let call = ToolCall::Message(MessageParams {
        peers: vec![peer_id.clone()],
        payload: "fyi".to_string(),
    });

    let reply = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Approved,
        call,
        "base-message",
        0,
        &http,
        &mut runner,
        &mut audit,
    )
    .await
    .expect("approved message must succeed");

    match reply {
        ToolReply::Message(res) => {
            assert_eq!(res.delivered, vec![peer_id.clone()]);
        }
        other => panic!("expected Message reply, got {other:?}"),
    }
    assert_eq!(audit.entries()[0].kind, EntryKind::Message);

    let deliveries = peer.deliveries();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].kind, DeliveryKind::Message);
    assert_eq!(deliveries[0].from, "me");
    assert_eq!(deliveries[0].text, "fyi");
}

// ---------------------------------------------------------------------
// Transfer: over a Runner, NOT over A2A
// ---------------------------------------------------------------------

#[tokio::test]
async fn approved_transfer_runs_over_runner_and_never_touches_a2a() {
    let (self_id, self_host) = ids();
    // A live peer exists but transfer must never reach it (host-addressed, not
    // peer-addressed, and it does not go over A2A at all).
    let peer = spawn_echo().await;
    let registry = PeerRegistry::new();

    let ctx = ToolCtx {
        self_id: &self_id,
        self_host: &self_host,
        registry: &registry,
    };
    let http = http_client();
    let mut runner = FakeRunner::new();
    let mut audit = AuditLog::new(MemSink::new());

    let call = ToolCall::Transfer {
        tool: Tool::Scp,
        params: TransferParams {
            src_host: host("h1"),
            dst_hosts: vec![host("h2"), host("h3")],
            path: "/data/f".to_string(),
        },
    };

    let reply = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Approved,
        call,
        "base-transfer",
        0,
        &http,
        &mut runner,
        &mut audit,
    )
    .await
    .expect("approved transfer must succeed");

    match reply {
        ToolReply::Transfer(res) => {
            assert_eq!(res.delivered, vec![host("h2"), host("h3")]);
        }
        other => panic!("expected Transfer reply, got {other:?}"),
    }
    // One runner job per destination host; the A2A fake adapter was NOT involved.
    assert_eq!(runner.argvs.len(), 2);
    assert!(
        peer.deliveries().is_empty(),
        "transfer must not go over A2A"
    );
    assert_eq!(audit.entries()[0].kind, EntryKind::Transfer);
    assert_eq!(audit.entries()[0].decision, Some(GateDecision::Approve));
}

#[tokio::test]
async fn transfer_with_no_destinations_yields_empty_delivered() {
    let (self_id, self_host) = ids();
    let registry = PeerRegistry::new();

    let ctx = ToolCtx {
        self_id: &self_id,
        self_host: &self_host,
        registry: &registry,
    };
    let http = http_client();
    let mut runner = FakeRunner::new();
    let mut audit = AuditLog::new(MemSink::new());

    let call = ToolCall::Transfer {
        tool: Tool::Scp,
        params: TransferParams {
            src_host: host("h1"),
            dst_hosts: vec![],
            path: "/data/f".to_string(),
        },
    };

    let reply = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Approved,
        call,
        "base-empty",
        0,
        &http,
        &mut runner,
        &mut audit,
    )
    .await
    .expect("empty transfer is a valid (recorded) outcome");

    match reply {
        ToolReply::Transfer(res) => assert!(
            res.delivered.is_empty(),
            "no destinations → nothing delivered"
        ),
        other => panic!("expected Transfer reply, got {other:?}"),
    }
    assert!(runner.argvs.is_empty(), "no destinations → no runner jobs");
}

#[tokio::test]
async fn transfer_failing_runner_excludes_failed_host_from_delivered() {
    // Per the unchanged contract, `wasl_transfer::execute` collects per-job
    // failures into its (dropped) failures vec and returns `Ok((result, _))`;
    // only `NoDestinations` is an `Err`. So a failing job is NOT a `Blocked`
    // reply — the failed host is simply absent from `delivered` (partial
    // multicast survives).
    let (self_id, self_host) = ids();
    let registry = PeerRegistry::new();

    let ctx = ToolCtx {
        self_id: &self_id,
        self_host: &self_host,
        registry: &registry,
    };
    let http = http_client();
    let mut runner = FakeRunner::failing("h2");
    let mut audit = AuditLog::new(MemSink::new());

    let call = ToolCall::Transfer {
        tool: Tool::Scp,
        params: TransferParams {
            src_host: host("h1"),
            dst_hosts: vec![host("h2"), host("h3")],
            path: "/data/f".to_string(),
        },
    };

    let reply = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Approved,
        call,
        "base-fail",
        0,
        &http,
        &mut runner,
        &mut audit,
    )
    .await
    .expect("a failing transfer job does not error the call");

    match reply {
        ToolReply::Transfer(res) => {
            assert_eq!(
                res.delivered,
                vec![host("h3")],
                "the failed host is excluded; the healthy one still delivers"
            );
        }
        other => panic!("expected Transfer reply, got {other:?}"),
    }
    assert_eq!(runner.argvs.len(), 2, "both jobs were attempted");
}

// ---------------------------------------------------------------------
// The gate is never bypassed: non-Approved never dispatches
// ---------------------------------------------------------------------

#[tokio::test]
async fn denied_is_blocked_and_a2a_never_touched() {
    let (self_id, self_host) = ids();
    let peer_id = agent("peerA");
    let peer = spawn_echo().await;

    let mut registry = PeerRegistry::new();
    register_peer(&mut registry, &peer_id, Some(peer.url.clone()));

    let ctx = ToolCtx {
        self_id: &self_id,
        self_host: &self_host,
        registry: &registry,
    };
    let http = http_client();
    let mut runner = FakeRunner::new();
    let mut audit = AuditLog::new(MemSink::new());

    let call = ToolCall::Consult(ConsultParams {
        peers: vec![peer_id.clone()],
        question: "secret?".to_string(),
    });

    let reply = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Denied {
            reason: "nope".to_string(),
        },
        call,
        "base-deny",
        0,
        &http,
        &mut runner,
        &mut audit,
    )
    .await
    .expect("denial is a valid (recorded) outcome, not an error");

    match reply {
        ToolReply::Blocked { reason } => assert_eq!(reason, "nope"),
        other => panic!("expected Blocked, got {other:?}"),
    }

    // The deny was recorded; nothing crossed the A2A wire.
    assert_eq!(
        audit.entries()[0].decision,
        Some(GateDecision::Deny {
            reason: "nope".to_string()
        })
    );
    assert!(
        peer.deliveries().is_empty(),
        "denied call must never dispatch"
    );
}

#[tokio::test]
async fn dismissed_is_blocked_records_no_decision_and_never_dispatches() {
    let (self_id, self_host) = ids();
    let peer_id = agent("peerA");
    let peer = spawn_echo().await;

    let mut registry = PeerRegistry::new();
    register_peer(&mut registry, &peer_id, Some(peer.url.clone()));

    let ctx = ToolCtx {
        self_id: &self_id,
        self_host: &self_host,
        registry: &registry,
    };
    let http = http_client();
    let mut runner = FakeRunner::new();
    let mut audit = AuditLog::new(MemSink::new());

    let call = ToolCall::Consult(ConsultParams {
        peers: vec![peer_id.clone()],
        question: "q?".to_string(),
    });

    let reply = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Dismissed,
        call,
        "base-dismiss",
        0,
        &http,
        &mut runner,
        &mut audit,
    )
    .await
    .expect("dismissal is recorded, not an error");

    match reply {
        ToolReply::Blocked { reason } => assert_eq!(reason, "dismissed"),
        other => panic!("expected Blocked, got {other:?}"),
    }
    // A dismissal carries no gate decision (per GateOutcome::to_decision).
    assert_eq!(audit.entries()[0].decision, None);
    assert!(
        peer.deliveries().is_empty(),
        "dismissed call must never dispatch"
    );
}

// ---------------------------------------------------------------------
// Durability-first: audit sink failure aborts before dispatch
// ---------------------------------------------------------------------

#[tokio::test]
async fn audit_failure_aborts_before_dispatch() {
    let (self_id, self_host) = ids();
    let peer_id = agent("peerA");
    let peer = spawn_echo().await;

    let mut registry = PeerRegistry::new();
    register_peer(&mut registry, &peer_id, Some(peer.url.clone()));

    let ctx = ToolCtx {
        self_id: &self_id,
        self_host: &self_host,
        registry: &registry,
    };
    let http = http_client();
    let mut runner = FakeRunner::new();
    // The sink always errors.
    let mut audit = AuditLog::new(FailingSink);

    let call = ToolCall::Consult(ConsultParams {
        peers: vec![peer_id.clone()],
        question: "q?".to_string(),
    });

    let err = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Approved, // even an approval must fail if it can't be recorded
        call,
        "base-audit",
        0,
        &http,
        &mut runner,
        &mut audit,
    )
    .await
    .expect_err("a sink failure must fail the call");

    match err {
        HandleError::Audit(detail) => assert!(
            detail.contains("disk full"),
            "error surfaces the sink detail, got: {detail}"
        ),
    }

    // Durability-first: nothing crossed the A2A wire because the decision could
    // not be durably recorded — even though the outcome was Approved.
    assert!(
        peer.deliveries().is_empty(),
        "no dispatch when the decision cannot be durably recorded"
    );
}

// ---------------------------------------------------------------------
// Unknown peer: partial multicast — the known peer still succeeds
// ---------------------------------------------------------------------

#[tokio::test]
async fn unknown_peer_partitioned_known_peer_still_replies() {
    let (self_id, self_host) = ids();
    let ghost = agent("ghost");
    let peer_id = agent("peerA");
    let peer = spawn_echo().await;

    let mut registry = PeerRegistry::new();
    // ghost is intentionally NOT registered.
    register_peer(&mut registry, &peer_id, Some(peer.url.clone()));

    let ctx = ToolCtx {
        self_id: &self_id,
        self_host: &self_host,
        registry: &registry,
    };
    let http = http_client();
    let mut runner = FakeRunner::new();
    let mut audit = AuditLog::new(MemSink::new());

    let call = ToolCall::Consult(ConsultParams {
        peers: vec![ghost.clone(), peer_id.clone()],
        question: "q".to_string(),
    });

    let reply = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Approved,
        call,
        "base-unknown",
        0,
        &http,
        &mut runner,
        &mut audit,
    )
    .await
    .expect("a co-addressed unknown peer does not abort the call");

    match reply {
        ToolReply::Consult(res) => {
            assert_eq!(
                res.replies.len(),
                1,
                "only the resolved peer produces a reply"
            );
            assert_eq!(res.replies[0].from, peer_id);
            assert_eq!(res.replies[0].body, "reply:q");
        }
        other => panic!("expected Consult reply, got {other:?}"),
    }

    // The known peer really was reached; the ghost never was.
    let deliveries = peer.deliveries();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].from, "me");
    assert_eq!(audit.entries()[0].decision, Some(GateDecision::Approve));
}
