#![allow(dead_code)]
//! Shared black-box fixtures for the `wasl_node` two-node loopback e2e suite.
//!
//! Nothing here touches `wasl-node`'s `src/`. These helpers stand up the REAL
//! RECEIVE side of a node — `wasl_node::receiver(card, fake_adapter)` returns a
//! REAL [`wasl_a2a_server::A2aServer`] (which internally wraps a REAL
//! `wasl_adapter::AdapterExecutor` over the FAKE innermost [`AgentAdapter`], the
//! ONLY fake in the path) — bound on an ephemeral `127.0.0.1:0` loopback port and
//! served by `axum`. The listener is bound BEFORE `axum::serve` is spawned (no
//! readiness race); the serve task is aborted when the [`ReceivePeer`] drops so
//! tests never hang. The fake adapter records every [`Delivery`] that crosses the
//! wire (verb + sender + body) and echoes `"echo:{text}"` (or fails with an
//! [`AdapterError`] when scripted). The SEND side of the suite drives a real
//! [`wasl_node::Node`] whose gate is fed by the [`FakeSurfacer`] and audited by a
//! recording / failing [`Sink`]. `e2e.rs` `mod common;`s this.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use wasl_a2a_types::{AgentCapabilities, AgentCard as A2aCard};
use wasl_adapter::{AdapterError, AgentAdapter, Delivery};
use wasl_audit::{AuditEntry, AuditError, Sink};
use wasl_proto::{AgentCard, AgentId, GateRequest, HostId};
use wasl_server::Surfacer;

// The gate-facade types live only in `wasl-gate`, which is NOT a dependency of
// `wasl-node`; they reach a consumer of the node's public API (this suite) via
// `wasl_node` re-exports (forced by `Node::new(caps)` / `mode()` / the `Surfacer`
// signature). Until the node lands these are unresolved — that is the point.
use wasl_node::{ClientCaps, GateMode, GateOutcome};

// ── Identity + card helpers ──────────────────────────────────────────────────

/// Build an `AgentId` from a `&str`.
pub fn agent(id: &str) -> AgentId {
    AgentId(id.to_string())
}

/// A Wasl `AgentCard` for peer `id` reachable at A2A base URL `url` (its
/// `endpoint` — the field dispatch routes on).
pub fn wasl_proto_card(id: &str, url: &str) -> AgentCard {
    AgentCard {
        id: agent(id),
        host: HostId("loopback".to_string()),
        driver: None,
        workstream: None,
        endpoint: Some(url.to_string()),
    }
}

/// The A2A discovery card each loopback receiver advertises. Content is
/// irrelevant to the send path (which routes via the Wasl registry), but the
/// server needs one.
pub fn a2a_card() -> A2aCard {
    A2aCard {
        name: "loopback-peer".to_string(),
        description: "wasl-node loopback peer".to_string(),
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

// ── Capabilities → gate mode ─────────────────────────────────────────────────

/// Caps that select `GateMode::Elicitation` (synchronous surface → decide now).
pub fn caps_elicitation() -> ClientCaps {
    ClientCaps {
        elicitation: true,
        external: false,
    }
}

/// Caps that select `GateMode::PrepareCommit` (Tier-2: defer → `commit`).
pub fn caps_prepare_commit() -> ClientCaps {
    ClientCaps {
        elicitation: false,
        external: false,
    }
}

// ── Fake Surfacer (approve / deny / dismiss) ─────────────────────────────────

/// What a [`FakeSurfacer`] answers when the gate surfaces a request.
#[derive(Debug, Clone)]
pub enum SurfaceDecision {
    Approve,
    Deny(String),
    Dismiss,
}

/// A scripted human at the gate. In synchronous tiers the node calls `surface`;
/// it returns the pre-set outcome. (Tier-2 never surfaces — it defers.)
pub struct FakeSurfacer {
    decision: SurfaceDecision,
}

impl FakeSurfacer {
    pub fn approve() -> Self {
        Self {
            decision: SurfaceDecision::Approve,
        }
    }

    pub fn deny(reason: &str) -> Self {
        Self {
            decision: SurfaceDecision::Deny(reason.to_string()),
        }
    }

    pub fn dismiss() -> Self {
        Self {
            decision: SurfaceDecision::Dismiss,
        }
    }
}

impl Surfacer for FakeSurfacer {
    fn surface(&mut self, _mode: GateMode, _req: &GateRequest) -> GateOutcome {
        match &self.decision {
            SurfaceDecision::Approve => GateOutcome::Approved,
            SurfaceDecision::Deny(reason) => GateOutcome::Denied {
                reason: reason.clone(),
            },
            SurfaceDecision::Dismiss => GateOutcome::Dismissed,
        }
    }
}

// ── Audit sinks (recording + failing) ────────────────────────────────────────

/// A durable sink that always accepts and records each entry (so a test may also
/// cross-check via [`RecordingSink::handle`]; the primary read is
/// `Node::audit_entries()`).
pub struct RecordingSink {
    entries: Arc<Mutex<Vec<AuditEntry>>>,
}

impl RecordingSink {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// A shared handle to what this sink has durably recorded.
    pub fn handle(&self) -> Arc<Mutex<Vec<AuditEntry>>> {
        self.entries.clone()
    }
}

impl Default for RecordingSink {
    fn default() -> Self {
        Self::new()
    }
}

impl Sink for RecordingSink {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
        self.entries.lock().expect("entries lock").push(entry.clone());
        Ok(())
    }
}

/// A sink that always rejects the append (models a durability failure). The node
/// must abort the send BEFORE dispatch when the decision cannot be recorded.
pub struct FailingSink;

impl Sink for FailingSink {
    fn append(&mut self, _entry: &AuditEntry) -> Result<(), AuditError> {
        Err(AuditError::Sink("sink offline".to_string()))
    }
}

// ── Fake innermost AgentAdapter (the only fake in the path) ───────────────────

/// The recording, scriptable innermost `AgentAdapter`. Every [`Delivery`] it
/// receives is pushed onto `recorded` (so a test can assert exactly what crossed
/// the wire: verb + sender + body). When `fail` is `Some(msg)` it returns an
/// [`AdapterError`] (→ a `Failed` A2A Task); otherwise it echoes `"echo:{text}"`.
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
            None => Ok(format!("echo:{}", d.text)),
        }
    }
}

/// A running loopback receiver node: its A2A base URL, a handle to the deliveries
/// its fake adapter observed, and the serve task (aborted on drop so tests never
/// hang).
pub struct ReceivePeer {
    pub url: String,
    recorded: Arc<Mutex<Vec<Delivery>>>,
    handle: tokio::task::JoinHandle<()>,
}

impl ReceivePeer {
    /// Snapshot of every [`Delivery`] this peer's adapter has received, in order.
    pub fn deliveries(&self) -> Vec<Delivery> {
        self.recorded.lock().expect("recorded lock").clone()
    }
}

impl Drop for ReceivePeer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Spin the REAL receive side via `wasl_node::receiver(card, fake_adapter)`,
/// scripted with `fail`. Binds the listener BEFORE spawning `axum::serve`.
async fn spawn_peer(fail: Option<String>) -> ReceivePeer {
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let adapter = FakeAdapter {
        recorded: recorded.clone(),
        fail,
    };

    let server = wasl_node::receiver(a2a_card(), adapter);
    let app = Arc::new(server).router();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    ReceivePeer {
        url: format!("http://127.0.0.1:{port}/"),
        recorded,
        handle,
    }
}

/// A healthy receiver whose adapter echoes `"echo:{text}"`.
pub async fn echo_peer() -> ReceivePeer {
    spawn_peer(None).await
}

/// A receiver whose adapter always returns an `AdapterError` (→ a `Failed` Task).
pub async fn failing_peer(message: &str) -> ReceivePeer {
    spawn_peer(Some(message.to_string())).await
}
