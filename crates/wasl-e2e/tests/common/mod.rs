#![allow(dead_code)]
//! Shared fakes/fixtures for the `wasl-e2e` cross-crate scenario suite.
//!
//! This module is compiled independently into each test binary, hence the
//! crate-wide `allow(dead_code)` (a given test binary uses only a subset).
//!
//! The GATE side wires the real trait seams of `wasl-server`:
//! * [`ScriptSurfacer`] — a scriptable [`wasl_server::Surfacer`] whose outcome is
//!   fixed per construction (Tier-1 Elicitation / Tier-3 External path — any
//!   external authority implements the same `Surfacer` seam).
//! * [`FakeSink`] — a recording [`wasl_audit::Sink`], scriptable to fail append.
//!
//! The DISPATCH side (post-W9) is the LIVE A2A path, mirroring
//! `wasl-dispatch/tests/common` and `wasl-node/tests/common`: a REAL
//! `wasl_a2a_server::A2aServer` (axum) on an ephemeral `127.0.0.1` port, driven
//! by a REAL `wasl_adapter::AdapterExecutor` wrapping a FAKE innermost
//! [`wasl_adapter::AgentAdapter`] (the ONLY fake in that half of the path):
//! * [`Peer`] — a running loopback receiver: its A2A base URL, the `Delivery`s
//!   its fake adapter observed, and a serve task aborted on drop.
//! * [`spawn_echo`]/[`spawn_failing`] — spin a healthy/erroring `Peer`.
//! * [`dead_url`] — an ephemeral port bound then immediately dropped, modelling
//!   a transport-level failure (peer registered but unreachable).
//! * [`register`]/[`card`] — register an `AgentCard` (with an A2A `endpoint`) in
//!   a `wasl_registry::PeerRegistry`, the way `wasl_dispatch` resolves peers.
//!
//! Transfer (X5) does not go through `wasl-dispatch` (it is host-addressed, not
//! peer-addressed) — it calls `wasl_transfer::execute` directly over a
//! [`FakeRunner`], exactly what the retired `wasl_daemon::transfer` used to do
//! internally (it was a one-line passthrough).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use wasl_a2a_server::A2aServer;
use wasl_a2a_types::{AgentCapabilities, AgentCard as A2aCard};
use wasl_adapter::{AdapterError, AdapterExecutor, AgentAdapter, Delivery};
use wasl_audit::{AuditEntry, AuditError, Sink};
use wasl_gate::{GateMode, GateOutcome};
use wasl_proto::{AgentCard, AgentId, GateRequest, HostId};
use wasl_registry::PeerRegistry;
use wasl_server::Surfacer;
use wasl_transfer::Runner;

// ---------------------------------------------------------------------
// Construction helpers
// ---------------------------------------------------------------------

pub fn agent(id: &str) -> AgentId {
    AgentId(id.to_string())
}

pub fn host(id: &str) -> HostId {
    HostId(id.to_string())
}

/// The A2A discovery card each loopback peer advertises. Content is irrelevant
/// to dispatch (which routes via the Wasl `PeerRegistry`), but the server needs
/// one.
fn a2a_card() -> A2aCard {
    A2aCard {
        name: "loopback-peer".to_string(),
        description: "wasl-e2e loopback peer".to_string(),
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

/// A Wasl `AgentCard` for peer `id`, reachable at A2A base URL `endpoint` (the
/// field `wasl_dispatch` resolves on). `host` is a display label only —
/// dispatch routes by `endpoint`, not `host`.
pub fn card(id: &str, host_id: &str, endpoint: Option<String>) -> AgentCard {
    AgentCard {
        id: agent(id),
        host: host(host_id),
        driver: None,
        workstream: None,
        endpoint,
    }
}

/// Register `id` in `reg` at loopback `url` (host label `"host-a"` — irrelevant
/// to routing, which is by `endpoint`).
pub fn register(reg: &mut PeerRegistry, id: &str, url: &str) {
    reg.register(card(id, "host-a", Some(url.to_string())));
}

/// A short-timeout `reqwest::Client`, shared across all peers in a test so a
/// dead port fails fast rather than hanging the suite.
pub fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build reqwest client")
}

// ---------------------------------------------------------------------
// ScriptSurfacer — a plain scriptable Surfacer (Tier-1/Tier-3 synchronous)
// ---------------------------------------------------------------------

/// A `Surfacer` whose outcome is fixed at construction and which records the
/// `(mode, request-id)` of every surface call so a test can assert it was (or
/// was not) consulted.
pub struct ScriptSurfacer {
    outcome: GateOutcome,
    pub calls: Vec<(GateMode, String)>,
}

impl ScriptSurfacer {
    pub fn approving() -> Self {
        Self {
            outcome: GateOutcome::Approved,
            calls: Vec::new(),
        }
    }

    pub fn denying(reason: &str) -> Self {
        Self {
            outcome: GateOutcome::Denied {
                reason: reason.to_string(),
            },
            calls: Vec::new(),
        }
    }

    pub fn dismissing() -> Self {
        Self {
            outcome: GateOutcome::Dismissed,
            calls: Vec::new(),
        }
    }
}

impl Surfacer for ScriptSurfacer {
    fn surface(&mut self, mode: GateMode, req: &GateRequest) -> GateOutcome {
        self.calls.push((mode, req.id.clone()));
        self.outcome.clone()
    }
}

// ---------------------------------------------------------------------
// FakeSink — recording audit Sink, scriptable to fail
// ---------------------------------------------------------------------

/// A recording `Sink`. When `fail` is set every `append` errors and nothing is
/// recorded (durability-first); otherwise it records the appended entry.
pub struct FakeSink {
    fail: bool,
    pub appended: Vec<AuditEntry>,
}

impl FakeSink {
    pub fn ok() -> Self {
        Self {
            fail: false,
            appended: Vec::new(),
        }
    }

    pub fn failing() -> Self {
        Self {
            fail: true,
            appended: Vec::new(),
        }
    }
}

impl Sink for FakeSink {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
        if self.fail {
            return Err(AuditError::Sink("disk full".to_string()));
        }
        self.appended.push(entry.clone());
        Ok(())
    }
}

// ---------------------------------------------------------------------
// FakeAdapter + Peer — the LIVE A2A loopback receive side (post-W9)
// ---------------------------------------------------------------------

/// The recording, scriptable innermost `AgentAdapter` — the ONLY fake on the
/// dispatch half of the suite. Every [`Delivery`] it receives is pushed onto
/// `recorded` (so a test can assert exactly what crossed the wire: verb +
/// sender + body). When `fail` is `Some(msg)` it returns an [`AdapterError`]
/// (→ the real `AdapterExecutor` yields an `ExecError` → the A2A Task ends
/// `Failed`); otherwise it echoes `"reply:{delivery.text}"`.
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

/// A running loopback peer: its A2A base URL, a handle to the deliveries its
/// fake adapter observed, and the serve task (aborted on drop so tests never
/// hang).
pub struct Peer {
    pub url: String,
    recorded: Arc<Mutex<Vec<Delivery>>>,
    handle: tokio::task::JoinHandle<()>,
}

impl Peer {
    /// Snapshot of every [`Delivery`] this peer's adapter has received, in
    /// order — proves whether (and what) the live A2A path actually delivered.
    pub fn deliveries(&self) -> Vec<Delivery> {
        self.recorded.lock().expect("recorded lock").clone()
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Spin a REAL `A2aServer` over a REAL `AdapterExecutor` wrapping a
/// `FakeAdapter` scripted with `fail`. Binds the listener BEFORE spawning
/// `axum::serve` (no readiness race).
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
pub async fn spawn_echo() -> Peer {
    spawn(None).await
}

/// A peer whose adapter always returns an `AdapterError` (→ a `Failed` Task).
pub async fn spawn_failing(message: &str) -> Peer {
    spawn(Some(message.to_string())).await
}

/// Bind an ephemeral loopback port, then immediately drop the listener so the
/// address is closed — a peer registered at this URL models a transport-level
/// delivery failure (the peer is unreachable).
pub async fn dead_url() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    format!("http://127.0.0.1:{port}/")
}

// ---------------------------------------------------------------------
// FakeRunner — per-dst scriptable transfer Runner (unchanged; transfer is
// host-addressed and never went through the daemon's Mesh/dispatch's A2A —
// `wasl_daemon::transfer` was a one-line passthrough to `wasl_transfer::execute`,
// which X5 now calls directly).
// ---------------------------------------------------------------------

/// A recording `Runner` fake that fails any job whose final argv token
/// (`<dst_host>:<path>`) references a destination host in `fail_dst`.
pub struct FakeRunner {
    fail_dst: Vec<String>,
    pub argvs: Vec<Vec<String>>,
}

impl FakeRunner {
    pub fn new() -> Self {
        Self {
            fail_dst: Vec::new(),
            argvs: Vec::new(),
        }
    }

    pub fn failing(mut self, dst: &str) -> Self {
        self.fail_dst.push(dst.to_string());
        self
    }
}

impl Default for FakeRunner {
    fn default() -> Self {
        Self::new()
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
