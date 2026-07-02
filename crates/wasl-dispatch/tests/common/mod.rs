#![allow(dead_code)]
//! Shared black-box fixtures for the `wasl_dispatch` loopback suite.
//!
//! Nothing here touches the dispatch crate's `src/`. These helpers stand up a
//! REAL `wasl_a2a_server` (axum) on an ephemeral loopback port, driven by a REAL
//! `wasl_adapter::AdapterExecutor` wrapping a FAKE innermost [`AgentAdapter`]
//! (the only fake in the path). The fake records every [`Delivery`] that crosses
//! the wire (so a test can assert the verb + sender + body) and is scriptable to
//! echo `"reply:{text}"` or fail with an [`AdapterError`]. Listeners are bound
//! BEFORE `axum::serve` is spawned (no readiness race); each spawned server is
//! aborted when its [`Peer`] drops so tests never hang. `loopback.rs` `mod
//! common;`s this.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use wasl_a2a_server::A2aServer;
use wasl_a2a_types::{AgentCapabilities, AgentCard as A2aCard};
use wasl_adapter::{AdapterError, AdapterExecutor, AgentAdapter, Delivery};
use wasl_proto::{AgentCard, AgentId, HostId};
use wasl_registry::PeerRegistry;

/// Build an `AgentId` from a `&str`.
pub fn agent(id: &str) -> AgentId {
    AgentId(id.to_string())
}

/// The A2A discovery card each loopback server advertises. Content is irrelevant
/// to dispatch (which routes via the `PeerRegistry`), but the server needs one.
pub fn a2a_card() -> A2aCard {
    A2aCard {
        name: "loopback-peer".to_string(),
        description: "wasl-dispatch loopback peer".to_string(),
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

/// A short-timeout `reqwest::Client`, shared across all peers in a test so a dead
/// port fails fast rather than hanging the suite.
pub fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build reqwest client")
}

/// Register `id` in `reg` with the given A2A `endpoint` (loopback URL, or `None`
/// to model an unroutable peer). Host is a constant — dispatch resolves by
/// endpoint, not host.
pub fn register(reg: &mut PeerRegistry, id: &AgentId, endpoint: Option<String>) {
    reg.register(AgentCard {
        id: id.clone(),
        host: HostId("loopback".to_string()),
        endpoint,
    });
}

/// Bind an ephemeral loopback port, then immediately drop the listener so the
/// address is closed — a peer registered at this URL models a transport failure.
pub async fn dead_url() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    format!("http://127.0.0.1:{port}/")
}

/// A recording, scriptable innermost `AgentAdapter` — the ONLY fake in the path.
///
/// Every [`Delivery`] it receives is pushed onto `recorded` (so a test can assert
/// exactly what crossed the wire). When `fail` is `Some(msg)` it returns an
/// [`AdapterError`] (→ the real `AdapterExecutor` yields an `ExecError` → the A2A
/// Task ends `Failed`); otherwise it echoes `"reply:{delivery.text}"`.
pub struct FakeAdapter {
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
pub struct Peer {
    pub url: String,
    recorded: Arc<Mutex<Vec<Delivery>>>,
    handle: tokio::task::JoinHandle<()>,
}

impl Peer {
    /// Snapshot of every [`Delivery`] this peer's adapter has received, in order.
    pub fn deliveries(&self) -> Vec<Delivery> {
        self.recorded.lock().expect("recorded lock").clone()
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Spin a REAL `A2aServer` over a REAL `AdapterExecutor` wrapping a `FakeAdapter`
/// scripted with `fail`. Binds the listener BEFORE spawning `axum::serve`.
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
