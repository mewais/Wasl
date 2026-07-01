#![allow(dead_code)]
//! Shared black-box fixtures for the `wasl_a2a_client` loopback suite.
//!
//! Nothing here touches the client's `src/`. These helpers stand up a REAL
//! `wasl_a2a_server` (axum) on an ephemeral loopback port driven by a scriptable
//! fake [`TaskExecutor`], plus the `AgentCard` fixture the card GET should echo.
//! `tests/loopback.rs` `mod common;`s this.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use wasl_a2a_server::{A2aServer, CounterIds, ExecError, TaskExecutor};
use wasl_a2a_types::{AgentCapabilities, AgentCard, AgentSkill, Message, Part};

/// A deterministic `AgentCard` the server advertises; CL4 asserts the client
/// round-trips its `name`/`url`/`capabilities`.
pub fn sample_card() -> AgentCard {
    AgentCard {
        name: "Test Agent".to_string(),
        description: "A2A peer under test".to_string(),
        url: "http://127.0.0.1/".to_string(),
        version: "0.1.0".to_string(),
        capabilities: AgentCapabilities {
            streaming: true,
            push_notifications: false,
        },
        default_input_modes: vec!["text/plain".to_string()],
        default_output_modes: vec!["text/plain".to_string()],
        skills: vec![AgentSkill {
            id: "echo".to_string(),
            name: "Echo".to_string(),
            description: "echoes text".to_string(),
            tags: vec!["demo".to_string()],
            examples: vec![],
        }],
    }
}

/// Fake executor that replies with a single text part `"pong:{received text}"`,
/// echoing the first text part of the incoming message.
pub struct PongExecutor;

#[async_trait::async_trait]
impl TaskExecutor for PongExecutor {
    async fn execute(&self, _context_id: String, message: Message) -> Result<Vec<Part>, ExecError> {
        let text = message
            .parts
            .iter()
            .find_map(wasl_a2a_types::part_text)
            .unwrap_or("")
            .to_string();
        Ok(vec![Part::Text {
            text: format!("pong:{text}"),
        }])
    }
}

/// Fake executor that always fails with an `ExecError` (→ a `failed` Task).
pub struct ErrExecutor {
    pub message: String,
}

#[async_trait::async_trait]
impl TaskExecutor for ErrExecutor {
    async fn execute(
        &self,
        _context_id: String,
        _message: Message,
    ) -> Result<Vec<Part>, ExecError> {
        Err(ExecError {
            message: self.message.clone(),
        })
    }
}

/// Bind an ephemeral loopback port, spawn a real `A2aServer` router driven by
/// `exec`, and return the bound address. The listener is bound BEFORE
/// `axum::serve` is spawned, so there is no readiness race.
pub async fn spawn_server(exec: Arc<dyn TaskExecutor>) -> SocketAddr {
    let server = Arc::new(A2aServer::with_ids(
        sample_card(),
        exec,
        Arc::new(CounterIds::default()),
    ));
    let app = server.router();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");

    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    addr
}

/// A reqwest client with a short timeout, for `A2aClient::with_client`.
pub fn reqwest_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client")
}
