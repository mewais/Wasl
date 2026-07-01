#![allow(dead_code)]
//! Shared fakes for the wasl-mcp test suite.
//!
//! Mirrors the `wasl-daemon/tests/common` style: a scriptable/recording `Mesh`
//! and `Runner`, plus an audit `Sink` that can be made to fail so the
//! durability-first behaviour of the `resolve_and_dispatch` heart is testable.

use std::collections::HashMap;

use wasl_audit::{AuditEntry, AuditError, Sink};
use wasl_daemon::Mesh;
use wasl_proto::{AgentCard, AgentId, Envelope, HostId};
use wasl_registry::{PeerRegistry, Resolved};
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

pub fn card(id: &str, host_id: &str) -> AgentCard {
    AgentCard {
        id: agent(id),
        host: host(host_id),
        driver: None,
        workstream: None,
        endpoint: None,
    }
}

/// A registry with the given `(agent, host)` cards registered.
pub fn registry_with(cards: &[(&str, &str)]) -> PeerRegistry {
    let mut reg = PeerRegistry::new();
    for (id, h) in cards {
        reg.register(card(id, h));
    }
    reg
}

// ---------------------------------------------------------------------
// FakeMesh
// ---------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct DeliverCall {
    pub resolved: Resolved,
    pub env: Envelope,
}

/// A scriptable, recording `Mesh` fake: `replies` maps a peer id to the body it
/// returns; `fail` maps a peer id to an error; `calls` records every delivery.
pub struct FakeMesh {
    replies: HashMap<String, String>,
    fail: HashMap<String, String>,
    pub calls: Vec<DeliverCall>,
}

impl FakeMesh {
    pub fn new() -> Self {
        Self {
            replies: HashMap::new(),
            fail: HashMap::new(),
            calls: Vec::new(),
        }
    }

    pub fn with_reply(mut self, peer: &str, body: &str) -> Self {
        self.replies.insert(peer.to_string(), body.to_string());
        self
    }

    pub fn with_failure(mut self, peer: &str, detail: &str) -> Self {
        self.fail.insert(peer.to_string(), detail.to_string());
        self
    }
}

impl Default for FakeMesh {
    fn default() -> Self {
        Self::new()
    }
}

impl Mesh for FakeMesh {
    fn deliver(&mut self, resolved: &Resolved, env: &Envelope) -> Result<String, String> {
        self.calls.push(DeliverCall {
            resolved: resolved.clone(),
            env: env.clone(),
        });
        let peer = env.to[0].as_str().to_string();
        if let Some(detail) = self.fail.get(&peer) {
            return Err(detail.clone());
        }
        match self.replies.get(&peer) {
            Some(body) => Ok(body.clone()),
            None => Ok(format!("ack:{peer}")),
        }
    }
}

// ---------------------------------------------------------------------
// FakeRunner
// ---------------------------------------------------------------------

/// A scriptable, recording `Runner` fake; fails any job whose destination host
/// (the `dst:path` token) is in `fail_dst`.
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

// ---------------------------------------------------------------------
// Audit sinks
// ---------------------------------------------------------------------

/// A recording, always-successful audit sink.
#[derive(Debug, Default, Clone)]
pub struct RecordingSink {
    pub entries: Vec<AuditEntry>,
}

impl RecordingSink {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Sink for RecordingSink {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
        self.entries.push(entry.clone());
        Ok(())
    }
}

/// An audit sink that always fails — to prove durability-first behaviour.
#[derive(Debug, Default, Clone)]
pub struct FailingSink;

impl Sink for FailingSink {
    fn append(&mut self, _entry: &AuditEntry) -> Result<(), AuditError> {
        Err(AuditError::Sink("disk full".to_string()))
    }
}
