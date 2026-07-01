#![allow(dead_code)]
//! Shared fakes for the wasl-daemon test suite.
//!
//! `FakeMesh` records every `(Resolved, Envelope)` it is asked to deliver so
//! tests can assert envelope ids/kind/to and the resolved route (Local vs
//! Remote), and is scriptable to return a specific reply body per peer or to
//! fail specific peers.
//!
//! `FakeRunner` records the argvs it is asked to run and is scriptable to fail
//! specific destination hosts (matched by the `dst_host:path` argv token).

use std::collections::HashMap;

use wasl_proto::{AgentCard, AgentId, Envelope, HostId};
use wasl_registry::Resolved;
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

/// An `AgentCard` for `id` living on `host`.
pub fn card(id: &str, host_id: &str) -> AgentCard {
    AgentCard {
        id: agent(id),
        host: host(host_id),
        driver: None,
        workstream: None,
        endpoint: None,
    }
}

// ---------------------------------------------------------------------
// FakeMesh
// ---------------------------------------------------------------------

/// One recorded delivery: the route the daemon resolved and the exact envelope
/// it handed to the mesh.
#[derive(Debug, Clone)]
pub struct DeliverCall {
    pub resolved: Resolved,
    pub env: Envelope,
}

/// A scriptable, recording `Mesh` fake.
///
/// * `replies`: peer-id -> reply body returned on success.
/// * `fail`: peer-id -> error string returned instead (takes precedence).
/// * `calls`: every delivery in the order it was attempted.
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

    /// Script a successful reply body for `peer`.
    pub fn with_reply(mut self, peer: &str, body: &str) -> Self {
        self.replies.insert(peer.to_string(), body.to_string());
        self
    }

    /// Script a delivery failure (error string) for `peer`.
    pub fn with_failure(mut self, peer: &str, detail: &str) -> Self {
        self.fail.insert(peer.to_string(), detail.to_string());
        self
    }

    /// The peer id addressed by a recorded envelope (single `to`).
    pub fn to_of(call: &DeliverCall) -> &AgentId {
        assert_eq!(
            call.env.to.len(),
            1,
            "fan_out must address exactly one peer per envelope"
        );
        &call.env.to[0]
    }
}

impl Default for FakeMesh {
    fn default() -> Self {
        Self::new()
    }
}

impl wasl_daemon::Mesh for FakeMesh {
    fn deliver(&mut self, resolved: &Resolved, env: &Envelope) -> Result<String, String> {
        self.calls.push(DeliverCall {
            resolved: resolved.clone(),
            env: env.clone(),
        });
        // The single addressed peer of this fanned-out envelope.
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

/// A scriptable, recording `Runner` fake. Fails any job whose argv references a
/// destination host in `fail_dst` (matched against the `dst:path` argv tokens).
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

    /// Script a failure for the given destination host id.
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
        // The last argv token is `<dst_host>:<path>`; match its host prefix.
        let dst_token = argv.last().cloned().unwrap_or_default();
        let dst_host = dst_token.split(':').next().unwrap_or("").to_string();
        if self.fail_dst.iter().any(|d| d == &dst_host) {
            return Err(format!("boom:{dst_host}"));
        }
        Ok(())
    }
}
