#![allow(dead_code)]
//! Shared fakes for the `wasl-e2e` cross-crate scenario suite.
//!
//! This module is compiled independently into each test binary, hence the
//! crate-wide `allow(dead_code)` (a given test binary uses only a subset).
//!
//! The fakes wire the real trait seams of the Wasl stack:
//! * [`ScriptSurfacer`] — a scriptable [`wasl_server::Surfacer`] whose outcome is
//!   fixed per construction (Tier-1 Elicitation / Tier-3 Marsad path).
//! * [`MarsadSurfacer`] — a *second* `Surfacer` impl that delegates to a real
//!   [`wasl_marsad::Connector`] over [`FakeLink`]; a shared handle exposes the
//!   connector's `feed()` for assertions after the gatekeeper consumed it.
//! * [`FakeSink`] — a recording [`wasl_audit::Sink`], scriptable to fail append.
//! * [`FakeMesh`] — a recording, per-peer scriptable [`wasl_daemon::Mesh`]; its
//!   `calls` prove whether the daemon was ever invoked.
//! * [`FakeRunner`] — a per-dst scriptable [`wasl_transfer::Runner`].
//! * [`FakeLink`] — a scriptable + recording [`wasl_marsad::MarsadLink`].

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use wasl_audit::{AuditEntry, AuditError, Sink};
use wasl_gate::{GateMode, GateOutcome};
use wasl_marsad::{ConnError, Connector, MarsadLink, Observation};
use wasl_proto::{AgentCard, AgentId, Envelope, GateDecision, GateRequest, HostId};
use wasl_registry::Resolved;
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

/// An `AgentCard` for `id` living on `host_id`.
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
// ScriptSurfacer — a plain scriptable Surfacer (Tier-1 / synchronous)
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
// FakeLink — a scriptable + recording MarsadLink
// ---------------------------------------------------------------------

/// A `MarsadLink` scripted with a fixed decision and recording every observation
/// it is asked to emit.
pub struct FakeLink {
    decision: GateDecision,
    fail_emit: bool,
    pub emitted: Vec<Observation>,
}

impl FakeLink {
    pub fn deciding(decision: GateDecision) -> Self {
        Self {
            decision,
            fail_emit: false,
            emitted: Vec::new(),
        }
    }

    pub fn approving() -> Self {
        Self::deciding(GateDecision::Approve)
    }
}

impl MarsadLink for FakeLink {
    fn request_decision(&mut self, _req: &GateRequest) -> Result<GateDecision, ConnError> {
        Ok(self.decision.clone())
    }

    fn emit(&mut self, obs: &Observation) -> Result<(), ConnError> {
        if self.fail_emit {
            return Err(ConnError::Transport("emit refused".to_string()));
        }
        self.emitted.push(obs.clone());
        Ok(())
    }
}

// ---------------------------------------------------------------------
// MarsadSurfacer — Surfacer backed by a real wasl_marsad::Connector
// ---------------------------------------------------------------------

/// A shared handle to a `Connector<FakeLink>`. The gatekeeper takes ownership of
/// the [`MarsadSurfacer`]; keeping the connector behind an `Rc<RefCell<_>>` lets a
/// test read the connector's `feed()` after the surfacer has been consumed.
pub type ConnectorHandle = Rc<RefCell<Connector<FakeLink>>>;

/// A `Surfacer` (Tier-3 Marsad mode) that routes each gate through a
/// `wasl_marsad::Connector`: `surface` calls `connector.gate(req, seq)` and maps
/// the resulting `GateOutcome`. The `seq` fed to the connector is caller-supplied
/// so its `Gate` observation is deterministically ordered.
pub struct MarsadSurfacer {
    connector: ConnectorHandle,
    seq: u64,
}

impl MarsadSurfacer {
    /// Build the surfacer over a fresh connector wrapping `link`, returning both
    /// the surfacer (to hand to the gatekeeper) and a shared handle to the
    /// connector (to assert its feed afterwards). `seq` is the sequence the
    /// connector will stamp on its `Gate` observation.
    pub fn new(link: FakeLink, seq: u64) -> (Self, ConnectorHandle) {
        let handle: ConnectorHandle = Rc::new(RefCell::new(Connector::new(link)));
        let surfacer = Self {
            connector: Rc::clone(&handle),
            seq,
        };
        (surfacer, handle)
    }
}

impl Surfacer for MarsadSurfacer {
    fn surface(&mut self, _mode: GateMode, req: &GateRequest) -> GateOutcome {
        // Delegate the human decision to Marsad via the connector; the connector
        // records the `Gate` observation (durability-first) as a side effect.
        self.connector
            .borrow_mut()
            .gate(req, self.seq)
            .expect("fake marsad link never fails the decision/emit in these scenarios")
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
// FakeMesh — recording + per-peer scriptable Mesh
// ---------------------------------------------------------------------

/// One recorded delivery: the resolved route + the exact envelope handed over.
#[derive(Debug, Clone)]
pub struct DeliverCall {
    pub resolved: Resolved,
    pub env: Envelope,
}

/// A recording `Mesh` fake, scriptable per peer:
/// * `replies`: peer-id -> reply body on success.
/// * `fail`: peer-id -> error string (takes precedence).
/// * `calls`: every delivery attempt, in order — proves whether the daemon ran.
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
// FakeRunner — per-dst scriptable transfer Runner
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
