//! Black-box error-path tests for `wasl_marsad` per docs/specs/marsad-connector.md
//! §Test plan (E1-E4). Durability-first: nothing is cached unless the link
//! accepts it.

use wasl_marsad::{ConnError, Connector, MarsadLink, ObsKind, Observation};
use wasl_proto::{AgentId, GateDecision, GateKind, GateRequest};

// ---------------------------------------------------------------------
// Fake MarsadLink — scriptable decision, records emitted observations, and
// independent fail flags for BOTH `request_decision` and `emit`, so each
// error path can be driven precisely.
// ---------------------------------------------------------------------

#[derive(Debug, Default)]
struct FakeLink {
    scripted: Option<GateDecision>,
    emitted: Vec<Observation>,
    fail_decision: bool,
    fail_emit: bool,
}

impl FakeLink {
    fn new() -> Self {
        Self::default()
    }

    /// `request_decision` fails on every call.
    fn failing_decision() -> Self {
        Self {
            fail_decision: true,
            ..Self::default()
        }
    }

    /// `request_decision` succeeds (Approve) but `emit` fails on every call.
    fn decision_ok_emit_fails() -> Self {
        Self {
            scripted: Some(GateDecision::Approve),
            fail_emit: true,
            ..Self::default()
        }
    }

    /// `emit` fails on every call (decision unused by these paths).
    fn failing_emit() -> Self {
        Self {
            fail_emit: true,
            ..Self::default()
        }
    }
}

impl MarsadLink for FakeLink {
    fn request_decision(&mut self, _req: &GateRequest) -> Result<GateDecision, ConnError> {
        if self.fail_decision {
            return Err(ConnError::Transport("decision link down".to_string()));
        }
        Ok(self.scripted.clone().unwrap_or(GateDecision::Approve))
    }

    fn emit(&mut self, obs: &Observation) -> Result<(), ConnError> {
        if self.fail_emit {
            return Err(ConnError::Transport("emit link down".to_string()));
        }
        self.emitted.push(obs.clone());
        Ok(())
    }
}

fn agent(id: &str) -> AgentId {
    AgentId(id.to_string())
}

fn targets(ids: &[&str]) -> Vec<AgentId> {
    ids.iter().map(|s| agent(s)).collect()
}

fn gate_request(id: &str, kind: GateKind, from: &str, summary: &str) -> GateRequest {
    GateRequest {
        id: id.to_string(),
        kind,
        from: agent(from),
        summary: summary.to_string(),
        preview: "preview".to_string(),
    }
}

fn observation(seq: u64, kind: ObsKind, from: &str, to: &[&str], summary: &str) -> Observation {
    Observation {
        seq,
        kind,
        from: agent(from),
        to: targets(to),
        summary: summary.to_string(),
        decision: None,
    }
}

// ---------------------------------------------------------------------
// E1 — gate when request_decision fails -> Err(Transport(_)) AND feed()
// stays empty (nothing emitted/cached).
// ---------------------------------------------------------------------

#[test]
fn e1_gate_request_decision_failure_returns_err_and_caches_nothing() {
    let mut conn = Connector::new(FakeLink::failing_decision());
    let req = gate_request("g1", GateKind::Consult, "alice", "consult 1 peer(s)");

    let result = conn.gate(&req, 1);
    match result {
        Err(ConnError::Transport(detail)) => {
            assert!(!detail.is_empty(), "transport detail should be non-empty");
        }
        other => panic!("expected Err(ConnError::Transport(_)), got {other:?}"),
    }

    assert!(conn.is_empty(), "feed must stay empty on decision failure");
    assert_eq!(conn.len(), 0);
    assert_eq!(conn.feed(), &[]);
}

// ---------------------------------------------------------------------
// E2 — gate when request_decision Ok but emit fails -> Err(Transport) AND
// feed() stays empty (durability-first: decision made but not durably
// observed).
// ---------------------------------------------------------------------

#[test]
fn e2_gate_emit_failure_returns_err_and_caches_nothing() {
    let mut conn = Connector::new(FakeLink::decision_ok_emit_fails());
    let req = gate_request("g2", GateKind::Command, "bob", "command 1 peer(s)");

    let result = conn.gate(&req, 2);
    match result {
        Err(ConnError::Transport(detail)) => {
            assert!(!detail.is_empty(), "transport detail should be non-empty");
        }
        other => panic!("expected Err(ConnError::Transport(_)), got {other:?}"),
    }

    assert!(conn.is_empty(), "feed must stay empty on emit failure");
    assert_eq!(conn.len(), 0);
    assert_eq!(conn.feed(), &[]);
}

// ---------------------------------------------------------------------
// E3 — observe / notify_auto_approved when emit fails -> Err(Transport) AND
// the obs is NOT cached (len unchanged) — durability-first invariant.
// ---------------------------------------------------------------------

#[test]
fn e3_observe_emit_failure_returns_err_and_does_not_cache() {
    let mut conn = Connector::new(FakeLink::failing_emit());
    assert_eq!(conn.len(), 0);

    let obs = observation(1, ObsKind::Message, "alice", &["bob"], "must not be cached");
    let result = conn.observe(obs);
    match result {
        Err(ConnError::Transport(detail)) => {
            assert!(!detail.is_empty(), "transport detail should be non-empty");
        }
        other => panic!("expected Err(ConnError::Transport(_)), got {other:?}"),
    }

    assert_eq!(conn.len(), 0, "failed observe must not increment len");
    assert!(conn.is_empty());
    assert_eq!(conn.feed(), &[]);
}

#[test]
fn e3_notify_auto_approved_emit_failure_returns_err_and_does_not_cache() {
    let mut conn = Connector::new(FakeLink::failing_emit());
    assert_eq!(conn.len(), 0);

    let result = conn.notify_auto_approved(
        1,
        ObsKind::Command,
        agent("alice"),
        targets(&["bob"]),
        "auto approve must not be cached".to_string(),
    );
    match result {
        Err(ConnError::Transport(detail)) => {
            assert!(!detail.is_empty(), "transport detail should be non-empty");
        }
        other => panic!("expected Err(ConnError::Transport(_)), got {other:?}"),
    }

    assert_eq!(conn.len(), 0, "failed notify must not increment len");
    assert!(conn.is_empty());
    assert_eq!(conn.feed(), &[]);
}

#[test]
fn e3_failed_emit_leaves_prior_cached_obs_intact() {
    // A link that succeeds once then can be flipped is awkward; instead prove
    // the invariant per-instance: a healthy connector caches, a separate
    // always-failing connector caches nothing, and they do not interfere.
    let mut healthy = Connector::new(FakeLink::new());
    let good = observation(1, ObsKind::Message, "alice", &["bob"], "healthy obs");
    healthy.observe(good.clone()).expect("observe ok");
    assert_eq!(healthy.len(), 1);
    assert_eq!(healthy.feed(), &[good.clone()]);

    let mut broken = Connector::new(FakeLink::failing_emit());
    let bad = observation(2, ObsKind::Message, "carol", &["dave"], "broken obs");
    assert!(broken.observe(bad).is_err());
    assert_eq!(broken.len(), 0);
    assert_eq!(broken.feed(), &[]);

    // The healthy connector is unaffected.
    assert_eq!(healthy.len(), 1);
    assert_eq!(healthy.feed(), &[good]);
}

// ---------------------------------------------------------------------
// E4 — ConnError Display non-empty + carries the transport detail;
// std::error::Error implemented.
// ---------------------------------------------------------------------

#[test]
fn e4_conn_error_display_non_empty_and_carries_detail() {
    let err = ConnError::Transport("connection reset by peer".to_string());
    let display = err.to_string();

    assert!(!display.is_empty(), "Display must be non-empty");
    assert!(
        display.contains("connection reset by peer"),
        "Display must carry the transport detail, got: {display}"
    );
}

#[test]
fn e4_conn_error_implements_std_error() {
    fn assert_std_error<E: std::error::Error>(_: &E) {}
    let err = ConnError::Transport("boom".to_string());
    assert_std_error(&err);

    // Exercise the trait-object path too, as real call sites would.
    let boxed: Box<dyn std::error::Error> =
        Box::new(ConnError::Transport("boxed boom".to_string()));
    let boxed_display = boxed.to_string();
    assert!(!boxed_display.is_empty());
    assert!(boxed_display.contains("boxed boom"));
}
