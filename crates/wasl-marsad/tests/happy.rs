//! Black-box happy-path tests for `wasl_marsad` per docs/specs/marsad-connector.md
//! §Test plan (C1-C6).

use wasl_gate::GateOutcome;
use wasl_marsad::{decision_to_outcome, ConnError, Connector, MarsadLink, ObsKind, Observation};
use wasl_proto::{AgentId, GateDecision, GateKind, GateRequest};

// ---------------------------------------------------------------------
// Fake MarsadLink — scriptable decision returned from `request_decision`,
// records every emitted observation in order, and (unused-here) fail flags
// for both methods so the error suite can drive failures with the same shape.
// ---------------------------------------------------------------------

#[derive(Debug, Default)]
struct FakeLink {
    /// Decision returned by `request_decision` (defaults to Approve if unset).
    scripted: Option<GateDecision>,
    /// Every observation handed to `emit`, in call order.
    emitted: Vec<Observation>,
    /// When true, `request_decision` fails.
    fail_decision: bool,
    /// When true, `emit` fails.
    fail_emit: bool,
}

impl FakeLink {
    fn new() -> Self {
        Self::default()
    }

    fn with_decision(decision: GateDecision) -> Self {
        Self {
            scripted: Some(decision),
            ..Self::default()
        }
    }
}

impl MarsadLink for FakeLink {
    fn request_decision(&mut self, _req: &GateRequest) -> Result<GateDecision, ConnError> {
        if self.fail_decision {
            return Err(ConnError::Transport("fake decision failure".to_string()));
        }
        Ok(self.scripted.clone().unwrap_or(GateDecision::Approve))
    }

    fn emit(&mut self, obs: &Observation) -> Result<(), ConnError> {
        if self.fail_emit {
            return Err(ConnError::Transport("fake emit failure".to_string()));
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
        preview: "preview text".to_string(),
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
// C1 — gate: Approve -> GateOutcome::Approved, and feed() has one Gate obs
// carrying Some(Approve), from == req.from, summary == req.summary. Deny
// case: Denied{reason} + feed obs carries Some(Deny).
// ---------------------------------------------------------------------

#[test]
fn c1_gate_approve_returns_approved_and_caches_gate_obs() {
    let mut conn = Connector::new(FakeLink::with_decision(GateDecision::Approve));
    let req = gate_request("g1", GateKind::Consult, "alice", "consult 2 peer(s)");

    let outcome = conn.gate(&req, 1).expect("gate ok");
    assert_eq!(outcome, GateOutcome::Approved);

    assert_eq!(conn.len(), 1);
    let feed = conn.feed();
    assert_eq!(feed.len(), 1);

    let obs = &feed[0];
    assert_eq!(obs.seq, 1);
    assert_eq!(obs.kind, ObsKind::Gate);
    assert_eq!(obs.from, req.from);
    assert_eq!(obs.summary, req.summary);
    assert_eq!(obs.decision, Some(GateDecision::Approve));
}

#[test]
fn c1_gate_deny_returns_denied_and_caches_gate_obs_with_deny() {
    let deny = GateDecision::Deny {
        reason: "not permitted".to_string(),
    };
    let mut conn = Connector::new(FakeLink::with_decision(deny.clone()));
    let req = gate_request("g2", GateKind::Command, "bob", "command 1 peer(s)");

    let outcome = conn.gate(&req, 5).expect("gate ok");
    assert_eq!(
        outcome,
        GateOutcome::Denied {
            reason: "not permitted".to_string()
        }
    );

    assert_eq!(conn.len(), 1);
    let obs = &conn.feed()[0];
    assert_eq!(obs.seq, 5);
    assert_eq!(obs.kind, ObsKind::Gate);
    assert_eq!(obs.from, req.from);
    assert_eq!(obs.summary, req.summary);
    assert_eq!(obs.decision, Some(deny));
}

// ---------------------------------------------------------------------
// C2 — notify_auto_approved: emits ONE AutoApproved obs with Some(Approve),
// the given kind/from/to/summary; feed() reflects it in order after a prior
// observe.
// ---------------------------------------------------------------------

#[test]
fn c2_notify_auto_approved_emits_auto_approved_obs_in_order() {
    let mut conn = Connector::new(FakeLink::new());

    // A prior plain observation so we can assert ordering afterwards.
    let prior = observation(1, ObsKind::Message, "alice", &["bob"], "just chatting");
    conn.observe(prior.clone()).expect("observe ok");

    conn.notify_auto_approved(
        2,
        ObsKind::Command,
        agent("alice"),
        targets(&["bob", "carol"]),
        "auto-approved command".to_string(),
    )
    .expect("notify ok");

    assert_eq!(conn.len(), 2);
    let feed = conn.feed();

    // Order preserved: prior observe first, notify second.
    assert_eq!(feed[0], prior);

    let auto = &feed[1];
    assert_eq!(auto.seq, 2);
    assert_eq!(auto.kind, ObsKind::AutoApproved);
    assert_eq!(auto.from, agent("alice"));
    assert_eq!(auto.to, targets(&["bob", "carol"]));
    assert_eq!(auto.summary, "auto-approved command");
    assert_eq!(auto.decision, Some(GateDecision::Approve));
}

// ---------------------------------------------------------------------
// C3 — observe: a plain obs (decision None) is emitted + cached; multiple
// observe/gate/notify calls preserve emit order; len/is_empty.
// ---------------------------------------------------------------------

#[test]
fn c3_observe_caches_plain_obs_and_len_is_empty() {
    let mut conn = Connector::new(FakeLink::new());

    assert!(conn.is_empty());
    assert_eq!(conn.len(), 0);
    assert_eq!(conn.feed(), &[]);

    let obs = observation(1, ObsKind::Transfer, "alice", &["bob"], "sending a file");
    assert_eq!(obs.decision, None);
    conn.observe(obs.clone()).expect("observe ok");

    assert!(!conn.is_empty());
    assert_eq!(conn.len(), 1);
    assert_eq!(conn.feed(), &[obs]);
}

#[test]
fn c3_emit_order_preserved_across_observe_gate_notify() {
    let mut conn = Connector::new(FakeLink::with_decision(GateDecision::Approve));

    // 1. observe (plain)
    let plain = observation(1, ObsKind::Consult, "alice", &["bob"], "a consult");
    conn.observe(plain.clone()).expect("observe ok");

    // 2. gate (Gate obs)
    let req = gate_request("g", GateKind::Command, "alice", "command 1 peer(s)");
    conn.gate(&req, 2).expect("gate ok");

    // 3. notify_auto_approved (AutoApproved obs)
    conn.notify_auto_approved(
        3,
        ObsKind::Message,
        agent("alice"),
        targets(&["carol"]),
        "auto message".to_string(),
    )
    .expect("notify ok");

    // 4. another observe
    let plain2 = observation(4, ObsKind::Transfer, "alice", &["dave"], "another transfer");
    conn.observe(plain2.clone()).expect("observe ok");

    assert_eq!(conn.len(), 4);
    let feed = conn.feed();

    // Emit order preserved by seq and kind.
    assert_eq!(feed[0], plain);

    assert_eq!(feed[1].seq, 2);
    assert_eq!(feed[1].kind, ObsKind::Gate);
    assert_eq!(feed[1].decision, Some(GateDecision::Approve));

    assert_eq!(feed[2].seq, 3);
    assert_eq!(feed[2].kind, ObsKind::AutoApproved);

    assert_eq!(feed[3], plain2);

    // Overall seq ordering is 1,2,3,4 as emitted.
    let seqs: Vec<u64> = feed.iter().map(|o| o.seq).collect();
    assert_eq!(seqs, vec![1, 2, 3, 4]);
}

// ---------------------------------------------------------------------
// C4 — by_kind (matching + non-matching present) and decisions (both an
// Approve-carrying and a Deny-carrying obs plus None-carrying present ->
// only the decision ones surface; assert Approve and Deny both surface).
// ---------------------------------------------------------------------

#[test]
fn c4_by_kind_filters_matching_only_in_order() {
    let mut conn = Connector::new(FakeLink::new());

    let m1 = observation(1, ObsKind::Message, "alice", &["bob"], "message 1");
    let c1 = observation(2, ObsKind::Consult, "alice", &["bob"], "consult 1");
    let m2 = observation(3, ObsKind::Message, "alice", &["carol"], "message 2");
    let t1 = observation(4, ObsKind::Transfer, "alice", &["dave"], "transfer 1");

    for o in [m1.clone(), c1.clone(), m2.clone(), t1.clone()] {
        conn.observe(o).expect("observe ok");
    }

    assert_eq!(conn.by_kind(ObsKind::Message), vec![&m1, &m2]);
    assert_eq!(conn.by_kind(ObsKind::Consult), vec![&c1]);
    assert_eq!(conn.by_kind(ObsKind::Transfer), vec![&t1]);
    // Non-matching kind that was never observed.
    assert!(conn.by_kind(ObsKind::Command).is_empty());
}

#[test]
fn c4_decisions_filters_only_some_decision_obs_approve_and_deny() {
    let mut conn = Connector::new(FakeLink::new());

    // A plain (None) observation.
    let plain = observation(1, ObsKind::Message, "alice", &["bob"], "no decision");
    conn.observe(plain).expect("observe ok");

    // An Approve-carrying obs via notify_auto_approved.
    conn.notify_auto_approved(
        2,
        ObsKind::Command,
        agent("alice"),
        targets(&["bob"]),
        "approved thing".to_string(),
    )
    .expect("notify ok");

    // Another plain (None) observation in between.
    let plain2 = observation(3, ObsKind::Consult, "alice", &["bob"], "another none");
    conn.observe(plain2).expect("observe ok");

    // A Deny-carrying obs via gate against a denying link.
    let deny = GateDecision::Deny {
        reason: "blocked".to_string(),
    };
    // Swap in a denying connector-driven decision by using a fresh gate call.
    // The current connector's link approves; drive a deny through a separate
    // observe of a decision-carrying observation to keep this connector's feed.
    let mut deny_obs = observation(4, ObsKind::Gate, "alice", &[], "denied gate");
    deny_obs.decision = Some(deny.clone());
    conn.observe(deny_obs.clone()).expect("observe ok");

    let decisions = conn.decisions();
    assert_eq!(decisions.len(), 2);
    // Only the decision-carrying ones, in order (Approve then Deny).
    assert!(decisions.iter().all(|o| o.decision.is_some()));
    assert!(decisions
        .iter()
        .any(|o| o.decision == Some(GateDecision::Approve)));
    assert!(decisions.iter().any(|o| o.decision == Some(deny.clone())));

    // First surfaced decision is the Approve (seq 2), second is the Deny (seq 4).
    assert_eq!(decisions[0].decision, Some(GateDecision::Approve));
    assert_eq!(decisions[0].seq, 2);
    assert_eq!(decisions[1].decision, Some(deny));
    assert_eq!(decisions[1].seq, 4);
}

// ---------------------------------------------------------------------
// C5 — decision_to_outcome: Approve -> Approved; Deny{reason} ->
// Denied{reason} (reason preserved).
// ---------------------------------------------------------------------

#[test]
fn c5_decision_to_outcome_approve_maps_to_approved() {
    let outcome = decision_to_outcome(&GateDecision::Approve);
    assert_eq!(outcome, GateOutcome::Approved);
}

#[test]
fn c5_decision_to_outcome_deny_maps_to_denied_preserving_reason() {
    let deny = GateDecision::Deny {
        reason: "insufficient trust".to_string(),
    };
    let outcome = decision_to_outcome(&deny);
    assert_eq!(
        outcome,
        GateOutcome::Denied {
            reason: "insufficient trust".to_string()
        }
    );
}

// ---------------------------------------------------------------------
// C6 — serde: Observation round-trips incl. decision None default (field
// absent) + Some(Approve)/Some(Deny); ObsKind snake_case round-trip for all
// 6 variants.
// ---------------------------------------------------------------------

#[test]
fn c6_obs_kind_snake_case_serde_round_trip_all_variants() {
    let cases = [
        (ObsKind::Consult, "\"consult\""),
        (ObsKind::Command, "\"command\""),
        (ObsKind::Message, "\"message\""),
        (ObsKind::Transfer, "\"transfer\""),
        (ObsKind::Gate, "\"gate\""),
        (ObsKind::AutoApproved, "\"auto_approved\""),
    ];
    for (kind, expected_json) in cases {
        let json = serde_json::to_string(&kind).expect("serialize ObsKind");
        assert_eq!(json, expected_json, "serialized form for {kind:?}");
        let back: ObsKind = serde_json::from_str(&json).expect("deserialize ObsKind");
        assert_eq!(back, kind, "round-trip for {kind:?}");
    }
}

#[test]
fn c6_observation_serde_round_trip_decision_none() {
    let obs = observation(7, ObsKind::Message, "alice", &["bob", "carol"], "hi there");
    assert_eq!(obs.decision, None);

    let json = serde_json::to_string(&obs).expect("serialize Observation");
    let back: Observation = serde_json::from_str(&json).expect("deserialize Observation");
    assert_eq!(back, obs);
    assert_eq!(back.decision, None);
}

#[test]
fn c6_observation_decision_defaults_to_none_when_field_absent() {
    // `decision` is `#[serde(default)]`; a payload omitting it must still
    // deserialize with `decision: None`.
    let json = serde_json::json!({
        "seq": 9,
        "kind": "consult",
        "from": "alice",
        "to": ["bob"],
        "summary": "no decision field present"
    })
    .to_string();

    let back: Observation = serde_json::from_str(&json).expect("deserialize without decision");
    assert_eq!(back.seq, 9);
    assert_eq!(back.kind, ObsKind::Consult);
    assert_eq!(back.from, agent("alice"));
    assert_eq!(back.to, targets(&["bob"]));
    assert_eq!(back.summary, "no decision field present");
    assert_eq!(back.decision, None);
}

#[test]
fn c6_observation_serde_round_trip_decision_some_approve() {
    let mut obs = observation(10, ObsKind::Gate, "alice", &[], "approved gate");
    obs.decision = Some(GateDecision::Approve);

    let json = serde_json::to_string(&obs).expect("serialize Observation");
    let back: Observation = serde_json::from_str(&json).expect("deserialize Observation");
    assert_eq!(back, obs);
    assert_eq!(back.decision, Some(GateDecision::Approve));
}

#[test]
fn c6_observation_serde_round_trip_decision_some_deny() {
    let mut obs = observation(11, ObsKind::Gate, "alice", &[], "denied gate");
    obs.decision = Some(GateDecision::Deny {
        reason: "policy violation".to_string(),
    });

    let json = serde_json::to_string(&obs).expect("serialize Observation");
    let back: Observation = serde_json::from_str(&json).expect("deserialize Observation");
    assert_eq!(back, obs);
    assert_eq!(
        back.decision,
        Some(GateDecision::Deny {
            reason: "policy violation".to_string()
        })
    );
}
