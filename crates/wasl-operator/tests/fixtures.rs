//! `tests/fixtures.rs` — CONFORMANCE tests: the producer half of the golden
//! fixture policy in `docs/specs/operator-api.md` §Fixtures. For every file
//! in `fixtures/operator-v1/`, build the corresponding live value (frames via
//! a fake-clock/counter-id hub, bodies via the real HTTP handlers where
//! reachable, else the serde types directly) and assert `serde_json::Value`
//! equality with the checked-in file. `decision_request_*.json` additionally
//! must deserialize into the expected `DecisionBody`.
//!
//! Test-first: this MUST fail to compile until `src/` defines the public API.

mod common;

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use common::{
    at, client, fresh_hub_with_clock, load_fixture, publish_n_filler, req_call10, req_call9,
    spawn_router, FakeClock, SseReader, T0_MS, T_DEADLINE_MS, T_DECIDE_MS, T_ELICIT_DECIDE_MS,
    T_ELICIT_REQ_MS,
};

use wasl_audit::{AuditEntry, EntryKind};
use wasl_gate::{GateMode, GateOutcome};
use wasl_proto::{AgentId, GateDecision};

use wasl_operator::{DecidedBy, DecisionBody, DecisionOutcome};

const SSE_TIMEOUT: Duration = Duration::from_secs(5);

/// The complete fixture table from operator-api.md §Fixtures. The directory
/// listing test below fails if this list and the on-disk directory ever
/// diverge in either direction.
const EXPECTED_FIXTURES: &[&str] = &[
    "info.json",
    "hello.json",
    "gate_requested_external.json",
    "gate_requested_elicitation.json",
    "gate_decided_approved_operator.json",
    "gate_decided_denied_operator.json",
    "gate_decided_dismissed_timeout.json",
    "gate_decided_approved_elicitation.json",
    "audit_decision.json",
    "audit_message.json",
    "gates_snapshot.json",
    "decision_request_approved.json",
    "decision_request_denied.json",
    "error_unknown_gate.json",
    "error_already_decided.json",
    "error_malformed.json",
];

#[test]
fn fixture_directory_matches_expected_table_exactly() {
    let dir = common::fixtures_dir();
    let mut on_disk: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read fixtures dir {dir:?}: {e}"))
        .map(|e| {
            e.expect("dir entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|n| n.ends_with(".json"))
        .collect();
    on_disk.sort();

    let mut expected: Vec<String> = EXPECTED_FIXTURES.iter().map(|s| s.to_string()).collect();
    expected.sort();

    assert_eq!(
        on_disk, expected,
        "fixtures/operator-v1 must contain exactly the files in operator-api.md's fixture table"
    );
}

// ── info.json ────────────────────────────────────────────────────────────

#[test]
fn fixture_info_matches_hub_info() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    assert_eq!(
        serde_json::to_value(hub.info()).unwrap(),
        load_fixture("info.json")
    );
}

// ── hello.json (via the real HTTP handler) ─────────────────────────────────

#[tokio::test]
async fn fixture_hello_matches_first_sse_frame_at_last_seq_42() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    publish_n_filler(&hub, 42); // drives last_seq to exactly 42, matching the fixture
    let addr = spawn_router(hub).await;

    let resp = client()
        .get(format!("http://{addr}/v1/events"))
        .send()
        .await
        .expect("GET /v1/events");
    let mut sse = SseReader::new(resp);
    let hello = sse.next_event(SSE_TIMEOUT).await.expect("hello frame");
    assert_eq!(hello.event, "hello");
    assert!(hello.id.is_none());
    assert_eq!(hello.data, load_fixture("hello.json"));
}

// ── gate_requested_* / gate_decided_* frames (via a fake-clock/counter hub) ─

#[test]
fn fixture_gate_requested_and_decided_operator_frames_match_hub_output() {
    // gate_requested_external.json + gate_decided_approved_operator.json (gate-1)
    {
        let clock = Arc::new(FakeClock::new(at(T0_MS)));
        let hub = fresh_hub_with_clock(clock.clone());
        let (_last, mut rx) = hub.subscribe();

        let _w = hub.open_gate(GateMode::External, &req_call9());
        let requested = rx.try_recv().expect("gate_requested").frame;
        assert_eq!(
            serde_json::to_value(&requested).unwrap(),
            load_fixture("gate_requested_external.json")
        );

        clock.set(at(T_DECIDE_MS));
        hub.decide(
            "gate-1",
            DecisionBody {
                outcome: DecisionOutcome::Approved,
                reason: None,
            },
        )
        .expect("decide ok");
        let decided = rx.try_recv().expect("gate_decided").frame;
        assert_eq!(
            serde_json::to_value(&decided).unwrap(),
            load_fixture("gate_decided_approved_operator.json")
        );
    }

    // gate_decided_denied_operator.json (fresh gate-1, denied with a reason)
    {
        let clock = Arc::new(FakeClock::new(at(T0_MS)));
        let hub = fresh_hub_with_clock(clock.clone());
        let (_last, mut rx) = hub.subscribe();
        let _w = hub.open_gate(GateMode::External, &req_call9());
        rx.try_recv().expect("gate_requested");

        clock.set(at(T_DECIDE_MS));
        hub.decide(
            "gate-1",
            DecisionBody {
                outcome: DecisionOutcome::Denied,
                reason: Some("not during business hours".to_string()),
            },
        )
        .expect("decide ok");
        let decided = rx.try_recv().expect("gate_decided").frame;
        assert_eq!(
            serde_json::to_value(&decided).unwrap(),
            load_fixture("gate_decided_denied_operator.json")
        );
    }

    // gate_decided_dismissed_timeout.json (fresh gate-1, timeout)
    {
        let clock = Arc::new(FakeClock::new(at(T0_MS)));
        let hub = fresh_hub_with_clock(clock.clone());
        let (_last, mut rx) = hub.subscribe();
        let _w = hub.open_gate(GateMode::External, &req_call9());
        rx.try_recv().expect("gate_requested");

        clock.set(at(T_DEADLINE_MS));
        hub.timeout_gate("gate-1").expect("timeout ok");
        let decided = rx.try_recv().expect("gate_decided").frame;
        assert_eq!(
            serde_json::to_value(&decided).unwrap(),
            load_fixture("gate_decided_dismissed_timeout.json")
        );
    }
}

#[test]
fn fixture_gate_requested_and_decided_elicitation_frames_match_hub_output() {
    let clock = Arc::new(FakeClock::new(at(T0_MS)));
    let hub = fresh_hub_with_clock(clock.clone());
    let (_last, mut rx) = hub.subscribe();

    // Burn gate-1 so the announced (observe-only) gate becomes gate-2, as in
    // the fixture files.
    let _burn = hub.open_gate(GateMode::External, &req_call9());
    rx.try_recv().expect("burn gate_requested");

    clock.set(at(T_ELICIT_REQ_MS));
    let gid = hub.announce(GateMode::Elicitation, &req_call10());
    assert_eq!(gid, "gate-2");
    let requested = rx.try_recv().expect("gate_requested (announced)").frame;
    assert_eq!(
        serde_json::to_value(&requested).unwrap(),
        load_fixture("gate_requested_elicitation.json")
    );

    clock.set(at(T_ELICIT_DECIDE_MS));
    hub.resolve_announced(&gid, &GateOutcome::Approved, DecidedBy::Elicitation);
    let decided = rx.try_recv().expect("gate_decided (announced)").frame;
    assert_eq!(
        serde_json::to_value(&decided).unwrap(),
        load_fixture("gate_decided_approved_elicitation.json")
    );
}

// ── audit_*.json ─────────────────────────────────────────────────────────

#[test]
fn fixture_audit_decision_matches_hub_broadcast() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let (_last, mut rx) = hub.subscribe();
    let entry = AuditEntry {
        seq: 3,
        kind: EntryKind::Decision,
        actor: AgentId("wasl-a".to_string()),
        targets: vec![AgentId("wasl-b".to_string())],
        summary: "gate Message".to_string(),
        decision: Some(GateDecision::Approve),
        ..Default::default()
    };
    hub.publish_audit(&entry);
    let f = rx.try_recv().expect("audit frame").frame;
    assert_eq!(
        serde_json::to_value(&f).unwrap(),
        load_fixture("audit_decision.json")
    );
}

#[test]
fn fixture_audit_message_matches_hub_broadcast() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let (_last, mut rx) = hub.subscribe();
    let entry = AuditEntry {
        seq: 1,
        kind: EntryKind::Message,
        actor: AgentId("wasl-a".to_string()),
        targets: vec![AgentId("wasl-b".to_string())],
        summary: "message to 1 peer(s)".to_string(),
        decision: None,
        ..Default::default()
    };
    hub.publish_audit(&entry);
    let f = rx.try_recv().expect("audit frame").frame;
    assert_eq!(
        serde_json::to_value(&f).unwrap(),
        load_fixture("audit_message.json")
    );
}

// ── gates_snapshot.json ──────────────────────────────────────────────────

#[test]
fn fixture_gates_snapshot_matches_pending_gate_at_last_seq_42() {
    let clock = Arc::new(FakeClock::new(at(T0_MS)));
    let hub = fresh_hub_with_clock(clock.clone());
    publish_n_filler(&hub, 41); // seq 1..=41
    let _w = hub.open_gate(GateMode::External, &req_call9()); // gate-1, seq 42

    clock.set(at(T0_MS + 1_500)); // age_ms = 1500, matching the fixture
    let snap = hub.snapshot();
    assert_eq!(
        serde_json::to_value(snap).unwrap(),
        load_fixture("gates_snapshot.json")
    );
}

// ── decision_request_*.json (request-shaped: value + Deserialize) ─────────

#[test]
fn fixture_decision_request_approved_matches_and_deserializes() {
    let v = load_fixture("decision_request_approved.json");
    assert_eq!(v, json!({ "outcome": "approved" }));

    let body: DecisionBody = serde_json::from_value(v).expect("deserialize DecisionBody");
    assert_eq!(body.outcome, DecisionOutcome::Approved);
    assert_eq!(body.reason, None);
}

#[test]
fn fixture_decision_request_denied_matches_and_deserializes() {
    let v = load_fixture("decision_request_denied.json");
    assert_eq!(
        v,
        json!({ "outcome": "denied", "reason": "not during business hours" })
    );

    let body: DecisionBody = serde_json::from_value(v).expect("deserialize DecisionBody");
    assert_eq!(body.outcome, DecisionOutcome::Denied);
    assert_eq!(body.reason, Some("not during business hours".to_string()));
}

// ── error_*.json (via the real HTTP handlers) ──────────────────────────────

#[tokio::test]
async fn fixture_error_unknown_gate_matches_404_body() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let addr = spawn_router(hub).await;

    let resp = client()
        .post(format!("http://{addr}/v1/gates/gate-9/decision"))
        .json(&json!({ "outcome": "approved" }))
        .send()
        .await
        .expect("POST unknown gate");
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    let body: serde_json::Value = resp.json().await.expect("404 body");
    assert_eq!(body, load_fixture("error_unknown_gate.json"));
}

#[tokio::test]
async fn fixture_error_already_decided_matches_409_body() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let addr = spawn_router(hub.clone()).await;

    let _w = hub.open_gate(GateMode::External, &req_call9()); // gate-1
    hub.decide(
        "gate-1",
        DecisionBody {
            outcome: DecisionOutcome::Approved,
            reason: None,
        },
    )
    .expect("decide ok");

    let resp = client()
        .post(format!("http://{addr}/v1/gates/gate-1/decision"))
        .json(&json!({ "outcome": "denied" }))
        .send()
        .await
        .expect("POST already-decided gate");
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    let body: serde_json::Value = resp.json().await.expect("409 body");
    assert_eq!(body, load_fixture("error_already_decided.json"));
}

#[tokio::test]
async fn fixture_error_malformed_matches_400_body() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let addr = spawn_router(hub.clone()).await;
    let _w = hub.open_gate(GateMode::External, &req_call9()); // gate-1, so this isn't also a 404

    // The representative malformed body per the fixture table: valid JSON,
    // missing the required `outcome` field.
    let resp = client()
        .post(format!("http://{addr}/v1/gates/gate-1/decision"))
        .json(&json!({}))
        .send()
        .await
        .expect("POST malformed body");
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.expect("400 body");
    assert_eq!(body, load_fixture("error_malformed.json"));
}
