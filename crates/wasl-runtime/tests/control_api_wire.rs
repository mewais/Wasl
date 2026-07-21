//! Phase-1 control-plane WIRE-SHAPE tests for the R1 public types that
//! `runtime-control-api.md` (R4) serializes verbatim onto the `/v1` wire.
//!
//! The control API states plainly that the wire payloads ARE the serialized R1
//! types — "The payload is exactly a serialized R1 `TurnEvent`" (line 140),
//! "the R1 `TurnReceipt`" (line 192), `capabilities` is "the exact R1
//! `RuntimeCapabilities`" (line 84), and the submit body's `source`/`causation`
//! are the R1 `TurnSource`/`Causation` (lines 186-190). Those types live in R1
//! (Phase-1's `wasl-runtime`), so their serde form is a Phase-1 obligation.
//!
//! The GOLDEN FIXTURE FILES themselves (control-api.md §Golden fixtures, the
//! append-only `crates/<serving-crate>/fixtures/runtime-control-v1/` directory)
//! belong to the R4 SERVING crate, which is NOT one of Phase-1's three crates —
//! so this file does NOT create fixture files. Instead it pins the SAME JSON
//! bodies the spec shows inline, exercised against the R1 wire types. When the
//! R4 serving crate is built, its fixtures must equal exactly these shapes; this
//! is the R1-side half of that contract.
//!
//! `ProtocolEvent` deliberately never appears here (the boundary invariant,
//! control-api.md lines 13-16 / §Golden fixtures line 296).
//!
//! TEST-FIRST / EXPECTED RED: will not compile until the coder creates
//! `wasl-runtime` with these types.

use serde_json::json;

use wasl_proto::{AgentId, DeliveryCertainty, RequestId};
use wasl_runtime::{
    BusyPolicy, Causation, EventSeq, RuntimeCapabilities, TargetStatus, TurnEvent, TurnId,
    TurnReceipt, TurnSource,
};

// ---------------------------------------------------------------------
// capabilities block inside GET /v1/targets (control-api.md lines 70-74).
// ---------------------------------------------------------------------

#[test]
fn control_api_capabilities_block_matches_targets_snapshot_shape() {
    // The exact 8-bool object shown in the targets snapshot fixture.
    let caps = RuntimeCapabilities {
        persistent_process: true,
        persistent_conversation: true,
        structured_turns: true,
        streaming_output: false,
        attachable: true,
        interruptible: false,
        structured_provenance: true,
        resumable: true,
    };
    assert_eq!(
        serde_json::to_value(&caps).unwrap(),
        json!({
            "persistent_process": true, "persistent_conversation": true,
            "structured_turns": true, "streaming_output": false,
            "attachable": true, "interruptible": false,
            "structured_provenance": true, "resumable": true
        })
    );
}

// ---------------------------------------------------------------------
// status field inside a target object: {"type":"idle","since":…}
// (control-api.md line 75).
// ---------------------------------------------------------------------

#[test]
fn control_api_status_idle_matches_target_object_shape() {
    let status = TargetStatus::Idle {
        since: "2026-07-02T12:00:00.000Z".to_string(),
    };
    assert_eq!(
        serde_json::to_value(&status).unwrap(),
        json!({ "type": "idle", "since": "2026-07-02T12:00:00.000Z" })
    );
}

// ---------------------------------------------------------------------
// turn_event frame payloads (control-api.md §turn_event, lines 144-149).
// The `data:` payload IS a serialized TurnEvent.
// ---------------------------------------------------------------------

#[test]
fn control_api_turn_event_assistant_delta_fixture_shape() {
    // turn_event_assistant_delta.json (line 144).
    let ev = TurnEvent::AssistantDelta {
        turn_id: TurnId("turn-7".to_string()),
        text: "PON".to_string(),
    };
    assert_eq!(
        serde_json::to_value(&ev).unwrap(),
        json!({ "type": "assistant_delta", "turn_id": "turn-7", "text": "PON" })
    );
}

#[test]
fn control_api_turn_event_turn_completed_fixture_shape() {
    // turn_event_turn_completed.json (line 148) — carries certainty.
    let ev = TurnEvent::TurnCompleted {
        turn_id: TurnId("turn-7".to_string()),
        reply: "PONG".to_string(),
        certainty: DeliveryCertainty::Completed,
    };
    assert_eq!(
        serde_json::to_value(&ev).unwrap(),
        json!({
            "type": "turn_completed", "turn_id": "turn-7",
            "reply": "PONG", "certainty": "completed"
        })
    );
}

// ---------------------------------------------------------------------
// TurnReceipt response bodies (control-api.md POST …/turns, lines 195-199 +
// fixture table receipt_*.json).
// ---------------------------------------------------------------------

#[test]
fn control_api_receipt_started_fixture_shape() {
    // receipt_started.json (line 195).
    let receipt = TurnReceipt::Started {
        turn_id: TurnId("turn-7".to_string()),
        first_seq: EventSeq(13),
    };
    assert_eq!(
        serde_json::to_value(&receipt).unwrap(),
        json!({ "type": "started", "turn_id": "turn-7", "first_seq": 13 })
    );
}

#[test]
fn control_api_receipt_queued_fixture_shape() {
    let receipt = TurnReceipt::Queued {
        turn_id: TurnId("turn-7".to_string()),
        position: 0,
    };
    assert_eq!(
        serde_json::to_value(&receipt).unwrap(),
        json!({ "type": "queued", "turn_id": "turn-7", "position": 0 })
    );
}

#[test]
fn control_api_receipt_rejected_busy_fixture_shape() {
    let receipt = TurnReceipt::RejectedBusy {
        active_turn: TurnId("turn-6".to_string()),
    };
    assert_eq!(
        serde_json::to_value(&receipt).unwrap(),
        json!({ "type": "rejected_busy", "active_turn": "turn-6" })
    );
}

#[test]
fn control_api_receipt_duplicate_fixture_shape() {
    // receipt_duplicate.json wraps the original verbatim (§3 line 240).
    let receipt = TurnReceipt::Duplicate {
        original: Box::new(TurnReceipt::Started {
            turn_id: TurnId("turn-7".to_string()),
            first_seq: EventSeq(13),
        }),
    };
    assert_eq!(
        serde_json::to_value(&receipt).unwrap(),
        json!({
            "type": "duplicate",
            "original": { "type": "started", "turn_id": "turn-7", "first_seq": 13 }
        })
    );
}

// ---------------------------------------------------------------------
// submit_request.json body pieces (control-api.md POST …/turns, lines 176-183):
// source local_human, busy_policy queue{max_wait_ms}, causation w/ transparent
// RequestId strings.
// ---------------------------------------------------------------------

#[test]
fn control_api_submit_source_local_human_fixture_shape() {
    // source: { "type": "local_human" } (line 179; R3 send always local_human).
    let source = TurnSource::LocalHuman;
    assert_eq!(
        serde_json::to_value(&source).unwrap(),
        json!({ "type": "local_human" })
    );
}

#[test]
fn control_api_submit_source_agent_fixture_shape() {
    let source = TurnSource::Agent {
        from: AgentId("wasl-b".to_string()),
    };
    assert_eq!(
        serde_json::to_value(&source).unwrap(),
        json!({ "type": "agent", "from": "wasl-b" })
    );
}

#[test]
fn control_api_submit_busy_policy_queue_fixture_shape() {
    // busy_policy: { "type": "queue", "max_wait_ms": 30000 } (line 181).
    let policy = BusyPolicy::Queue {
        max_wait: std::time::Duration::from_millis(30_000),
    };
    assert_eq!(
        serde_json::to_value(&policy).unwrap(),
        json!({ "type": "queue", "max_wait_ms": 30000 })
    );
}

#[test]
fn control_api_submit_causation_root_turn_fixture_shape() {
    // causation: { "root_request":"req-1", "parent_request":null, "path":[], "hop":0 }
    // (line 182). RequestId is transparent → bare strings.
    let causation = Causation {
        root_request: RequestId("req-1".to_string()),
        parent_request: None,
        path: vec![],
        hop: 0,
    };
    assert_eq!(
        serde_json::to_value(&causation).unwrap(),
        json!({
            "root_request": "req-1", "parent_request": null, "path": [], "hop": 0
        })
    );
}

#[test]
fn control_api_submit_causation_with_parent_and_path_fixture_shape() {
    let causation = Causation {
        root_request: RequestId("req-1".to_string()),
        parent_request: Some(RequestId("req-0".to_string())),
        path: vec![AgentId("wasl-a".to_string())],
        hop: 1,
    };
    assert_eq!(
        serde_json::to_value(&causation).unwrap(),
        json!({
            "root_request": "req-1", "parent_request": "req-0",
            "path": ["wasl-a"], "hop": 1
        })
    );
}
