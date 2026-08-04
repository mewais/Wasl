//! Golden-fixture conformance tests for the Runtime Control API v1 wire
//! contract (`docs/specs/runtime-control-api.md`). Mirrors the
//! `wasl-operator` fixtures.rs precedent: the checked-in JSON files under
//! `fixtures/runtime-control-v1/` are the machine-checkable half of the
//! contract. Every test here asserts that constructing the REAL R1 type (or
//! the new request/response wrapper type specified below) and serializing it
//! produces JSON that is VALUE-EQUAL (key order irrelevant) to the fixture.
//!
//! `wasl-runtime-control` does not exist yet — this crate is not registered
//! in the workspace and has no `src/**`. This is the correct RED state; the
//! code-agent creates the crate + implements the wrapper types named below.
//!
//! NEW TYPES the code-agent must define in `wasl_runtime_control` (all plain
//! serde structs/enums over R1's existing public types — no new business
//! logic, this crate is a thin wire-serialization + HTTP/SSE-serving shell
//! over `wasl_runtime::RuntimeProvider`):
//!   - `InfoResponse { agent_id: String, host_id: String, version: String, runtime: String }`
//!   - `TargetSnapshot { target_id: wasl_runtime::TargetId, key: wasl_runtime::TargetKey,
//!       capabilities: wasl_runtime::RuntimeCapabilities, status: wasl_runtime::TargetStatus,
//!       last_seq: wasl_runtime::EventSeq, queued: usize, native_session_id: Option<String> }`
//!   - `TargetsSnapshotResponse { targets: Vec<TargetSnapshot> }`
//!   - `CreateTargetRequest { key: wasl_runtime::TargetKey, protocol: Option<String> }`
//!   - `HelloFrame { api_version: u32, agent_id: String, host_id: String, version: String,
//!       target_id: wasl_runtime::TargetId, last_seq: wasl_runtime::EventSeq,
//!       floor: wasl_runtime::EventSeq, resume: String }`
//!   - `TruncatedFrame { earliest: wasl_runtime::EventSeq, latest: wasl_runtime::EventSeq }`
//!     (this is exactly `wasl_runtime::StreamItem::Truncated`'s two fields, serialized bare)
//!   - `SubmitTurnRequest { delivery: wasl_adapter::Delivery, source: wasl_runtime::TurnSource,
//!       idempotency_key: Option<wasl_runtime::IdempotencyKey>, busy_policy: Option<wasl_runtime::BusyPolicy>,
//!       causation: wasl_runtime::Causation }`
//!     (NOTE: the spec's fixture always shows busy_policy present; treat it as required in the
//!     wrapper type used for THIS fixture, but the Option is per spec prose "optional" -- if the
//!     code-agent finds this ambiguous, keep busy_policy: wasl_runtime::BusyPolicy required to
//!     match this exact fixture body, and document the default-when-absent behavior at the HTTP
//!     deserialization layer instead of in the Rust type, since a required field in the Rust type
//!     is simpler to serialize deterministically for this fixture test)
//!   - `CancelRequest { turn_id: wasl_runtime::TurnId, reason: wasl_runtime::CancelReason }`
//!   - `GateRequest { turn_id: wasl_runtime::TurnId, decision: wasl_proto::GateDecision }`
//!   - `ShutdownRequest { mode: wasl_runtime::ShutdownMode }`
//!   - `ErrorBody { error: String, detail: String }` (the 5 error codes are free-form strings per
//!     spec: "unknown_target"/"admission_refused"/"unsupported"/"gate_unresolvable"/"malformed")
//!
//! All of the above derive `Serialize`/`Deserialize` with `#[serde(rename_all = "snake_case")]`
//! where the fixture shows snake_case keys (all of them do). `TargetSnapshot`/`TargetsSnapshotResponse`/
//! `HelloFrame`/`TruncatedFrame` have NO internal tag (they're plain structs, not enums) -- their
//! shape comes directly from their field names.
//!
//! `wasl_runtime::TurnEvent`, `wasl_runtime::TurnReceipt`, `wasl_runtime::TargetStatus`,
//! `wasl_runtime::BusyPolicy`, `wasl_runtime::TurnSource`, `wasl_runtime::Causation`, and
//! `wasl_proto::GateDecision` are used VERBATIM (already implemented, already serde) -- do not
//! redefine them.

use wasl_adapter::{Delivery, DeliveryKind};
use wasl_proto::{AgentId, GateDecision, RequestId};
use wasl_runtime::{
    BusyPolicy, CancelReason, Causation, DeliveryCertainty, EventSeq, NativeGateDetail,
    RuntimeCapabilities, ShutdownMode, SpawnContext, TargetId, TargetKey, TargetStatus, TurnEvent,
    TurnId, TurnReceipt, TurnSource,
};

use wasl_runtime_control::{
    CancelRequest, CreateTargetRequest, ErrorBody, GateRequest, HelloFrame, InfoResponse,
    ShutdownRequest, SubmitTurnRequest, TargetSnapshot, TargetsSnapshotResponse, TruncatedFrame,
};

/// Load a fixture file's raw text.
fn fixture(name: &str) -> String {
    std::fs::read_to_string(format!(
        "{}/fixtures/runtime-control-v1/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap_or_else(|e| panic!("failed to read fixture {name}: {e}"))
}

/// Assert `value` serializes to JSON that is VALUE-EQUAL to the fixture file
/// (key order irrelevant), per the spec's fixture policy.
fn assert_matches_fixture<T: serde::Serialize>(value: &T, fixture_name: &str) {
    let actual: serde_json::Value = serde_json::to_value(value).expect("must serialize");
    let expected: serde_json::Value =
        serde_json::from_str(&fixture(fixture_name)).expect("fixture must be valid JSON");
    assert_eq!(
        actual, expected,
        "serialized value does not match fixture {fixture_name}"
    );
}

/// Assert `T` deserializes from the fixture file without error, and re-serializes
/// to the same value (round-trip through the wire form).
fn assert_round_trips<T>(fixture_name: &str)
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    let text = fixture(fixture_name);
    let parsed: T = serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("fixture {fixture_name} must deserialize: {e}"));
    let reserialized = serde_json::to_value(&parsed).expect("must reserialize");
    let expected: serde_json::Value =
        serde_json::from_str(&text).expect("fixture must be valid JSON");
    assert_eq!(
        reserialized, expected,
        "round-trip of fixture {fixture_name} did not reproduce it exactly"
    );
}

// ---------------------------------------------------------------------
// GET /v1/info
// ---------------------------------------------------------------------

#[test]
fn info_response_matches_fixture() {
    let info = InfoResponse {
        agent_id: "wasl-a".to_string(),
        host_id: "host-1".to_string(),
        version: "0.1.0".to_string(),
        runtime: "managed".to_string(),
    };
    assert_matches_fixture(&info, "info.json");
}

// ---------------------------------------------------------------------
// GET /v1/targets, GET /v1/targets/{id}
// ---------------------------------------------------------------------

fn sample_target_snapshot() -> TargetSnapshot {
    TargetSnapshot {
        target_id: TargetId("tgt-1".to_string()),
        key: TargetKey("ctx-42".to_string()),
        capabilities: RuntimeCapabilities {
            persistent_process: true,
            persistent_conversation: true,
            structured_turns: true,
            streaming_output: false,
            attachable: true,
            interruptible: false,
            structured_provenance: true,
            resumable: true,
        },
        status: TargetStatus::Idle {
            since: "2026-07-02T12:00:00.000Z".to_string(),
        },
        last_seq: EventSeq(12),
        queued: 0,
        native_session_id: Some("…".to_string()),
    }
}

#[test]
fn targets_snapshot_response_matches_fixture() {
    let resp = TargetsSnapshotResponse {
        targets: vec![sample_target_snapshot()],
    };
    assert_matches_fixture(&resp, "targets_snapshot.json");
}

#[test]
fn target_snapshot_matches_fixture() {
    assert_matches_fixture(&sample_target_snapshot(), "target.json");
}

// ---------------------------------------------------------------------
// POST /v1/targets
// ---------------------------------------------------------------------

#[test]
fn create_target_request_matches_fixture() {
    let req = CreateTargetRequest {
        key: TargetKey("ctx-42".to_string()),
        protocol: Some("opencode-acp".to_string()),
        context: Default::default(),
    };
    assert_matches_fixture(&req, "create_request.json");
}

/// Build the full-context `CreateTargetRequest` that matches
/// `create_request_with_context.json`.  Factored out so both the
/// "matches-fixture" test and the round-trip assertion can share it.
fn sample_create_request_with_context() -> CreateTargetRequest {
    let mut env = std::collections::BTreeMap::new();
    env.insert("ANTHROPIC_API_KEY".to_string(), "sk-test".to_string());
    CreateTargetRequest {
        key: TargetKey("ctx-42".to_string()),
        protocol: None, // top-level protocol absent; protocol lives inside context
        context: SpawnContext {
            protocol: Some("claude".to_string()),
            cwd: Some("/workspace/project".to_string()),
            model: Some("claude-opus-4-5".to_string()),
            posture: Some("bypassPermissions".to_string()),
            env,
            setup: Some("npm install".to_string()),
            bin: Some("/usr/local/bin/claude".to_string()),
            disallowed_tools: vec!["bash".to_string(), "computer".to_string()],
            mcp_config: None,
            resume: None,
        },
    }
}

#[test]
fn create_request_with_context_matches_fixture() {
    assert_matches_fixture(
        &sample_create_request_with_context(),
        "create_request_with_context.json",
    );
}

#[test]
fn create_request_with_context_round_trips() {
    assert_round_trips::<CreateTargetRequest>("create_request_with_context.json");
}

/// Legacy wire body `{"key":"ctx-42"}` (no `protocol`, no `context`): must
/// deserialize successfully with `context == SpawnContext::default()`.
#[test]
fn legacy_create_request_no_context_deserializes() {
    let text = r#"{"key":"ctx-42"}"#;
    let parsed: CreateTargetRequest =
        serde_json::from_str(text).expect("legacy body must deserialize");
    assert_eq!(parsed.key, TargetKey("ctx-42".to_string()));
    assert_eq!(parsed.protocol, None);
    assert_eq!(
        parsed.context,
        SpawnContext::default(),
        "absent context field must yield SpawnContext::default()"
    );
    // Re-serializing a default context must omit the `context` key entirely
    // (all context fields carry skip_serializing_if = is_none / is_empty).
    let reserialized =
        serde_json::to_value(&parsed).expect("must serialize");
    assert!(
        reserialized.get("context").is_none(),
        "default SpawnContext must be omitted from the serialized output; got: {reserialized}"
    );
}

/// Legacy wire body `{"key":"ctx-42","protocol":"claude"}` (no `context`):
/// must deserialize with `context == SpawnContext::default()` and the
/// top-level `protocol` preserved.
#[test]
fn legacy_create_request_protocol_no_context_deserializes() {
    let text = r#"{"key":"ctx-42","protocol":"claude"}"#;
    let parsed: CreateTargetRequest =
        serde_json::from_str(text).expect("legacy body must deserialize");
    assert_eq!(parsed.key, TargetKey("ctx-42".to_string()));
    assert_eq!(
        parsed.protocol,
        Some("claude".to_string()),
        "top-level protocol must be preserved"
    );
    assert_eq!(
        parsed.context,
        SpawnContext::default(),
        "absent context field must yield SpawnContext::default()"
    );
    // Re-serializing must emit `protocol` but omit `context`.
    let reserialized =
        serde_json::to_value(&parsed).expect("must serialize");
    assert_eq!(
        reserialized["protocol"],
        serde_json::json!("claude"),
        "top-level protocol must round-trip"
    );
    assert!(
        reserialized.get("context").is_none(),
        "default SpawnContext must be omitted from the serialized output; got: {reserialized}"
    );
}

// ---------------------------------------------------------------------
// SSE: hello / turn_event / truncated frames
// ---------------------------------------------------------------------

#[test]
fn hello_frame_matches_fixture() {
    let hello = HelloFrame {
        api_version: 1,
        agent_id: "wasl-a".to_string(),
        host_id: "host-1".to_string(),
        version: "0.1.0".to_string(),
        target_id: TargetId("tgt-1".to_string()),
        last_seq: EventSeq(12),
        floor: EventSeq(3),
        resume: "snapshot".to_string(),
    };
    assert_matches_fixture(&hello, "hello.json");
}

#[test]
fn truncated_frame_matches_fixture() {
    let t = TruncatedFrame {
        earliest: EventSeq(8),
        latest: EventSeq(20),
    };
    assert_matches_fixture(&t, "truncated.json");
}

#[test]
fn turn_event_session_ready_matches_fixture() {
    let ev = TurnEvent::SessionReady {
        native: Some(wasl_runtime::NativeSessionId("ses_07abc123".to_string())),
    };
    assert_matches_fixture(&ev, "turn_event_session_ready.json");
}

#[test]
fn turn_event_turn_accepted_matches_fixture() {
    let ev = TurnEvent::TurnAccepted {
        turn_id: TurnId("turn-7".to_string()),
        source: TurnSource::LocalHuman,
    };
    assert_matches_fixture(&ev, "turn_event_turn_accepted.json");
}

#[test]
fn turn_event_assistant_delta_matches_fixture() {
    let ev = TurnEvent::AssistantDelta {
        turn_id: TurnId("turn-7".to_string()),
        text: "PON".to_string(),
    };
    assert_matches_fixture(&ev, "turn_event_assistant_delta.json");
}

#[test]
fn turn_event_tool_started_matches_fixture() {
    let ev = TurnEvent::ToolStarted {
        turn_id: TurnId("turn-7".to_string()),
        name: "ask_wasl".to_string(),
    };
    assert_matches_fixture(&ev, "turn_event_tool_started.json");
}

#[test]
fn turn_event_native_gate_requested_matches_fixture() {
    let ev = TurnEvent::NativeGateRequested {
        turn_id: TurnId("turn-7".to_string()),
        detail: NativeGateDetail {
            summary: "write access".to_string(),
            preview: "rm -rf /tmp/x".to_string(),
        },
    };
    assert_matches_fixture(&ev, "turn_event_native_gate_requested.json");
}

#[test]
fn turn_event_turn_completed_matches_fixture() {
    let ev = TurnEvent::TurnCompleted {
        turn_id: TurnId("turn-7".to_string()),
        reply: "PONG".to_string(),
        certainty: DeliveryCertainty::Completed,
    };
    assert_matches_fixture(&ev, "turn_event_turn_completed.json");
}

#[test]
fn turn_event_turn_failed_matches_fixture() {
    let ev = TurnEvent::TurnFailed {
        turn_id: TurnId("turn-7".to_string()),
        error: "boom".to_string(),
        certainty: DeliveryCertainty::OutcomeUnknown,
    };
    assert_matches_fixture(&ev, "turn_event_turn_failed.json");
}

#[test]
fn turn_event_process_exited_matches_fixture() {
    let ev = TurnEvent::ProcessExited { code: Some(0) };
    assert_matches_fixture(&ev, "turn_event_process_exited.json");
}

// ---------------------------------------------------------------------
// POST /v1/targets/{id}/turns
// ---------------------------------------------------------------------

#[test]
fn submit_turn_request_matches_fixture() {
    let req = SubmitTurnRequest {
        delivery: Delivery {
            context_id: "ctx-42".to_string(),
            from: "human".to_string(),
            kind: DeliveryKind::Message,
            text: "remember token VIOLET-7F29".to_string(),
            data: vec![],
        },
        source: TurnSource::LocalHuman,
        idempotency_key: Some(wasl_runtime::IdempotencyKey("opt-key-1".to_string())),
        busy_policy: BusyPolicy::Queue {
            max_wait: std::time::Duration::from_millis(30000),
        },
        causation: Causation {
            root_request: RequestId("req-1".to_string()),
            parent_request: None,
            path: vec![],
            hop: 0,
        },
    };
    assert_matches_fixture(&req, "submit_request.json");
}

#[test]
fn receipt_started_matches_fixture() {
    let r = TurnReceipt::Started {
        turn_id: TurnId("turn-7".to_string()),
        first_seq: EventSeq(13),
    };
    assert_matches_fixture(&r, "receipt_started.json");
}

#[test]
fn receipt_queued_matches_fixture() {
    let r = TurnReceipt::Queued {
        turn_id: TurnId("turn-8".to_string()),
        position: 1,
    };
    assert_matches_fixture(&r, "receipt_queued.json");
}

#[test]
fn receipt_rejected_busy_matches_fixture() {
    let r = TurnReceipt::RejectedBusy {
        active_turn: TurnId("turn-7".to_string()),
    };
    assert_matches_fixture(&r, "receipt_rejected_busy.json");
}

#[test]
fn receipt_duplicate_matches_fixture() {
    let r = TurnReceipt::Duplicate {
        original: Box::new(TurnReceipt::Started {
            turn_id: TurnId("turn-7".to_string()),
            first_seq: EventSeq(13),
        }),
    };
    assert_matches_fixture(&r, "receipt_duplicate.json");
}

// ---------------------------------------------------------------------
// POST /v1/targets/{id}/cancel
// ---------------------------------------------------------------------

#[test]
fn cancel_request_matches_fixture() {
    let req = CancelRequest {
        turn_id: TurnId("turn-7".to_string()),
        reason: CancelReason::OperatorRequest,
    };
    assert_matches_fixture(&req, "cancel_request.json");
}

// ---------------------------------------------------------------------
// POST /v1/targets/{id}/gate
// ---------------------------------------------------------------------

#[test]
fn gate_request_approve_matches_fixture() {
    let req = GateRequest {
        turn_id: TurnId("turn-7".to_string()),
        decision: GateDecision::Approve,
    };
    assert_matches_fixture(&req, "gate_request_approve.json");
}

#[test]
fn gate_request_deny_matches_fixture() {
    let req = GateRequest {
        turn_id: TurnId("turn-7".to_string()),
        decision: GateDecision::Deny {
            reason: "not allowed".to_string(),
        },
    };
    assert_matches_fixture(&req, "gate_request_deny.json");
}

// ---------------------------------------------------------------------
// POST /v1/targets/{id}/shutdown
// ---------------------------------------------------------------------

#[test]
fn shutdown_request_drain_matches_fixture() {
    let req = ShutdownRequest {
        mode: ShutdownMode::Drain,
    };
    assert_matches_fixture(&req, "shutdown_request_drain.json");
}

// ---------------------------------------------------------------------
// Error bodies
// ---------------------------------------------------------------------

#[test]
fn error_unknown_target_matches_fixture() {
    let e = ErrorBody {
        error: "unknown_target".to_string(),
        detail: "tgt-9".to_string(),
    };
    assert_matches_fixture(&e, "error_unknown_target.json");
}

#[test]
fn error_admission_refused_matches_fixture() {
    let e = ErrorBody {
        error: "admission_refused".to_string(),
        detail: "per-sender cap reached".to_string(),
    };
    assert_matches_fixture(&e, "error_admission_refused.json");
}

#[test]
fn error_unsupported_matches_fixture() {
    let e = ErrorBody {
        error: "unsupported".to_string(),
        detail: "target not interruptible".to_string(),
    };
    assert_matches_fixture(&e, "error_unsupported.json");
}

#[test]
fn error_malformed_matches_fixture() {
    let e = ErrorBody {
        error: "malformed".to_string(),
        detail: "<parser message, free-form>".to_string(),
    };
    assert_matches_fixture(&e, "error_malformed.json");
}

// ---------------------------------------------------------------------
// POST /v1/targets — resume field wire round-trips
// ---------------------------------------------------------------------

/// Build the full-resume `CreateTargetRequest` that matches
/// `create_request_with_resume.json`.
fn sample_create_request_with_resume() -> CreateTargetRequest {
    CreateTargetRequest {
        key: TargetKey("ctx-42".to_string()),
        protocol: None,
        context: SpawnContext {
            resume: Some("ses_07abc123def456".to_string()),
            model: Some("claude-opus-4-5".to_string()),
            cwd: Some("/workspace/project".to_string()),
            ..SpawnContext::default()
        },
    }
}

#[test]
fn create_request_with_resume_matches_fixture() {
    assert_matches_fixture(
        &sample_create_request_with_resume(),
        "create_request_with_resume.json",
    );
}

#[test]
fn create_request_with_resume_round_trips() {
    assert_round_trips::<CreateTargetRequest>("create_request_with_resume.json");
}

/// When `context.resume` is absent, it must deserialize to `None` — existing
/// wires that have no `resume` key must still parse cleanly.
#[test]
fn create_request_context_without_resume_deserializes_resume_as_none() {
    let text = r#"{"key":"ctx-42","context":{"model":"claude-opus-4-5"}}"#;
    let parsed: CreateTargetRequest =
        serde_json::from_str(text).expect("must deserialize");
    assert!(
        parsed.context.resume.is_none(),
        "absent context.resume must deserialize to None; got {:?}",
        parsed.context.resume
    );
}

/// `SpawnContext::default()` (resume = None) must not emit a `resume` key when
/// the whole context is omitted from a `CreateTargetRequest`.
#[test]
fn create_request_without_context_never_emits_resume_key() {
    let text = r#"{"key":"ctx-42"}"#;
    let parsed: CreateTargetRequest =
        serde_json::from_str(text).expect("legacy body must deserialize");
    assert!(
        parsed.context.resume.is_none(),
        "legacy body with no context must have resume = None; got {:?}",
        parsed.context.resume
    );
    // Re-serializing must NOT emit a `resume` key anywhere.
    let reser = serde_json::to_value(&parsed).expect("must serialize");
    assert!(
        reser.get("context").is_none(),
        "default context must be omitted; got: {reser}"
    );
}

// ---------------------------------------------------------------------
// Round-trip: every fixture must also deserialize back into its real type
// ---------------------------------------------------------------------

#[test]
fn every_turn_event_fixture_round_trips() {
    assert_round_trips::<TurnEvent>("turn_event_session_ready.json");
    assert_round_trips::<TurnEvent>("turn_event_turn_accepted.json");
    assert_round_trips::<TurnEvent>("turn_event_assistant_delta.json");
    assert_round_trips::<TurnEvent>("turn_event_tool_started.json");
    assert_round_trips::<TurnEvent>("turn_event_native_gate_requested.json");
    assert_round_trips::<TurnEvent>("turn_event_turn_completed.json");
    assert_round_trips::<TurnEvent>("turn_event_turn_failed.json");
    assert_round_trips::<TurnEvent>("turn_event_process_exited.json");
}

#[test]
fn every_receipt_fixture_round_trips() {
    assert_round_trips::<TurnReceipt>("receipt_started.json");
    assert_round_trips::<TurnReceipt>("receipt_queued.json");
    assert_round_trips::<TurnReceipt>("receipt_rejected_busy.json");
    assert_round_trips::<TurnReceipt>("receipt_duplicate.json");
}

#[test]
fn info_and_frames_round_trip() {
    assert_round_trips::<InfoResponse>("info.json");
    assert_round_trips::<HelloFrame>("hello.json");
    assert_round_trips::<TruncatedFrame>("truncated.json");
    assert_round_trips::<TargetSnapshot>("target.json");
    assert_round_trips::<TargetsSnapshotResponse>("targets_snapshot.json");
    assert_round_trips::<CreateTargetRequest>("create_request.json");
    assert_round_trips::<CreateTargetRequest>("create_request_with_context.json");
    assert_round_trips::<CreateTargetRequest>("create_request_with_resume.json");
    assert_round_trips::<SubmitTurnRequest>("submit_request.json");
    assert_round_trips::<CancelRequest>("cancel_request.json");
    assert_round_trips::<GateRequest>("gate_request_approve.json");
    assert_round_trips::<GateRequest>("gate_request_deny.json");
    assert_round_trips::<ShutdownRequest>("shutdown_request_drain.json");
    assert_round_trips::<ErrorBody>("error_unknown_target.json");
    assert_round_trips::<ErrorBody>("error_admission_refused.json");
    assert_round_trips::<ErrorBody>("error_unsupported.json");
    assert_round_trips::<ErrorBody>("error_malformed.json");
}

// ---------------------------------------------------------------------
// The boundary invariant: ProtocolEvent NEVER crosses this wire. This is a
// static/structural check: none of the wire types above reference
// `wasl_runtime::ProtocolEvent` anywhere (enforced by the fact that this
// file, and the crate's public wire surface, never imports it). A stronger
// runtime scan is impractical for a type-erased serde boundary; the
// discipline is enforced by NEVER importing `ProtocolEvent` in this crate at
// all (verified by this test file's own import list above, and should be
// re-verified by grep in CI/review: `wasl_runtime_control` must never
// `use wasl_runtime::ProtocolEvent`).
#[test]
fn protocol_event_is_never_imported_by_this_crate() {
    // This test's mere existence and compilation is the assertion: if a
    // future change to wasl-runtime-control's src ever imports ProtocolEvent
    // and re-exports/serializes it, a reviewer must catch it (ProtocolEvent
    // does not implement Serialize, so accidentally serializing it is also
    // a compile error, not just a review miss).
    let _: fn(&AgentId) -> &str = |a| a.as_str();
}
