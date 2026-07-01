//! Black-box happy-path tests for `wasl_proto`'s public API.
//!
//! Covers docs/specs/proto.md §Test plan cases H1–H12. Every test is a real
//! `#[test]` with meaningful assertions on serde wire forms and helper
//! behavior. This file will NOT compile until the code-agent implements the
//! spec in `crates/wasl-proto/src/**` — that is the expected state.

use wasl_proto::{
    is_multicast, AgentCard, AgentId, ApprovalToken, CommandParams, CommandResult, ConsultParams,
    ConsultResult, ElicitationAction, ElicitationCreate, ElicitationResponse, Envelope,
    EnvelopeKind, GateDecision, GateKind, HostId, MessageParams, MessageResult, Reply,
    TransferParams, TransferResult,
};

// ---------------------------------------------------------------------
// H1 — AgentId/HostId serde transparent → bare string; round-trip; as_str.
// ---------------------------------------------------------------------

#[test]
fn h1_agent_id_serializes_as_bare_string() {
    let id = AgentId("a".to_string());
    let json = serde_json::to_string(&id).unwrap();
    assert_eq!(json, "\"a\"");
}

#[test]
fn h1_agent_id_round_trips() {
    let id = AgentId("agent-42".to_string());
    let json = serde_json::to_string(&id).unwrap();
    let back: AgentId = serde_json::from_str(&json).unwrap();
    assert_eq!(back, id);
}

#[test]
fn h1_agent_id_deserializes_from_bare_string() {
    let id: AgentId = serde_json::from_str("\"claude\"").unwrap();
    assert_eq!(id, AgentId("claude".to_string()));
}

#[test]
fn h1_agent_id_as_str() {
    let id = AgentId("codex".to_string());
    assert_eq!(id.as_str(), "codex");
}

#[test]
fn h1_host_id_serializes_as_bare_string() {
    let id = HostId("h".to_string());
    let json = serde_json::to_string(&id).unwrap();
    assert_eq!(json, "\"h\"");
}

#[test]
fn h1_host_id_round_trips() {
    let id = HostId("host-1".to_string());
    let json = serde_json::to_string(&id).unwrap();
    let back: HostId = serde_json::from_str(&json).unwrap();
    assert_eq!(back, id);
}

#[test]
fn h1_host_id_deserializes_from_bare_string() {
    let id: HostId = serde_json::from_str("\"laptop\"").unwrap();
    assert_eq!(id, HostId("laptop".to_string()));
}

#[test]
fn h1_host_id_as_str() {
    let id = HostId("desktop".to_string());
    assert_eq!(id.as_str(), "desktop");
}

// ---------------------------------------------------------------------
// H2 — AgentCard round-trips; optional fields omitted-and-default to None;
// a full card round-trips.
// ---------------------------------------------------------------------

#[test]
fn h2_agent_card_minimal_defaults_optional_fields_to_none() {
    let json = r#"{"id":"a","host":"h"}"#;
    let card: AgentCard = serde_json::from_str(json).unwrap();
    assert_eq!(card.id, AgentId("a".to_string()));
    assert_eq!(card.host, HostId("h".to_string()));
    assert_eq!(card.driver, None);
    assert_eq!(card.workstream, None);
    assert_eq!(card.endpoint, None);
}

#[test]
fn h2_agent_card_minimal_round_trips() {
    let card = AgentCard {
        id: AgentId("a".to_string()),
        host: HostId("h".to_string()),
        driver: None,
        workstream: None,
        endpoint: None,
    };
    let json = serde_json::to_string(&card).unwrap();
    let back: AgentCard = serde_json::from_str(&json).unwrap();
    assert_eq!(back, card);
}

#[test]
fn h2_agent_card_full_round_trips() {
    let card = AgentCard {
        id: AgentId("claude-1".to_string()),
        host: HostId("host-a".to_string()),
        driver: Some("claude".to_string()),
        workstream: Some("wasl-proto".to_string()),
        endpoint: Some("tcp://host-a:9000".to_string()),
    };
    let json = serde_json::to_string(&card).unwrap();
    let back: AgentCard = serde_json::from_str(&json).unwrap();
    assert_eq!(back, card);
    assert_eq!(back.driver.as_deref(), Some("claude"));
    assert_eq!(back.workstream.as_deref(), Some("wasl-proto"));
    assert_eq!(back.endpoint.as_deref(), Some("tcp://host-a:9000"));
}

// ---------------------------------------------------------------------
// H3 — ConsultParams/CommandParams/MessageParams round-trip with 1 and many
// peers.
// ---------------------------------------------------------------------

#[test]
fn h3_consult_params_round_trips_one_peer() {
    let p = ConsultParams {
        peers: vec![AgentId("a".to_string())],
        question: "what time is it?".to_string(),
    };
    let json = serde_json::to_string(&p).unwrap();
    assert!(json.contains("\"peers\":[\"a\"]"));
    assert!(json.contains("\"question\":\"what time is it?\""));
    let back: ConsultParams = serde_json::from_str(&json).unwrap();
    assert_eq!(back, p);
}

#[test]
fn h3_consult_params_round_trips_many_peers() {
    let p = ConsultParams {
        peers: vec![AgentId("a".to_string()), AgentId("b".to_string())],
        question: "status?".to_string(),
    };
    let json = serde_json::to_string(&p).unwrap();
    let back: ConsultParams = serde_json::from_str(&json).unwrap();
    assert_eq!(back, p);
    assert_eq!(back.peers.len(), 2);
}

#[test]
fn h3_command_params_round_trips_one_peer() {
    let p = CommandParams {
        peers: vec![AgentId("a".to_string())],
        task: "run tests".to_string(),
    };
    let json = serde_json::to_string(&p).unwrap();
    assert!(json.contains("\"peers\":[\"a\"]"));
    assert!(json.contains("\"task\":\"run tests\""));
    let back: CommandParams = serde_json::from_str(&json).unwrap();
    assert_eq!(back, p);
}

#[test]
fn h3_command_params_round_trips_many_peers() {
    let p = CommandParams {
        peers: vec![
            AgentId("a".to_string()),
            AgentId("b".to_string()),
            AgentId("c".to_string()),
        ],
        task: "build".to_string(),
    };
    let json = serde_json::to_string(&p).unwrap();
    let back: CommandParams = serde_json::from_str(&json).unwrap();
    assert_eq!(back, p);
    assert_eq!(back.peers.len(), 3);
}

#[test]
fn h3_message_params_round_trips_one_peer() {
    let p = MessageParams {
        peers: vec![AgentId("a".to_string())],
        payload: "hello".to_string(),
    };
    let json = serde_json::to_string(&p).unwrap();
    assert!(json.contains("\"peers\":[\"a\"]"));
    assert!(json.contains("\"payload\":\"hello\""));
    let back: MessageParams = serde_json::from_str(&json).unwrap();
    assert_eq!(back, p);
}

#[test]
fn h3_message_params_round_trips_many_peers() {
    let p = MessageParams {
        peers: vec![AgentId("a".to_string()), AgentId("b".to_string())],
        payload: "broadcast".to_string(),
    };
    let json = serde_json::to_string(&p).unwrap();
    let back: MessageParams = serde_json::from_str(&json).unwrap();
    assert_eq!(back, p);
    assert_eq!(back.peers.len(), 2);
}

// ---------------------------------------------------------------------
// H4 — TransferParams round-trips (src_host + multiple dst_hosts + path).
// ---------------------------------------------------------------------

#[test]
fn h4_transfer_params_round_trips() {
    let p = TransferParams {
        src_host: HostId("host-a".to_string()),
        dst_hosts: vec![HostId("host-b".to_string()), HostId("host-c".to_string())],
        path: "/tmp/artifact.tar".to_string(),
    };
    let json = serde_json::to_string(&p).unwrap();
    assert!(json.contains("\"src_host\":\"host-a\""));
    assert!(json.contains("\"dst_hosts\":[\"host-b\",\"host-c\"]"));
    assert!(json.contains("\"path\":\"/tmp/artifact.tar\""));
    let back: TransferParams = serde_json::from_str(&json).unwrap();
    assert_eq!(back, p);
    assert_eq!(back.dst_hosts.len(), 2);
}

#[test]
fn h4_transfer_params_round_trips_single_dst_host() {
    let p = TransferParams {
        src_host: HostId("host-a".to_string()),
        dst_hosts: vec![HostId("host-b".to_string())],
        path: "/data".to_string(),
    };
    let json = serde_json::to_string(&p).unwrap();
    let back: TransferParams = serde_json::from_str(&json).unwrap();
    assert_eq!(back, p);
}

// ---------------------------------------------------------------------
// H5 — results (ConsultResult/CommandResult/MessageResult/TransferResult +
// Reply) round-trip.
// ---------------------------------------------------------------------

#[test]
fn h5_reply_round_trips() {
    let r = Reply {
        from: AgentId("a".to_string()),
        body: "ack".to_string(),
    };
    let json = serde_json::to_string(&r).unwrap();
    assert!(json.contains("\"from\":\"a\""));
    assert!(json.contains("\"body\":\"ack\""));
    let back: Reply = serde_json::from_str(&json).unwrap();
    assert_eq!(back, r);
}

#[test]
fn h5_consult_result_round_trips() {
    let res = ConsultResult {
        replies: vec![
            Reply {
                from: AgentId("a".to_string()),
                body: "42".to_string(),
            },
            Reply {
                from: AgentId("b".to_string()),
                body: "43".to_string(),
            },
        ],
    };
    let json = serde_json::to_string(&res).unwrap();
    let back: ConsultResult = serde_json::from_str(&json).unwrap();
    assert_eq!(back, res);
    assert_eq!(back.replies.len(), 2);
}

#[test]
fn h5_command_result_round_trips() {
    let res = CommandResult {
        replies: vec![Reply {
            from: AgentId("a".to_string()),
            body: "done".to_string(),
        }],
    };
    let json = serde_json::to_string(&res).unwrap();
    let back: CommandResult = serde_json::from_str(&json).unwrap();
    assert_eq!(back, res);
    assert_eq!(back.replies.len(), 1);
}

#[test]
fn h5_message_result_round_trips() {
    let res = MessageResult {
        delivered: vec![AgentId("a".to_string()), AgentId("b".to_string())],
    };
    let json = serde_json::to_string(&res).unwrap();
    assert!(json.contains("\"delivered\":[\"a\",\"b\"]"));
    let back: MessageResult = serde_json::from_str(&json).unwrap();
    assert_eq!(back, res);
}

#[test]
fn h5_transfer_result_round_trips() {
    let res = TransferResult {
        delivered: vec![HostId("host-b".to_string())],
    };
    let json = serde_json::to_string(&res).unwrap();
    assert!(json.contains("\"delivered\":[\"host-b\"]"));
    let back: TransferResult = serde_json::from_str(&json).unwrap();
    assert_eq!(back, res);
}

// ---------------------------------------------------------------------
// H6 — is_multicast: &[x] -> false; &[x,y] -> true; empty -> false. For both
// AgentId and HostId.
// ---------------------------------------------------------------------

#[test]
fn h6_is_multicast_agent_id_empty_is_false() {
    let targets: Vec<AgentId> = vec![];
    assert!(!is_multicast(&targets));
}

#[test]
fn h6_is_multicast_agent_id_single_is_false() {
    let targets = vec![AgentId("a".to_string())];
    assert!(!is_multicast(&targets));
}

#[test]
fn h6_is_multicast_agent_id_many_is_true() {
    let targets = vec![AgentId("a".to_string()), AgentId("b".to_string())];
    assert!(is_multicast(&targets));
}

#[test]
fn h6_is_multicast_host_id_empty_is_false() {
    let targets: Vec<HostId> = vec![];
    assert!(!is_multicast(&targets));
}

#[test]
fn h6_is_multicast_host_id_single_is_false() {
    let targets = vec![HostId("h".to_string())];
    assert!(!is_multicast(&targets));
}

#[test]
fn h6_is_multicast_host_id_many_is_true() {
    let targets = vec![
        HostId("h1".to_string()),
        HostId("h2".to_string()),
        HostId("h3".to_string()),
    ];
    assert!(is_multicast(&targets));
}

// ---------------------------------------------------------------------
// H7 — EnvelopeKind/GateKind serde <-> lowercase strings, each variant.
// ---------------------------------------------------------------------

#[test]
fn h7_envelope_kind_consult_lowercase() {
    let json = serde_json::to_string(&EnvelopeKind::Consult).unwrap();
    assert_eq!(json, "\"consult\"");
    let back: EnvelopeKind = serde_json::from_str(&json).unwrap();
    assert_eq!(back, EnvelopeKind::Consult);
}

#[test]
fn h7_envelope_kind_command_lowercase() {
    let json = serde_json::to_string(&EnvelopeKind::Command).unwrap();
    assert_eq!(json, "\"command\"");
    let back: EnvelopeKind = serde_json::from_str(&json).unwrap();
    assert_eq!(back, EnvelopeKind::Command);
}

#[test]
fn h7_envelope_kind_message_lowercase() {
    let json = serde_json::to_string(&EnvelopeKind::Message).unwrap();
    assert_eq!(json, "\"message\"");
    let back: EnvelopeKind = serde_json::from_str(&json).unwrap();
    assert_eq!(back, EnvelopeKind::Message);
}

#[test]
fn h7_gate_kind_consult_lowercase() {
    let json = serde_json::to_string(&GateKind::Consult).unwrap();
    assert_eq!(json, "\"consult\"");
    let back: GateKind = serde_json::from_str(&json).unwrap();
    assert_eq!(back, GateKind::Consult);
}

#[test]
fn h7_gate_kind_command_lowercase() {
    let json = serde_json::to_string(&GateKind::Command).unwrap();
    assert_eq!(json, "\"command\"");
    let back: GateKind = serde_json::from_str(&json).unwrap();
    assert_eq!(back, GateKind::Command);
}

#[test]
fn h7_gate_kind_message_lowercase() {
    let json = serde_json::to_string(&GateKind::Message).unwrap();
    assert_eq!(json, "\"message\"");
    let back: GateKind = serde_json::from_str(&json).unwrap();
    assert_eq!(back, GateKind::Message);
}

#[test]
fn h7_gate_kind_transfer_lowercase() {
    let json = serde_json::to_string(&GateKind::Transfer).unwrap();
    assert_eq!(json, "\"transfer\"");
    let back: GateKind = serde_json::from_str(&json).unwrap();
    assert_eq!(back, GateKind::Transfer);
}

// ---------------------------------------------------------------------
// H8 — Envelope round-trips.
// ---------------------------------------------------------------------

#[test]
fn h8_envelope_round_trips() {
    let env = Envelope {
        id: "env-1".to_string(),
        kind: EnvelopeKind::Message,
        from: AgentId("a".to_string()),
        to: vec![AgentId("b".to_string()), AgentId("c".to_string())],
        body: "payload".to_string(),
    };
    let json = serde_json::to_string(&env).unwrap();
    assert!(json.contains("\"id\":\"env-1\""));
    assert!(json.contains("\"kind\":\"message\""));
    assert!(json.contains("\"from\":\"a\""));
    assert!(json.contains("\"to\":[\"b\",\"c\"]"));
    assert!(json.contains("\"body\":\"payload\""));
    let back: Envelope = serde_json::from_str(&json).unwrap();
    assert_eq!(back, env);
}

#[test]
fn h8_envelope_round_trips_consult_kind() {
    let env = Envelope {
        id: "env-2".to_string(),
        kind: EnvelopeKind::Consult,
        from: AgentId("x".to_string()),
        to: vec![AgentId("y".to_string())],
        body: "question?".to_string(),
    };
    let json = serde_json::to_string(&env).unwrap();
    let back: Envelope = serde_json::from_str(&json).unwrap();
    assert_eq!(back, env);
}

// ---------------------------------------------------------------------
// H9 — GateDecision serde: Approve -> {"decision":"approve"}; Deny{reason} ->
// {"decision":"deny","reason":"..."}; is_approved for each.
// ---------------------------------------------------------------------

#[test]
fn h9_gate_decision_approve_wire_form() {
    let d = GateDecision::Approve;
    let json = serde_json::to_string(&d).unwrap();
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(value, serde_json::json!({"decision": "approve"}));
}

#[test]
fn h9_gate_decision_deny_wire_form() {
    let d = GateDecision::Deny {
        reason: "not now".to_string(),
    };
    let json = serde_json::to_string(&d).unwrap();
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(
        value,
        serde_json::json!({"decision": "deny", "reason": "not now"})
    );
}

#[test]
fn h9_gate_decision_approve_round_trips() {
    let d = GateDecision::Approve;
    let json = serde_json::to_string(&d).unwrap();
    let back: GateDecision = serde_json::from_str(&json).unwrap();
    assert_eq!(back, d);
}

#[test]
fn h9_gate_decision_deny_round_trips() {
    let d = GateDecision::Deny {
        reason: "declined".to_string(),
    };
    let json = serde_json::to_string(&d).unwrap();
    let back: GateDecision = serde_json::from_str(&json).unwrap();
    assert_eq!(back, d);
}

#[test]
fn h9_gate_decision_approve_is_approved_true() {
    assert!(GateDecision::Approve.is_approved());
}

#[test]
fn h9_gate_decision_deny_is_approved_false() {
    let d = GateDecision::Deny {
        reason: "no".to_string(),
    };
    assert!(!d.is_approved());
}

// ---------------------------------------------------------------------
// H10 — ApprovalToken transparent string round-trip.
// ---------------------------------------------------------------------

#[test]
fn h10_approval_token_serializes_as_bare_string() {
    let t = ApprovalToken("tok-123".to_string());
    let json = serde_json::to_string(&t).unwrap();
    assert_eq!(json, "\"tok-123\"");
}

#[test]
fn h10_approval_token_round_trips() {
    let t = ApprovalToken("tok-abc".to_string());
    let json = serde_json::to_string(&t).unwrap();
    let back: ApprovalToken = serde_json::from_str(&json).unwrap();
    assert_eq!(back, t);
}

// ---------------------------------------------------------------------
// H11 — ElicitationCreate round-trip; ElicitationAction <->
// "accept"/"decline"/"cancel".
// ---------------------------------------------------------------------

#[test]
fn h11_elicitation_create_round_trips() {
    let e = ElicitationCreate {
        message: "Approve sending message to a, b?".to_string(),
    };
    let json = serde_json::to_string(&e).unwrap();
    assert!(json.contains("\"message\":\"Approve sending message to a, b?\""));
    let back: ElicitationCreate = serde_json::from_str(&json).unwrap();
    assert_eq!(back, e);
}

#[test]
fn h11_elicitation_action_accept_lowercase() {
    let json = serde_json::to_string(&ElicitationAction::Accept).unwrap();
    assert_eq!(json, "\"accept\"");
    let back: ElicitationAction = serde_json::from_str(&json).unwrap();
    assert_eq!(back, ElicitationAction::Accept);
}

#[test]
fn h11_elicitation_action_decline_lowercase() {
    let json = serde_json::to_string(&ElicitationAction::Decline).unwrap();
    assert_eq!(json, "\"decline\"");
    let back: ElicitationAction = serde_json::from_str(&json).unwrap();
    assert_eq!(back, ElicitationAction::Decline);
}

#[test]
fn h11_elicitation_action_cancel_lowercase() {
    let json = serde_json::to_string(&ElicitationAction::Cancel).unwrap();
    assert_eq!(json, "\"cancel\"");
    let back: ElicitationAction = serde_json::from_str(&json).unwrap();
    assert_eq!(back, ElicitationAction::Cancel);
}

// ---------------------------------------------------------------------
// H12 — ElicitationResponse::to_decision: Accept->Approve;
// Decline->Deny("declined"); Cancel->None.
// ---------------------------------------------------------------------

#[test]
fn h12_to_decision_accept_maps_to_approve() {
    let resp = ElicitationResponse {
        action: ElicitationAction::Accept,
    };
    assert_eq!(resp.to_decision(), Some(GateDecision::Approve));
}

#[test]
fn h12_to_decision_decline_maps_to_deny_declined() {
    let resp = ElicitationResponse {
        action: ElicitationAction::Decline,
    };
    assert_eq!(
        resp.to_decision(),
        Some(GateDecision::Deny {
            reason: "declined".to_string()
        })
    );
}

#[test]
fn h12_to_decision_cancel_maps_to_none() {
    let resp = ElicitationResponse {
        action: ElicitationAction::Cancel,
    };
    assert_eq!(resp.to_decision(), None);
}

#[test]
fn h12_elicitation_response_round_trips() {
    let resp = ElicitationResponse {
        action: ElicitationAction::Decline,
    };
    let json = serde_json::to_string(&resp).unwrap();
    assert!(json.contains("\"action\":\"decline\""));
    let back: ElicitationResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(back, resp);
}
