//! `tests/gating_unit.rs` — the PURE seams of W4 gating: no transport, no
//! tokio runtime. Mirrors the spec's §Test plan U1–U6 (`docs/specs/mcp-gating.md`).
//!
//! - U1 `detect_caps` against real rmcp capability shapes.
//! - U2 `select_mode`'s full authority×caps truth table (§3), including the
//!   `ElicitationFirst ≡ wasl_gate::choose_mode` equivalence.
//! - U3 `proto_action` (type conversion only) composed with the tested
//!   `wasl_gate::outcome_of` (semantics), proving the dedup in §6.
//! - U4 `ledger_decision_of`'s outcome→decision mapping (§5).
//! - U5 the `[gate]` config section: parsing, defaults, `UnknownGateAuthority`
//!   (pinned `Display`), and a full-example round trip.
//! - U6 `RunSink`: `Plain` vs `Teed` delegation, including the TeeSink
//!   append-before-broadcast / no-frame-on-error contract re-asserted through
//!   the wrapper.
//!
//! Test-first: this MUST fail to compile until `src/` defines the public API
//! (`detect_caps`, `select_mode`, `proto_action`, `ledger_decision_of`,
//! `GateAuthority`, `GateConfig`, `ConfigError::UnknownGateAuthority`,
//! `RunSink`, ...).

use std::path::Path;

use rmcp::model::{ClientCapabilities, ClientInfo};

use wasl_audit::{AuditEntry, AuditError, EntryKind, Sink};
use wasl_gate::{choose_mode, outcome_of, ClientCaps, GateMode};
use wasl_mcp::{detect_caps, ledger_decision_of, proto_action, select_mode};
use wasl_mcp::{Config, ConfigError, DefaultSink, GateAuthority, GateConfig, MemSink, RunSink};
use wasl_operator::{HubConfig, NodeInfo, OperatorHub, TeeSink};
use wasl_proto::{
    AgentId, ElicitationAction as ProtoElicitationAction, ElicitationResponse, GateDecision, HostId,
};

// ---------------------------------------------------------------------
// U1 — detect_caps against real rmcp capability shapes
// ---------------------------------------------------------------------

#[test]
fn detect_caps_none_peer_info_counts_as_no_elicitation() {
    // `None` (pre-initialize) must count as `false`, matching rmcp's own
    // `supports_elicitation` rule for an absent peer_info.
    assert_eq!(
        detect_caps(None, false),
        ClientCaps {
            elicitation: false,
            external: false
        }
    );
    assert_eq!(
        detect_caps(None, true),
        ClientCaps {
            elicitation: false,
            external: true
        }
    );
}

#[test]
fn detect_caps_real_handshake_with_elicitation_enabled() {
    let mut info = ClientInfo::default();
    info.capabilities = ClientCapabilities::builder().enable_elicitation().build();
    // Equivalence with rmcp's own rule: `capabilities.elicitation.is_some()`.
    assert!(info.capabilities.elicitation.is_some());

    assert_eq!(
        detect_caps(Some(&info), false),
        ClientCaps {
            elicitation: true,
            external: false
        }
    );
    assert_eq!(
        detect_caps(Some(&info), true),
        ClientCaps {
            elicitation: true,
            external: true
        }
    );
}

#[test]
fn detect_caps_real_handshake_without_elicitation() {
    // `ClientInfo::default()` carries `ClientCapabilities::default()`, which
    // declares no elicitation support.
    let info = ClientInfo::default();
    assert!(info.capabilities.elicitation.is_none());

    assert_eq!(
        detect_caps(Some(&info), false),
        ClientCaps {
            elicitation: false,
            external: false
        }
    );
    assert_eq!(
        detect_caps(Some(&info), true),
        ClientCaps {
            elicitation: false,
            external: true
        }
    );
}

// ---------------------------------------------------------------------
// U2 — select_mode: the FULL truth table of §3
// ---------------------------------------------------------------------

#[test]
fn select_mode_elicitation_authority_truth_table() {
    // elicitation, *  -> Elicitation
    assert_eq!(
        select_mode(
            GateAuthority::Elicitation,
            &ClientCaps {
                elicitation: true,
                external: false
            }
        ),
        Some(GateMode::Elicitation)
    );
    assert_eq!(
        select_mode(
            GateAuthority::Elicitation,
            &ClientCaps {
                elicitation: true,
                external: true
            }
        ),
        Some(GateMode::Elicitation)
    );
    // !elicitation, *  -> None (deny)
    assert_eq!(
        select_mode(
            GateAuthority::Elicitation,
            &ClientCaps {
                elicitation: false,
                external: false
            }
        ),
        None
    );
    assert_eq!(
        select_mode(
            GateAuthority::Elicitation,
            &ClientCaps {
                elicitation: false,
                external: true
            }
        ),
        None
    );
}

#[test]
fn select_mode_elicitation_first_truth_table_matches_choose_mode() {
    let cases = [
        (
            ClientCaps {
                elicitation: true,
                external: false,
            },
            GateMode::Elicitation,
        ),
        (
            ClientCaps {
                elicitation: true,
                external: true,
            },
            GateMode::Elicitation,
        ),
        (
            ClientCaps {
                elicitation: false,
                external: true,
            },
            GateMode::External,
        ),
        (
            ClientCaps {
                elicitation: false,
                external: false,
            },
            GateMode::PrepareCommit,
        ),
    ];
    for (caps, expected) in cases {
        assert_eq!(
            select_mode(GateAuthority::ElicitationFirst, &caps),
            Some(expected),
            "caps={caps:?}"
        );
        // ElicitationFirst is `wasl_gate::choose_mode` verbatim — asserted for
        // every one of the four caps values, not re-derived.
        assert_eq!(
            select_mode(GateAuthority::ElicitationFirst, &caps),
            Some(choose_mode(&caps)),
            "must equal wasl_gate::choose_mode verbatim for caps={caps:?}"
        );
    }
}

#[test]
fn select_mode_external_first_truth_table() {
    assert_eq!(
        select_mode(
            GateAuthority::ExternalFirst,
            &ClientCaps {
                elicitation: true,
                external: true
            }
        ),
        Some(GateMode::External)
    );
    assert_eq!(
        select_mode(
            GateAuthority::ExternalFirst,
            &ClientCaps {
                elicitation: false,
                external: true
            }
        ),
        Some(GateMode::External)
    );
    assert_eq!(
        select_mode(
            GateAuthority::ExternalFirst,
            &ClientCaps {
                elicitation: true,
                external: false
            }
        ),
        Some(GateMode::Elicitation)
    );
    assert_eq!(
        select_mode(
            GateAuthority::ExternalFirst,
            &ClientCaps {
                elicitation: false,
                external: false
            }
        ),
        Some(GateMode::PrepareCommit)
    );
}

#[test]
fn select_mode_prepare_commit_authority_always_prepare_commit() {
    for elicitation in [false, true] {
        for external in [false, true] {
            let caps = ClientCaps {
                elicitation,
                external,
            };
            assert_eq!(
                select_mode(GateAuthority::PrepareCommit, &caps),
                Some(GateMode::PrepareCommit),
                "caps={caps:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------
// U3 — proto_action (type conversion only) + outcome_of (semantics)
// ---------------------------------------------------------------------

#[test]
fn proto_action_converts_all_three_variants() {
    assert_eq!(
        proto_action(rmcp::model::ElicitationAction::Accept),
        ProtoElicitationAction::Accept
    );
    assert_eq!(
        proto_action(rmcp::model::ElicitationAction::Decline),
        ProtoElicitationAction::Decline
    );
    assert_eq!(
        proto_action(rmcp::model::ElicitationAction::Cancel),
        ProtoElicitationAction::Cancel
    );
}

#[test]
fn proto_action_composed_with_outcome_of_matches_the_dedup_mapping() {
    let approved = outcome_of(&ElicitationResponse {
        action: proto_action(rmcp::model::ElicitationAction::Accept),
    });
    assert_eq!(approved, wasl_gate::GateOutcome::Approved);

    let denied = outcome_of(&ElicitationResponse {
        action: proto_action(rmcp::model::ElicitationAction::Decline),
    });
    assert_eq!(
        denied,
        wasl_gate::GateOutcome::Denied {
            reason: "declined".to_string()
        }
    );

    let dismissed = outcome_of(&ElicitationResponse {
        action: proto_action(rmcp::model::ElicitationAction::Cancel),
    });
    assert_eq!(dismissed, wasl_gate::GateOutcome::Dismissed);
}

// ---------------------------------------------------------------------
// U4 — ledger_decision_of
// ---------------------------------------------------------------------

#[test]
fn ledger_decision_of_maps_every_outcome() {
    assert_eq!(
        ledger_decision_of(&wasl_gate::GateOutcome::Approved),
        GateDecision::Approve
    );
    assert_eq!(
        ledger_decision_of(&wasl_gate::GateOutcome::Denied {
            reason: "nope".to_string()
        }),
        GateDecision::Deny {
            reason: "nope".to_string()
        }
    );
    assert_eq!(
        ledger_decision_of(&wasl_gate::GateOutcome::Dismissed),
        GateDecision::Deny {
            reason: "dismissed".to_string()
        }
    );
}

// ---------------------------------------------------------------------
// U5 — the `[gate]` config section
// ---------------------------------------------------------------------

#[test]
fn gate_authority_parses_each_of_the_four_values() {
    let cases = [
        ("elicitation", GateAuthority::Elicitation),
        ("elicitation-first", GateAuthority::ElicitationFirst),
        ("external-first", GateAuthority::ExternalFirst),
        ("prepare-commit", GateAuthority::PrepareCommit),
    ];
    for (raw, expected) in cases {
        let toml = format!("[gate]\nauthority = \"{raw}\"\n");
        let cfg = Config::from_toml_str(&toml)
            .unwrap_or_else(|e| panic!("authority={raw:?} must parse: {e}"));
        assert_eq!(cfg.gate.authority, expected, "authority={raw:?}");
    }
}

#[test]
fn gate_authority_absent_section_or_key_defaults_to_elicitation() {
    let empty = Config::from_toml_str("").expect("empty document parses");
    assert_eq!(empty.gate.authority, GateAuthority::Elicitation);

    let empty_section = Config::from_toml_str("[gate]\n").expect("empty [gate] section parses");
    assert_eq!(empty_section.gate.authority, GateAuthority::Elicitation);
}

#[test]
fn gate_authority_unknown_value_is_rejected_with_value_preserved() {
    for bad in ["Elicitation", "bogus"] {
        let toml = format!("[gate]\nauthority = \"{bad}\"\n");
        match Config::from_toml_str(&toml) {
            Err(ConfigError::UnknownGateAuthority(v)) => {
                assert_eq!(v, bad, "the offending value must be preserved verbatim")
            }
            other => panic!("authority={bad:?}: expected UnknownGateAuthority, got {other:?}"),
        }
    }
}

#[test]
fn gate_authority_unknown_value_display_is_pinned() {
    let err = ConfigError::UnknownGateAuthority("bogus".to_string());
    assert_eq!(err.to_string(), "unknown gate authority: bogus");
}

#[test]
fn gate_section_unknown_key_is_a_toml_error() {
    match Config::from_toml_str("[gate]\nbogus_key = 1\n") {
        Err(ConfigError::Toml(_)) => {}
        other => panic!("expected ConfigError::Toml, got {other:?}"),
    }
}

#[test]
fn full_example_round_trip_includes_the_gate_section() {
    let toml = r#"
[identity]
agent_id = "planner"
host_id  = "hostA"

[a2a]
bind = "127.0.0.1:4242"

[operator]
bind = "127.0.0.1:4243"

[gate]
authority = "external-first"

[audit]
path = "/var/lib/wasl/audit.jsonl"

[adapter]
kind = "claude"
bin  = "/opt/claude/claude"

[[peers]]
agent_id = "reviewer"
host_id  = "hostB"
url      = "http://10.0.0.7:4242/"

[[peers]]
agent_id = "builder"
host_id  = "hostA"
url      = "http://127.0.0.1:4343/"
"#;
    let cfg = Config::from_toml_str(toml).expect("full example (with [gate]) must parse");
    assert_eq!(cfg.gate.authority, GateAuthority::ExternalFirst);
    assert_eq!(cfg.identity.agent_id, "planner");
    assert_eq!(cfg.peers.len(), 2);
}

#[test]
fn config_default_gate_is_elicitation() {
    assert_eq!(Config::default().gate.authority, GateAuthority::Elicitation);
    assert_eq!(
        Config::default().gate,
        GateConfig {
            authority: GateAuthority::Elicitation
        }
    );
    assert_eq!(GateAuthority::default(), GateAuthority::Elicitation);
}

// ---------------------------------------------------------------------
// U6 — RunSink
// ---------------------------------------------------------------------

fn entry(seq: u64) -> AuditEntry {
    AuditEntry {
        seq,
        kind: EntryKind::Message,
        actor: AgentId("actor".to_string()),
        targets: vec![AgentId("target".to_string())],
        summary: "gate Message".to_string(),
        decision: None,
        ..Default::default()
    }
}

fn node_info() -> NodeInfo {
    NodeInfo {
        agent_id: AgentId("node".to_string()),
        host_id: HostId("host".to_string()),
        version: "0.0.0".to_string(),
    }
}

#[test]
fn run_sink_plain_appends_to_the_inner_sink() {
    let mut sink = RunSink::Plain(DefaultSink::Mem(MemSink::new()));
    sink.append(&entry(1)).expect("plain append must succeed");
    match sink {
        RunSink::Plain(DefaultSink::Mem(mem)) => {
            assert_eq!(mem.entries, vec![entry(1)]);
        }
        _ => panic!("expected RunSink::Plain(DefaultSink::Mem(_))"),
    }
}

#[test]
fn run_sink_teed_appends_and_broadcasts_the_equal_audit_frame() {
    let hub = OperatorHub::new(node_info(), HubConfig::default());
    let (last_seq, mut rx) = hub.subscribe();
    assert_eq!(last_seq, 0, "no frames published yet");

    let mut sink = RunSink::Teed(TeeSink::new(DefaultSink::Mem(MemSink::new()), hub));
    let e = entry(7);
    sink.append(&e).expect("teed append must succeed");

    let seq_frame = rx
        .try_recv()
        .expect("the append must have broadcast a frame");
    match seq_frame.frame {
        wasl_operator::Frame::Audit { entry: got } => assert_eq!(got, e),
        other => panic!("expected an Audit frame, got {other:?}"),
    }

    match sink {
        RunSink::Teed(tee) => match tee.into_inner() {
            DefaultSink::Mem(mem) => assert_eq!(mem.entries, vec![e]),
            _ => panic!("expected the DefaultSink::Mem inner sink"),
        },
        _ => panic!("expected RunSink::Teed(_)"),
    }
}

#[test]
fn run_sink_teed_inner_error_propagates_and_broadcasts_no_frame() {
    // `/dev/full` always fails a write with ENOSPC — the same idiom
    // `wasl-filesink`'s own error tests use to force a durable-sink failure.
    assert!(
        Path::new("/dev/full").exists(),
        "this environment must provide /dev/full"
    );
    let failing = wasl_filesink::FileSink::open("/dev/full").expect("open /dev/full for append");

    let hub = OperatorHub::new(node_info(), HubConfig::default());
    let (_last_seq, mut rx) = hub.subscribe();

    let mut sink = RunSink::Teed(TeeSink::new(DefaultSink::File(failing), hub));
    let err = sink
        .append(&entry(1))
        .expect_err("a write to /dev/full must fail");
    match err {
        AuditError::Sink(_) => {}
    }

    assert!(
        matches!(
            rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ),
        "an inner-sink failure must broadcast NOTHING"
    );
}
