//! Phase-1 boundary test: `ProtocolEvent` is INTERNAL and never crosses the
//! public / control wire.
//!
//! runtime.md §5 (lines 428-434) + §6 (lines 528-532): `ProtocolEvent` is the
//! NORMALIZED driver vocabulary consumed by the actor and translated INTO the
//! PUBLIC `TurnEvent`; it "never leaks onto any public surface" and is
//! "never serialized" onto the public/control wire (§5 guarantee 6, line 491;
//! control-api.md §Boundary invariant, lines 13-16; §Golden fixtures, line 296).
//! Its derives per §6 are `Debug, Clone, PartialEq, Eq` ONLY — deliberately NOT
//! `Serialize`/`Deserialize` — which is the type-level guarantee that it cannot
//! be placed on the wire.
//!
//! What this file asserts (TYPE-ONLY, no behavior):
//!   1. `ProtocolEvent` EXISTS as a public-in-crate type (the generic driver
//!      contract lives in R1, §6) and its variants are constructible.
//!   2. The PUBLIC vocabulary `TurnEvent` IS serde (it crosses the wire), so the
//!      internal/public split is real and observable.
//!
//! It deliberately does NOT serialize `ProtocolEvent` onto the control wire —
//! doing so would violate the boundary invariant this test exists to protect.
//!
//! TEST-FIRST / EXPECTED RED: will not compile until the coder creates
//! `wasl-runtime` with the §6 `ProtocolEvent` and §5 `TurnEvent`.

use wasl_runtime::{
    AgentMode, CompactionInfo, NativeGateDetail, NativeSessionId, PlanEntry, ProtocolEvent,
    SlashCommandInfo, TurnEvent, TurnId, UsageSnapshot,
};

// =====================================================================
// (1) ProtocolEvent exists as the internal, normalized vocabulary (§6). Every
// variant the spec lists is constructible and matchable. This is the generic
// contract R2 drivers map their native wire ONTO.
// =====================================================================

#[test]
fn protocol_event_all_nine_variants_construct_and_match() {
    // §6 lines 533-542 enumerate exactly these nine variants.
    let events = vec![
        ProtocolEvent::SessionReady {
            native: Some(NativeSessionId("ses_1".to_string())),
        },
        ProtocolEvent::TurnAccepted { native_turn: None },
        ProtocolEvent::AssistantDelta {
            text: "hi".to_string(),
        },
        ProtocolEvent::ToolStarted {
            name: "grep".to_string(),
            input: serde_json::Value::Null,
        },
        ProtocolEvent::ToolFinished {
            name: "grep".to_string(),
            ok: true,
            result: None,
        },
        ProtocolEvent::NativeGateRequested {
            detail: NativeGateDetail {
                summary: "s".to_string(),
                preview: "p".to_string(),
            },
        },
        ProtocolEvent::TurnCompleted {
            reply: "PONG".to_string(),
        },
        ProtocolEvent::TurnFailed {
            error: "boom".to_string(),
        },
        ProtocolEvent::ProcessExited { code: Some(0) },
        // Informational / observability variants (additive, forward-compat).
        ProtocolEvent::ThinkingDelta { text: "hmm".to_string() },
        ProtocolEvent::UsageUpdate {
            usage: UsageSnapshot::default(),
            context_pct: Some(42),
        },
        ProtocolEvent::PlanUpdated {
            entries: vec![PlanEntry { content: "step 1".to_string(), priority: "high".to_string() }],
        },
        ProtocolEvent::ModesUpdated {
            available_modes: vec![AgentMode { id: "plan".to_string(), label: "Plan".to_string(), description: String::new() }],
            current_mode: Some("plan".to_string()),
        },
        ProtocolEvent::ModelChanged { model: "claude-sonnet-4.6".to_string() },
        ProtocolEvent::SlashCommandsUpdated {
            commands: vec![SlashCommandInfo { name: "/help".to_string(), description: String::new(), argument_hint: String::new() }],
        },
        ProtocolEvent::Compaction {
            info: CompactionInfo { trigger: "manual".to_string(), ..Default::default() },
        },
        // Feature B: the session-init capability catalog variant.
        ProtocolEvent::SessionCapabilities {
            capabilities: wasl_runtime::SessionCapabilities {
                tools: vec!["Read".to_string()],
                ..Default::default()
            },
        },
    ];
    assert_eq!(
        events.len(),
        17,
        "ProtocolEvent has 17 variants (9 original + 7 informational + SessionCapabilities)"
    );

    // Exhaustive match proves every variant is present (a missing/renamed variant
    // fails to compile).
    for ev in &events {
        match ev {
            ProtocolEvent::SessionReady { .. }
            | ProtocolEvent::TurnAccepted { .. }
            | ProtocolEvent::AssistantDelta { .. }
            | ProtocolEvent::ToolStarted { .. }
            | ProtocolEvent::ToolFinished { .. }
            | ProtocolEvent::NativeGateRequested { .. }
            | ProtocolEvent::TurnCompleted { .. }
            | ProtocolEvent::TurnFailed { .. }
            | ProtocolEvent::ProcessExited { .. }
            | ProtocolEvent::ThinkingDelta { .. }
            | ProtocolEvent::UsageUpdate { .. }
            | ProtocolEvent::PlanUpdated { .. }
            | ProtocolEvent::ModesUpdated { .. }
            | ProtocolEvent::ModelChanged { .. }
            | ProtocolEvent::SlashCommandsUpdated { .. }
            | ProtocolEvent::Compaction { .. }
            | ProtocolEvent::SessionCapabilities { .. } => {}
        }
    }

    // Clone + Eq (the §6 derives), used by the actor's translation step.
    assert_eq!(events[0].clone(), events[0]);
    assert_ne!(events[0], events[8]);
}

// =====================================================================
// (2) The PUBLIC vocabulary IS serde (TurnEvent crosses the wire, §5), so the
// internal-vs-public split the boundary invariant relies on is real: TurnEvent
// serializes; ProtocolEvent is a bare enum that does not appear on the wire.
// =====================================================================

#[test]
fn public_turn_event_is_serde_wire_type() {
    // A representative public event serializes (it is what the actor emits AFTER
    // translating a ProtocolEvent). This asserts the PUBLIC half of the split.
    let public = TurnEvent::AssistantDelta {
        turn_id: TurnId("turn-7".to_string()),
        text: "PON".to_string(),
    };
    let json_s = serde_json::to_string(&public).expect("TurnEvent is a serde wire type");
    assert!(json_s.contains("\"type\":\"assistant_delta\""));
    let back: TurnEvent = serde_json::from_str(&json_s).expect("TurnEvent round-trips");
    assert_eq!(back, public);
}
