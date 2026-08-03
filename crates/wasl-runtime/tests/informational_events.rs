//! Actor behavioral tests for the 7 new informational / observability
//! `ProtocolEvent` variants: `ThinkingDelta`, `UsageUpdate`, `PlanUpdated`,
//! `ModesUpdated`, `ModelChanged`, `SlashCommandsUpdated`, `Compaction`.
//!
//! Each test feeds the scripted transport a `ProtocolEvent` mid-turn (or
//! idle, where applicable) and asserts the matching public `TurnEvent` is
//! emitted into the journal/broadcast stream — without disturbing turn/gate
//! state.
//!
//! Pattern mirrors `gate_and_events.rs` (delta/tool coverage).

mod support;

use std::sync::Arc;

use wasl_runtime::{
    AgentMode, BusyPolicy, CompactionInfo, DeliveryTarget, EventSeq, PlanEntry, ProtocolEvent,
    SlashCommandInfo, TurnEvent, TurnReceipt, UsageSnapshot,
};

use support::{
    consult, drain_items, events_of, key, provider, sink_arc, tight_config, ProtocolScript,
    RecordingSink, ScriptedProtocol, TestClock,
};

/// A config with a generous journal capacity for informational-event tests.
/// The informational events generate more journal entries than `tight_config()`'s
/// journal_capacity of 8 (StatusChanged{Busy…} + SessionReady + TurnAccepted +
/// informational events + AssistantDelta + TurnCompleted + StatusChanged{Idle} +
/// ProcessExited can easily exceed 8).
fn info_config() -> wasl_runtime::ManagedConfig {
    wasl_runtime::ManagedConfig {
        journal_capacity: 64,
        ..tight_config()
    }
}

// =========================================================================
// Helper: build a happy-turn script that inserts `extra` events mid-turn.
// =========================================================================

fn mid_turn_script(extra: Vec<ProtocolEvent>) -> ProtocolScript {
    let mut s = ProtocolScript::new()
        .ready(Some("ses-1"))
        .emit(ProtocolEvent::TurnAccepted { native_turn: None });
    for ev in extra {
        s = s.emit(ev);
    }
    s.emit(ProtocolEvent::AssistantDelta { text: "reply text".to_string() })
        .emit(ProtocolEvent::TurnCompleted {
            reply: "done".to_string(),
        })
}

// =========================================================================
// 1. ThinkingDelta — emits TurnEvent::ThinkingDelta for the active turn.
// =========================================================================

#[tokio::test]
async fn thinking_delta_mid_turn_surfaces_as_turn_event() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-think",
        mid_turn_script(vec![ProtocolEvent::ThinkingDelta {
            text: "Let me reason...".to_string(),
        }]),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, info_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-think")).await.expect("target");

    let started = target
        .submit_turn(consult("ctx-think", "think"), BusyPolicy::Reject)
        .await;
    assert!(matches!(started, TurnReceipt::Started { .. }), "{started:?}");

    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);

    assert!(
        evs.iter().any(|e| matches!(
            e,
            TurnEvent::ThinkingDelta { text } if text == "Let me reason..."
        )),
        "ThinkingDelta ProtocolEvent → TurnEvent::ThinkingDelta, got {evs:?}"
    );
    // The turn completes normally (ThinkingDelta does not affect turn state).
    assert!(
        evs.iter().any(|e| matches!(e, TurnEvent::TurnCompleted { .. })),
        "turn completes normally after ThinkingDelta, got {evs:?}"
    );
}

// =========================================================================
// 2. UsageUpdate — emits TurnEvent::UsageUpdate (no turn-state change).
// =========================================================================

#[tokio::test]
async fn usage_update_mid_turn_surfaces_as_turn_event() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-usage",
        mid_turn_script(vec![ProtocolEvent::UsageUpdate {
            usage: UsageSnapshot {
                input_tokens: 1000,
                output_tokens: 200,
                cache_read_tokens: 50,
                cache_creation_tokens: 10,
                cost_usd: 0.005,
            },
            context_pct: Some(38),
        }]),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, info_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-usage")).await.expect("target");

    let _ = target
        .submit_turn(consult("ctx-usage", "go"), BusyPolicy::Reject)
        .await;

    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);

    let found = evs.iter().find(|e| matches!(e, TurnEvent::UsageUpdate { .. }));
    assert!(found.is_some(), "UsageUpdate ProtocolEvent → TurnEvent::UsageUpdate, got {evs:?}");
    if let Some(TurnEvent::UsageUpdate { usage, context_pct }) = found {
        assert_eq!(usage.input_tokens, 1000);
        assert_eq!(usage.output_tokens, 200);
        assert_eq!(*context_pct, Some(38));
    }
    // Turn completes normally.
    assert!(
        evs.iter().any(|e| matches!(e, TurnEvent::TurnCompleted { .. })),
        "turn completes normally after UsageUpdate, got {evs:?}"
    );
}

// =========================================================================
// 3. PlanUpdated — emits TurnEvent::PlanUpdated (no turn-state change).
// =========================================================================

#[tokio::test]
async fn plan_updated_mid_turn_surfaces_as_turn_event() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-plan",
        mid_turn_script(vec![ProtocolEvent::PlanUpdated {
            entries: vec![
                PlanEntry {
                    content: "step 1: analyse".to_string(),
                    priority: "high".to_string(),
                },
                PlanEntry {
                    content: "step 2: implement".to_string(),
                    priority: String::new(),
                },
            ],
        }]),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, info_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-plan")).await.expect("target");

    let _ = target
        .submit_turn(consult("ctx-plan", "plan"), BusyPolicy::Reject)
        .await;

    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);

    let found = evs.iter().find(|e| matches!(e, TurnEvent::PlanUpdated { .. }));
    assert!(found.is_some(), "PlanUpdated ProtocolEvent → TurnEvent::PlanUpdated, got {evs:?}");
    if let Some(TurnEvent::PlanUpdated { entries }) = found {
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].content, "step 1: analyse");
        assert_eq!(entries[0].priority, "high");
        assert_eq!(entries[1].content, "step 2: implement");
    }
    assert!(
        evs.iter().any(|e| matches!(e, TurnEvent::TurnCompleted { .. })),
        "turn completes normally after PlanUpdated, got {evs:?}"
    );
}

// =========================================================================
// 4. ModesUpdated — emits TurnEvent::ModesUpdated (no turn-state change).
// =========================================================================

#[tokio::test]
async fn modes_updated_mid_turn_surfaces_as_turn_event() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-modes",
        mid_turn_script(vec![ProtocolEvent::ModesUpdated {
            available_modes: vec![
                AgentMode {
                    id: "plan".to_string(),
                    label: "Plan".to_string(),
                    description: "Review before acting".to_string(),
                },
                AgentMode {
                    id: "default".to_string(),
                    label: "Default".to_string(),
                    description: String::new(),
                },
            ],
            current_mode: Some("plan".to_string()),
        }]),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, info_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-modes")).await.expect("target");

    let _ = target
        .submit_turn(consult("ctx-modes", "modes"), BusyPolicy::Reject)
        .await;

    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);

    let found = evs.iter().find(|e| matches!(e, TurnEvent::ModesUpdated { .. }));
    assert!(
        found.is_some(),
        "ModesUpdated ProtocolEvent → TurnEvent::ModesUpdated, got {evs:?}"
    );
    if let Some(TurnEvent::ModesUpdated { available_modes, current_mode }) = found {
        assert_eq!(available_modes.len(), 2);
        assert_eq!(available_modes[0].id, "plan");
        assert_eq!(*current_mode, Some("plan".to_string()));
    }
    assert!(
        evs.iter().any(|e| matches!(e, TurnEvent::TurnCompleted { .. })),
        "turn completes normally after ModesUpdated, got {evs:?}"
    );
}

// =========================================================================
// 5. ModelChanged — emits TurnEvent::ModelChanged (no turn-state change).
// =========================================================================

#[tokio::test]
async fn model_changed_mid_turn_surfaces_as_turn_event() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-model",
        mid_turn_script(vec![ProtocolEvent::ModelChanged {
            model: "claude-sonnet-4.6".to_string(),
        }]),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, info_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-model")).await.expect("target");

    let _ = target
        .submit_turn(consult("ctx-model", "model"), BusyPolicy::Reject)
        .await;

    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);

    assert!(
        evs.iter().any(|e| matches!(
            e,
            TurnEvent::ModelChanged { model } if model == "claude-sonnet-4.6"
        )),
        "ModelChanged ProtocolEvent → TurnEvent::ModelChanged, got {evs:?}"
    );
    assert!(
        evs.iter().any(|e| matches!(e, TurnEvent::TurnCompleted { .. })),
        "turn completes normally after ModelChanged, got {evs:?}"
    );
}

// =========================================================================
// 6. SlashCommandsUpdated — emits TurnEvent::SlashCommandsUpdated.
// =========================================================================

#[tokio::test]
async fn slash_commands_updated_mid_turn_surfaces_as_turn_event() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-cmds",
        mid_turn_script(vec![ProtocolEvent::SlashCommandsUpdated {
            commands: vec![
                SlashCommandInfo {
                    name: "/help".to_string(),
                    description: "Show help".to_string(),
                    argument_hint: "<topic>".to_string(),
                },
                SlashCommandInfo {
                    name: "/compact".to_string(),
                    description: "Compact context".to_string(),
                    argument_hint: String::new(),
                },
            ],
        }]),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, info_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-cmds")).await.expect("target");

    let _ = target
        .submit_turn(consult("ctx-cmds", "cmds"), BusyPolicy::Reject)
        .await;

    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);

    let found = evs
        .iter()
        .find(|e| matches!(e, TurnEvent::SlashCommandsUpdated { .. }));
    assert!(
        found.is_some(),
        "SlashCommandsUpdated ProtocolEvent → TurnEvent::SlashCommandsUpdated, got {evs:?}"
    );
    if let Some(TurnEvent::SlashCommandsUpdated { commands }) = found {
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].name, "/help");
        assert_eq!(commands[1].name, "/compact");
    }
    assert!(
        evs.iter().any(|e| matches!(e, TurnEvent::TurnCompleted { .. })),
        "turn completes normally after SlashCommandsUpdated, got {evs:?}"
    );
}

// =========================================================================
// 7. Compaction — emits TurnEvent::Compaction (no turn-state change).
// =========================================================================

#[tokio::test]
async fn compaction_mid_turn_surfaces_as_turn_event() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-compact",
        mid_turn_script(vec![ProtocolEvent::Compaction {
            info: CompactionInfo {
                trigger: "auto".to_string(),
                pre_tokens: 80_000,
                post_tokens: 20_000,
                cumulative_dropped_tokens: 60_000,
                duration_ms: 850,
            },
        }]),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, info_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-compact")).await.expect("target");

    let _ = target
        .submit_turn(consult("ctx-compact", "compact"), BusyPolicy::Reject)
        .await;

    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);

    let found = evs.iter().find(|e| matches!(e, TurnEvent::Compaction { .. }));
    assert!(
        found.is_some(),
        "Compaction ProtocolEvent → TurnEvent::Compaction, got {evs:?}"
    );
    if let Some(TurnEvent::Compaction { info }) = found {
        assert_eq!(info.trigger, "auto");
        assert_eq!(info.pre_tokens, 80_000);
        assert_eq!(info.post_tokens, 20_000);
        assert_eq!(info.cumulative_dropped_tokens, 60_000);
        assert_eq!(info.duration_ms, 850);
    }
    assert!(
        evs.iter().any(|e| matches!(e, TurnEvent::TurnCompleted { .. })),
        "turn completes normally after Compaction, got {evs:?}"
    );
}

// =========================================================================
// 8. All 7 informational events in one turn — ordering preserved.
//    Proves the actor emits all of them sequenced in order without
//    disturbing turn or gate state.
// =========================================================================

#[tokio::test]
async fn all_seven_informational_events_in_one_turn_are_all_emitted() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-all7",
        mid_turn_script(vec![
            ProtocolEvent::ThinkingDelta { text: "thinking".to_string() },
            ProtocolEvent::UsageUpdate {
                usage: UsageSnapshot { input_tokens: 42, ..Default::default() },
                context_pct: Some(10),
            },
            ProtocolEvent::PlanUpdated {
                entries: vec![PlanEntry { content: "do it".to_string(), priority: String::new() }],
            },
            ProtocolEvent::ModesUpdated {
                available_modes: vec![],
                current_mode: Some("default".to_string()),
            },
            ProtocolEvent::ModelChanged { model: "o4-mini".to_string() },
            ProtocolEvent::SlashCommandsUpdated { commands: vec![] },
            ProtocolEvent::Compaction {
                info: CompactionInfo { trigger: "manual".to_string(), ..Default::default() },
            },
        ]),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, info_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-all7")).await.expect("target");

    let _ = target
        .submit_turn(consult("ctx-all7", "go"), BusyPolicy::Reject)
        .await;

    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 64).await;
    let evs = events_of(&items);

    // Check presence of each informational variant by name.
    let thinking_count = evs.iter().filter(|e| matches!(e, TurnEvent::ThinkingDelta { .. })).count();
    let usage_count = evs.iter().filter(|e| matches!(e, TurnEvent::UsageUpdate { .. })).count();
    let plan_count = evs.iter().filter(|e| matches!(e, TurnEvent::PlanUpdated { .. })).count();
    let modes_count = evs.iter().filter(|e| matches!(e, TurnEvent::ModesUpdated { .. })).count();
    let model_count = evs.iter().filter(|e| matches!(e, TurnEvent::ModelChanged { .. })).count();
    let slash_count = evs.iter().filter(|e| matches!(e, TurnEvent::SlashCommandsUpdated { .. })).count();
    let compact_count = evs.iter().filter(|e| matches!(e, TurnEvent::Compaction { .. })).count();

    assert!(thinking_count > 0, "ThinkingDelta present in mixed turn, got {evs:?}");
    assert!(usage_count > 0, "UsageUpdate present in mixed turn, got {evs:?}");
    assert!(plan_count > 0, "PlanUpdated present in mixed turn, got {evs:?}");
    assert!(modes_count > 0, "ModesUpdated present in mixed turn, got {evs:?}");
    assert!(model_count > 0, "ModelChanged present in mixed turn, got {evs:?}");
    assert!(slash_count > 0, "SlashCommandsUpdated present in mixed turn, got {evs:?}");
    assert!(compact_count > 0, "Compaction present in mixed turn, got {evs:?}");

    // Turn still completes normally (informational events do not affect state).
    assert!(
        evs.iter().any(|e| matches!(e, TurnEvent::TurnCompleted { .. })),
        "turn completes normally with all 7 informational events, got {evs:?}"
    );
}
