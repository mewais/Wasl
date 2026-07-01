//! Black-box suite for `wasl-adapter` (spec `docs/specs/adapter.md`, §Test plan
//! A1-A7). A FAKE `AgentAdapter` proves the wiring: `delivery_from_message`
//! extraction, `DeliveryKind` serde, and the `AdapterExecutor` → `TaskExecutor`
//! mapping. The real CLI-driving adapters are a separate, untested-here block.

mod common;

use serde_json::json;

use wasl_a2a_server::{ExecError, TaskExecutor};
use wasl_a2a_types::Part;
use wasl_adapter::{delivery_from_message, AdapterError, AdapterExecutor, Delivery, DeliveryKind};

use common::{msg, wasl_meta, FakeAdapter};

// ── A1 — delivery_from_message: full extraction ──────────────────────────────

#[test]
fn a1_delivery_from_message_extracts_kind_from_text_and_data() {
    let message = msg(
        Some(wasl_meta("consult", "A")),
        vec![
            Part::Text { text: "hi".into() },
            Part::Data {
                data: json!({ "x": 1 }),
            },
        ],
    );

    let delivery = delivery_from_message("ctx-42".to_string(), &message);

    assert_eq!(
        delivery,
        Delivery {
            context_id: "ctx-42".to_string(),
            from: "A".to_string(),
            kind: DeliveryKind::Consult,
            text: "hi".to_string(),
            data: vec![json!({ "x": 1 })],
        }
    );
}

// ── A2 — delivery_from_message: fail-safe defaults ───────────────────────────

#[test]
fn a2_no_metadata_defaults_to_message_from_unknown() {
    let message = msg(None, vec![Part::Text { text: "yo".into() }]);

    let delivery = delivery_from_message("ctx-1".to_string(), &message);

    assert_eq!(delivery.kind, DeliveryKind::Message);
    assert_eq!(delivery.from, "unknown");
    assert_eq!(delivery.text, "yo");
    assert_eq!(delivery.context_id, "ctx-1");
    assert!(delivery.data.is_empty());
}

#[test]
fn a2_malformed_wasl_blob_defaults_to_message_from_unknown() {
    // `wasl` present but not the expected object shape.
    let message = msg(
        Some(json!({ "wasl": "not-an-object" })),
        vec![Part::Text { text: "hi".into() }],
    );

    let delivery = delivery_from_message("ctx-2".to_string(), &message);

    assert_eq!(delivery.kind, DeliveryKind::Message);
    assert_eq!(delivery.from, "unknown");
}

#[test]
fn a2_absent_wasl_key_defaults_to_message_from_unknown() {
    // Metadata present, but no `wasl` key at all.
    let message = msg(
        Some(json!({ "other": { "kind": "command" } })),
        vec![Part::Text { text: "hi".into() }],
    );

    let delivery = delivery_from_message("ctx-3".to_string(), &message);

    assert_eq!(delivery.kind, DeliveryKind::Message);
    assert_eq!(delivery.from, "unknown");
}

#[test]
fn a2_no_text_part_yields_empty_text() {
    // Valid metadata, but the parts carry no Text (only a DataPart).
    let message = msg(
        Some(wasl_meta("consult", "A")),
        vec![Part::Data {
            data: json!({ "k": "v" }),
        }],
    );

    let delivery = delivery_from_message("ctx-4".to_string(), &message);

    assert_eq!(delivery.text, "");
    // The verb/provenance still extract; only the text falls back.
    assert_eq!(delivery.kind, DeliveryKind::Consult);
    assert_eq!(delivery.from, "A");
    assert_eq!(delivery.data, vec![json!({ "k": "v" })]);
}

// ── A3 — DeliveryKind serde round-trips lowercase ────────────────────────────

#[test]
fn a3_delivery_kind_serializes_lowercase() {
    assert_eq!(
        serde_json::to_string(&DeliveryKind::Consult).unwrap(),
        "\"consult\""
    );
    assert_eq!(
        serde_json::to_string(&DeliveryKind::Command).unwrap(),
        "\"command\""
    );
    assert_eq!(
        serde_json::to_string(&DeliveryKind::Message).unwrap(),
        "\"message\""
    );
}

#[test]
fn a3_delivery_kind_deserializes_and_round_trips() {
    for (text, kind) in [
        ("\"consult\"", DeliveryKind::Consult),
        ("\"command\"", DeliveryKind::Command),
        ("\"message\"", DeliveryKind::Message),
    ] {
        let parsed: DeliveryKind = serde_json::from_str(text).unwrap();
        assert_eq!(parsed, kind);

        let reserialized = serde_json::to_string(&parsed).unwrap();
        assert_eq!(reserialized, text);
    }
}

// ── A4 — AdapterExecutor::execute success ────────────────────────────────────

#[tokio::test]
async fn a4_execute_success_wraps_reply_and_passes_exact_delivery() {
    let fake = FakeAdapter::returning("answer");
    let executor = AdapterExecutor::new(fake.clone());

    let message = msg(
        Some(wasl_meta("consult", "B")),
        vec![
            Part::Text { text: "hi".into() },
            Part::Data {
                data: json!({ "x": 1 }),
            },
        ],
    );

    let parts = executor
        .execute("ctx-7".to_string(), message)
        .await
        .expect("execute should succeed");

    assert_eq!(
        parts,
        vec![Part::Text {
            text: "answer".to_string()
        }]
    );

    // The adapter saw exactly the Delivery derived from the message.
    assert_eq!(
        fake.only_delivery(),
        Delivery {
            context_id: "ctx-7".to_string(),
            from: "B".to_string(),
            kind: DeliveryKind::Consult,
            text: "hi".to_string(),
            data: vec![json!({ "x": 1 })],
        }
    );
}

// ── A5 — AdapterExecutor::execute error mapping ──────────────────────────────

#[tokio::test]
async fn a5_execute_maps_adapter_error_to_exec_error() {
    let fake = FakeAdapter::failing(AdapterError {
        message: "boom".to_string(),
    });
    let executor = AdapterExecutor::new(fake);

    let message = msg(
        Some(wasl_meta("consult", "A")),
        vec![Part::Text { text: "hi".into() }],
    );

    let err = executor
        .execute("ctx-9".to_string(), message)
        .await
        .expect_err("execute should surface the adapter error");

    assert_eq!(
        err,
        ExecError {
            message: "boom".to_string()
        }
    );
}

// ── A6 — the Command verb reaches the adapter ────────────────────────────────

#[tokio::test]
async fn a6_command_kind_reaches_the_adapter() {
    let fake = FakeAdapter::returning("ok");
    let executor = AdapterExecutor::new(fake.clone());

    let message = msg(
        Some(wasl_meta("command", "A")),
        vec![Part::Text {
            text: "do it".into(),
        }],
    );

    executor
        .execute("ctx-11".to_string(), message)
        .await
        .expect("execute should succeed");

    let seen = fake.only_delivery();
    assert_eq!(seen.kind, DeliveryKind::Command);
    assert_eq!(seen.from, "A");
    assert_eq!(seen.text, "do it");
}

// ── A7 — AdapterError: Display + std::error::Error ───────────────────────────

#[test]
fn a7_adapter_error_display_and_error_trait() {
    let err = AdapterError {
        message: "delivery failed".to_string(),
    };

    let shown = format!("{err}");
    assert!(!shown.is_empty(), "Display output must be non-empty");
    assert!(shown.contains("delivery failed"));

    // Must satisfy std::error::Error (usable as a boxed trait object).
    fn assert_is_error<E: std::error::Error>(_: &E) {}
    assert_is_error(&err);

    let boxed: Box<dyn std::error::Error> = Box::new(err);
    assert!(!boxed.to_string().is_empty());
}
