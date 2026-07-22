//! Phase-2A behavioral suite — delivery-certainty + write-ahead audit emission
//! (spec item 11 — SAFETY-CRITICAL). §8.1 / §12.
//!
//! BEHAVIORAL, not type-only: asserts, via call-order recording shared between
//! the fake Sink and the fake transport, that for a `command` the durable
//! `Sink::append(Injected)` happens STRICTLY BEFORE `transport.send_turn` is
//! called; that Completed/OutcomeUnknown are appended after the outcome; that if
//! the durable append FAILS the command is NEVER injected; and that a
//! non-command delivery does NOT require the same write-ahead ordering. MUST
//! currently fail (no actor yet).
//!
//! Spec citations (docs/specs/runtime.md):
//!   * write-ahead-durable command carve-out — §State durability (lines 94-100),
//!     §8.1 (lines 669-679): "the durable record is written to the Sink BEFORE
//!     transport.send_turn is called … only after Sink::append returns Ok do we
//!     call transport.send_turn."
//!   * audit emission contract — §12 (lines 788-800), §8.1 (lines 654-668):
//!     kind: RuntimeTurn; phase + certainty are first-class STRUCTURAL fields,
//!     never stuffed into summary; per-transition Started/Injected/Completed/
//!     Failed with the matching DeliveryCertainty.
//!   * non-command deliveries: audited (Started/Injected/Completed) but do NOT
//!     require the write-ahead-before-inject ordering (§8.1 — the carve-out is
//!     specifically for `command`).

mod support;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use wasl_audit::{AuditEntry, AuditError, DeliveryCertainty, EntryKind, RuntimeTurnPhase, Sink};
use wasl_runtime::{BusyPolicy, DeliveryTarget, EventSeq, ProtocolEvent, TurnReceipt};

use support::{
    command, consult, drain_items, key, provider, tight_config, ProtocolScript, ScriptedProtocol,
    TestClock, TransportCall,
};

/// A Sink that stamps every append with a global order counter SHARED with the
/// transport, so we can compare "when the durable append happened" against
/// "when send_turn happened" on one monotonic timeline. Records the full
/// AuditEntry so phase/certainty structural fields are assertable.
struct OrderStampingSink {
    counter: Arc<AtomicU64>,
    entries: Arc<Mutex<Vec<(u64, AuditEntry)>>>,
}

impl OrderStampingSink {
    fn new(counter: Arc<AtomicU64>) -> (Self, Arc<Mutex<Vec<(u64, AuditEntry)>>>) {
        let entries = Arc::new(Mutex::new(Vec::new()));
        (
            OrderStampingSink {
                counter,
                entries: entries.clone(),
            },
            entries,
        )
    }
}

impl Sink for OrderStampingSink {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
        let order = self.counter.fetch_add(1, Ordering::SeqCst);
        self.entries.lock().unwrap().push((order, entry.clone()));
        Ok(())
    }
}

// =====================================================================
// Item 11a — COMMAND: Sink::append(Injected) BEFORE transport.send_turn.
// =====================================================================

#[tokio::test]
async fn command_durable_injected_append_precedes_send_turn() {
    let proto = ScriptedProtocol::new();
    let handle = proto.install(
        "ctx-wa",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .happy_turn("d", "command reply"),
    );
    // The transport shares this counter (via ScriptedProtocol::shared_counter),
    // and so does the sink — one global timeline.
    let counter = proto.shared_counter();
    let (sink, entries) = OrderStampingSink::new(counter);
    let sink_arc: Arc<Mutex<dyn Sink + Send>> = Arc::new(Mutex::new(sink));
    let clock = TestClock::new();
    let prov = provider(proto, tight_config(), clock, sink_arc);
    let target = prov.target_for(&key("ctx-wa")).await.expect("target");

    let started = target
        .submit_turn(command("ctx-wa", "rm -rf danger"), BusyPolicy::Reject)
        .await;
    assert!(matches!(started, TurnReceipt::Started { .. }));

    // Drain so the whole lifecycle (Started, Injected, Completed) is emitted.
    let stream = target.events(EventSeq(0)).await;
    let _ = drain_items(stream, 32).await;

    // Find the order stamp of the durable Injected append.
    let recorded = entries.lock().unwrap().clone();
    let injected_order = recorded
        .iter()
        .find(|(_, e)| {
            e.kind == EntryKind::RuntimeTurn
                && e.phase == Some(RuntimeTurnPhase::Injected)
                && e.certainty == Some(DeliveryCertainty::Injected)
        })
        .map(|(o, _)| *o)
        .expect("a command must emit a durable RuntimeTurn{phase:Injected, certainty:Injected}");

    // Find the order stamp of the FIRST send_turn on the transport.
    let first_send_order = handle
        .shared
        .calls()
        .iter()
        .find_map(|c| match c {
            TransportCall::SendTurn { order, .. } => Some(*order),
            _ => None,
        })
        .expect("the command must be injected via send_turn");

    assert!(
        injected_order < first_send_order,
        "SAFETY: for a command, the durable Injected append (order {injected_order}) MUST happen \
         strictly BEFORE transport.send_turn (order {first_send_order}) — §8.1 write-ahead ordering"
    );
}

// =====================================================================
// Item 11b — COMMAND: Completed is appended AFTER the outcome, with the matching
// certainty; phase+certainty live in STRUCTURAL fields, never in summary.
// =====================================================================

#[tokio::test]
async fn command_completed_append_follows_outcome_in_structural_fields() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-wa2",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .happy_turn("d", "the-reply"),
    );
    let counter = proto.shared_counter();
    let (sink, entries) = OrderStampingSink::new(counter);
    let sink_arc: Arc<Mutex<dyn Sink + Send>> = Arc::new(Mutex::new(sink));
    let clock = TestClock::new();
    let prov = provider(proto, tight_config(), clock, sink_arc);
    let target = prov.target_for(&key("ctx-wa2")).await.expect("target");

    let _ = target
        .submit_turn(command("ctx-wa2", "do it"), BusyPolicy::Reject)
        .await;
    let stream = target.events(EventSeq(0)).await;
    let _ = drain_items(stream, 32).await;

    let recorded = entries.lock().unwrap().clone();
    let injected_order = recorded
        .iter()
        .find(|(_, e)| e.phase == Some(RuntimeTurnPhase::Injected))
        .map(|(o, _)| *o)
        .expect("Injected entry");
    let (completed_order, completed_entry) = recorded
        .iter()
        .find(|(_, e)| e.phase == Some(RuntimeTurnPhase::Completed))
        .cloned()
        .expect("a completed command must emit RuntimeTurn{phase:Completed}");

    assert!(
        completed_order > injected_order,
        "Completed (order {completed_order}) is appended AFTER Injected (order {injected_order})"
    );
    assert_eq!(
        completed_entry.certainty,
        Some(DeliveryCertainty::Completed),
        "the Completed entry carries certainty:Completed as a STRUCTURAL field (§8.1/§12)"
    );
    // The safety fields are structural, NOT smuggled into the free-text summary.
    let s = completed_entry.summary.to_lowercase();
    assert!(
        !s.contains("injected") && !s.contains("outcomeunknown") && !s.contains("certainty"),
        "phase/certainty must live in structural fields, never in `summary` (§12 line 795); \
         summary was: {:?}",
        completed_entry.summary
    );
}

// =====================================================================
// Item 11c — COMMAND crash after injection: the durable record already reads
// certainty:Injected, and the terminal audit reflects OutcomeUnknown — the
// evidence a command MAY have run is not erasable. §8.1 lines 671-679.
// =====================================================================

#[tokio::test]
async fn command_crash_after_injection_audit_shows_injected_then_outcome_unknown() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-wa-crash",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .emit(ProtocolEvent::ProcessExited { code: Some(139) }),
    );
    let counter = proto.shared_counter();
    let (sink, entries) = OrderStampingSink::new(counter);
    let sink_arc: Arc<Mutex<dyn Sink + Send>> = Arc::new(Mutex::new(sink));
    let clock = TestClock::new();
    let prov = provider(proto, tight_config(), clock, sink_arc);
    let target = prov.target_for(&key("ctx-wa-crash")).await.expect("target");

    let _ = target
        .submit_turn(command("ctx-wa-crash", "danger"), BusyPolicy::Reject)
        .await;
    let stream = target.events(EventSeq(0)).await;
    let _ = drain_items(stream, 32).await;

    let recorded = entries.lock().unwrap().clone();
    // The write-ahead Injected record exists (written BEFORE the crash).
    assert!(
        recorded
            .iter()
            .any(|(_, e)| e.phase == Some(RuntimeTurnPhase::Injected)
                && e.certainty == Some(DeliveryCertainty::Injected)),
        "the durable Injected record must exist (written before injection, survives the crash) §8.1"
    );
    // A terminal audit entry classifies the crash as OutcomeUnknown.
    assert!(
        recorded
            .iter()
            .any(|(_, e)| e.certainty == Some(DeliveryCertainty::OutcomeUnknown)),
        "a command crashed after injection is audited OutcomeUnknown (§8.1 line 677), got {recorded:?}"
    );
}

// =====================================================================
// Item 11d — if the durable append FAILS, the command is NEVER injected (no
// injection without durability). §8.1 line 674 ("only after Sink::append returns
// Ok do we call transport.send_turn").
// =====================================================================

#[tokio::test]
async fn command_not_injected_when_durable_append_fails() {
    let proto = ScriptedProtocol::new();
    let handle = proto.install(
        "ctx-wa-fail",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .happy_turn("d", "should-not-happen"),
    );
    // A sink that fails its FIRST append (the write-ahead Injected record).
    let sink = support::FailingSink::new(1);
    let appended = {
        // Keep a handle to inspect what the sink recorded (nothing before the
        // failing append succeeds).
        let s: Arc<Mutex<dyn Sink + Send>> = Arc::new(Mutex::new(sink));
        s
    };
    let clock = TestClock::new();
    let prov = provider(proto, tight_config(), clock, appended);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-wa-fail")).await.expect("target");

    let _ = target
        .submit_turn(command("ctx-wa-fail", "unsafe"), BusyPolicy::Reject)
        .await;
    let stream = target.events(EventSeq(0)).await;
    let _ = drain_items(stream, 16).await;

    // The decisive safety assertion: because the durable append failed, the
    // transport was NEVER asked to inject the command.
    assert_eq!(
        handle.shared.send_turn_count(),
        0,
        "a command whose write-ahead durable append FAILS must NEVER be injected (§8.1 line 674)"
    );
}

// =====================================================================
// Item 11e — a NON-command (consult) delivery is audited but does NOT require
// the write-ahead-before-inject ordering: injection may precede its own audit
// (the carve-out is command-only). We assert the consult IS injected and IS
// audited, and we DO NOT require append-before-send for it.
// =====================================================================

#[tokio::test]
async fn non_command_delivery_does_not_require_write_ahead_ordering() {
    let proto = ScriptedProtocol::new();
    let handle = proto.install(
        "ctx-consult-audit",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .happy_turn("d", "consult reply"),
    );
    let counter = proto.shared_counter();
    let (sink, entries) = OrderStampingSink::new(counter);
    let sink_arc: Arc<Mutex<dyn Sink + Send>> = Arc::new(Mutex::new(sink));
    let clock = TestClock::new();
    let prov = provider(proto, tight_config(), clock, sink_arc);
    let target = prov
        .target_for(&key("ctx-consult-audit"))
        .await
        .expect("target");

    let _ = target
        .submit_turn(consult("ctx-consult-audit", "ask"), BusyPolicy::Reject)
        .await;
    let stream = target.events(EventSeq(0)).await;
    let _ = drain_items(stream, 32).await;

    // The consult is injected exactly once and audited as a RuntimeTurn.
    assert_eq!(
        handle.shared.send_turn_count(),
        1,
        "a consult is injected once"
    );
    let recorded = entries.lock().unwrap().clone();
    assert!(
        recorded
            .iter()
            .any(|(_, e)| e.kind == EntryKind::RuntimeTurn
                && e.phase == Some(RuntimeTurnPhase::Completed)),
        "a consult turn is still audited (Started/Injected/Completed) via the same Sink (§12), \
         got {recorded:?}"
    );
    // Note: unlike §11a we deliberately do NOT assert append-before-send here —
    // the write-ahead ordering is the command-only carve-out (§8.1). This test's
    // value is proving the NON-command path is audited without that constraint.
}
