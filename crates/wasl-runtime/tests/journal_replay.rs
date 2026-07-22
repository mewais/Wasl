//! Phase-2A behavioral suite — event journal + replay + atomic replay-to-live
//! handoff, multi-observer independent cursors, attach/detach mid-stream, and
//! the journal-floor `Truncated` recovery (spec items 4, 5).
//!
//! BEHAVIORAL, not type-only: each asserts concrete exactly-once/no-gap/no-dup
//! sequencing across the tail→live boundary, independent cursors, and the
//! below-floor truncation control item. MUST currently fail (no actor yet).
//!
//! Spec citations (docs/specs/runtime.md):
//!   * Journal ring + snapshot + atomic replay-to-live handoff — §4.3
//!     (lines 342-380): subscribe live receiver FIRST (step 2) THEN copy the
//!     retained tail (step 3), yield tail then live, dedupe boundary by dropping
//!     any live item whose seq <= last_tail_seq (step 5). No gap, no dup.
//!   * `events(after_seq)` exactly-once/ascending/gap-free — §5 guarantee 1
//!     (line 473); multi-observer independent cursors — §5 guarantee 5 (line 490).
//!   * Below-floor ⇒ `StreamItem::Truncated{earliest, latest}` then STOP — §3
//!     (lines 216-223), §4.3 (line 352, 370-376), §5 guarantee 4 (line 489).
//!   * Attach/detach: multiple observers hold INDEPENDENT cursors, dropping one
//!     subscriber does not disturb another — §3 (line 200), §5 guarantee 5.

mod support;

use std::sync::Arc;
use std::time::Duration;

use wasl_runtime::{BusyPolicy, DeliveryTarget, EventSeq, ProtocolEvent, StreamItem, TurnEvent};

use support::{
    assert_ascending_gapfree_unique, consult, drain_items, events_of, key, provider, seq_events,
    sink_arc, tight_config, ProtocolScript, RecordingSink, ScriptedProtocol, TestClock,
};

/// A config with a TINY journal ring so the floor is easy to overrun for the
/// truncation test, but otherwise permissive.
fn small_journal_config() -> wasl_runtime::ManagedConfig {
    wasl_runtime::ManagedConfig {
        journal_capacity: 4,
        ..wasl_runtime::ManagedConfig::default()
    }
}

// =====================================================================
// Item 4a — replay from EventSeq(0) yields every journalled event once, in
// ascending gap-free order, ending with the terminal event.
// =====================================================================

#[tokio::test]
async fn replay_from_zero_is_exactly_once_ascending_gapfree() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-replay",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .happy_turn("hello ", "hello world"),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov.target_for(&key("ctx-replay")).await.expect("target");

    let _r = target
        .submit_turn(consult("ctx-replay", "hi"), BusyPolicy::Reject)
        .await;

    // Subscribe from the very beginning; drain the whole turn's public events.
    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 16).await;
    let pairs = seq_events(&items);
    assert!(!pairs.is_empty(), "the turn must have journalled events");
    assert_ascending_gapfree_unique(&pairs);

    // The per-target sequence starts at 1 (§1) and the terminal event is present.
    assert_eq!(
        pairs[0].0,
        EventSeq(1),
        "per-target EventSeq starts at 1 (§1)"
    );
    let evs = events_of(&items);
    assert!(
        evs.iter()
            .any(|e| matches!(e, TurnEvent::TurnCompleted { .. })),
        "replay must include the turn's terminal TurnCompleted event"
    );
}

// =====================================================================
// Item 4b — multi-observer independent cursors: two subscribers at DIFFERENT
// after_seq each get their own gap-free view; neither disturbs the other.
// =====================================================================

#[tokio::test]
async fn multi_observer_independent_cursors_each_gap_free() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-multi",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .happy_turn("a", "reply-a"),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov.target_for(&key("ctx-multi")).await.expect("target");
    let _r = target
        .submit_turn(consult("ctx-multi", "hi"), BusyPolicy::Reject)
        .await;

    // Observer A from the start.
    let stream_a = target.events(EventSeq(0)).await;
    let items_a = drain_items(stream_a, 16).await;
    let pairs_a = seq_events(&items_a);
    assert_ascending_gapfree_unique(&pairs_a);
    assert!(pairs_a.len() >= 2, "observer A sees the full turn");

    // Observer B resuming AFTER the first event (after_seq = 1) sees a strict
    // suffix — no first event, still gap-free, and independent of A's cursor.
    let stream_b = target.events(EventSeq(1)).await;
    let items_b = drain_items(stream_b, 16).await;
    let pairs_b = seq_events(&items_b);
    assert_ascending_gapfree_unique(&pairs_b);
    assert!(
        pairs_b.iter().all(|(s, _)| s.0 > 1),
        "observer B (after_seq=1) must see only seq>1, got {pairs_b:?}"
    );
    // B is a proper suffix of A (same events at the same seqs) — identical
    // (seq,event) pairs, independent cursors (§5 guarantee 5).
    let a_suffix: Vec<_> = pairs_a.iter().filter(|(s, _)| s.0 > 1).cloned().collect();
    assert_eq!(
        a_suffix, pairs_b,
        "each observer gets identical (seq,event) pairs for overlapping seqs (§5 g5)"
    );
}

// =====================================================================
// Item 4c — atomic tail→live handoff under a FORCED race: a subscriber begins
// subscribing at the exact moment a new event is about to be published (a Gated
// script step held until the test releases it). Per §4.3 the live receiver is
// subscribed BEFORE the tail copy, and the boundary is deduped by dropping any
// live item whose seq <= last_tail_seq — so the seam has NEITHER a gap NOR a
// duplicate regardless of which side of the copy the event lands on.
// =====================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tail_to_live_handoff_has_no_gap_or_dup_under_forced_race() {
    let proto = ScriptedProtocol::new();
    // Script: ready, accept, one delta already journalled, THEN a gated delta
    // ("boundary") the test releases to land right at the subscribe seam, then
    // completion. Capture the ScriptHandle to release the gate.
    let handle = proto.install(
        "ctx-seam",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .emit(ProtocolEvent::AssistantDelta { text: "pre".into() })
            .gated(
                "boundary",
                ProtocolEvent::AssistantDelta {
                    text: "boundary".into(),
                },
            )
            .emit(ProtocolEvent::TurnCompleted {
                reply: "done".into(),
            }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov.target_for(&key("ctx-seam")).await.expect("target");
    let _r = target
        .submit_turn(consult("ctx-seam", "hi"), BusyPolicy::Reject)
        .await;

    // Subscribe from the start, then release the boundary event so it is
    // published concurrently with the subscriber draining the tail.
    let stream = target.events(EventSeq(0)).await;
    handle.release("boundary");
    let items = drain_items(stream, 32).await;

    let pairs = seq_events(&items);
    // The core guarantee: no gap, no duplicate across the whole stream,
    // INCLUDING the boundary event that raced the subscribe (§4.3 step 5).
    assert_ascending_gapfree_unique(&pairs);
    let texts: Vec<String> = events_of(&items)
        .into_iter()
        .filter_map(|e| match e {
            TurnEvent::AssistantDelta { text, .. } => Some(text),
            _ => None,
        })
        .collect();
    assert_eq!(
        texts.iter().filter(|t| *t == "boundary").count(),
        1,
        "the boundary event that raced the subscribe must appear EXACTLY once, not zero (gap) \
         and not twice (dup), across the tail→live seam (§4.3 step 5); got {texts:?}"
    );
}

// =====================================================================
// Item 4d — journal-floor Truncated: a subscriber whose after_seq is BELOW the
// retained ring floor gets a single StreamItem::Truncated{earliest, latest} and
// the stream STOPS (client must re-snapshot). §4.3 line 352 / §5 guarantee 4.
// =====================================================================

#[tokio::test]
async fn below_floor_subscription_yields_truncated_and_stops() {
    let proto = ScriptedProtocol::new();
    // Emit MORE events than journal_capacity (4) so the earliest seqs fall off
    // the ring: two happy turns produce well over 4 events.
    proto.install(
        "ctx-trunc",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .happy_turn("a", "r1")
            .happy_turn("b", "r2")
            .happy_turn("c", "r3"),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, small_journal_config(), clock, sink);
    let target = prov.target_for(&key("ctx-trunc")).await.expect("target");

    // Three sequential turns (Reject policy; each completes before the next).
    for i in 0..3 {
        let r = target
            .submit_turn(consult("ctx-trunc", &format!("t{i}")), BusyPolicy::Reject)
            .await;
        // Give the actor room to drain each turn's events before the next submit
        // by draining the stream from the last seq — deterministic, script-driven.
        let _ = r;
        let s = target.events(EventSeq(0)).await;
        let _ = drain_items(s, 32).await;
    }

    // Now subscribe from seq 1 — which has almost certainly fallen below the
    // 4-event ring floor after 3 turns. Expect a Truncated control item.
    let stream = target.events(EventSeq(1)).await;
    let items = drain_items(stream, 8).await;
    let truncated = items
        .iter()
        .find(|it| matches!(it, StreamItem::Truncated { .. }));
    match truncated {
        Some(StreamItem::Truncated { earliest, latest }) => {
            assert!(
                earliest <= latest,
                "Truncated window must be well-formed: earliest {earliest:?} <= latest {latest:?}"
            );
            assert!(
                earliest.0 > 1,
                "if seq 1 fell off the floor, earliest retained must be > 1, got {earliest:?}"
            );
        }
        _ => panic!(
            "below-floor after_seq must yield StreamItem::Truncated (§4.3 line 352), got {items:?}"
        ),
    }
}

// =====================================================================
// Item 5 — attach/detach mid-stream, no cross-subscriber interference. One
// observer attaches, drains part of a turn, then DROPS its stream; a second
// observer attaching afterward still sees a complete, gap-free view. Dropping a
// subscriber must not corrupt the journal or another cursor.
// =====================================================================

#[tokio::test]
async fn detach_of_one_subscriber_does_not_disturb_another() {
    let proto = ScriptedProtocol::new();
    let handle = proto.install(
        "ctx-attach",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .emit(ProtocolEvent::AssistantDelta { text: "one".into() }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov.target_for(&key("ctx-attach")).await.expect("target");
    let _r = target
        .submit_turn(consult("ctx-attach", "hi"), BusyPolicy::Reject)
        .await;

    // Observer 1 attaches and drains what's available, then is DROPPED.
    {
        let s1 = target.events(EventSeq(0)).await;
        let first = drain_items(s1, 4).await;
        assert!(
            !seq_events(&first).is_empty(),
            "observer 1 sees the events emitted so far"
        );
        // s1 drops here → detach.
    }

    // More events arrive after the detach.
    handle.push_event(ProtocolEvent::AssistantDelta { text: "two".into() });
    handle.push_event(ProtocolEvent::TurnCompleted {
        reply: "final".into(),
    });

    // Observer 2 attaches from the start and must see a COMPLETE, gap-free view
    // — the earlier detach did not corrupt the journal or the sequence.
    let s2 = target.events(EventSeq(0)).await;
    let all = drain_items(s2, 32).await;
    let pairs = seq_events(&all);
    assert_ascending_gapfree_unique(&pairs);
    let evs = events_of(&all);
    let deltas: Vec<String> = evs
        .iter()
        .filter_map(|e| match e {
            TurnEvent::AssistantDelta { text, .. } => Some(text.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        deltas,
        vec!["one".to_string(), "two".to_string()],
        "observer 2 after a detach sees both deltas in order (no cross-subscriber interference)"
    );
    assert!(
        evs.iter()
            .any(|e| matches!(e, TurnEvent::TurnCompleted { .. })),
        "observer 2 sees the terminal event"
    );
}

// Keep the Duration import meaningful (used by max_wait durations elsewhere in
// the suite; referenced here so a reader sees the deterministic-clock contract).
#[allow(dead_code)]
fn _durations_are_deterministic() -> Duration {
    Duration::from_millis(0)
}
