//! Phase-2A behavioral suite — FIFO queueing + BusyPolicy admission matrix +
//! no-input-interleaving (spec items 1, 2, 3).
//!
//! These drive the single-writer per-target actor against the in-`tests/`
//! scripted `InteractiveTransport` (`support::ScriptedProtocol`, per runtime.md
//! §14 — NOT the R2 fake). They are BEHAVIORAL (not type-only): each asserts a
//! concrete admission outcome, queue position, event ordering, or transport
//! call-count. They MUST currently fail — the actor / `WaslManagedRuntimeProvider
//! ::with_seams` / `ManagedConfig` do not exist yet (frozen surface is
//! type-only). That is the correct Phase-2A red state.
//!
//! Spec citations (docs/specs/runtime.md):
//!   * FIFO order + `Queued{position}` — §5 ordering guarantee 2 (lines 471-487),
//!     §7.1 (`Busy` + `Queue` ⇒ `Queued{pos = Q length}`), receipt §3 (line 233).
//!   * BusyPolicy admission matrix — §4.2 (lines 320-340), §7.1 (lines 574-586):
//!     Queue / Reject / QueueRelated, decided against AUTHORITATIVE internal
//!     state, never against a stale `status()` (§3 line 189, §4.2 line 340).
//!   * No input interleaving — §Single-writer invariant (lines 67-81), §14
//!     (line 844: "concurrent submits never corrupt the single-writer stream").
//!
//! FROZEN-SURFACE GAP (flagged, per support/mod.rs doc): the coder MUST add
//! `ManagedConfig` + `WaslManagedRuntimeProvider::with_seams(protocol, config,
//! clock, sink)` (runtime.md §10/§11/§1/§12). These tests route through
//! `support::provider(..)`; if the coder renames, re-map only there.

mod support;

use std::sync::Arc;

use wasl_adapter::DeliveryKind;
use wasl_runtime::{BusyPolicy, DeliveryTarget, TurnReceipt};

use support::{
    agent_request, consult, drain_items, events_of, key, provider, root_causation, sink_arc,
    tight_config, ProtocolScript, RecordingSink, ScriptedProtocol, TestClock,
};
use wasl_runtime::{EventSeq, TurnEvent};

use std::time::Duration;

/// A busy target: the first (active) turn is `TurnAccepted` but never completes
/// within the test window (no `TurnCompleted` scripted), so subsequent submits
/// find the target genuinely `Busy` — the authoritative internal state, not a
/// polled `status()`.
fn busy_forever_script() -> ProtocolScript {
    ProtocolScript::new()
        .ready(Some("ses-1"))
        .emit(wasl_runtime::ProtocolEvent::TurnAccepted { native_turn: None })
}

// =====================================================================
// Item 1 — FIFO queueing: order preserved, correct Queued{position}.
// =====================================================================

#[tokio::test]
async fn queued_turns_get_ascending_positions_starting_at_zero() {
    let proto = ScriptedProtocol::new();
    proto.install("ctx-fifo", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);

    let target = prov.target_for(&key("ctx-fifo")).await.expect("target");

    // First submit finds the target Idle → Started (becomes the active turn).
    let r0 = target
        .submit_turn(consult("ctx-fifo", "active"), BusyPolicy::Reject)
        .await;
    assert!(
        matches!(r0, TurnReceipt::Started { .. }),
        "first submit on an idle target must be Started, got {r0:?}"
    );

    // The next three, while busy with Queue policy, take FIFO positions 0,1,2.
    let long_wait = BusyPolicy::Queue {
        max_wait: Duration::from_millis(120_000),
    };
    let mut positions = Vec::new();
    for i in 0..3 {
        let r = target
            .submit_turn(consult("ctx-fifo", &format!("q{i}")), long_wait.clone())
            .await;
        match r {
            TurnReceipt::Queued { position, .. } => positions.push(position),
            other => panic!("expected Queued for submit {i}, got {other:?}"),
        }
    }
    assert_eq!(
        positions,
        vec![0, 1, 2],
        "queue positions must be FIFO 0-based (0 = next after active), §3 line 233 / §7.1"
    );
}

#[tokio::test]
async fn queued_turn_ids_are_distinct_and_order_is_preserved_on_the_stream() {
    let proto = ScriptedProtocol::new();
    proto.install("ctx-fifo2", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov.target_for(&key("ctx-fifo2")).await.expect("target");

    let _active = target
        .submit_turn(consult("ctx-fifo2", "active"), BusyPolicy::Reject)
        .await;

    let long_wait = BusyPolicy::Queue {
        max_wait: Duration::from_millis(120_000),
    };
    let a = target
        .submit_turn(consult("ctx-fifo2", "first"), long_wait.clone())
        .await;
    let b = target
        .submit_turn(consult("ctx-fifo2", "second"), long_wait.clone())
        .await;

    let (ta, pa) = match a {
        TurnReceipt::Queued { turn_id, position } => (turn_id, position),
        other => panic!("expected Queued, got {other:?}"),
    };
    let (tb, pb) = match b {
        TurnReceipt::Queued { turn_id, position } => (turn_id, position),
        other => panic!("expected Queued, got {other:?}"),
    };
    assert_ne!(ta, tb, "each queued turn gets a distinct TurnId");
    assert!(
        pa < pb,
        "earlier submit gets earlier FIFO position: {pa} < {pb}"
    );

    // The public stream shows TurnQueued for both, in submission order, at the
    // positions the receipts reported (§5 ordering guarantee 2 — TurnQueued at
    // admission).
    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 16).await;
    let evs = events_of(&items);
    let queued: Vec<(&wasl_runtime::TurnId, usize)> = evs
        .iter()
        .filter_map(|e| match e {
            TurnEvent::TurnQueued { turn_id, position } => Some((turn_id, *position)),
            _ => None,
        })
        .collect();
    assert_eq!(
        queued,
        vec![(&ta, pa), (&tb, pb)],
        "TurnQueued events must mirror the receipts in FIFO order"
    );
}

// =====================================================================
// Item 2 — BusyPolicy admission matrix (Queue / Reject / QueueRelated).
// =====================================================================

#[tokio::test]
async fn reject_policy_refuses_a_busy_target_with_rejectedbusy() {
    let proto = ScriptedProtocol::new();
    proto.install("ctx-rej", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov.target_for(&key("ctx-rej")).await.expect("target");

    let active = target
        .submit_turn(consult("ctx-rej", "active"), BusyPolicy::Reject)
        .await;
    let active_turn = match active {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    let r = target
        .submit_turn(consult("ctx-rej", "rejected"), BusyPolicy::Reject)
        .await;
    assert_eq!(
        r,
        TurnReceipt::RejectedBusy {
            active_turn: active_turn.clone()
        },
        "Reject on a busy target ⇒ RejectedBusy{{active}} (§7.1), not RejectedUnavailable"
    );
}

#[tokio::test]
async fn queue_policy_queues_a_busy_target() {
    let proto = ScriptedProtocol::new();
    proto.install("ctx-q", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov.target_for(&key("ctx-q")).await.expect("target");

    let _active = target
        .submit_turn(consult("ctx-q", "active"), BusyPolicy::Reject)
        .await;

    let r = target
        .submit_turn(
            consult("ctx-q", "queued"),
            BusyPolicy::Queue {
                max_wait: Duration::from_millis(30_000),
            },
        )
        .await;
    assert!(
        matches!(r, TurnReceipt::Queued { position: 0, .. }),
        "Queue on a busy target ⇒ Queued{{position:0}}, got {r:?}"
    );
}

#[tokio::test]
async fn queue_related_queues_only_a_shared_root_else_rejects() {
    let proto = ScriptedProtocol::new();
    proto.install("ctx-qr", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target = prov.target_for(&key("ctx-qr")).await.expect("target");

    // Active turn carries causation root "root-A".
    let active = target
        .submit_turn(
            agent_request(
                "ctx-qr",
                DeliveryKind::Consult,
                "active",
                None,
                root_causation("root-A"),
            ),
            BusyPolicy::Reject,
        )
        .await;
    let active_turn = match active {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    let qr = BusyPolicy::QueueRelated {
        max_wait: Duration::from_millis(120_000),
    };

    // A RELATED turn (same root "root-A") ⇒ Queued.
    let related = target
        .submit_turn(
            agent_request(
                "ctx-qr",
                DeliveryKind::Consult,
                "related",
                None,
                root_causation("root-A"),
            ),
            qr.clone(),
        )
        .await;
    assert!(
        matches!(related, TurnReceipt::Queued { .. }),
        "QueueRelated with a SHARED causation root ⇒ Queued (§4.2/§7.1), got {related:?}"
    );

    // An UNRELATED turn (root "root-B") ⇒ RejectedBusy.
    let unrelated = target
        .submit_turn(
            agent_request(
                "ctx-qr",
                DeliveryKind::Consult,
                "unrelated",
                None,
                root_causation("root-B"),
            ),
            qr,
        )
        .await;
    assert_eq!(
        unrelated,
        TurnReceipt::RejectedBusy { active_turn },
        "QueueRelated with a DIFFERENT root ⇒ RejectedBusy (§4.2 line 329/§7.1)"
    );
}

// =====================================================================
// Item 2 (race) — admission is decided INSIDE the actor against authoritative
// internal state, not against a possibly-stale status(). Two concurrent
// submits (Barrier-synchronised) onto an idle target: EXACTLY ONE wins Started;
// the other, seeing the now-busy authoritative state, is Queued/Rejected per
// its policy — NEVER two Started, never a spurious RejectedUnavailable.
// =====================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_submit_admission_never_double_starts() {
    use tokio::sync::Barrier;

    let proto = ScriptedProtocol::new();
    proto.install("ctx-race", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov.target_for(&key("ctx-race")).await.expect("target");

    // Two racers hit `submit_turn` at the same instant, released by a Barrier
    // (deterministic rendezvous, NOT a sleep). Both use Queue so a loser is
    // Queued rather than Rejected — the point is exactly-one-Started.
    let barrier = Arc::new(Barrier::new(2));
    let long_wait = BusyPolicy::Queue {
        max_wait: Duration::from_millis(120_000),
    };

    let mut handles = Vec::new();
    for i in 0..2 {
        let t = target.clone();
        let b = barrier.clone();
        let policy = long_wait.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            t.submit_turn(consult("ctx-race", &format!("racer{i}")), policy)
                .await
        }));
    }
    let mut receipts = Vec::new();
    for h in handles {
        receipts.push(h.await.expect("racer task"));
    }

    let started = receipts
        .iter()
        .filter(|r| matches!(r, TurnReceipt::Started { .. }))
        .count();
    let queued = receipts
        .iter()
        .filter(|r| matches!(r, TurnReceipt::Queued { .. }))
        .count();
    assert_eq!(
        started, 1,
        "exactly ONE concurrent submit may win Started (single-writer admission), got {receipts:?}"
    );
    assert_eq!(
        queued, 1,
        "the loser must be Queued against the now-busy authoritative state, got {receipts:?}"
    );
    // No spurious unavailable on an otherwise-live target (§4.6 back-pressure).
    assert!(
        !receipts
            .iter()
            .any(|r| matches!(r, TurnReceipt::RejectedUnavailable { .. })),
        "an idle-then-busy target must never spuriously RejectedUnavailable a racer: {receipts:?}"
    );
}

// =====================================================================
// Item 3 — No input interleaving under concurrent submit_turn: every submit
// funnels through the ONE actor, so no two prompts corrupt the single-writer
// stream. With N concurrent submits (1 active + N-1 queued) the transport sees
// send_turn calls that are serialized and each carries exactly one prompt
// (never a spliced/interleaved buffer), and the accepted turn's TurnAccepted
// precedes every queued turn's TurnQueued-then-activation on the stream.
// =====================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_submits_do_not_interleave_the_single_writer_stream() {
    use tokio::sync::Barrier;

    let proto = ScriptedProtocol::new();
    // Active turn accepts but does not complete, so the others queue. Capture
    // the ScriptHandle so we can inspect the transport's send_turn call log.
    let handle = proto.install("ctx-nointer", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-nointer")).await.expect("target");

    let n = 3usize; // 1 active + 2 queued (under queue_max_len=3 in tight_config)
    let barrier = Arc::new(Barrier::new(n));
    let long_wait = BusyPolicy::Queue {
        max_wait: Duration::from_millis(120_000),
    };
    let mut handles = Vec::new();
    for i in 0..n {
        let t = target.clone();
        let b = barrier.clone();
        let policy = long_wait.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            t.submit_turn(consult("ctx-nointer", &format!("p{i}")), policy)
                .await
        }));
    }
    let mut receipts = Vec::new();
    for h in handles {
        receipts.push(h.await.expect("submit task"));
    }

    // Exactly one Started; the rest Queued (no interleaving corrupted admission).
    let started = receipts
        .iter()
        .filter(|r| matches!(r, TurnReceipt::Started { .. }))
        .count();
    assert_eq!(started, 1, "single-writer admits exactly one active turn");
    assert_eq!(
        receipts.len() - started,
        n - 1,
        "all others queued behind the single active turn"
    );

    // Only the ACTIVE turn was injected — the queued ones have not been sent to
    // the transport yet, so send_turn happened at most once so far (single
    // physical writer, no interleaved raw writes). §Single-writer invariant.
    assert!(
        handle.shared.send_turn_count() <= 1,
        "at most the active turn is injected while others queue; the single writer never \
         issues an interleaved second send_turn, got {} send_turns",
        handle.shared.send_turn_count()
    );
}
