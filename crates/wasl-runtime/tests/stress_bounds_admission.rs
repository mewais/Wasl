//! Phase-2A behavioral suite — concurrency stress (item 12), bounds enforcement
//! (item 13), and target admission control (item 14). §11 / §10.
//!
//! BEHAVIORAL, not type-only. The stress test is Barrier-driven with a bounded,
//! machine-adaptive thread count (available_parallelism clamped 2..=16 — the M3
//! precedent from wasl-operator/tests/gate_requested_ordering.rs; NEVER hundreds
//! of threads, so it is portable on a low-core box). MUST currently fail (no
//! actor yet).
//!
//! Spec citations (docs/specs/runtime.md):
//!   * concurrency hardening — §14 (lines 848-851): a Barrier-driven adversarial
//!     concurrent-submit stress test; concurrent submits never corrupt the
//!     single-writer stream; no lost/duplicated events.
//!   * bounds — §11 (lines 768-784): every retained structure is bounded; a full
//!     FIFO queue over queue_max_len ⇒ RejectedBusy; the journal ring below-floor
//!     ⇒ Truncated (covered in journal_replay.rs).
//!   * admission control — §10 (lines 753-757): max_targets_global /
//!     max_targets_per_sender; exceeding ⇒ RuntimeError::AdmissionRefused ⇒
//!     RejectedUnavailable at submit.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use wasl_runtime::{
    BusyPolicy, DeliveryTarget, EventSeq, ProtocolEvent, RuntimeError, TurnReceipt,
};

use support::{
    assert_ascending_gapfree_unique, consult, drain_items, key, provider, seq_events, sink_arc,
    tight_config, ProtocolScript, RecordingSink, ScriptedProtocol, TestClock,
};

/// Machine-adaptive worker count, clamped small so this is bounded on tiny hosts
/// and never spawns hundreds of tasks (the M3 precedent).
fn worker_count() -> usize {
    thread::available_parallelism()
        .map_or(2, |n| n.get())
        .clamp(2, 16)
}

fn busy_forever_script() -> ProtocolScript {
    ProtocolScript::new()
        .ready(Some("ses-1"))
        .emit(ProtocolEvent::TurnAccepted { native_turn: None })
}

// =====================================================================
// Item 12 — Barrier-driven concurrent-submit stress over many rounds: across
// WORKERS concurrent submitters (each released together by a Barrier), the
// single-writer actor admits EXACTLY ONE active turn and queues/rejects the
// rest; the resulting event stream is always ascending, gap-free, and
// duplicate-free (no lost or duplicated events), on every round.
// =====================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn barrier_driven_concurrent_submits_preserve_single_writer_invariant() {
    use tokio::sync::Barrier;

    let workers = worker_count();
    // Reduced from 40 → 8 rounds (still enough independent Barrier races to
    // catch rare admission orderings) so the whole test finishes well under a
    // minute. Each round keeps the winning turn busy-forever (the strongest form
    // of the single-writer race — losers must genuinely queue behind a live
    // active turn), so the round's terminal `drain_items` necessarily meets its
    // 5 s hang-guard ONCE (the stream of a busy-forever target never ends); at
    // 8 rounds that is ~40 s total, comfortably inside CI/tool limits. The
    // successful queue-pop-and-activate path this variant does NOT drive is
    // covered directly by
    // `shutdown_and_activation::completing_active_turn_auto_activates_next_queued_turn`.
    let rounds = 8;

    for round in 0..rounds {
        let proto = ScriptedProtocol::new();
        let ctx = format!("ctx-stress-{round}");
        proto.install(&ctx, busy_forever_script());
        let clock = TestClock::new();
        let sink = sink_arc(RecordingSink::default());
        // Give the queue enough room for all losers so we test queue integrity,
        // not queue-cap rejection (that is item 13).
        let mut cfg = tight_config();
        cfg.queue_max_len = workers + 2;
        let prov = provider(proto, cfg, clock, sink);
        let target: Arc<dyn DeliveryTarget> = prov.target_for(&key(&ctx)).await.expect("target");

        let barrier = Arc::new(Barrier::new(workers));
        let started = Arc::new(AtomicUsize::new(0));
        let queued = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for i in 0..workers {
            let t = target.clone();
            let b = barrier.clone();
            let s = started.clone();
            let q = queued.clone();
            let ctx = ctx.clone();
            handles.push(tokio::spawn(async move {
                b.wait().await;
                let r = t
                    .submit_turn(
                        consult(&ctx, &format!("r{i}")),
                        BusyPolicy::Queue {
                            max_wait: Duration::from_millis(120_000),
                        },
                    )
                    .await;
                match r {
                    TurnReceipt::Started { .. } => {
                        s.fetch_add(1, Ordering::SeqCst);
                    }
                    TurnReceipt::Queued { .. } => {
                        q.fetch_add(1, Ordering::SeqCst);
                    }
                    other => panic!("round {round}: unexpected receipt {other:?}"),
                }
            }));
        }
        for h in handles {
            h.await.expect("stress submitter task");
        }

        assert_eq!(
            started.load(Ordering::SeqCst),
            1,
            "round {round}: exactly ONE concurrent submit wins the single active slot"
        );
        assert_eq!(
            queued.load(Ordering::SeqCst),
            workers - 1,
            "round {round}: all other concurrent submits queue behind the single writer"
        );

        // The event stream is intact — ascending, gap-free, no duplicates — no
        // matter how the submits raced (no lost/duplicated events, §14). The
        // busy-forever target's stream never ends, so this meets its 5 s
        // hang-guard once per round (bounded rounds keep the total small).
        let stream = target.events(EventSeq(0)).await;
        let items = drain_items(stream, 128).await;
        let pairs = seq_events(&items);
        assert_ascending_gapfree_unique(&pairs);
    }
}

// =====================================================================
// Item 13 — bounds: a FIFO queue at capacity refuses further submits with
// RejectedBusy rather than growing unbounded. §11 (queue_max_len row).
// =====================================================================

#[tokio::test]
async fn queue_at_capacity_rejects_rather_than_growing_unbounded() {
    let proto = ScriptedProtocol::new();
    proto.install("ctx-qcap", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    // tight_config: queue_max_len = 3.
    let cfg = tight_config();
    let cap = cfg.queue_max_len;
    let prov = provider(proto, cfg, clock, sink);
    let target = prov.target_for(&key("ctx-qcap")).await.expect("target");

    // One active turn.
    let active = target
        .submit_turn(consult("ctx-qcap", "active"), BusyPolicy::Reject)
        .await;
    assert!(matches!(active, TurnReceipt::Started { .. }));

    let long = BusyPolicy::Queue {
        max_wait: Duration::from_millis(120_000),
    };
    // Fill the queue exactly to capacity — all Queued.
    for i in 0..cap {
        let r = target
            .submit_turn(consult("ctx-qcap", &format!("q{i}")), long.clone())
            .await;
        assert!(
            matches!(r, TurnReceipt::Queued { .. }),
            "submit {i} within capacity must be Queued, got {r:?}"
        );
    }

    // One MORE past capacity ⇒ RejectedBusy (the queue does not grow past
    // queue_max_len — §11 line 778).
    let overflow = target
        .submit_turn(consult("ctx-qcap", "overflow"), long)
        .await;
    assert!(
        matches!(overflow, TurnReceipt::RejectedBusy { .. }),
        "a submit past queue_max_len ({cap}) ⇒ RejectedBusy, not unbounded growth (§11), got {overflow:?}"
    );
}

// =====================================================================
// Item 14 — target admission control: creating more targets than
// max_targets_global refuses with AdmissionRefused (⇒ the provider surfaces
// RuntimeError::AdmissionRefused; a submit-time refusal is RejectedUnavailable).
// §10 lines 753-757.
//
// NOTE (Phase-2A surface): admission control lives on the PROVIDER
// (target_for), driven by ManagedConfig.max_targets_global. If the Phase-2A
// surface does not yet wire per-sender caps/rate-limits (those are noted as the
// Phase-3 A2A wiring, §10 line 762), this test exercises ONLY the global cap,
// which the core owns. The per-sender/rate-limit checks are Phase-3
// config-wired and are asserted there, not here.
// =====================================================================

#[tokio::test]
async fn exceeding_max_targets_global_is_admission_refused() {
    let proto = ScriptedProtocol::new();
    // Install scripts for more contexts than the global cap allows.
    for i in 0..4 {
        proto.install(&format!("ctx-adm-{i}"), busy_forever_script());
    }
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    // tight_config: max_targets_global = 2.
    let cfg = tight_config();
    let cap = cfg.max_targets_global;
    let prov = provider(proto, cfg, clock, sink);

    // Create up to the cap — all succeed.
    let mut ok_targets = 0usize;
    for i in 0..cap {
        let t = prov.target_for(&key(&format!("ctx-adm-{i}"))).await;
        assert!(
            t.is_ok(),
            "target #{i} within max_targets_global ({cap}) must succeed, got {t:?}"
        );
        ok_targets += 1;
    }
    assert_eq!(ok_targets, cap);

    // One MORE distinct target past the global cap ⇒ AdmissionRefused (§10).
    let refused = prov.target_for(&key(&format!("ctx-adm-{cap}"))).await;
    assert!(
        matches!(refused, Err(RuntimeError::AdmissionRefused { .. })),
        "creating a target past max_targets_global ({cap}) ⇒ RuntimeError::AdmissionRefused \
         (§10 lines 753-757), got {refused:?}"
    );
}

// =====================================================================
// Item 14 (idempotent get-or-create) — target_for is idempotent: the SAME key
// returns the SAME live target and does NOT count against the cap twice (§10
// line 736 "the same key returns the same live managed target").
// =====================================================================

#[tokio::test]
async fn target_for_is_idempotent_for_the_same_key() {
    let proto = ScriptedProtocol::new();
    proto.install("ctx-getcreate", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);

    let a = prov.target_for(&key("ctx-getcreate")).await.expect("first");
    let b = prov
        .target_for(&key("ctx-getcreate"))
        .await
        .expect("second");

    // Same Wasl-side target id (same live managed target), not a second target.
    assert_eq!(
        a.target_id(),
        b.target_id(),
        "target_for(same key) must return the SAME live target (§10 line 736)"
    );

    // And it appears once in the provider's listing.
    let listed = prov.list_targets().await;
    let count = listed.iter().filter(|id| *id == a.target_id()).count();
    assert_eq!(
        count, 1,
        "an idempotently-created target appears exactly once in list_targets(), got {listed:?}"
    );
}
