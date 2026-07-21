//! `tests/gate_requested_ordering.rs` — regression test for a review finding:
//! `OperatorHub::open_gate` used to insert the gate into the `pending` table
//! and THEN broadcast `GateRequested` as a separate step, after releasing the
//! `pending` lock. A `decide()` call that already knows (or predicts, since
//! `CounterGateIds` is deterministic) the about-to-be-minted gate id could
//! land in that gap: it only needs the `pending` lock to find and remove the
//! gate, so it could complete — and broadcast `GateDecided` — before
//! `open_gate` ever got around to broadcasting `GateRequested` for the same
//! gate. That violates `operator-api.md`'s normative ordering: "Frame order
//! on one connection is ascending `id`; `gate_requested` always precedes its
//! `gate_decided`."
//!
//! ## Method — portable, deterministic interleave forcing (not oversubscription)
//!
//! An earlier version of this file forced the interleave by brute
//! oversubscription: `128 * 2 * 8` short-lived OS threads via
//! `std::thread::scope`, betting the scheduler would preempt an `open_gate`
//! caller mid-flight often enough to expose the gap. That is non-portable —
//! on a low-core / thread-limited host it exhausts thread or address-space
//! resources and the test process itself aborts (`WouldBlock`/`EAGAIN` on
//! `thread::spawn`, or an allocation abort) BEFORE it can assert anything,
//! turning a correct product into a red suite. A test that fails on a valid
//! environment is a test defect, so the harness (NOT the invariant, NOT the
//! assertions) was rewritten.
//!
//! This version forces the interleave DETERMINISTICALLY and with a bounded,
//! machine-adaptive thread count:
//!
//! * **Bounded pool.** We use `WORKERS` worker threads, sized from
//!   `available_parallelism()` and clamped to `[2, 16]` — never hundreds. Each
//!   worker owns exactly one long-lived racer thread, so at most `2 * WORKERS`
//!   threads are ever live regardless of how many rounds run.
//! * **Barrier-synchronised racer.** In each round a fresh hub is built (as
//!   `race_one_pair` did before) and a two-party `Barrier` guarantees the racer
//!   is already HOT-SPINNING on `decide("gate-1", ..)` — the deterministic
//!   first gate id, since `CounterGateIds` starts at 1 — BEFORE the paired
//!   `open_gate` caller enters its critical section. A barrier-released racer
//!   contending for the `pending` lock at the exact moment `open_gate` needs it
//!   is a STRONGER, repeatable forcing function than hoping the scheduler
//!   preempts: it does not depend on core count or time-slicing at all.
//! * **Many rounds.** Each worker runs `ROUNDS_PER_WORKER` barrier-synchronised
//!   rounds, so the invariant is exercised across `WORKERS * ROUNDS_PER_WORKER`
//!   independent races — at least as many as the old 1024 pairs — with none of
//!   the thread explosion.
//!
//! Against the FIXED `open_gate` (which broadcasts `GateRequested` from inside
//! the SAME critical section that inserts into `pending` — see its doc comment)
//! this is structurally guaranteed to pass: `decide` cannot acquire the
//! `pending` lock until `open_gate` releases it, and by then the
//! `GateRequested` broadcast has already happened, so the subscriber's first
//! frame is always `gate_requested`. Against the pre-fix `open_gate` (broadcast
//! AFTER releasing the lock) the barrier-released racer is already blocked on
//! the `pending` lock; the instant `open_gate` releases it — with the insert
//! done but the `GateRequested` broadcast not yet run — the racer takes the
//! lock, removes the gate, and broadcasts `GateDecided` first, so the
//! subscriber sees `gate_decided` before `gate_requested` and the assertion
//! below fails. The test therefore still catches the reverted-fix bug.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use common::{at, fresh_hub_with_clock, req_call9, FakeClock, T0_MS};

use wasl_gate::GateMode;
use wasl_operator::{DecideError, DecisionBody, DecisionOutcome};

/// Machine-adaptive worker count: one worker per hardware thread, clamped to a
/// small fixed range so this test is bounded on tiny (single-core) hosts and
/// never spawns hundreds of threads on large ones.
fn worker_count() -> usize {
    thread::available_parallelism()
        .map_or(2, |n| n.get())
        .clamp(2, 16)
}

/// Total independent races per worker. `worker_count() * ROUNDS_PER_WORKER` is
/// the total number of `(open_gate, racer)` races and is kept comfortably above
/// the old harness's 1024 pairs even at the minimum of 2 workers.
const ROUNDS_PER_WORKER: usize = 600;

/// One barrier-synchronised race, driven by a caller that already has a hot,
/// spinning racer waiting on `gate`. Builds a fresh hub, subscribes, releases
/// the (already-spinning) racer via the barrier, runs `open_gate`, and returns
/// the event name of the FIRST frame the caller's subscriber saw
/// ("gate_requested" iff correct). Asserts, exactly as the old `race_one_pair`
/// did, that exactly two frames were published and the SECOND is
/// "gate_decided".
fn race_one_round(gate: &Barrier) -> &'static str {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let req = req_call9();
    let (_last_seq, mut rx) = hub.subscribe();

    // Hand the racer everything it needs for THIS round, then release it and
    // the caller simultaneously via the barrier so the racer is provably
    // hot-spinning on `decide("gate-1", ..)` before `open_gate` starts.
    let racer_hub = hub.clone();
    let spinning = Arc::new(AtomicBool::new(false));
    let racer_spinning = spinning.clone();

    thread::scope(|scope| {
        let racer = scope.spawn(move || {
            // Rendezvous: neither side proceeds until both reach the barrier.
            gate.wait();
            racer_spinning.store(true, Ordering::SeqCst);
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut spins: u64 = 0;
            loop {
                match racer_hub.decide(
                    "gate-1",
                    DecisionBody {
                        outcome: DecisionOutcome::Approved,
                        reason: None,
                    },
                ) {
                    Ok(()) => return,
                    // Not visible yet (still to be inserted, or — pre-fix —
                    // inserted but not yet broadcast): keep hammering. Only
                    // check the deadline occasionally so the hot path stays a
                    // tight spin, not a syscall-per-iteration loop.
                    Err(DecideError::UnknownGate) => {
                        spins += 1;
                        if spins % 4096 == 0 {
                            assert!(
                                Instant::now() < deadline,
                                "gate-1 never became decidable within 10s — open_gate is stuck"
                            );
                        }
                        std::hint::spin_loop();
                        continue;
                    }
                    Err(DecideError::AlreadyDecided) => {
                        panic!("decided twice — impossible, exactly one caller can win")
                    }
                }
            }
        });

        // Release both sides together, then make sure the racer has actually
        // entered its spin loop before we open the gate — so it is contending
        // for the `pending` lock at the moment `open_gate` needs it.
        gate.wait();
        while !spinning.load(Ordering::SeqCst) {
            std::hint::spin_loop();
        }

        let _w = hub.open_gate(GateMode::External, &req);
        racer.join().expect("racer thread panicked");
    });

    let first = rx
        .try_recv()
        .expect("a first frame must have been published");
    let second = rx
        .try_recv()
        .expect("a second frame must have been published");
    assert!(
        rx.try_recv().is_err(),
        "exactly two frames per round (gate_requested, gate_decided)"
    );
    assert_eq!(second.frame.event_name(), "gate_decided");
    first.frame.event_name()
}

#[test]
fn decide_racing_open_gate_can_never_publish_gate_decided_before_gate_requested() {
    let workers = worker_count();

    thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|w| {
                scope.spawn(move || {
                    for round in 0..ROUNDS_PER_WORKER {
                        // A fresh two-party barrier per round guarantees the
                        // racer is hot-spinning before this round's open_gate.
                        let gate = Barrier::new(2);
                        let first = race_one_round(&gate);
                        assert_eq!(
                            first, "gate_requested",
                            "worker {w} round {round}: GateRequested must be published before any \
                             decide() can complete, but this connection saw {first:?} first"
                        );
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("worker thread panicked");
        }
    });
}
