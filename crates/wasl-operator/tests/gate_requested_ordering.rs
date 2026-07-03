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
//! This test forces the interleave rather than hoping for it (the review's
//! own stress harness only reproduced it 3/20000 trials, one shot per
//! trial): it runs MANY independent (`open_gate` caller, `decide` racer)
//! thread pairs truly concurrently, each racer already hot-spinning on its
//! predicted gate id (`CounterGateIds` is deterministic, so it does not need
//! to learn the id from `open_gate`'s return value) before its paired
//! `open_gate` call even starts. Running many pairs at once — rather than
//! one pair at a time plus idle "noise" threads to manufacture contention —
//! naturally oversubscribes the CPU (forcing the real scheduler preemption
//! the hazard needs: with `PAIRS_PER_BATCH * 2` threads on a machine with
//! far fewer cores, the OS must time-slice) while every thread does
//! productive racing work, not wasted spinning. Empirically this reliably
//! reproduces the pre-fix ordering violation within the first couple of
//! batches (verified by temporarily reverting the `open_gate` fix locally
//! and re-running this file). Against the FIXED `open_gate` (which
//! broadcasts `GateRequested` from inside the SAME critical section that
//! inserts into `pending` — see its doc comment) this is not merely
//! unlikely to fail, it is structurally guaranteed to pass: `decide` cannot
//! acquire the `pending` lock until `open_gate` releases it, and by then the
//! broadcast has already happened.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{at, fresh_hub_with_clock, req_call9, FakeClock, T0_MS};

use wasl_gate::GateMode;
use wasl_operator::{DecideError, DecisionBody, DecisionOutcome};

/// Run one (`open_gate`, racer) pair on a hub of its own and return which
/// frame the `open_gate` caller's subscriber saw first ("gate_requested" iff
/// correct). The racer spins on `decide("gate-1", ..)` from the moment it
/// starts (before `open_gate` is even called), so it is already hot with no
/// thread-wake latency once the hazard window (if any) opens.
fn race_one_pair() -> &'static str {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let req = req_call9();
    let (_last_seq, mut rx) = hub.subscribe();

    let started = Arc::new(AtomicBool::new(false));
    let racer_hub = hub.clone();
    let racer_started = started.clone();
    let racer = std::thread::spawn(move || {
        racer_started.store(true, Ordering::SeqCst);
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

    // Wait for the racer to actually be spinning (not merely spawned — OS
    // thread creation itself can take microseconds) before starting the
    // operation under race.
    while !started.load(Ordering::SeqCst) {
        std::hint::spin_loop();
    }

    let _w = hub.open_gate(GateMode::External, &req);
    racer.join().expect("racer thread panicked");

    let first = rx
        .try_recv()
        .expect("a first frame must have been published");
    let second = rx
        .try_recv()
        .expect("a second frame must have been published");
    assert!(
        rx.try_recv().is_err(),
        "exactly two frames per pair (gate_requested, gate_decided)"
    );
    assert_eq!(second.frame.event_name(), "gate_decided");
    first.frame.event_name()
}

#[test]
fn decide_racing_open_gate_can_never_publish_gate_decided_before_gate_requested() {
    // Run many (open_gate, racer) pairs truly concurrently, in batches: with
    // 2 threads/pair on a machine with far fewer cores than
    // `PAIRS_PER_BATCH * 2`, the OS scheduler is forced to preempt threads
    // mid-flight (unlike a single pair on an otherwise-idle multi-core
    // machine, where both threads simply run to completion in true
    // parallelism with no preemption at all, and the hazard essentially
    // never surfaces). Every thread does productive racing work — no idle
    // "noise" threads are needed to manufacture the contention.
    const PAIRS_PER_BATCH: usize = 128;
    const BATCHES: usize = 8;

    for batch in 0..BATCHES {
        let results: Vec<&'static str> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..PAIRS_PER_BATCH)
                .map(|_| scope.spawn(race_one_pair))
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("pair thread panicked"))
                .collect()
        });

        for (i, first) in results.iter().enumerate() {
            assert_eq!(
                *first, "gate_requested",
                "batch {batch} pair {i}: GateRequested must be published before any decide() \
                 can complete, but this connection saw {first:?} first"
            );
        }
    }
}
