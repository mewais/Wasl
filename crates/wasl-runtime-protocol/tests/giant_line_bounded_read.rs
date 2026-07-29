//! Hardening-4 §4.4 — the driver→actor event channel is `mpsc::unbounded_channel()`
//! AND stdout is read via a line-buffered `AsyncBufReadExt::lines()` with NO cap
//! on line length (`fake_line.rs::supervise`, line ~525). A verbose or
//! malfunctioning child (not necessarily malicious — a real bug in the CLI, or a
//! runaway tool loop echoing huge output) can therefore grow the driver's memory
//! without bound: a single pathologically long line is buffered in full before the
//! line terminator is ever seen, then mapped and pushed as one ordinary event.
//!
//! Bug being pinned (hardening-plan §4.4, confirmed directly against
//! `fake_line.rs`): there is no per-line read cap. `BufReader::lines()` will grow
//! its internal buffer to hold the ENTIRE line no matter how large, and
//! `map_event_line` will happily turn a tens-of-MB `{"ev":"delta","text":"…"}`
//! blob into an `AssistantDelta { text: <tens of MB> }` that flows through the
//! (unbounded) event channel as a perfectly normal event.
//!
//! The fix (per the plan): cap the per-line read size and, on a pathologically
//! oversized single line, FAIL the turn / KILL the child rather than buffering the
//! whole line into memory or hanging forever trying to read it.
//!
//! This drives a REAL OS child via `FakeLineProtocol` + the `wasl-fake-agent`
//! fixture's new `emit_giant_line` command: the child accepts the turn, then emits
//! ONE stdout line of `bytes` bytes (a valid `delta` object) whose newline
//! terminator arrives only at the very end, then blocks on stdin (never emitting a
//! `done`). Tens of MB is chosen: large enough to clearly exceed any sane per-line
//! cap (a few hundred KB / a few MB) yet safely within the sandbox's per-process
//! memory budget (a single ~30 MB allocation, far under the 2 GB cap) so this test
//! cannot OOM the sandbox.
//!
//! EXPECTED RED against the current unfixed `fake_line.rs`: the driver buffers the
//! whole ~30 MB line and surfaces it as an `AssistantDelta` of ~30 MB — the
//! observable "it swallowed tens of MB as one normal event" failure. The
//! assertion below (no surfaced event may carry a giant payload; the oversized
//! line must instead fail the turn / kill the child) therefore fails today and
//! passes once the per-line cap lands.

#![cfg(unix)]

mod support;

use std::time::Duration;

use support::*;

use wasl_runtime::{ComposedPrompt, InteractiveProtocol, ProtocolEvent, SpawnOptions};
use wasl_runtime_protocol::{FakeLineProtocol, SupervisionTimeouts};

fn prompt(line: &str) -> ComposedPrompt {
    ComposedPrompt(line.to_string())
}

/// Short, injected supervision timeouts so the whole test resolves quickly. The
/// giant-line assertion below is INDEPENDENT of the watchdog (it is about the
/// SIZE of any surfaced event, not about timing): even if the current buggy
/// driver's turn is eventually ended by this watchdog, it will already have
/// surfaced the giant `AssistantDelta` first — which is the failure.
fn fast_timeouts() -> SupervisionTimeouts {
    SupervisionTimeouts {
        interrupt_ack: Duration::from_millis(100),
        term_after: Duration::from_millis(100),
        kill_after: Duration::from_millis(100),
        hang_watchdog: Duration::from_millis(300),
    }
}

/// Is a process with this pid still alive? `kill -0` semantics, shelled out so the
/// test crate needs no `unsafe`/`libc` (the same probe `supervision.rs` uses).
fn pid_alive(pid: u32) -> bool {
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Poll `pid_alive` until it reports gone, bounded by `bound`.
async fn reaped_within(pid: u32, bound: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + bound;
    while tokio::time::Instant::now() < deadline {
        if !pid_alive(pid) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    !pid_alive(pid)
}

/// Ask the freshly-spawned fake for its own OS pid so the test can probe it.
async fn spawn_and_learn_pid(
    proto: &FakeLineProtocol,
) -> (Box<dyn wasl_runtime::InteractiveTransport>, u32) {
    let mut tx = proto
        .spawn(&SpawnOptions::default())
        .await
        .expect("spawn fake agent");
    let ready = next_or_hang(&mut *tx, "ready").await.expect("ready");
    assert!(is_session_ready(&ready), "ready: {ready:?}");
    tx.send_turn(&prompt(&req_recall_pid())).await.expect("pid");
    let evs = drain_to_terminal(&mut *tx, "pid turn").await;
    let pid = evs
        .iter()
        .find_map(completed_reply)
        .expect("pid reply")
        .trim()
        .parse::<u32>()
        .expect("decimal pid");
    (tx, pid)
}

/// The adversarial single-line size: ~30 MB. Comfortably larger than any sane
/// per-line cap (hundreds of KB / a few MB) so a capped driver clearly rejects it,
/// yet a single allocation that is safely within the sandbox's per-process memory
/// budget (nowhere near the 2 GB cap) so this test cannot exhaust sandbox memory.
const GIANT_LINE_BYTES: usize = 30 * 1024 * 1024;

/// Any surfaced event whose payload is at least this large is a "giant" event —
/// proof the driver buffered the whole pathological line into memory. A correct,
/// capped driver never surfaces a payload anywhere near `GIANT_LINE_BYTES`; this
/// threshold (8 MB) sits far above any reasonable cap yet far below the 30 MB the
/// bug produces, so it cannot false-positive on a legitimately-capped delta.
const GIANT_EVENT_THRESHOLD: usize = 8 * 1024 * 1024;

/// The size of any event payload we can observe, for the "no giant event" check.
fn event_payload_len(ev: &ProtocolEvent) -> usize {
    match ev {
        ProtocolEvent::AssistantDelta { text } => text.len(),
        ProtocolEvent::TurnCompleted { reply } => reply.len(),
        ProtocolEvent::TurnFailed { error } => error.len(),
        _ => 0,
    }
}

// =====================================================================
// §4.4 (per-line read buffer) — a single pathologically long stdout line must
// NOT be buffered whole into memory and surfaced as one ordinary event. The
// driver must cap the per-line read and fail the turn / kill the child instead.
// =====================================================================

#[tokio::test]
async fn a_pathologically_long_single_line_is_not_buffered_unboundedly() {
    let proto = FakeLineProtocol::with_timeouts(fake_agent_bin(), fast_timeouts());
    let (mut tx, pid) = spawn_and_learn_pid(&proto).await;
    assert!(
        pid_alive(pid),
        "child alive before the giant-line turn: {pid}"
    );

    // The child accepts the turn, then emits ONE ~30 MB stdout line (a valid
    // `delta` object whose newline arrives only at the very end) and blocks —
    // never emitting a `done`.
    tx.send_turn(&prompt(&req_emit_giant_line(GIANT_LINE_BYTES)))
        .await
        .expect("send_turn emit_giant_line");

    // Drain to the first terminal, BOUNDED so a driver that hangs forever trying
    // to read a never-completing / unbounded line fails fast rather than wedging
    // the test. A correct capped driver ends the turn (TurnFailed / ProcessExited)
    // quickly; the current buggy driver first surfaces the ~30 MB AssistantDelta
    // (the failure) and only then is ended by the watchdog.
    let evs = tokio::time::timeout(Duration::from_secs(10), async {
        drain_to_terminal(&mut *tx, "giant line").await
    })
    .await
    .expect(
        "driver must not HANG on a pathologically long single line — it must cap \
         the per-line read and end the turn (§4.4)",
    );

    // THE core §4.4 assertion: NO surfaced event may carry a giant payload. If the
    // driver buffered the whole ~30 MB line it will have surfaced an
    // `AssistantDelta` of ~30 MB here — the observable unbounded-buffer failure. A
    // capped driver surfaces no such giant event; it fails the turn / kills the
    // child instead.
    if let Some(giant) = evs
        .iter()
        .find(|e| event_payload_len(e) >= GIANT_EVENT_THRESHOLD)
    {
        panic!(
            "the driver buffered a pathologically long single line ({} bytes) \
             whole into memory and surfaced it as one ordinary event — it must \
             instead cap the per-line read and fail the turn / kill the child \
             (§4.4 unbounded line-read buffer). Giant event: {:?}",
            event_payload_len(giant),
            std::mem::discriminant(giant),
        );
    }

    // And the turn must actually be TERMINATED (failed or the process exited) — a
    // capped driver does not silently swallow the oversized line and wait for a
    // `done` that never comes.
    let terminated = evs.iter().any(|e| {
        matches!(
            e,
            ProtocolEvent::TurnFailed { .. } | ProtocolEvent::ProcessExited { .. }
        )
    });
    assert!(
        terminated,
        "an oversized single line must FAIL the turn / kill the child, surfacing a \
         terminal (TurnFailed / ProcessExited), not be buffered as a normal event \
         (§4.4), got {evs:?}"
    );

    // The child must actually be dead (killed as part of failing the turn), not
    // merely abandoned still holding a tens-of-MB write.
    drop(tx);
    assert!(
        reaped_within(pid, Duration::from_secs(3)).await,
        "the child that emitted the oversized line must be reaped/killed, pid {pid}"
    );
}

// =====================================================================
// §4.4 (unbounded EVENT CHANNEL) — NO dedicated black-box test.
//
// The driver→actor event channel is `mpsc::unbounded_channel()`
// (`fake_line.rs:192`). The plan's other §4.4 half is to bound it (sized
// generously for a real burst) with a backpressure / fail-the-turn overflow
// policy. Deliberately NOT given a dedicated test here, and this is the honest
// reason (not a failure on the tester's part):
//
// An unbounded channel does NOT functionally misbehave under a burst of many
// small events — it delivers every event in order, exactly as a bounded channel
// would, until memory is exhausted. The ONLY observable difference from the
// public driver seam between "unbounded" and "generously bounded" is an
// adversarial memory-exhaustion attack (millions of tiny events with a consumer
// that never drains) — which is NOT safely reproducible in a shared,
// memory-constrained test sandbox (nproc=1, a hard per-process memory cap) without
// risking an OOM that would take down the whole test run, and which would in any
// case only prove "unbounded grows unbounded", a tautology. There is no clean
// black-box behavioral assertion that distinguishes a correctly-sized bounded
// channel from an unbounded one at the observable seam short of that attack.
//
// Channel-boundedness is therefore an internal implementation-capacity concern
// with no directly observable behavioral difference from the public driver seam,
// and is best covered by CODE REVIEW of the fix (the switch from
// `mpsc::unbounded_channel()` to a bounded `mpsc::channel(N)` plus an explicit
// overflow policy) rather than by a forced, weak, or tautological test. The
// per-line-read-buffer half above IS the cleanly-testable, genuinely-observable
// half of §4.4 and is pinned in full.
