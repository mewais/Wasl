//! Phase-2B item 7 — REAL process supervision the Phase-2A scripted in-memory
//! transport never exercised (it was a fake, not an OS process). Per
//! `docs/specs/runtime-protocol.md` §1.4:
//!
//!   * **reaping** — no zombie/orphan child after the transport is dropped/shut
//!     down (the actor/provider IO shell reaps normal & abnormal exit);
//!   * **signal escalation** — interrupt → SIGINT → TERM → KILL, each stage
//!     bounded by an INJECTED short timeout (tested without real long waits);
//!   * **stderr draining** — the child's stderr does not block/deadlock the
//!     stdout pipe;
//!   * **hang watchdog** — a non-terminating child (opencode's known overflow
//!     loop, `adapter-opencode.md` §Non-termination guard) is detected-and-killed
//!     within a bounded time rather than hanging forever. This is a FAST test
//!     (short injected watchdog, never minutes).
//!
//! All timeouts are INJECTED via `FakeLineProtocol::with_timeouts(..)` and kept
//! sub-second so the escalation ladder + watchdog are provable quickly. The
//! bounded `tokio::time::timeout`s here wait ONLY for a REAL OS event (a child
//! actually dying) — that is the one place real-world timing is unavoidable, and
//! it is kept SHORT.
//!
//! EXPECTED RED: `FakeLineProtocol`, `with_timeouts`, `SupervisionTimeouts`, and
//! the fake dev-bin's `spin` mode do not exist yet.

mod support;

use std::time::Duration;

use support::*;

use wasl_runtime::{ComposedPrompt, InteractiveProtocol, ProtocolEvent, SpawnOptions};
use wasl_runtime_protocol::{FakeLineProtocol, SupervisionTimeouts};

fn prompt(line: &str) -> ComposedPrompt {
    ComposedPrompt(line.to_string())
}

/// Short, injected supervision timeouts so every escalation stage + the hang
/// watchdog fire quickly. Sub-second per stage → the whole ladder resolves in
/// well under a second of real time.
fn fast_timeouts() -> SupervisionTimeouts {
    SupervisionTimeouts {
        interrupt_ack: Duration::from_millis(100),
        term_after: Duration::from_millis(100),
        kill_after: Duration::from_millis(100),
        hang_watchdog: Duration::from_millis(200),
    }
}

fn driver_fast() -> FakeLineProtocol {
    FakeLineProtocol::with_timeouts(fake_agent_bin(), fast_timeouts())
}

/// Is a process with this pid still alive? Uses `kill -0` semantics via a plain
/// `kill(2)` probe through the standard library's process facilities. On Unix a
/// reaped child's pid is gone (ESRCH) OR is a zombie — we treat "kill 0 fails"
/// as "reaped/gone". Kept tester-side (no dep on the driver's internals) so the
/// reap assertion is black-box.
#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    // SAFETY-FREE: no `unsafe` in the test crate. Shell out to `kill -0`, which
    // exits 0 iff the process exists and is signalable, non-zero otherwise. This
    // avoids `libc`/`unsafe` in tests while giving a real liveness probe.
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Poll `pid_alive` until it reports gone, bounded by a short guard. Returns true
/// if the pid was reaped within the bound. Polls (no fixed sleep for logic — a
/// short poll interval only to let the OS finish the reap).
#[cfg(unix)]
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

/// Ask the freshly-spawned fake for its own OS pid so the test can probe it
/// directly for the reap assertion.
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
        .find_map(|e| completed_reply(e))
        .expect("pid reply")
        .trim()
        .parse::<u32>()
        .expect("decimal pid");
    (tx, pid)
}

// =====================================================================
// Reaping — no zombie/orphan after the transport is dropped.
// =====================================================================

#[cfg(unix)]
#[tokio::test]
async fn dropping_the_transport_reaps_the_child_no_orphan() {
    let proto = driver_fast();
    let (tx, pid) = spawn_and_learn_pid(&proto).await;
    assert!(
        pid_alive(pid),
        "child should be alive while held: pid {pid}"
    );

    // Drop the transport — the supervision shell MUST reap the child (kill its
    // process group, wait). No orphan/zombie left behind (§1.4).
    drop(tx);

    assert!(
        reaped_within(pid, Duration::from_secs(2)).await,
        "child pid {pid} must be reaped shortly after the transport is dropped \
         (§1.4 reaping — no orphan)"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn dropping_the_transport_of_a_busy_child_still_reaps_it() {
    // Even a child mid-turn (busy, not idle) must be reaped on drop — the harder
    // case that a naive "only reap idle children" impl would leak.
    let proto = driver_fast();
    let (mut tx, pid) = spawn_and_learn_pid(&proto).await;

    // Put it into a hold (busy, blocked) — no terminal will come.
    tx.send_turn(&prompt(&req_hold())).await.expect("hold");
    // Consume the accept if the fake emits one, but do NOT wait for a terminal
    // (there is none while held). A short bounded read only.
    let _ = tokio::time::timeout(Duration::from_millis(300), tx.next_event()).await;
    assert!(pid_alive(pid), "held child alive: pid {pid}");

    drop(tx);
    assert!(
        reaped_within(pid, Duration::from_secs(2)).await,
        "a BUSY child must still be reaped on transport drop (§1.4): pid {pid}"
    );
}

// =====================================================================
// Signal escalation — interrupt → SIGINT → TERM → KILL, each bounded by the
// injected short timeout. A child that ignores gentle signals is escalated to
// KILL within the bound and reaped (no minutes-long wait).
// =====================================================================

#[cfg(unix)]
#[tokio::test]
async fn a_signal_ignoring_child_is_escalated_to_kill_within_the_bound() {
    let proto = driver_fast();
    // `ignore_interrupt` puts the fake into a state where it ignores the gentle
    // interrupt/SIGINT; the escalation ladder must climb to TERM then KILL, each
    // stage bounded by the injected 100ms timeout, so total kill is fast.
    let (mut tx, pid) = spawn_and_learn_pid(&proto).await;
    tx.send_turn(&prompt(&req_ignore_interrupt()))
        .await
        .expect("send_turn");
    let _ = tokio::time::timeout(Duration::from_millis(300), tx.next_event()).await;
    assert!(pid_alive(pid), "child alive before shutdown: {pid}");

    // Dropping/shutting down escalates. With three 100ms stages the child must be
    // dead well within ~1s — assert it is reaped fast (NOT minutes).
    drop(tx);
    assert!(
        reaped_within(pid, Duration::from_secs(2)).await,
        "a signal-ignoring child must be ESCALATED to KILL within the bounded \
         ladder and reaped fast (§1.4 escalation), pid {pid}"
    );
}

// =====================================================================
// stderr draining — a child that floods stderr must not block/deadlock the
// stdout event pipe. We flood stderr and assert the turn's stdout events still
// flow to completion (a full, unread stderr pipe would wedge a naive driver).
// =====================================================================

#[tokio::test]
async fn heavy_stderr_output_does_not_deadlock_the_stdout_event_stream() {
    let proto = FakeLineProtocol::with_timeouts(fake_agent_bin(), fast_timeouts());
    let mut tx = proto
        .spawn(&SpawnOptions::default())
        .await
        .expect("spawn fake agent");
    let _ = next_or_hang(&mut *tx, "ready").await.expect("ready");

    // Flood stderr with enough lines to overflow a typical pipe buffer (64 KiB),
    // then complete the turn on stdout. If the driver does not concurrently drain
    // stderr, the child blocks on its stderr write and never reaches the stdout
    // `done` → the drain hangs (caught by HANG_GUARD).
    tx.send_turn(&prompt(&req_flood_stderr(20_000)))
        .await
        .expect("send_turn flood");
    let evs = drain_to_terminal(&mut *tx, "stderr flood").await;
    assert_eq!(
        count_completions(&evs),
        1,
        "the turn must complete despite a flooded stderr (stderr drained \
         concurrently, §1.4), got {evs:?}"
    );
}

// =====================================================================
// Hang watchdog — a non-terminating child (opencode's overflow loop) is
// detected-and-killed within the bounded injected watchdog, NOT hung forever.
// This must be FAST: the watchdog is 200ms; the whole test resolves sub-second.
// =====================================================================

#[cfg(unix)]
#[tokio::test]
async fn non_terminating_turn_is_detected_and_killed_by_the_hang_watchdog() {
    let proto = driver_fast();
    let (mut tx, pid) = spawn_and_learn_pid(&proto).await;

    // `spin` never emits a terminal event and never exits — the pathological
    // non-termination class the watchdog exists for. The watchdog (200ms) must
    // fire, kill the child, and surface a terminal (ProcessExited or TurnFailed)
    // instead of hanging.
    tx.send_turn(&prompt(&req_spin()))
        .await
        .expect("send_turn spin");

    // The drain must TERMINATE within the watchdog + a small margin — NOT the
    // HANG_GUARD. We wrap it in a tight bound to prove the watchdog, not the
    // coarse guard, is what ended it.
    let drained = tokio::time::timeout(Duration::from_secs(2), async {
        drain_to_terminal(&mut *tx, "spin (watchdog)").await
    })
    .await;
    let evs = drained.expect("hang watchdog must end a non-terminating turn FAST, not hang");

    let terminal = evs.iter().any(|e| {
        matches!(
            e,
            ProtocolEvent::ProcessExited { .. } | ProtocolEvent::TurnFailed { .. }
        )
    });
    assert!(
        terminal,
        "the hang watchdog must surface a terminal (ProcessExited/TurnFailed) \
         for a non-terminating turn (§1.4), got {evs:?}"
    );

    // And the child must actually be dead (killed by the watchdog), not merely
    // abandoned.
    drop(tx);
    assert!(
        reaped_within(pid, Duration::from_secs(2)).await,
        "the watchdog must KILL the spinning child, pid {pid}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn hang_watchdog_fires_fast_not_after_minutes() {
    // Explicit timing assertion: with a 200ms injected watchdog, the terminal
    // must arrive in far under the multi-second range — proving the bound is the
    // INJECTED watchdog, guarding against a regression that reverts to a minutes-
    // long (or absent) watchdog.
    let proto = driver_fast();
    let mut tx = proto
        .spawn(&SpawnOptions::default())
        .await
        .expect("spawn fake agent");
    let _ = next_or_hang(&mut *tx, "ready").await.expect("ready");

    let start = tokio::time::Instant::now();
    tx.send_turn(&prompt(&req_spin())).await.expect("spin");
    let evs = tokio::time::timeout(Duration::from_secs(2), async {
        drain_to_terminal(&mut *tx, "spin timing").await
    })
    .await
    .expect("watchdog must fire fast");
    let elapsed = start.elapsed();

    assert!(
        evs.iter().any(|e| matches!(
            e,
            ProtocolEvent::ProcessExited { .. } | ProtocolEvent::TurnFailed { .. }
        )),
        "watchdog surfaced a terminal: {evs:?}"
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "watchdog (200ms injected) must fire well under 1s, took {elapsed:?}"
    );
}
