//! Hardening-2 §4.1 — a child that closes its stdout (EOF) but does NOT actually
//! exit must be FORCE-TERMINATED via the bounded escalation ladder, not waited on
//! with an UNBOUNDED `child.wait()` that ignores the kill-request channel from
//! that point on.
//!
//! Bug being pinned (hardening-plan §4.1, confirmed against both real drivers'
//! `supervise()` loop): on stdout `Ok(None)`/`Err(_)` (EOF or read error) the
//! code does `child.wait().await` with NO bound and NO route through the
//! escalation ladder, ignoring `kill_rx` entirely from that point on. A child
//! that closes stdout but does not exit (a real, observed misbehavior class —
//! half-dead processes, a hung cleanup handler, a wedged child) then hangs
//! supervision until the child's OWN eventual exit, with no way to force a kill.
//!
//! The fix: treat unexpected stdout closure as a supervision failure and route
//! it through the SAME bounded `escalate()` ladder already used for the
//! kill-request path.
//!
//! This drives a REAL OS child via `FakeLineProtocol` + the `wasl-fake-agent`
//! fixture's new `close_stdout_stay_alive` command: the child closes FD 1 (driver
//! sees stdout EOF) but stays alive for 10 s before exiting on its own. The test
//! then requests termination (drops the transport) and asserts the child is dead
//! well within a bound MUCH shorter than that 10 s natural exit — proving the
//! driver force-killed it rather than waiting for the natural exit.
//!
//! EXPECTED RED against the current unfixed `fake_line.rs`: on stdout EOF the
//! supervisor blocks in the unbounded `child.wait()` and never honors the kill
//! request, so the child survives past the (short) assertion bound.

#![cfg(unix)]

mod support;

use std::time::Duration;

use support::*;

use wasl_runtime::{ComposedPrompt, InteractiveProtocol, SpawnOptions};
use wasl_runtime_protocol::{FakeLineProtocol, SupervisionTimeouts};

fn prompt(line: &str) -> ComposedPrompt {
    ComposedPrompt(line.to_string())
}

/// Is a process with this pid still alive? `kill -0` semantics (exit 0 iff the
/// process exists and is signalable), shelled out so the test crate needs no
/// `unsafe`/`libc` — the same black-box liveness probe pattern `supervision.rs`
/// uses.
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

/// Poll `pid_alive` until it reports gone, bounded by `bound`. Returns true if
/// the pid was reaped within the bound.
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

// =====================================================================
// §4.1 — stdout EOF while the child stays alive MUST NOT wedge supervision in an
// unbounded wait: a termination request must still force-kill the child within
// the bounded ladder, far sooner than the child's own 10 s natural exit.
//
// The hang watchdog is set LONGER (5 s) than the assertion bound (3 s) on
// purpose, so this test isolates the §4.1 EOF-path bug rather than accidentally
// passing because the watchdog happened to fire: the ONLY way the child dies
// within 3 s is if the driver routes the stdout closure / kill request through
// the bounded escalation ladder (the fix), NOT the unbounded `child.wait()`.
// =====================================================================

#[tokio::test]
async fn stdout_eof_with_live_child_is_force_killed_via_escalation_not_unbounded_wait() {
    let proto = FakeLineProtocol::with_timeouts(
        fake_agent_bin(),
        SupervisionTimeouts {
            interrupt_ack: Duration::from_millis(100),
            term_after: Duration::from_millis(100),
            kill_after: Duration::from_millis(100),
            // Deliberately LONG so the watchdog cannot mask the §4.1 EOF bug: a
            // child dead within the 3 s assertion bound below proves the
            // escalation ladder acted, not a 5 s watchdog nor the 10 s natural
            // exit.
            hang_watchdog: Duration::from_secs(5),
        },
    );

    let (mut tx, pid) = spawn_and_learn_pid(&proto).await;
    assert!(pid_alive(pid), "child alive before the EOF turn: {pid}");

    // The child closes its stdout (driver sees EOF) but stays alive for 10 s.
    tx.send_turn(&prompt(&req_close_stdout_stay_alive(10_000)))
        .await
        .expect("send_turn close_stdout_stay_alive");

    // Give the driver a moment to observe the EOF and (per the bug) enter its
    // unbounded `child.wait()`. A short bounded read; we do not require an event.
    let _ = tokio::time::timeout(Duration::from_millis(300), tx.next_event()).await;
    assert!(
        pid_alive(pid),
        "the child stays alive after closing stdout (it does not exit): {pid}"
    );

    // Request termination. On the CURRENT driver the supervisor is already parked
    // in the unbounded `child.wait()` from the EOF handler and ignores this kill
    // request, so the child survives until its 10 s natural exit. The fix routes
    // the stdout closure through the bounded escalation ladder, killing it fast.
    drop(tx);

    assert!(
        reaped_within(pid, Duration::from_secs(3)).await,
        "on stdout EOF with a still-alive child, the driver must FORCE-TERMINATE \
         it via the bounded escalation ladder — NOT block in an unbounded \
         child.wait() ignoring the kill request until the child's own ~10 s \
         natural exit (§4.1). pid {pid} was still alive after 3 s"
    );
}

// =====================================================================
// §4.2 — escalate()'s OWN final fallback wait is unbounded — NO dedicated test.
//
// Deliberately NOT tested here (a good test cannot be devised cleanly without a
// SYNTHETIC driver). §4.2's bug is: after SIGKILL, if `start_kill()` itself
// errors OR the kernel is slow to report the exit, the "last resort"
// `child.wait()` is unbounded. Reproducing it requires making SIGKILL FAIL or be
// arbitrarily slow — which is not possible against a REAL OS child (a real
// `sleep` cannot survive or delay SIGKILL), and this test crate drives only real
// children through `FakeLineProtocol`. Exercising the arm would need a
// synthetic/mock driver whose `wait()` never completes and whose `start_kill()`
// errors — infrastructure well beyond the fake-agent fixture. The hardening plan
// itself classifies §4.2 as "extremely unlikely to matter in practice" (SIGKILL
// essentially always works); it is a consistency/defense-in-depth bound, and
// forcing a synthetic test for it would be a bad test. The §4.1 test above and
// `supervision.rs`'s existing escalation-ladder tests DO exercise the ladder's
// earlier bounded stages (interrupt→TERM→KILL), so a regression that made those
// unbounded would still be caught; only the post-SIGKILL last-resort wait is left
// to the coder + review to bound, per the plan's own priority call.
//
// §4.10 — best_effort_signal shells out to `kill` via $PATH instead of a direct
// syscall — also NO dedicated test (an implementation-quality nit). The existing
// escalation-ladder tests (`supervision.rs`
// `a_signal_ignoring_child_is_escalated_to_kill_within_the_bound`, plus the §4.9
// `process_group_kill.rs` test) already exercise real signal delivery end-to-end
// and will keep validating that signalling works once the coder switches from a
// `Command::new("kill")` shell-out to a direct `kill(2)` syscall — no behavior
// change is observable at the driver's seam, so a dedicated test would only pin an
// internal implementation detail.
