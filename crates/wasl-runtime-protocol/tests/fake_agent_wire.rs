//! Phase-2B item 1 — the fake agent's OWN line-protocol contract, pinned by
//! driving the compiled dev-bin DIRECTLY as a subprocess (raw stdin/stdout),
//! WITHOUT `FakeLineProtocol`. This isolates the fake's wire behavior from the
//! driver's mapping, so a bug in one cannot mask a bug in the other (the same
//! separation the adapter crates get from testing `parse_reply` as a pure
//! function alongside the process plumbing).
//!
//! It asserts the fake honors `docs/specs/runtime-protocol.md` §1.2 (the exact
//! event-line JSON shapes) and §1.1 (persistence: same process, fact map,
//! `recall pid`) at the BYTES level — the ground truth the driver maps.
//!
//! NOTE on ownership: the fake agent's INTERNAL unit tests (its own
//! `#[cfg(test)]` line-parser/fact-map micro-tests) live in
//! `src/bin/wasl-fake-agent.rs` and are the CODER's to write (that is `src/**`).
//! This tester-owned file pins the fake's OBSERVABLE wire contract from outside
//! the process — the black-box counterpart, appropriate for `tests/**`.
//!
//! EXPECTED RED: the `wasl-fake-agent` dev-bin does not exist yet, so
//! `env!("CARGO_BIN_EXE_wasl-fake-agent")` does not expand.

mod support;

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::Duration;

use support::*;

/// Spawn the fake agent bin with piped stdio for raw wire inspection.
fn spawn_raw() -> (Child, ChildStdin, BufReader<ChildStdout>) {
    let mut child = Command::new(fake_agent_bin())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn wasl-fake-agent");
    let stdin = child.stdin.take().expect("stdin");
    let stdout = BufReader::new(child.stdout.take().expect("stdout"));
    (child, stdin, stdout)
}

/// Read one line of the fake's stdout, trimmed of the trailing newline. Bounded
/// by spawning the read on a thread with a join timeout would be heavy; instead
/// we rely on the fake being prompt and the overall `cargo test` harness timeout.
/// A correct fake writes a line immediately after each request.
fn read_line(stdout: &mut BufReader<ChildStdout>) -> String {
    let mut line = String::new();
    let n = stdout.read_line(&mut line).expect("read line");
    assert!(n > 0, "fake agent closed stdout unexpectedly");
    line.trim_end().to_string()
}

fn write_line(stdin: &mut ChildStdin, line: &str) {
    stdin
        .write_all(line.as_bytes())
        .expect("write request line");
    stdin.write_all(b"\n").expect("write newline");
    stdin.flush().expect("flush stdin");
}

/// Parse a fake event line into `(ev, serde_json::Value)`.
fn ev_of(line: &str) -> (String, serde_json::Value) {
    let v: serde_json::Value =
        serde_json::from_str(line).unwrap_or_else(|e| panic!("event line not JSON: {line:?}: {e}"));
    let ev = v
        .get("ev")
        .and_then(|e| e.as_str())
        .unwrap_or_else(|| panic!("event line has no string `ev` field: {line:?}"))
        .to_string();
    (ev, v)
}

// =====================================================================
// §1.2 — the fake's first line is a `ready` event carrying a `native` id.
// =====================================================================

#[test]
fn first_line_is_a_ready_event_with_a_native_id() {
    let (mut child, _stdin, mut stdout) = spawn_raw();
    let (ev, v) = ev_of(&read_line(&mut stdout));
    assert_eq!(ev, "ready", "the first event is `ready` (§1.2)");
    assert!(
        v.get("native").and_then(|n| n.as_str()).is_some(),
        "`ready` carries a string `native` id (§1.2): {v}"
    );
    let _ = child.kill();
    let _ = child.wait();
}

// =====================================================================
// §1.2 — a plain `say` turn emits `accepted` then `done` with the right shapes.
// =====================================================================

#[test]
fn say_turn_emits_accepted_then_done_with_reply() {
    let (mut child, mut stdin, mut stdout) = spawn_raw();
    let _ready = read_line(&mut stdout); // consume ready

    write_line(&mut stdin, &req_say("wire-reply"));

    // accepted
    let (a, _) = ev_of(&read_line(&mut stdout));
    assert_eq!(a, "accepted", "a `say` emits `accepted` first (§1.2)");
    // done (skip any deltas the fake may emit)
    let mut last = ev_of(&read_line(&mut stdout));
    while last.0 == "delta" {
        last = ev_of(&read_line(&mut stdout));
    }
    assert_eq!(last.0, "done", "a `say` completes with `done` (§1.2)");
    assert_eq!(
        last.1.get("reply").and_then(|r| r.as_str()),
        Some("wire-reply"),
        "`done` carries `reply` verbatim (§1.2): {}",
        last.1
    );
    let _ = child.kill();
    let _ = child.wait();
}

// =====================================================================
// §1.1 — remember/recall retains a fact IN THE SAME PROCESS across turns.
// =====================================================================

#[test]
fn remember_then_recall_over_raw_wire_retains_the_fact() {
    let (mut child, mut stdin, mut stdout) = spawn_raw();
    let _ready = read_line(&mut stdout);

    // remember
    write_line(&mut stdin, &req_remember("kk", "PERSISTED"));
    // drain to that turn's done
    loop {
        let (ev, _) = ev_of(&read_line(&mut stdout));
        if ev == "done" {
            break;
        }
    }

    // recall — over the SAME never-closed stdin
    write_line(&mut stdin, &req_recall("kk"));
    let reply = loop {
        let (ev, v) = ev_of(&read_line(&mut stdout));
        if ev == "done" {
            break v
                .get("reply")
                .and_then(|r| r.as_str())
                .map(|s| s.to_string());
        }
    };
    assert_eq!(
        reply.as_deref(),
        Some("PERSISTED"),
        "the fact survives across turns in the SAME live process (§1.1)"
    );
    let _ = child.kill();
    let _ = child.wait();
}

// =====================================================================
// §1.1 — `recall pid` reports the fake's own OS pid, and it matches the child's
// real pid — proving the same-process property AND the tester's PID contract.
// =====================================================================

#[test]
fn recall_pid_reports_the_processes_own_os_pid() {
    let (mut child, mut stdin, mut stdout) = spawn_raw();
    let child_pid = child.id();
    let _ready = read_line(&mut stdout);

    write_line(&mut stdin, &req_recall_pid());
    let reply = loop {
        let (ev, v) = ev_of(&read_line(&mut stdout));
        if ev == "done" {
            break v
                .get("reply")
                .and_then(|r| r.as_str())
                .expect("pid reply")
                .to_string();
        }
    };
    let reported: u32 = reply.trim().parse().expect("decimal pid");
    assert_eq!(
        reported, child_pid,
        "`recall pid` must report the fake's OWN OS pid (§1.1 same-process proof)"
    );
    let _ = child.kill();
    let _ = child.wait();
}

// =====================================================================
// §1.1 — `crash` exits the process with the requested code (raw exit-status
// contract the driver maps to ProcessExited{code}).
// =====================================================================

#[test]
fn crash_exits_the_process_with_the_requested_code() {
    let (mut child, mut stdin, mut stdout) = spawn_raw();
    let _ready = read_line(&mut stdout);

    write_line(&mut stdin, &req_crash(9));
    // The process must exit; wait for its status and assert the code.
    // (A short bounded busy-wait on try_wait keeps this from hanging if the fake
    //  is broken; a correct fake exits promptly.)
    let mut status = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        if let Some(s) = child.try_wait().expect("try_wait") {
            status = Some(s);
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let status = status.expect("crash must make the fake exit promptly");
    assert_eq!(
        status.code(),
        Some(9),
        "`crash` exits with the requested code (§1.1)"
    );
}

// =====================================================================
// §1.1 — the fake stays running across MANY turns (never EOFs stdin between
// turns): three back-to-back turns all complete on one process.
// =====================================================================

#[test]
fn fake_stays_running_across_many_turns() {
    let (mut child, mut stdin, mut stdout) = spawn_raw();
    let _ready = read_line(&mut stdout);

    for i in 0..3 {
        let want = format!("turn-{i}");
        write_line(&mut stdin, &req_say(&want));
        let got = loop {
            let (ev, v) = ev_of(&read_line(&mut stdout));
            if ev == "done" {
                break v.get("reply").and_then(|r| r.as_str()).unwrap().to_string();
            }
        };
        assert_eq!(got, want, "turn {i} served by the persistent process");
    }
    // Still alive after three turns.
    assert!(
        child.try_wait().expect("try_wait").is_none(),
        "the fake remains running across turns (§1.1 persistent)"
    );
    let _ = child.kill();
    let _ = child.wait();
}
