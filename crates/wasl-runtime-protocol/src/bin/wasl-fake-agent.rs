//! `wasl-fake-agent` — the long-lived in-repo fake structured agent
//! (`docs/specs/runtime-protocol.md` §1.1).
//!
//! A **dev-only** persistent test binary: it reads one JSON request object per
//! line on stdin and writes one JSON event object per line on stdout, per the
//! §1.2 line protocol. It stays running across MANY turns over a never-EOF
//! stdin, maintains an **in-memory fact map** (`remember`/`recall`) proving
//! context retention IN THE SAME OS PROCESS, and is scriptable to emit the §1.3
//! HOSTILE-WIRE the `FakeLineProtocol` driver's line-framing must survive.
//!
//! ## Request wire (stdin, one JSON object per line)
//!
//! `{"cmd":"<name>", ...}` — the exact vocabulary the tester's `req_*` helpers
//! build (`crates/wasl-runtime-protocol/tests/support/mod.rs`). Unknown or
//! malformed request lines are ignored (the fake never crashes on bad input).
//!
//! ## Event wire (stdout, one JSON object per line) — §1.2
//!
//! `{"ev":"ready","native":"<id>"}` / `{"ev":"accepted"}` /
//! `{"ev":"delta","text":"…"}` / `{"ev":"gate","summary":"…","preview":"…"}` /
//! `{"ev":"done","reply":"…"}` — plus the raw process-exit contract (`crash`,
//! `emit_then_exit`, `gate_then_exit`, `eof_without_code`) the driver maps to
//! `ProcessExited`.
//!
//! Signal-ignoring (for the escalation ladder) is done via tokio's `signal`
//! seam, never a raw `libc`/`sigaction` call. The ONE narrow `unsafe` block in
//! this FIXTURE binary is `close_stdout_stay_alive`'s std-only FD-1 close (see
//! that command) — there is no safe stdlib API to close the process's own stdout
//! while the process keeps running, and adding a crate (`nix`/`rustix`) is out of
//! scope for a test fixture; the block is std-only (`OwnedFd::from_raw_fd`), no
//! external dependency.
#![allow(unsafe_code)]

use std::collections::HashMap;
use std::io::Write as _;
use std::process::exit;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// The in-memory fact map — retained across turns in this ONE process (§1.1).
type Facts = HashMap<String, String>;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // A stable per-process native session id (§1.2 `ready.native`). The exact
    // value is opaque to the driver; using the OS pid keeps it distinct per
    // process and human-debuggable.
    let native = format!("fake-{}", std::process::id());

    let mut out = tokio::io::stdout();
    // First line: the `ready` event carrying the native id (§1.2).
    write_event(
        &mut out,
        &serde_json::json!({ "ev": "ready", "native": native }),
    )
    .await;

    let mut facts: Facts = HashMap::new();
    let mut reader = BufReader::new(tokio::io::stdin()).lines();

    // Serve turns until stdin EOFs (never between turns — the driver keeps it
    // open). A missing/malformed line is skipped, not fatal.
    loop {
        let line = match reader.next_line().await {
            Ok(Some(line)) => line,
            // stdin closed / read error → the session ends cleanly.
            Ok(None) | Err(_) => break,
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let req: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            // Malformed request line: ignore it and keep serving.
            Err(_) => continue,
        };
        handle_request(&mut out, &mut reader, &mut facts, &req, &native).await;
    }
}

/// Dispatch one request line to its behavior.
async fn handle_request(
    out: &mut tokio::io::Stdout,
    reader: &mut tokio::io::Lines<BufReader<tokio::io::Stdin>>,
    facts: &mut Facts,
    req: &Value,
    _native: &str,
) {
    let cmd = req.get("cmd").and_then(Value::as_str).unwrap_or("");
    match cmd {
        // ---- §1.1 core turns ----
        "say" => {
            let reply = req.get("reply").and_then(Value::as_str).unwrap_or("");
            accepted(out).await;
            done(out, reply).await;
        }
        "remember" => {
            let key = req.get("key").and_then(Value::as_str).unwrap_or("");
            let value = req.get("value").and_then(Value::as_str).unwrap_or("");
            facts.insert(key.to_string(), value.to_string());
            accepted(out).await;
            // The `remember` turn completes; the reply echoes the stored value
            // (tests only assert the subsequent `recall`).
            done(out, value).await;
        }
        "recall" => {
            let key = req.get("key").and_then(Value::as_str).unwrap_or("");
            accepted(out).await;
            // `recall pid` is the same-process proof (§1.1): reply with our OWN
            // OS pid as a decimal string so a second turn returning the same pid
            // proves one process served both turns.
            let reply = recall_reply(facts, key, std::process::id());
            done(out, &reply).await;
        }
        "hold" => {
            // Accept, then block deterministically until a `release` line arrives
            // (§1.1) — no sleeps, purely event-driven.
            accepted(out).await;
            loop {
                let line = match reader.next_line().await {
                    Ok(Some(line)) => line,
                    Ok(None) | Err(_) => return, // stdin closed while held.
                };
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let Ok(v) = serde_json::from_str::<Value>(trimmed) else {
                    continue;
                };
                if v.get("cmd").and_then(Value::as_str) == Some("release") {
                    let reply = v.get("reply").and_then(Value::as_str).unwrap_or("");
                    done(out, reply).await;
                    return;
                }
                // Any non-release line during a hold: apply it too (so a fact set
                // while held is honored), but keep holding.
                Box::pin(handle_request(out, reader, facts, &v, _native)).await;
            }
        }
        "release" => {
            // A stray `release` with no active hold: complete a bare turn so the
            // driver never hangs waiting for a terminal.
            let reply = req.get("reply").and_then(Value::as_str).unwrap_or("");
            done(out, reply).await;
        }
        "crash" => {
            // Exit mid-turn with the requested code (§1.1) — no terminal event.
            let code = req.get("code").and_then(Value::as_i64).unwrap_or(1) as i32;
            let _ = out.flush().await;
            exit(code);
        }
        "gate" => {
            // Emit the native-gate request then block awaiting resolution (§1.1).
            let summary = req.get("summary").and_then(Value::as_str).unwrap_or("");
            let preview = req.get("preview").and_then(Value::as_str).unwrap_or("");
            gate(out, summary, preview).await;
            // Block until a release/other line arrives; if the driver answers via
            // stdin, complete the turn.
            loop {
                match reader.next_line().await {
                    Ok(Some(line)) => {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        done(out, "gate-resolved").await;
                        return;
                    }
                    Ok(None) | Err(_) => return,
                }
            }
        }
        "ask_wasl_gate" => {
            // Phase-2C probe (A) — the WORKS case. Mid-turn emit a `gate` (→
            // NativeGateRequested), BLOCK reading exactly ONE more stdin line (the
            // actor's `resolve_native_gate` answer, over the SAME pipe), then
            // complete `done` with a correlated `reply:"answered:<key>"`.
            let key = req.get("key").and_then(Value::as_str).unwrap_or("");
            accepted(out).await;
            gate(out, "ask_wasl", key).await;
            // Block until the actor answers over stdin (any line unblocks — the
            // resolution is the round-trip proof; we need not parse it).
            loop {
                match reader.next_line().await {
                    Ok(Some(line)) => {
                        if line.trim().is_empty() {
                            continue;
                        }
                        done(out, &format!("answered:{key}")).await;
                        return;
                    }
                    Ok(None) | Err(_) => return, // stdin closed while blocked.
                }
            }
        }
        "ask_wasl_tool" => {
            // Phase-2C probe (B) — the LEAK case (the sharper probe). Mid-turn
            // emit a `tool_start` name `ask_wasl` (→ ToolStarted), BLOCK reading
            // exactly ONE more stdin line that must arrive over the SAME pipe as
            // an ANSWER TO THIS TOOL CALL, and ONLY after that line arrives emit
            // `tool_end` then `done` with `reply:"answered:<key>"`.
            let key = req.get("key").and_then(Value::as_str).unwrap_or("");
            accepted(out).await;
            tool_start(out, "ask_wasl").await;
            // Block until the answer arrives over stdin. Through the FROZEN API
            // there is no way for the single-writer actor to send this line AS AN
            // ANSWER TO THIS TOOL CALL, so the turn stays blocked (the demonstrated
            // hang); with the Phase-2C `answer_tool_call` fix the actor's answer
            // arrives here and the turn proceeds.
            loop {
                match reader.next_line().await {
                    Ok(Some(line)) => {
                        if line.trim().is_empty() {
                            continue;
                        }
                        tool_end(out, "ask_wasl", true).await;
                        done(out, &format!("answered:{key}")).await;
                        return;
                    }
                    Ok(None) | Err(_) => return, // stdin closed while blocked.
                }
            }
        }
        "gate_then_exit" => {
            // §1.3 gate-then-exit: emit the gate, then the process exits (→
            // NativeGateRequested then ProcessExited).
            let summary = req.get("summary").and_then(Value::as_str).unwrap_or("");
            let preview = req.get("preview").and_then(Value::as_str).unwrap_or("");
            let code = req.get("code").and_then(Value::as_i64).unwrap_or(0) as i32;
            gate(out, summary, preview).await;
            let _ = out.flush().await;
            exit(code);
        }
        "spin" => {
            // §1.4 hang-watchdog target: accept, then loop forever emitting NO
            // terminal event and never exiting.
            accepted(out).await;
            let _ = out.flush().await;
            // A cooperative busy-wait that never yields a terminal and never
            // exits. `std::thread::park` blocks this (current-thread) runtime's
            // only thread indefinitely with zero CPU — the pathological
            // non-terminating turn the watchdog must kill.
            loop {
                std::thread::park();
            }
        }
        "ignore_interrupt" => {
            // §1.3 ignored interrupt: accept, install ignore handlers for the
            // gentle signals (SIGINT/SIGTERM) so only SIGKILL can end us, then
            // block forever without ever acking.
            accepted(out).await;
            let _ = out.flush().await;
            ignore_gentle_signals_forever().await;
        }
        "stop_reading_stdin" => {
            // The M2 EPIPE setup: ack this turn, then STOP reading stdin so a
            // subsequent driver write hits BrokenPipe/WriteZero. We keep the
            // process (and stdout) alive briefly, but never read stdin again.
            accepted(out).await;
            done(out, "stopped-reading").await;
            let _ = out.flush().await;
            // Block forever WITHOUT touching stdin. The kernel stdin pipe buffer
            // fills and the driver's write eventually EPIPEs (or WriteZeros).
            loop {
                std::thread::park();
            }
        }
        "close_stdout_stay_alive" => {
            // Hardening-2 §4.1 setup: the child CLOSES its stdout file descriptor
            // (so the driver's stdout read hits EOF) but KEEPS RUNNING — it does
            // NOT exit — for several seconds before finally exiting on its own.
            //
            // This reproduces the real observed misbehavior class the plan cites
            // ("a hung cleanup handler, a wedged child"): stdout is gone but the
            // process is still alive. A driver that, on stdout EOF, does an
            // UNBOUNDED `child.wait()` (§4.1's bug) would then block until this
            // child's own eventual exit, ignoring the kill channel. A test with a
            // much SHORTER escalation bound proves the driver force-terminates the
            // child instead of waiting `stay_alive_ms` for the natural exit.
            let stay_alive_ms = req
                .get("stay_alive_ms")
                .and_then(Value::as_u64)
                .unwrap_or(10_000);
            // Close FD 1 so the driver's stdout reader observes EOF. We deliberately
            // do NOT emit any tokio-stdout event here first: FD 1 is the sole
            // reference to the stdout pipe write-end (tokio's `Stdout` writes go
            // straight to FD 1 via the blocking pool, no dup — verified), so closing
            // it closes the pipe and the reader sees EOF. Touching tokio's async
            // `Stdout` and then closing FD 1 underneath it risks a blocking-pool
            // race, so we avoid it entirely. There is no safe stdlib API to close
            // the process's own stdout in place; `OwnedFd::from_raw_fd(1)` takes
            // ownership of FD 1 and dropping it closes it — std-only, no external
            // crate. SAFETY: FD 1 is this process's stdout, a valid open descriptor;
            // we take exclusive ownership and immediately drop (close) it, and never
            // write to stdout again afterwards.
            {
                use std::os::fd::FromRawFd as _;
                let stdout_fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(1) };
                drop(stdout_fd);
            }
            // Stay alive (NOT exiting) so the ONLY way the driver ends this child is
            // by force-terminating it, not by waiting for a natural exit. Use a
            // plain blocking sleep (not tokio's) so the (now-closed-stdout) tokio
            // runtime is not asked to do any more IO before we exit.
            std::thread::sleep(std::time::Duration::from_millis(stay_alive_ms));
            exit(0);
        }
        "spawn_child_process" => {
            // Hardening-2 §4.9 setup: the fake spawns a REAL grandchild OS process
            // (`sleep <grandchild_secs>`) — the analog of the real `claude` CLI
            // spawning its own subprocess MCP servers — reports the grandchild's
            // PID via a stdout event (a `delta`, so the turn stays active with no
            // terminal), then NEVER exits on its own and IGNORES the gentle
            // signals, forcing the escalation ladder all the way to SIGKILL.
            //
            // The grandchild inherits the fake's process group. Signalling only
            // the LEADER pid (§4.9's bug) leaves the grandchild orphaned-and-alive;
            // signalling the whole process group (the fix) kills it too. A test
            // probes the grandchild's PID after the fake is force-killed and
            // asserts the GRANDCHILD is also dead.
            let grandchild_secs = req
                .get("grandchild_secs")
                .and_then(Value::as_u64)
                .unwrap_or(30);
            accepted(out).await;
            let child = std::process::Command::new("sleep")
                .arg(grandchild_secs.to_string())
                // The grandchild must NOT hold the fake's stdout pipe (so the
                // driver's own stdout EOF is governed purely by the fake), and it
                // must not read the fake's stdin.
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn();
            match child {
                Ok(c) => {
                    // Report the grandchild pid over stdout as an assistant delta
                    // (maps to `AssistantDelta{text:"<pid>"}`), so the turn stays
                    // active (no terminal) and the test can learn the pid.
                    emit_delta(out, &c.id().to_string()).await;
                    let _ = out.flush().await;
                    // Deliberately LEAK the grandchild handle so Rust does not
                    // reap/kill it on drop — it must survive the fake's own death
                    // (the orphan the leader-only signal would leave behind).
                    std::mem::forget(c);
                }
                Err(e) => {
                    // If `sleep` is somehow unavailable, fail the turn loudly so a
                    // test sees a definite signal rather than a silent hang.
                    write_event(
                        out,
                        &serde_json::json!({ "ev": "failed", "error": format!("spawn sleep: {e}") }),
                    )
                    .await;
                    let _ = out.flush().await;
                }
            }
            // Never exit on our own; ignore SIGINT/SIGTERM so only SIGKILL ends us
            // (forces the driver's escalation ladder to its final stage).
            ignore_gentle_signals_forever().await;
        }

        // ---- §1.3 hostile-wire emission scripts ----
        "emit_split_done" => {
            // A single valid `done` object written across MULTIPLE partial
            // flushes — the driver's line buffer must reassemble it into ONE
            // event.
            let reply = req.get("reply").and_then(Value::as_str).unwrap_or("");
            accepted(out).await;
            let obj = serde_json::json!({ "ev": "done", "reply": reply }).to_string();
            let mid = obj.len() / 2;
            let (a, b) = obj.split_at(mid);
            out.write_all(a.as_bytes()).await.ok();
            out.flush().await.ok();
            out.write_all(b.as_bytes()).await.ok();
            out.write_all(b"\n").await.ok();
            out.flush().await.ok();
        }
        "emit_batched" => {
            // accepted+delta+done in ONE stdout write (§1.3 "multiple messages in
            // one read") — the driver must split them into three ordered events.
            let delta = req.get("delta").and_then(Value::as_str).unwrap_or("");
            let reply = req.get("reply").and_then(Value::as_str).unwrap_or("");
            let batch = format!(
                "{}\n{}\n{}\n",
                serde_json::json!({ "ev": "accepted" }),
                serde_json::json!({ "ev": "delta", "text": delta }),
                serde_json::json!({ "ev": "done", "reply": reply }),
            );
            out.write_all(batch.as_bytes()).await.ok();
            out.flush().await.ok();
        }
        "emit_malformed_between" => {
            // A MALFORMED JSON line between two valid events (§1.3) — ignored +
            // logged, framing continues.
            let delta = req.get("delta").and_then(Value::as_str).unwrap_or("");
            let reply = req.get("reply").and_then(Value::as_str).unwrap_or("");
            accepted(out).await;
            emit_delta(out, delta).await;
            out.write_all(b"{not valid json at all]}\n").await.ok();
            out.flush().await.ok();
            done(out, reply).await;
        }
        "emit_done_no_accept" => {
            // §1.3 missing `accepted`: straight to `done`.
            let reply = req.get("reply").and_then(Value::as_str).unwrap_or("");
            done(out, reply).await;
        }
        "emit_duplicate_done" => {
            // §1.3 duplicate `done`: the second must be ignored by the driver.
            let reply = req.get("reply").and_then(Value::as_str).unwrap_or("");
            accepted(out).await;
            done(out, reply).await;
            done(out, reply).await;
        }
        "emit_delta_after_done" => {
            // §1.3 delta-after-completion: a `delta` AFTER `done` (R1 §7.4).
            let reply = req.get("reply").and_then(Value::as_str).unwrap_or("");
            let late = req.get("late").and_then(Value::as_str).unwrap_or("");
            accepted(out).await;
            done(out, reply).await;
            emit_delta(out, late).await;
        }
        "eof_without_code" => {
            // §1.3 EOF without a ProcessExited-worthy code → ProcessExited{None}.
            // Die by a SIGNAL (not `exit(code)`) so `status.code()` is None: the
            // driver maps a signal death to `code: None`.
            let _ = out.flush().await;
            // Drop stdout so the reader sees EOF promptly, then die by signal.
            drop(std::io::stdout());
            std::process::abort();
        }
        "emit_then_exit" => {
            // §1.3 exit-before-stdout-drained: buffer events then exit; the driver
            // must drain the buffered events FIRST, THEN ProcessExited{code}.
            let delta = req.get("delta").and_then(Value::as_str).unwrap_or("");
            let reply = req.get("reply").and_then(Value::as_str).unwrap_or("");
            let code = req.get("code").and_then(Value::as_i64).unwrap_or(0) as i32;
            accepted(out).await;
            emit_delta(out, delta).await;
            done(out, reply).await;
            let _ = out.flush().await;
            exit(code);
        }
        "flood_stderr" => {
            // §1.4 stderr-drain: write a burst of stderr lines (enough to overflow
            // a pipe buffer), THEN complete the turn on stdout. If the driver does
            // not concurrently drain stderr, the child blocks on its stderr write
            // and never reaches the stdout `done`.
            let lines = req.get("lines").and_then(Value::as_u64).unwrap_or(0);
            accepted(out).await;
            let mut err = std::io::stderr();
            for i in 0..lines {
                // Plain blocking stderr writes: if unread they will block once the
                // pipe fills — exactly the deadlock the driver must avoid.
                let _ = writeln!(err, "fake-agent stderr flood line {i}");
            }
            let _ = err.flush();
            done(out, "flooded").await;
        }

        // Unknown command: ignore (never crash on unexpected input).
        _ => {}
    }
}

/// Pure `recall` resolution (§1.1): `recall pid` returns the process's OWN pid as
/// a decimal string (the same-process proof); any other key returns the stored
/// fact, or empty when unknown. Factored out so it is unit-testable without IO.
fn recall_reply(facts: &Facts, key: &str, own_pid: u32) -> String {
    if key == "pid" {
        own_pid.to_string()
    } else {
        facts.get(key).cloned().unwrap_or_default()
    }
}

/// Install ignore handlers for SIGINT and SIGTERM (so the escalation ladder must
/// reach SIGKILL) and then block forever. tokio's `signal` seam — no `unsafe`.
async fn ignore_gentle_signals_forever() {
    use tokio::signal::unix::{signal, SignalKind};
    // Registering a stream for a signal replaces the default terminate action
    // with delivery-to-the-stream; by never reacting, we effectively IGNORE it.
    let mut sigint = signal(SignalKind::interrupt()).expect("register SIGINT");
    let mut sigterm = signal(SignalKind::terminate()).expect("register SIGTERM");
    loop {
        tokio::select! {
            _ = sigint.recv() => { /* ignore — keep running */ }
            _ = sigterm.recv() => { /* ignore — keep running */ }
        }
    }
}

// --- §1.2 event-line emitters ---------------------------------------------

async fn write_event(out: &mut tokio::io::Stdout, ev: &Value) {
    let mut line = ev.to_string();
    line.push('\n');
    // A closed stdout (the driver went away) is benign: just stop trying.
    let _ = out.write_all(line.as_bytes()).await;
    let _ = out.flush().await;
}

async fn accepted(out: &mut tokio::io::Stdout) {
    write_event(out, &serde_json::json!({ "ev": "accepted" })).await;
}

async fn emit_delta(out: &mut tokio::io::Stdout, text: &str) {
    write_event(out, &serde_json::json!({ "ev": "delta", "text": text })).await;
}

async fn done(out: &mut tokio::io::Stdout, reply: &str) {
    write_event(out, &serde_json::json!({ "ev": "done", "reply": reply })).await;
}

async fn gate(out: &mut tokio::io::Stdout, summary: &str, preview: &str) {
    write_event(
        out,
        &serde_json::json!({ "ev": "gate", "summary": summary, "preview": preview }),
    )
    .await;
}

async fn tool_start(out: &mut tokio::io::Stdout, name: &str) {
    write_event(
        out,
        &serde_json::json!({ "ev": "tool_start", "name": name }),
    )
    .await;
}

async fn tool_end(out: &mut tokio::io::Stdout, name: &str, ok: bool) {
    write_event(
        out,
        &serde_json::json!({ "ev": "tool_end", "name": name, "ok": ok }),
    )
    .await;
}

// The fake's own `#[cfg(test)]` fact-map micro-tests (the CODER's to write, per
// `tests/support/mod.rs`): the pure `recall` resolution incl. the `pid`
// same-process special case. The OBSERVABLE wire contract is pinned black-box
// from outside the process by the tester-owned `tests/fake_agent_wire.rs`.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recall_pid_returns_the_supplied_own_pid() {
        let facts = Facts::new();
        assert_eq!(recall_reply(&facts, "pid", 4242), "4242");
    }

    #[test]
    fn recall_known_key_returns_the_stored_fact() {
        let mut facts = Facts::new();
        facts.insert("codeword".to_string(), "VIOLET-7F29".to_string());
        assert_eq!(recall_reply(&facts, "codeword", 1), "VIOLET-7F29");
    }

    #[test]
    fn recall_unknown_key_returns_empty() {
        let facts = Facts::new();
        assert_eq!(recall_reply(&facts, "nope", 1), "");
    }
}
