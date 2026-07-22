#![allow(dead_code)]
//! Phase-2B (R2) test support for `wasl-runtime-protocol`.
//!
//! Per `docs/specs/runtime.md` §14 this is the **layer-2** testability tier —
//! the "process and framing plumbing" layer, distinct from R1's scripted
//! in-memory `InteractiveTransport` tests (layer 1) and the real-CLI
//! `--features live-cli` tests (layer 3). Everything here drives a **REAL OS
//! child process** — the long-lived in-repo fake structured agent — through the
//! `FakeLineProtocol` driver, exactly as the adapter crates' `process_invoker`
//! tests drive real children via stand-in commands, but here against a
//! purpose-built persistent line-oriented-JSON agent (runtime-protocol.md §1).
//!
//! ## Ownership of the fake agent SOURCE (coder vs tester territory)
//!
//! The fake agent BINARY and the `FakeLineProtocol` driver are **coder
//! territory** — they live under `crates/wasl-runtime-protocol/src/**`
//! (`src/bin/wasl-fake-agent.rs` for the dev-bin; the driver in the crate
//! `src`). This module (under `tests/**`) only *references* them:
//!   * the driver via `wasl_runtime_protocol::FakeLineProtocol` (public API);
//!   * the dev-bin via `env!("CARGO_BIN_EXE_wasl-fake-agent")` — the SAME
//!     mechanism `wasl-mcp/tests/config_bin.rs` uses to reach its own `[[bin]]`
//!     (`env!("CARGO_BIN_EXE_wasl-mcpd")`). Cargo sets `CARGO_BIN_EXE_<name>`
//!     for every `[[bin]]` declared IN THIS CRATE when compiling this crate's
//!     integration tests, so declaring the fake agent as a `[[bin]]` here is how
//!     it is exposed to these tests. (No `Cargo.toml` edit is done by the
//!     tester — that is coder/`Cargo.toml` territory; see the "CODER MUST ADD"
//!     block below.)
//!
//! ## CODER MUST ADD (Phase-2B `src/**` + `Cargo.toml`), referenced below
//!
//! The empty R2 skeleton lacks all of this; these tests are the RED spec for it.
//! NAMES are chosen minimal and cited to `docs/specs/runtime-protocol.md`; if the
//! coder names something differently, THIS module is the single re-map point.
//!
//! ```ignore
//! // ---- Cargo.toml (coder territory) ----
//! // [[bin]]
//! // name = "wasl-fake-agent"
//! // path = "src/bin/wasl-fake-agent.rs"
//! // (+ a `serde`/`serde_json`/`tokio` dep already present; the bin reads
//! //  line-oriented JSON on stdin and writes one JSON event per line on stdout.)
//!
//! // ---- src/bin/wasl-fake-agent.rs (coder territory) — the long-lived fake ----
//! // The persistent structured agent (runtime-protocol.md §1.1). Its REQUEST
//! // wire (one JSON object per line on stdin) is a `{"cmd": "...", ...}` line;
//! // its EVENT wire (one JSON object per line on stdout) is exactly the §1.2
//! // table (`{"ev":"ready","native":...}` etc). Behaviors REQUIRED by §1.1/§1.3:
//! //   * stays running across many turns over a never-EOF stdin;
//! //   * `remember K=V` / `recall K` in-memory fact map (context retention in
//! //     the SAME process); `recall pid` MUST reply with the fake's own OS PID
//! //     (decimal) so a test can prove one process served both turns;
//! //   * `hold` — block the turn until a `release` request line arrives
//! //     (deterministic queueing, no sleeps);
//! //   * `crash` — exit mid-turn with a chosen non-zero code;
//! //   * `gate` — emit a `{"ev":"gate",...}` then either wait or exit;
//! //   * a `spin` / non-terminating mode — an infinite loop that emits NO
//! //     terminal event and never exits (the hang-watchdog target, §1.4);
//! //   * scripted HOSTILE-WIRE emission on demand (§1.3 list) so the driver's
//! //     line framing is exercised against adversarial bytes;
//! //   * writes some lines to STDERR (so the driver's stderr-drain is exercised)
//! //     without ever blocking the turn.
//! // The EXACT request-line vocabulary the tester scripts is enumerated in the
//! // `req_*` helpers below — the coder implements the fake to honor them.
//!
//! // ---- src (coder territory) — the FakeLineProtocol driver + its ctor ----
//! pub struct FakeLineProtocol { /* private: bin path, spawn config */ }
//! impl FakeLineProtocol {
//!     /// Build a driver that spawns the given fake-agent binary path.
//!     /// (`SpawnOptions` fields are crate-private in R1; the R2 driver owns its
//!     ///  own constructor that names the bin — runtime-protocol.md §1.)
//!     pub fn new(bin: impl Into<std::path::PathBuf>) -> Self;
//!     /// The escalation/hang-watchdog timeouts the supervision shell uses, made
//!     /// injectable so the supervision tests run FAST (short bounded timeouts,
//!     /// never minutes — runtime-protocol.md §1.4 + the CONSTRAINTS in this
//!     /// block's task). Each stage (interrupt-ack, SIGTERM, SIGKILL) and the
//!     /// hang watchdog is one of these.
//!     pub fn with_timeouts(bin: impl Into<std::path::PathBuf>, t: SupervisionTimeouts) -> Self;
//! }
//! impl wasl_runtime::InteractiveProtocol for FakeLineProtocol { /* spawn/resume/capabilities */ }
//!
//! /// Injectable, SHORT supervision timeouts (§1.4). All sub-second by default in
//! /// tests so the escalation ladder + hang watchdog are provable without real
//! /// long waits.
//! pub struct SupervisionTimeouts {
//!     pub interrupt_ack: std::time::Duration,
//!     pub term_after: std::time::Duration,
//!     pub kill_after: std::time::Duration,
//!     pub hang_watchdog: std::time::Duration,
//! }
//! ```
//!
//! ## Timing discipline (CONSTRAINTS)
//!
//! No real sleeps for logic. The `hold`/`release` and hostile scripts are
//! deterministic (event-driven). Bounded `tokio::time::timeout`s appear ONLY
//! where a REAL child's real-world timing genuinely requires waiting for the OS
//! (a process actually exiting, an escalation stage elapsing) — and every such
//! bound is SHORT (sub-second to a few seconds), so the whole suite is fast.

use std::path::PathBuf;
use std::time::Duration;

use wasl_runtime::{ProtocolEvent, RuntimeError};

/// A short hang-guard so a mis-behaving driver fails the test FAST instead of
/// hanging CI forever. A correct driver never trips this on the happy paths;
/// the supervision tests use even tighter injected timeouts.
pub const HANG_GUARD: Duration = Duration::from_secs(5);

/// The path to the compiled fake-agent dev-bin. Uses `CARGO_BIN_EXE_<name>`,
/// which Cargo defines for a `[[bin]]` declared in THIS crate when building this
/// crate's integration tests (mirrors `wasl-mcp/tests/config_bin.rs`).
///
/// CODER MUST ADD the `[[bin]] name = "wasl-fake-agent"` entry to
/// `crates/wasl-runtime-protocol/Cargo.toml`, or this `env!` fails to expand and
/// the whole suite stays RED (expected in Phase-2B until the bin exists).
pub fn fake_agent_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_wasl-fake-agent"))
}

// =====================================================================
// Request-line builders — the EXACT wire the tester writes into the fake's
// stdin (one JSON object per line). The coder implements the fake to honor
// these. Kept as string builders (not typed) so the tester controls the exact
// bytes, incl. deliberately malformed ones for the §1.3 hostile scripts.
// =====================================================================

/// `remember K=V` — store a fact in the fake's in-memory map (§1.1).
pub fn req_remember(k: &str, v: &str) -> String {
    format!(r#"{{"cmd":"remember","key":"{k}","value":"{v}"}}"#)
}

/// `recall K` — reply with the stored fact (proves retention in-process, §1.1).
pub fn req_recall(k: &str) -> String {
    format!(r#"{{"cmd":"recall","key":"{k}"}}"#)
}

/// `recall pid` — reply with the fake's own OS PID (decimal). The SAME-process
/// proof (§1.1): a second turn returning the same PID string proves one OS
/// process served both turns.
pub fn req_recall_pid() -> String {
    req_recall("pid")
}

/// A plain turn that completes with `reply`.
pub fn req_say(reply: &str) -> String {
    format!(r#"{{"cmd":"say","reply":"{reply}"}}"#)
}

/// `hold` — accept the turn, then block until a `release` line arrives (§1.1).
pub fn req_hold() -> String {
    r#"{"cmd":"hold"}"#.to_string()
}

/// `release` — release a held turn so it completes with `reply`.
pub fn req_release(reply: &str) -> String {
    format!(r#"{{"cmd":"release","reply":"{reply}"}}"#)
}

/// `crash` — exit mid-turn with `code` (§1.1) — recovery + ProcessExited.
pub fn req_crash(code: i32) -> String {
    format!(r#"{{"cmd":"crash","code":{code}}}"#)
}

/// `gate` — emit a `{"ev":"gate",...}` then wait to be resolved (§1.1).
pub fn req_gate(summary: &str, preview: &str) -> String {
    format!(r#"{{"cmd":"gate","summary":"{summary}","preview":"{preview}"}}"#)
}

/// `gate` then process exit — the §1.3 "gate-then-exit" script.
pub fn req_gate_then_exit(summary: &str, preview: &str, code: i32) -> String {
    format!(
        r#"{{"cmd":"gate_then_exit","summary":"{summary}","preview":"{preview}","code":{code}}}"#
    )
}

/// `spin` — a NON-TERMINATING turn: never emits a terminal event, never exits
/// (the hang-watchdog target — §1.4, mirrors opencode's overflow loop).
pub fn req_spin() -> String {
    r#"{"cmd":"spin"}"#.to_string()
}

/// `ignore_interrupt` — accept the turn but NEVER ack an interrupt (§1.3
/// "ignored interrupt"): the actor's `Interrupting` timeout must apply.
pub fn req_ignore_interrupt() -> String {
    r#"{"cmd":"ignore_interrupt"}"#.to_string()
}

/// `stop_reading` — the child stops reading its stdin (but keeps its stdout open
/// briefly) so a subsequent driver write hits `BrokenPipe`/`WriteZero` (the M2
/// EPIPE lesson — must be benign, not a call failure).
pub fn req_stop_reading_stdin() -> String {
    r#"{"cmd":"stop_reading_stdin"}"#.to_string()
}

// --- §1.3 hostile-wire emission scripts (the fake emits adversarial bytes) ---

/// Emit a single valid event OBJECT split across multiple stdin reads (the fake
/// writes it in >1 partial flushes) — the driver's line buffer must reassemble
/// it (§1.3 "split JSON across reads").
pub fn req_emit_split_completion(reply: &str) -> String {
    format!(r#"{{"cmd":"emit_split_done","reply":"{reply}"}}"#)
}

/// Emit MULTIPLE event objects in ONE stdin write (§1.3 "multiple messages in
/// one read"): accepted+delta+done all in a single flush.
pub fn req_emit_batched_turn(delta: &str, reply: &str) -> String {
    format!(r#"{{"cmd":"emit_batched","delta":"{delta}","reply":"{reply}"}}"#)
}

/// Emit a MALFORMED JSON line BETWEEN two valid events (§1.3 "malformed JSON
/// between valid events" — ignored + logged, framing continues).
pub fn req_emit_malformed_between(delta: &str, reply: &str) -> String {
    format!(r#"{{"cmd":"emit_malformed_between","delta":"{delta}","reply":"{reply}"}}"#)
}

/// Complete a turn WITHOUT ever emitting `accepted` (§1.3 "missing accepted":
/// the turn proceeds straight to `done`).
pub fn req_emit_done_without_accept(reply: &str) -> String {
    format!(r#"{{"cmd":"emit_done_no_accept","reply":"{reply}"}}"#)
}

/// Emit `done` TWICE (§1.3 "duplicate completion": the second is ignored — no
/// second TurnCompleted).
pub fn req_emit_duplicate_done(reply: &str) -> String {
    format!(r#"{{"cmd":"emit_duplicate_done","reply":"{reply}"}}"#)
}

/// Emit a `delta` AFTER `done` (§1.3 "delta-after-completion": ignored — an
/// event referencing a completed turn, R1 §7.4).
pub fn req_emit_delta_after_done(reply: &str, late_delta: &str) -> String {
    format!(r#"{{"cmd":"emit_delta_after_done","reply":"{reply}","late":"{late_delta}"}}"#)
}

/// EOF the stdout with no explicit exit code path (§1.3 "EOF without a
/// ProcessExited-worthy code" → `ProcessExited{code: None}`). The fake closes
/// stdout and exits via a signal-like path so `code` is `None`.
pub fn req_eof_without_code() -> String {
    r#"{"cmd":"eof_without_code"}"#.to_string()
}

/// Buffer several events THEN exit before the driver has drained them (§1.3
/// "exit-before-stdout-drained": drain buffered events first, THEN ProcessExited).
pub fn req_emit_then_exit(delta: &str, reply: &str, code: i32) -> String {
    format!(r#"{{"cmd":"emit_then_exit","delta":"{delta}","reply":"{reply}","code":{code}}}"#)
}

/// Write a burst of lines to STDERR (the driver's stderr-drain must not deadlock
/// the stdout pipe — §1.4).
pub fn req_flood_stderr(lines: usize) -> String {
    format!(r#"{{"cmd":"flood_stderr","lines":{lines}}}"#)
}

// =====================================================================
// Event-classification helpers (assertion sugar over ProtocolEvent).
// =====================================================================

pub fn is_session_ready(ev: &ProtocolEvent) -> bool {
    matches!(ev, ProtocolEvent::SessionReady { .. })
}

pub fn is_accepted(ev: &ProtocolEvent) -> bool {
    matches!(ev, ProtocolEvent::TurnAccepted { .. })
}

pub fn delta_text(ev: &ProtocolEvent) -> Option<&str> {
    match ev {
        ProtocolEvent::AssistantDelta { text } => Some(text.as_str()),
        _ => None,
    }
}

pub fn completed_reply(ev: &ProtocolEvent) -> Option<&str> {
    match ev {
        ProtocolEvent::TurnCompleted { reply } => Some(reply.as_str()),
        _ => None,
    }
}

pub fn exit_code(ev: &ProtocolEvent) -> Option<Option<i32>> {
    match ev {
        ProtocolEvent::ProcessExited { code } => Some(*code),
        _ => None,
    }
}

pub fn is_gate(ev: &ProtocolEvent) -> bool {
    matches!(ev, ProtocolEvent::NativeGateRequested { .. })
}

/// Count how many events in a drained sequence are `TurnCompleted` — the key
/// no-double-completion assertion for the duplicate-done hostile script.
pub fn count_completions(evs: &[ProtocolEvent]) -> usize {
    evs.iter()
        .filter(|e| matches!(e, ProtocolEvent::TurnCompleted { .. }))
        .count()
}

/// Count `ProcessExited` events — must be at most one (no re-emitted terminal
/// event, §1.3).
pub fn count_process_exits(evs: &[ProtocolEvent]) -> usize {
    evs.iter()
        .filter(|e| matches!(e, ProtocolEvent::ProcessExited { .. }))
        .count()
}

// =====================================================================
// Bounded drain helpers over an `InteractiveTransport` (a REAL child).
// Every wait is a SHORT `tokio::time::timeout` hang-guard — the sequence is
// driven by the child's real stdout, not by sleeping.
// =====================================================================

use wasl_runtime::InteractiveTransport;

/// Pull the next event, failing the test with `ctx` if the child produces
/// nothing within `HANG_GUARD` (a correct driver never hits this on a live turn;
/// `None` from `next_event` — a clean end — is returned as `None`).
pub async fn next_or_hang(tx: &mut dyn InteractiveTransport, ctx: &str) -> Option<ProtocolEvent> {
    match tokio::time::timeout(HANG_GUARD, tx.next_event()).await {
        Ok(ev) => ev,
        Err(_) => panic!("driver hung waiting for event: {ctx}"),
    }
}

/// Drain events until the FIRST terminal event (`TurnCompleted`/`TurnFailed`/
/// `ProcessExited`) inclusive, or the stream ends, or the hang-guard trips.
/// Returns the events collected. Deterministic: bounded ONLY by the child's real
/// output + a short per-event hang-guard.
pub async fn drain_to_terminal(tx: &mut dyn InteractiveTransport, ctx: &str) -> Vec<ProtocolEvent> {
    let mut out = Vec::new();
    loop {
        match next_or_hang(tx, ctx).await {
            Some(ev) => {
                let terminal = matches!(
                    ev,
                    ProtocolEvent::TurnCompleted { .. }
                        | ProtocolEvent::TurnFailed { .. }
                        | ProtocolEvent::ProcessExited { .. }
                );
                out.push(ev);
                if terminal {
                    return out;
                }
            }
            None => return out,
        }
    }
}

/// Drain until `ProcessExited` (inclusive) or stream end/hang-guard. Used by the
/// exit/crash scripts that MUST end in a `ProcessExited` and MUST NOT hang.
pub async fn drain_to_process_exit(
    tx: &mut dyn InteractiveTransport,
    ctx: &str,
) -> Vec<ProtocolEvent> {
    let mut out = Vec::new();
    loop {
        match next_or_hang(tx, ctx).await {
            Some(ev) => {
                let exited = matches!(ev, ProtocolEvent::ProcessExited { .. });
                out.push(ev);
                if exited {
                    return out;
                }
            }
            None => return out,
        }
    }
}

/// Assert a `send_turn` result treated `BrokenPipe`/`WriteZero` as BENIGN — i.e.
/// it is `Ok(())` (the M2 EPIPE lesson: a child that stopped reading stdin must
/// not fail the whole call). A `RuntimeError::Transport` mentioning broken-pipe
/// is the FAILING (pre-fix) shape.
pub fn assert_write_benign(res: &Result<(), RuntimeError>, ctx: &str) {
    assert!(
        res.is_ok(),
        "send_turn to a child that stopped reading stdin must be benign \
         (BrokenPipe/WriteZero swallowed), got {res:?} — {ctx}"
    );
}
