//! `FakeLineProtocol` — the R2 driver over the long-lived in-repo fake
//! structured agent's line-oriented JSON protocol (`docs/specs/runtime-protocol.md`
//! §1). It spawns the `wasl-fake-agent` dev-bin as a REAL OS child, speaks its
//! §1.2 wire over real piped stdio, and maps each JSONL event to R1's generic
//! [`wasl_runtime::ProtocolEvent`] (the §1.2 mapping table). It also implements
//! the §1.4 process supervision the R2 IO shell owns: concurrent stderr draining,
//! a hang watchdog, a bounded interrupt→SIGTERM→SIGKILL escalation ladder, the
//! M2 `BrokenPipe`/`WriteZero`-is-benign stdin fix (mirroring the adapter
//! crates), and reaping-on-drop (no zombie/orphan child).

use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use crate::bounded_io::{
    discard_one_bounded_line, read_bounded_line, BoundedLineRead, EVENT_CHANNEL_CAPACITY,
    MAX_STDOUT_LINE_BYTES,
};
use crate::process_supervision::{best_effort_signal, escalate, next_wait, wait_for_exit};
use async_trait::async_trait;
use serde_json::Value;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[cfg(unix)]
use nix::sys::signal::Signal;
#[cfg(unix)]
use std::os::unix::process::CommandExt;

use wasl_runtime::{
    ComposedPrompt, InteractiveProtocol, InteractiveTransport, NativeGateDetail, NativeSessionId,
    ProtocolEvent, RuntimeCapabilities, RuntimeError, SpawnOptions,
};

/// Injectable supervision timeouts (`docs/specs/runtime-protocol.md` §1.4). Made
/// injectable so the supervision tests run FAST (short bounded stages, never
/// minutes): each escalation stage (SIGINT/interrupt-ack → SIGTERM → SIGKILL) and
/// the hang watchdog is one of these durations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupervisionTimeouts {
    /// How long to wait for the gentle interrupt (SIGINT) to take effect before
    /// escalating to SIGTERM.
    pub interrupt_ack: Duration,
    /// How long to wait after SIGTERM before escalating to SIGKILL.
    pub term_after: Duration,
    /// How long to wait after SIGKILL for the child to actually die (bounding the
    /// final reap).
    pub kill_after: Duration,
    /// If no event arrives for this long while a turn is active, the child is
    /// considered hung: the watchdog kills it and surfaces a terminal
    /// (`ProcessExited`) instead of letting the driver hang forever.
    pub hang_watchdog: Duration,
}

impl Default for SupervisionTimeouts {
    fn default() -> Self {
        // Generous defaults for the happy-path driver tests (`FakeLineProtocol::
        // new`): the watchdog never trips a well-behaved multi-turn session, and
        // the escalation ladder is only ever as slow as a cooperative child takes
        // to die (each stage races the child's real exit — see `escalate`).
        SupervisionTimeouts {
            interrupt_ack: Duration::from_secs(2),
            term_after: Duration::from_secs(2),
            kill_after: Duration::from_secs(2),
            hang_watchdog: Duration::from_secs(60),
        }
    }
}

/// How long `next_event` will block waiting for the next event on a quiescent
/// (no buffered event) turn before yielding `None` so the generic R1 actor can
/// service its mailbox (the actor parks on `None` and re-polls on `Poke`). Kept
/// LONGER than the supervision tests' injected `hang_watchdog` (200ms) so a
/// genuinely-hung turn is surfaced as a watchdog terminal rather than a spurious
/// `None`, and LONGER than the driver-level held tests' 50ms negative-check slice
/// so a held turn never resolves to `None` inside their window (it times out
/// instead, the "still held" signal). This is a bounded wait for a REAL child's
/// timing, per the §1.4 CONSTRAINTS, not a sleep driving logic.
const POLL_IDLE: Duration = Duration::from_millis(500);

/// The `FakeLineProtocol` driver: spawns the given fake-agent binary and maps its
/// line wire onto [`ProtocolEvent`].
pub struct FakeLineProtocol {
    bin: PathBuf,
    timeouts: SupervisionTimeouts,
}

impl FakeLineProtocol {
    /// Build a driver that spawns the given fake-agent binary path with default
    /// (generous) supervision timeouts.
    pub fn new(bin: impl Into<PathBuf>) -> Self {
        FakeLineProtocol {
            bin: bin.into(),
            timeouts: SupervisionTimeouts::default(),
        }
    }

    /// Build a driver with injected (typically SHORT) supervision timeouts so the
    /// escalation ladder + hang watchdog are provable FAST (§1.4).
    pub fn with_timeouts(bin: impl Into<PathBuf>, t: SupervisionTimeouts) -> Self {
        FakeLineProtocol {
            bin: bin.into(),
            timeouts: t,
        }
    }

    /// The honest fixed capability report for the fake (§14: the fake proves the
    /// state machine, so — unlike a real un-captured CLI — it may claim a full
    /// managed report). Never all-false (an all-false report is a FAIL).
    fn caps() -> RuntimeCapabilities {
        RuntimeCapabilities {
            persistent_process: true,
            persistent_conversation: true,
            structured_turns: true,
            streaming_output: true,
            attachable: true,
            interruptible: true,
            structured_provenance: true,
            resumable: true,
        }
    }

    async fn spawn_child(&self) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        let mut cmd = Command::new(&self.bin);
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // Belt-and-braces: even if the supervisor task is torn down abnormally
            // the kernel SIGKILLs the child when the handle drops (no orphan).
            .kill_on_drop(true);
        #[cfg(unix)]
        {
            // Put the child in its own process group so escalation can target the
            // whole session (leader + descendants) via a negative pid.
            cmd.as_std_mut().process_group(0);
        }
        let mut child = cmd.spawn().map_err(|e| RuntimeError::SpawnFailed {
            detail: format!("spawn {}: {e}", self.bin.display()),
        })?;

        let pid = child.id();
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| RuntimeError::SpawnFailed {
                detail: "fake agent: no stdin pipe".to_string(),
            })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| RuntimeError::SpawnFailed {
                detail: "fake agent: no stdout pipe".to_string(),
            })?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| RuntimeError::SpawnFailed {
                detail: "fake agent: no stderr pipe".to_string(),
            })?;

        // Concurrently drain stderr so a flooding child never blocks on its
        // stderr write and wedges the stdout event pipe (§1.4). The reader task
        // discards the bytes (they are audit-noise for the fake) but MUST keep
        // reading so the pipe never fills.
        let stderr_task = tokio::spawn(async move {
            // Bounded the same way as stdout (§4.4): an unbounded
            // `BufReader::lines()` here would let a pathological single
            // stderr line (a runaway tool dumping a huge stack trace/blob)
            // grow this drain's buffer without limit — the exact bug the
            // stdout fix closed, just on the other pipe. Uses the
            // allocation-free discard variant (the content is thrown away
            // either way, so there's no reason to build a `String` per line).
            let mut reader = BufReader::new(stderr);
            while let Ok(true) = discard_one_bounded_line(&mut reader, MAX_STDOUT_LINE_BYTES).await
            {
                // drain — never block the child on a full stderr pipe.
            }
        });

        // The event channel the supervisor pushes mapped `ProtocolEvent`s into.
        let (ev_tx, ev_rx) = mpsc::channel::<ProtocolEvent>(EVENT_CHANNEL_CAPACITY);
        // The kill-request channel: the transport (or watchdog) asks the
        // supervisor to run the escalation ladder now. Dropping the sender (on
        // transport drop) is itself the request.
        let (kill_tx, kill_rx) = mpsc::channel::<()>(1);

        let timeouts = self.timeouts;
        let supervisor = tokio::spawn(supervise(
            child,
            stdout,
            ev_tx,
            kill_rx,
            stderr_task,
            timeouts,
        ));

        Ok(Box::new(FakeLineTransport {
            stdin: Some(stdin),
            events: ev_rx,
            kill_tx: Some(kill_tx),
            supervisor: Some(supervisor),
            pid,
            timeouts,
            turn_in_flight: false,
            last_event_at: Instant::now(),
            ended: false,
            turn_terminal_seen: false,
        }))
    }
}

#[async_trait]
impl InteractiveProtocol for FakeLineProtocol {
    fn capabilities(&self) -> RuntimeCapabilities {
        Self::caps()
    }

    async fn spawn(
        &self,
        _opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        // The fake agent needs no options: it mints its own native id and holds
        // its own in-memory state. (`SpawnOptions` fields are crate-private in R1;
        // the R2 driver owns its bin via its own constructor — §1.)
        self.spawn_child().await
    }

    async fn resume(
        &self,
        _native: &NativeSessionId,
        _opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        // The fake is a fresh persistent process per spawn; "resume" re-spawns a
        // new one (the fake cannot re-attach to a dead process's in-memory map —
        // resume scope is one node-process lifetime, and here that is one spawn).
        self.spawn_child().await
    }
}

/// The live connection to ONE running fake-agent process.
struct FakeLineTransport {
    stdin: Option<ChildStdin>,
    events: mpsc::Receiver<ProtocolEvent>,
    kill_tx: Option<mpsc::Sender<()>>,
    supervisor: Option<JoinHandle<()>>,
    pid: Option<u32>,
    timeouts: SupervisionTimeouts,
    /// A turn has been injected (`send_turn`) and no terminal has been delivered
    /// for it yet — governs the watchdog + the quiescent-wait behavior.
    turn_in_flight: bool,
    /// When the last event was observed (watchdog reset point).
    last_event_at: Instant,
    /// The event stream has ended (a `ProcessExited` was delivered / the channel
    /// closed) — subsequent `next_event` calls return `None` (no re-emit).
    ended: bool,
    /// The current turn's terminal (`done`/`failed`) has already been surfaced.
    /// While set, any further NON-process-exit event referencing this completed
    /// turn — a duplicate `done`, or a `delta`/tool event AFTER `done` — is
    /// IGNORED (§1.3 duplicate-completion / delta-after-completion; R1 §7.4 "an
    /// event referencing a completed turn"). Cleared on the next `send_turn`.
    turn_terminal_seen: bool,
}

impl FakeLineTransport {
    /// Write ONE `text` line (plus the newline frame) to the child's stdin and
    /// flush, applying the M2 `BrokenPipe`/`WriteZero`-is-benign discipline
    /// (mirrors `wasl-adapter-{codex,claude}::ProcessCliInvoker`). Shared by BOTH
    /// stdin-writing paths — `send_turn` (a new turn's request line) and
    /// `answer_tool_call` (a mid-turn tool-call answer over the SAME pipe) — so
    /// the exact write+flush+EPIPE handling is identical for both.
    async fn write_stdin_line(&mut self, text: &str) -> Result<(), RuntimeError> {
        let Some(stdin) = self.stdin.as_mut() else {
            // stdin already gone (child exited): a write to a dead child is the
            // benign EPIPE case — swallow it (the M2 lesson), do not fail.
            return Ok(());
        };

        let mut line = text.as_bytes().to_vec();
        line.push(b'\n');

        // M2 EPIPE lesson: a child that stopped reading its stdin (or exited)
        // yields BrokenPipe / WriteZero on the write — that is BENIGN (swallowed),
        // NOT a failure of the whole call. Any other IO error is a real transport
        // error.
        match stdin.write_all(&line).await {
            Ok(()) => {}
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::WriteZero
                ) =>
            {
                return Ok(());
            }
            Err(e) => {
                return Err(RuntimeError::Transport {
                    detail: format!("fake agent stdin write: {e}"),
                })
            }
        }
        match stdin.flush().await {
            Ok(()) => Ok(()),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::WriteZero
                ) =>
            {
                Ok(())
            }
            Err(e) => Err(RuntimeError::Transport {
                detail: format!("fake agent stdin flush: {e}"),
            }),
        }
    }

    /// Note a returned event: reset the watchdog and clear the in-flight flag on a
    /// terminal.
    fn observe(&mut self, ev: &ProtocolEvent) {
        self.last_event_at = Instant::now();
        match ev {
            ProtocolEvent::TurnCompleted { .. } | ProtocolEvent::TurnFailed { .. } => {
                self.turn_in_flight = false;
                self.turn_terminal_seen = true;
            }
            ProtocolEvent::ProcessExited { .. } => {
                self.turn_in_flight = false;
                self.ended = true;
            }
            _ => {}
        }
    }

    /// Should a freshly-pulled event be IGNORED as referencing an already-
    /// completed turn (§1.3 / R1 §7.4)? True for any non-process-exit event once
    /// this turn's terminal was surfaced: a duplicate `done`, or a late
    /// `delta`/tool event after `done`. `ProcessExited` is the transport terminal
    /// and always passes through.
    fn is_stale_after_terminal(&self, ev: &ProtocolEvent) -> bool {
        self.turn_terminal_seen && !matches!(ev, ProtocolEvent::ProcessExited { .. })
    }

    /// Fire the hang watchdog: ask the supervisor to kill the child, then await
    /// the resulting terminal `ProcessExited` (§1.4). Returns the terminal event
    /// (or `None` if the channel closed without one — treated as a clean end).
    /// Returns the RAW event (the caller's filter loop `observe`s it).
    async fn fire_watchdog(&mut self) -> Option<ProtocolEvent> {
        if let Some(tx) = &self.kill_tx {
            let _ = tx.send(()).await;
        }
        // The supervisor runs the escalation ladder then pushes `ProcessExited`.
        match self.events.recv().await {
            Some(ev) => Some(ev),
            None => {
                self.ended = true;
                None
            }
        }
    }

    /// Pull one RAW mapped event from the supervisor channel (no staleness
    /// filtering, no `observe`), applying the quiescent-wait + hang-watchdog
    /// timing. Returns `None` on a clean end of stream.
    async fn pull_one(&mut self) -> Option<ProtocolEvent> {
        if self.ended {
            return None;
        }
        // Fast path: an event is already buffered.
        match self.events.try_recv() {
            Ok(ev) => return Some(ev),
            Err(mpsc::error::TryRecvError::Disconnected) => {
                self.ended = true;
                return None;
            }
            Err(mpsc::error::TryRecvError::Empty) => {}
        }

        // Nothing buffered right now. While a turn is in flight, wait the full
        // remaining watchdog budget; mailbox responsiveness is handled at actor
        // level by racing mailbox commands against `next_event`.
        let wait = next_wait(
            self.turn_in_flight,
            self.last_event_at.elapsed(),
            self.timeouts.hang_watchdog,
            POLL_IDLE,
        );

        match tokio::time::timeout(wait, self.events.recv()).await {
            Ok(Some(ev)) => Some(ev),
            Ok(None) => {
                // The channel closed (supervisor finished): clean end of stream.
                self.ended = true;
                None
            }
            Err(_) => {
                // The slice elapsed with no event. If a turn is in flight and the
                // FULL hang-watchdog has now elapsed, the child is hung: kill it
                // and surface the terminal (§1.4). Otherwise yield `None` so the
                // caller (driver test / generic actor) can make progress.
                if self.turn_in_flight
                    && self.last_event_at.elapsed() >= self.timeouts.hang_watchdog
                {
                    self.fire_watchdog().await
                } else {
                    None
                }
            }
        }
    }
}

#[async_trait]
impl InteractiveTransport for FakeLineTransport {
    async fn send_turn(&mut self, prompt: &ComposedPrompt) -> Result<(), RuntimeError> {
        // Mark a turn as in flight for the watchdog / quiescent-wait bookkeeping.
        // A fresh turn resets the "terminal already seen" suppression so the new
        // turn's events flow again.
        self.turn_in_flight = true;
        self.turn_terminal_seen = false;
        self.last_event_at = Instant::now();

        // One request per line: the composed prompt text IS the JSON request line
        // (the tester controls the exact bytes); the helper appends the newline
        // frame, flushes, and applies the M2 EPIPE-is-benign discipline.
        self.write_stdin_line(&prompt.0).await
    }

    async fn next_event(&mut self) -> Option<ProtocolEvent> {
        // Cancellation-safety audit: once `pull_one()` resolves with a concrete
        // event, this method performs no further awaits before returning it.
        // Pull events, DROPPING any that reference an already-completed turn
        // (duplicate `done`, `delta`-after-`done`, §1.3 / R1 §7.4) — never a
        // re-emitted terminal, never a spurious late delta. `ProcessExited` always
        // passes. A dropped event does not end the stream; keep pulling.
        loop {
            match self.pull_one().await {
                Some(ev) if self.is_stale_after_terminal(&ev) => {
                    // Ignore + continue framing (do NOT surface, do NOT end).
                    continue;
                }
                Some(ev) => {
                    self.observe(&ev);
                    return Some(ev);
                }
                None => return None,
            }
        }
    }

    async fn interrupt(&mut self) -> Result<(), RuntimeError> {
        // Best-effort interrupt of the active turn: send a single gentle SIGINT to
        // the child and return promptly. An agent that IGNORES it (the §1.3
        // ignored-interrupt script) simply keeps running — we NEVER fabricate a
        // cancellation ack, and we NEVER hang (the send is fire-and-forget).
        if let Some(pid) = self.pid {
            if let Ok(raw_pid) = i32::try_from(pid) {
                best_effort_signal(raw_pid, Signal::SIGINT);
            }
        }
        Ok(())
    }

    async fn answer_tool_call(&mut self, _name: &str, value: &str) -> Result<(), RuntimeError> {
        // The tool-channel answer-back (§1 bidirectional callback): the fake's
        // `ask_wasl_tool` command emitted `tool_start` then BLOCKED reading exactly
        // ONE more stdin line — this write unblocks it, so the turn can proceed to
        // `tool_end` + `done`. We write `value` as that one line over the SAME pipe
        // `send_turn` writes to, using the IDENTICAL write+flush+EPIPE-is-benign
        // path (the fake's parser needs only a non-empty line; it does not parse
        // the payload). BrokenPipe/WriteZero here is benign for the same M2 reason:
        // the child may have already moved on / exited.
        self.write_stdin_line(value).await
    }
}

impl Drop for FakeLineTransport {
    fn drop(&mut self) {
        // Reap on drop — no zombie / orphan (§1.4). Ask the supervisor to run the
        // escalation ladder (dropping `kill_tx` also signals it), then let the
        // supervisor task run to completion detached so it `wait()`s the child.
        // `kill_on_drop(true)` on the child is the kernel backstop even if the
        // supervisor is already gone.
        if let Some(tx) = self.kill_tx.take() {
            // Non-blocking best-effort; the drop below is the guaranteed signal.
            let _ = tx.try_send(());
            drop(tx);
        }
        // Detach the supervisor so it finishes reaping after we return (do NOT
        // abort it — aborting before it `wait()`s would leak a zombie).
        if let Some(handle) = self.supervisor.take() {
            drop(handle);
        }
    }
}

/// The per-child supervisor task: reads + maps stdout events, drains stderr
/// (already spawned), reaps normal/abnormal exit, and runs the bounded escalation
/// ladder on a kill request. It owns the `Child` so it can `wait()` (reap) it.
async fn supervise(
    mut child: tokio::process::Child,
    stdout: tokio::process::ChildStdout,
    ev_tx: mpsc::Sender<ProtocolEvent>,
    mut kill_rx: mpsc::Receiver<()>,
    stderr_task: JoinHandle<()>,
    timeouts: SupervisionTimeouts,
) {
    let mut lines = BufReader::new(stdout);
    let pid = child.id();
    let signal_target = pid.and_then(|p| i32::try_from(p).ok()).map(|p| -p);

    loop {
        tokio::select! {
            // A stdout line, EOF, or a read error.
            line = read_bounded_line(&mut lines, MAX_STDOUT_LINE_BYTES) => {
                match line {
                    Ok(BoundedLineRead::Line(line)) => {
                        if let Some(ev) = map_event_line(&line) {
                            // If the receiver is gone the transport was dropped —
                            // stop pushing; the kill branch / drop handles reaping.
                            if ev_tx.send(ev).await.is_err() {
                                break;
                            }
                        }
                        // Non-event / malformed lines are ignored: the framing
                        // continues (§1.3 malformed-between).
                    }
                    // EOF (or a read error): stdout can close while the child is
                    // still alive. Do NOT block in an unbounded wait: keep
                    // servicing kill requests and perform only bounded exit polls.
                    Ok(BoundedLineRead::TooLong) => {
                        let _ = ev_tx
                            .send(ProtocolEvent::TurnFailed {
                                error: format!(
                                    "fake agent stdout line exceeded {} bytes",
                                    MAX_STDOUT_LINE_BYTES
                                ),
                            })
                            .await;
                        let code = escalate(
                            &mut child,
                            signal_target,
                            timeouts.interrupt_ack,
                            timeouts.term_after,
                            timeouts.kill_after,
                        )
                        .await;
                        let _ = ev_tx.send(ProtocolEvent::ProcessExited { code }).await;
                        break;
                    }
                    Ok(BoundedLineRead::Eof) | Err(_) => {
                        loop {
                            tokio::select! {
                                msg = kill_rx.recv() => {
                                    let _ = msg;
                                    let code = escalate(
                                        &mut child,
                                        signal_target,
                                        timeouts.interrupt_ack,
                                        timeouts.term_after,
                                        timeouts.kill_after,
                                    )
                                    .await;
                                    let _ = ev_tx.send(ProtocolEvent::ProcessExited { code }).await;
                                    break;
                                }
                                code = wait_for_exit(&mut child, POLL_IDLE) => {
                                    if let Some(code) = code {
                                        let _ = ev_tx.send(ProtocolEvent::ProcessExited { code }).await;
                                        break;
                                    }
                                }
                            }
                        }
                        break;
                    }
                }
            }
            // A kill request (watchdog fired, or the transport is shutting down).
            msg = kill_rx.recv() => {
                let _ = msg; // `Some(())` explicit request or `None` sender dropped
                let code = escalate(
                    &mut child,
                    signal_target,
                    timeouts.interrupt_ack,
                    timeouts.term_after,
                    timeouts.kill_after,
                )
                .await;
                let _ = ev_tx.send(ProtocolEvent::ProcessExited { code }).await;
                break;
            }
        }
    }

    // Belt-and-braces reap (idempotent): ensure the child is not left a zombie
    // even if we broke out via the receiver-gone path.
    let _ = wait_for_exit(&mut child, timeouts.kill_after).await;
    // Stop draining stderr — the pipe is closed now.
    stderr_task.abort();
}

/// Map one fake event line onto a [`ProtocolEvent`] per the §1.2 table. A
/// malformed or unrecognized line yields `None` (ignored — framing continues,
/// §1.3). Robust to split/batched framing because `lines()` already reassembles
/// whole lines regardless of how the child flushed them.
fn map_event_line(line: &str) -> Option<ProtocolEvent> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    let v: Value = serde_json::from_str(trimmed).ok()?;
    let ev = v.get("ev").and_then(Value::as_str)?;
    match ev {
        "ready" => Some(ProtocolEvent::SessionReady {
            native: v
                .get("native")
                .and_then(Value::as_str)
                .map(|s| NativeSessionId(s.to_string())),
        }),
        "accepted" => Some(ProtocolEvent::TurnAccepted { native_turn: None }),
        "delta" => Some(ProtocolEvent::AssistantDelta {
            text: v
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        }),
        "tool_start" => Some(ProtocolEvent::ToolStarted {
            name: v
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            // Optional `input` on the fake wire → the tool's JSON input; absent
            // → `Null` (mirrors a driver that doesn't surface args).
            input: v.get("input").cloned().unwrap_or(Value::Null),
        }),
        "tool_end" => Some(ProtocolEvent::ToolFinished {
            name: v
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            ok: v.get("ok").and_then(Value::as_bool).unwrap_or(false),
            // Optional `result` on the fake wire → the tool's output; absent →
            // `None` (mirrors a driver that only reports success/failure).
            result: v.get("result").cloned(),
        }),
        "gate" => Some(ProtocolEvent::NativeGateRequested {
            detail: NativeGateDetail {
                summary: v
                    .get("summary")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                preview: v
                    .get("preview")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            },
        }),
        "done" => Some(ProtocolEvent::TurnCompleted {
            reply: v
                .get("reply")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        }),
        "failed" => Some(ProtocolEvent::TurnFailed {
            error: v
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        }),
        // Unknown event name: ignore (framing continues).
        _ => None,
    }
}

// =====================================================================
// Coder-owned `#[cfg(test)]` unit tests (src/**, NOT tests/**): the pure
// `map_event_line` §1.2 mapping table (every event row, incl. the tool/failed
// rows the black-box suite's fake never emits, plus the missing-field
// `.unwrap_or("")` fallbacks and the malformed/empty/unknown drops) and the
// small pure accessors. These prove the mapping function is total and correct
// WITHOUT spawning a process — the pure-function counterpart to the black-box
// wire tests, exactly the split `wasl-adapter-*` use for `parse_reply`.
// =====================================================================
#[cfg(test)]
mod tests {
    use super::*;

    fn m(line: &str) -> Option<ProtocolEvent> {
        map_event_line(line)
    }

    #[test]
    fn maps_ready_with_and_without_native() {
        assert_eq!(
            m(r#"{"ev":"ready","native":"ses-1"}"#),
            Some(ProtocolEvent::SessionReady {
                native: Some(NativeSessionId("ses-1".to_string())),
            })
        );
        // Missing `native` → None native (still a SessionReady).
        assert_eq!(
            m(r#"{"ev":"ready"}"#),
            Some(ProtocolEvent::SessionReady { native: None })
        );
    }

    #[test]
    fn maps_accepted() {
        assert_eq!(
            m(r#"{"ev":"accepted"}"#),
            Some(ProtocolEvent::TurnAccepted { native_turn: None })
        );
    }

    #[test]
    fn maps_delta_with_and_without_text() {
        assert_eq!(
            m(r#"{"ev":"delta","text":"hi"}"#),
            Some(ProtocolEvent::AssistantDelta {
                text: "hi".to_string()
            })
        );
        // Missing `text` → empty string fallback.
        assert_eq!(
            m(r#"{"ev":"delta"}"#),
            Some(ProtocolEvent::AssistantDelta {
                text: String::new()
            })
        );
    }

    #[test]
    fn maps_tool_start_and_end_incl_missing_fields() {
        assert_eq!(
            m(r#"{"ev":"tool_start","name":"grep","input":{"pattern":"foo"}}"#),
            Some(ProtocolEvent::ToolStarted {
                name: "grep".to_string(),
                input: serde_json::json!({"pattern": "foo"}),
            })
        );
        // Missing `name`/`input` → empty name, Null input fallback.
        assert_eq!(
            m(r#"{"ev":"tool_start"}"#),
            Some(ProtocolEvent::ToolStarted {
                name: String::new(),
                input: Value::Null,
            })
        );
        assert_eq!(
            m(r#"{"ev":"tool_end","name":"grep","ok":true,"result":"3 matches"}"#),
            Some(ProtocolEvent::ToolFinished {
                name: "grep".to_string(),
                ok: true,
                result: Some(Value::String("3 matches".to_string())),
            })
        );
        // Missing `name`/`ok`/`result` → empty name, ok=false, None result.
        assert_eq!(
            m(r#"{"ev":"tool_end"}"#),
            Some(ProtocolEvent::ToolFinished {
                name: String::new(),
                ok: false,
                result: None,
            })
        );
    }

    #[test]
    fn maps_gate_incl_missing_fields() {
        assert_eq!(
            m(r#"{"ev":"gate","summary":"s","preview":"p"}"#),
            Some(ProtocolEvent::NativeGateRequested {
                detail: NativeGateDetail {
                    summary: "s".to_string(),
                    preview: "p".to_string(),
                }
            })
        );
        assert_eq!(
            m(r#"{"ev":"gate"}"#),
            Some(ProtocolEvent::NativeGateRequested {
                detail: NativeGateDetail {
                    summary: String::new(),
                    preview: String::new(),
                }
            })
        );
    }

    #[test]
    fn maps_done_incl_missing_reply() {
        assert_eq!(
            m(r#"{"ev":"done","reply":"r"}"#),
            Some(ProtocolEvent::TurnCompleted {
                reply: "r".to_string()
            })
        );
        assert_eq!(
            m(r#"{"ev":"done"}"#),
            Some(ProtocolEvent::TurnCompleted {
                reply: String::new()
            })
        );
    }

    #[test]
    fn maps_failed_incl_missing_error() {
        assert_eq!(
            m(r#"{"ev":"failed","error":"boom"}"#),
            Some(ProtocolEvent::TurnFailed {
                error: "boom".to_string()
            })
        );
        assert_eq!(
            m(r#"{"ev":"failed"}"#),
            Some(ProtocolEvent::TurnFailed {
                error: String::new()
            })
        );
    }

    #[test]
    fn drops_empty_malformed_unknown_and_ev_less_lines() {
        // Empty / whitespace-only lines → None.
        assert_eq!(m(""), None);
        assert_eq!(m("   \t "), None);
        // Malformed JSON → None (framing continues, §1.3).
        assert_eq!(m("{not json]"), None);
        // Valid JSON with no `ev` field → None.
        assert_eq!(m(r#"{"foo":"bar"}"#), None);
        // Valid JSON, `ev` not a string → None.
        assert_eq!(m(r#"{"ev":123}"#), None);
        // Unknown event name → None.
        assert_eq!(m(r#"{"ev":"nonsense"}"#), None);
    }

    #[test]
    fn supervision_timeouts_default_is_non_trivial() {
        let d = SupervisionTimeouts::default();
        assert!(d.hang_watchdog > d.interrupt_ack);
        assert!(d.interrupt_ack > Duration::ZERO);
        assert!(d.term_after > Duration::ZERO);
        assert!(d.kill_after > Duration::ZERO);
    }

    #[test]
    fn capabilities_are_honest_non_all_false() {
        let caps = FakeLineProtocol::caps();
        assert!(caps.persistent_process);
        assert!(caps.persistent_conversation);
        assert!(caps.structured_turns);
    }
}
