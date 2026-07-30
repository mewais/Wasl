//! `ClaudeStreamProtocol` — maps the real claude `stream-json` duplex onto R1's
//! generic [`wasl_runtime::ProtocolEvent`].

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use async_trait::async_trait;
#[cfg(unix)]
use nix::sys::signal::{kill, Signal};
#[cfg(unix)]
use nix::unistd::Pid;
use serde_json::Value;
use std::io;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use uuid::Uuid;
use wasl_runtime::{
    ComposedPrompt, InteractiveProtocol, InteractiveTransport, NativeSessionId, ProtocolEvent,
    RuntimeCapabilities, RuntimeError, SpawnContext, SpawnOptions,
};

/// Identical UUIDv5 namespace to `wasl-adapter-claude::WASL_SESSION_NS`,
/// declared independently here to keep R2 decoupled from adapter crates.
const WASL_SESSION_NS: Uuid = uuid::uuid!("2441e851-1801-440d-a8e9-214d1df08e6e");

fn session_uuid(context_id: &str) -> Uuid {
    Uuid::new_v5(&WASL_SESSION_NS, context_id.as_bytes())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SupervisionTimeouts {
    interrupt_ack: Duration,
    term_after: Duration,
    kill_after: Duration,
    hang_watchdog: Duration,
    stdin_write: Duration,
}

impl Default for SupervisionTimeouts {
    fn default() -> Self {
        SupervisionTimeouts {
            interrupt_ack: Duration::from_secs(2),
            term_after: Duration::from_secs(2),
            kill_after: Duration::from_secs(2),
            hang_watchdog: Duration::from_secs(60),
            stdin_write: Duration::from_secs(15),
        }
    }
}

const POLL_IDLE: Duration = Duration::from_millis(500);
const MAX_STDOUT_LINE_BYTES: usize = 4 * 1024 * 1024;
const EVENT_CHANNEL_CAPACITY: usize = 4096;

enum BoundedLineRead {
    Line(String),
    Eof,
    TooLong,
}

async fn discard_until_newline<R: AsyncBufRead + Unpin>(reader: &mut R) -> io::Result<()> {
    loop {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            return Ok(());
        }
        if let Some(pos) = chunk.iter().position(|b| *b == b'\n') {
            reader.consume(pos + 1);
            return Ok(());
        }
        let len = chunk.len();
        reader.consume(len);
    }
}

/// Consume and discard exactly one line (bounded the same way as
/// [`read_bounded_line`]) WITHOUT ever allocating its content into a `String`.
/// Used for drains whose content is thrown away regardless (stderr, §4.4) —
/// building a `String` per line there is pure overhead for content nobody
/// reads. Returns `Ok(true)` if a line (or an EOF-terminated partial line) was
/// consumed, `Ok(false)` on a clean EOF with nothing pending.
async fn discard_one_bounded_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max_bytes: usize,
) -> io::Result<bool> {
    let mut seen = 0usize;
    loop {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            return Ok(seen > 0);
        }
        if let Some(pos) = chunk.iter().position(|b| *b == b'\n') {
            reader.consume(pos + 1);
            return Ok(true);
        }
        let len = chunk.len();
        seen = seen.saturating_add(len);
        reader.consume(len);
        if seen > max_bytes {
            discard_until_newline(reader).await?;
            return Ok(true);
        }
    }
}

async fn read_bounded_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max_bytes: usize,
) -> io::Result<BoundedLineRead> {
    let mut line = Vec::new();
    loop {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            if line.is_empty() {
                return Ok(BoundedLineRead::Eof);
            }
            return Ok(BoundedLineRead::Line(
                String::from_utf8_lossy(&line).to_string(),
            ));
        }
        if let Some(pos) = chunk.iter().position(|b| *b == b'\n') {
            let take = pos + 1;
            if line.len().saturating_add(take) > max_bytes {
                reader.consume(take);
                return Ok(BoundedLineRead::TooLong);
            }
            line.extend_from_slice(&chunk[..take]);
            reader.consume(take);
            if line.last() == Some(&b'\n') {
                line.pop();
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
            }
            return Ok(BoundedLineRead::Line(
                String::from_utf8_lossy(&line).to_string(),
            ));
        }

        if line.len().saturating_add(chunk.len()) > max_bytes {
            let len = chunk.len();
            reader.consume(len);
            discard_until_newline(reader).await?;
            return Ok(BoundedLineRead::TooLong);
        }

        line.extend_from_slice(chunk);
        let len = chunk.len();
        reader.consume(len);
    }
}

/// Compute how long `pull_one` should block on the next event.
///
/// While a turn is in flight, wait the FULL remaining hang-watchdog budget (so a
/// real but slow event — e.g. claude's `system/init`, which lands seconds after
/// `send_turn` on a cold invocation — is always retrieved, never lost to the
/// generic R1 actor's park-on-`None` race). NEVER cap this at `poll_idle`: the
/// bug this guards against was capping the in-flight wait at the short `poll_idle`
/// breather, so a `None` was returned before the real event arrived; the actor
/// then parked on its mailbox with no re-poll timer and the event was lost
/// forever. When idle (no turn), a `poll_idle` breather is correct — nothing is
/// coming until the next `send_turn`, so the actor SHOULD park.
fn next_wait(
    turn_in_flight: bool,
    elapsed: Duration,
    hang_watchdog: Duration,
    poll_idle: Duration,
) -> Duration {
    if turn_in_flight {
        hang_watchdog
            .saturating_sub(elapsed)
            .max(Duration::from_millis(1))
    } else {
        poll_idle
    }
}

/// Build the `claude` command-line argument vector from a session id, a resume
/// flag, and a [`SpawnContext`].
///
/// The returned `Vec<String>` has `bin` as its first element so the caller can
/// pass `argv[0]` directly to `Command::new` and `argv[1..]` as args.
///
/// The body of the new-flags section (model / permission-mode /
/// disallowed-tools application) is `todo!()` — Phase-0 scaffold; Phase-2 will
/// fill the real flag-application logic.
pub fn claude_argv(
    bin: &str,
    session_id: &str,
    resume: bool,
    context: &SpawnContext,
) -> Vec<String> {
    let mut argv: Vec<String> = vec![
        bin.to_string(),
        "-p".to_string(),
        "--input-format".to_string(),
        "stream-json".to_string(),
        "--output-format".to_string(),
        "stream-json".to_string(),
        "--verbose".to_string(),
    ];

    if resume {
        argv.push("--resume".to_string());
    } else {
        argv.push("--session-id".to_string());
    }
    argv.push(session_id.to_string());

    if let Some(model) = &context.model {
        argv.push("--model".to_string());
        argv.push(model.clone());
    }

    if let Some(posture) = &context.posture {
        argv.push("--permission-mode".to_string());
        argv.push(posture.clone());
    }

    if !context.disallowed_tools.is_empty() {
        argv.push("--disallowedTools".to_string());
        argv.push(context.disallowed_tools.join(","));
    }

    argv
}

pub struct ClaudeStreamProtocol {
    bin: PathBuf,
    timeouts: SupervisionTimeouts,
}

impl ClaudeStreamProtocol {
    pub fn new(bin: impl Into<PathBuf>) -> Self {
        ClaudeStreamProtocol {
            bin: bin.into(),
            timeouts: SupervisionTimeouts::default(),
        }
    }

    fn caps() -> RuntimeCapabilities {
        RuntimeCapabilities {
            persistent_process: true,
            persistent_conversation: true,
            structured_turns: true,
            streaming_output: false,
            attachable: true,
            interruptible: false,
            structured_provenance: true,
            resumable: true,
        }
    }

    async fn spawn_child(
        &self,
        session_id: &str,
        context: &SpawnContext,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        let bin = context.bin.as_deref().unwrap_or(
            self.bin
                .to_str()
                .unwrap_or("claude"),
        );
        let argv = claude_argv(bin, session_id, false, context);
        // argv[0] is the binary, remaining args follow.
        let mut cmd = Command::new(&argv[0]);
        for arg in &argv[1..] {
            cmd.arg(arg);
        }
        if let Some(cwd) = &context.cwd {
            cmd.current_dir(cwd);
        }
        for (k, v) in &context.env {
            cmd.env(k, v);
        }
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        {
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
                detail: "claude: no stdin pipe".to_string(),
            })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| RuntimeError::SpawnFailed {
                detail: "claude: no stdout pipe".to_string(),
            })?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| RuntimeError::SpawnFailed {
                detail: "claude: no stderr pipe".to_string(),
            })?;

        let stderr_task = tokio::spawn(async move {
            // Bounded the same way as stdout (§4.4): the content is discarded
            // either way, but an unbounded `BufReader::lines()` here would let
            // a pathological single stderr line grow this drain's buffer
            // without limit — the exact bug the stdout fix closed, just on
            // the other pipe.
            let mut reader = BufReader::new(stderr);
            while let Ok(true) = discard_one_bounded_line(&mut reader, MAX_STDOUT_LINE_BYTES).await
            {
            }
        });

        let (ev_tx, ev_rx) = mpsc::channel::<ProtocolEvent>(EVENT_CHANNEL_CAPACITY);
        let (kill_tx, kill_rx) = mpsc::channel::<()>(1);

        let supervisor = tokio::spawn(supervise(
            child,
            stdout,
            ev_tx,
            kill_rx,
            stderr_task,
            self.timeouts,
        ));

        Ok(Box::new(ClaudeStreamTransport {
            stdin: Some(stdin),
            events: ev_rx,
            kill_tx: Some(kill_tx),
            supervisor: Some(supervisor),
            pid,
            timeouts: self.timeouts,
            turn_in_flight: false,
            last_event_at: Instant::now(),
            ended: false,
            turn_terminal_seen: false,
            session_ready_seen: false,
        }))
    }
}

#[async_trait]
impl InteractiveProtocol for ClaudeStreamProtocol {
    fn capabilities(&self) -> RuntimeCapabilities {
        Self::caps()
    }

    async fn spawn(
        &self,
        opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        let target_key = opts.target_key().ok_or_else(|| RuntimeError::SpawnFailed {
            detail: "ClaudeStreamProtocol::spawn requires SpawnOptions::target_key".to_string(),
        })?;
        let native = session_uuid(&target_key.0).to_string();
        self.spawn_child(&native, opts.context()).await
    }

    async fn resume(
        &self,
        native: &NativeSessionId,
        opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        self.spawn_child(&native.0, opts.context()).await
    }
}

struct ClaudeStreamTransport {
    stdin: Option<ChildStdin>,
    events: mpsc::Receiver<ProtocolEvent>,
    kill_tx: Option<mpsc::Sender<()>>,
    supervisor: Option<JoinHandle<()>>,
    pid: Option<u32>,
    timeouts: SupervisionTimeouts,
    turn_in_flight: bool,
    last_event_at: Instant,
    ended: bool,
    turn_terminal_seen: bool,
    /// Claudes emits `system/init` on every turn; R1 public `SessionReady` is
    /// first-turn-only, so this transport suppresses repeats after the first.
    session_ready_seen: bool,
}

impl ClaudeStreamTransport {
    async fn fail_write_with_unknown_outcome(&mut self, detail: String) -> RuntimeError {
        // A timed-out `write_all` can leave a partial frame in the pipe. Treat
        // this transport as fatally desynced: never write again and trigger kill.
        self.stdin = None;
        self.turn_in_flight = false;
        if let Some(tx) = &self.kill_tx {
            let _ = tx.send(()).await;
        }
        RuntimeError::TransportOutcomeUnknown { detail }
    }

    async fn write_stdin_line(&mut self, text: &str) -> Result<(), RuntimeError> {
        let Some(stdin) = self.stdin.as_mut() else {
            return Ok(());
        };

        let mut line = text.as_bytes().to_vec();
        line.push(b'\n');
        match tokio::time::timeout(self.timeouts.stdin_write, stdin.write_all(&line)).await {
            Err(_) => {
                let err = self
                    .fail_write_with_unknown_outcome(format!(
                        "claude stdin write timed out after {:?}",
                        self.timeouts.stdin_write
                    ))
                    .await;
                return Err(err);
            }
            Ok(Ok(())) => {}
            Ok(Err(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::WriteZero
                ) =>
            {
                return Ok(());
            }
            Ok(Err(e)) => {
                return Err(RuntimeError::Transport {
                    detail: format!("claude stdin write: {e}"),
                });
            }
        }
        match tokio::time::timeout(self.timeouts.stdin_write, stdin.flush()).await {
            Err(_) => {
                let err = self
                    .fail_write_with_unknown_outcome(format!(
                        "claude stdin flush timed out after {:?}",
                        self.timeouts.stdin_write
                    ))
                    .await;
                Err(err)
            }
            Ok(Ok(())) => Ok(()),
            Ok(Err(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::WriteZero
                ) =>
            {
                Ok(())
            }
            Ok(Err(e)) => Err(RuntimeError::Transport {
                detail: format!("claude stdin flush: {e}"),
            }),
        }
    }

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

    fn is_stale_after_terminal(&self, ev: &ProtocolEvent) -> bool {
        self.turn_terminal_seen && !matches!(ev, ProtocolEvent::ProcessExited { .. })
    }

    async fn fire_watchdog(&mut self) -> Option<ProtocolEvent> {
        if let Some(tx) = &self.kill_tx {
            let _ = tx.send(()).await;
        }
        match self.events.recv().await {
            Some(ev) => Some(ev),
            None => {
                self.ended = true;
                None
            }
        }
    }

    async fn pull_one(&mut self) -> Option<ProtocolEvent> {
        if self.ended {
            return None;
        }
        match self.events.try_recv() {
            Ok(ev) => return Some(ev),
            Err(mpsc::error::TryRecvError::Disconnected) => {
                self.ended = true;
                return None;
            }
            Err(mpsc::error::TryRecvError::Empty) => {}
        }

        // While a turn is in flight, BLOCK for the full remaining hang-watchdog
        // budget on the next real event — do NOT return `None` on a mere
        // `POLL_IDLE` breather. The generic R1 actor, on a `None` from an active
        // turn, parks purely on its mailbox with NO re-poll timer (actor.rs
        // `run`: `None if self.active.is_some() => self.mailbox.recv().await`); it
        // is only re-woken by a `Poke`, which `events()` sends ONCE at subscribe.
        // A real event that arrives AFTER the actor has parked (e.g. claude's
        // `system/init`, which on a cold invocation lands seconds later — well
        // past `POLL_IDLE` — because of MCP-server connection time) would sit
        // unretrieved in the channel FOREVER. Waiting the full watchdog budget
        // here means an in-flight event is always delivered whenever it arrives
        // (bounded by the watchdog), never lost to that park race. When idle (no
        // turn in flight) a `POLL_IDLE` `None` breather is still correct: nothing
        // is coming until the next `send_turn`, so the actor SHOULD park. Tradeoff:
        // while in-flight, mailbox-only control messages (e.g. cancel/shutdown)
        // can be delayed until this wait returns (real event or watchdog expiry).
        let wait = next_wait(
            self.turn_in_flight,
            self.last_event_at.elapsed(),
            self.timeouts.hang_watchdog,
            POLL_IDLE,
        );

        match tokio::time::timeout(wait, self.events.recv()).await {
            Ok(Some(ev)) => Some(ev),
            Ok(None) => {
                self.ended = true;
                None
            }
            Err(_) => {
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
impl InteractiveTransport for ClaudeStreamTransport {
    async fn send_turn(&mut self, prompt: &ComposedPrompt) -> Result<(), RuntimeError> {
        self.turn_in_flight = true;
        self.turn_terminal_seen = false;
        self.last_event_at = Instant::now();

        let line = serde_json::json!({
            "type": "user",
            "message": {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt.0
                    }
                ]
            }
        });
        let payload = serde_json::to_string(&line).map_err(|e| RuntimeError::Transport {
            detail: format!("claude send_turn serialization: {e}"),
        })?;

        self.write_stdin_line(&payload).await
    }

    async fn next_event(&mut self) -> Option<ProtocolEvent> {
        // Cancellation-safety audit: once `pull_one()` resolves with an event,
        // this method does not await again before returning it.
        loop {
            match self.pull_one().await {
                Some(ev) if self.is_stale_after_terminal(&ev) => continue,
                Some(ProtocolEvent::SessionReady { .. }) if self.session_ready_seen => {
                    continue;
                }
                Some(ev @ ProtocolEvent::SessionReady { .. }) => {
                    self.session_ready_seen = true;
                    self.observe(&ev);
                    return Some(ev);
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
        let _ = self.pid;
        Ok(())
    }
}

impl Drop for ClaudeStreamTransport {
    fn drop(&mut self) {
        if let Some(tx) = self.kill_tx.take() {
            let _ = tx.try_send(());
            drop(tx);
        }
        if let Some(handle) = self.supervisor.take() {
            drop(handle);
        }
    }
}

#[cfg(unix)]
fn best_effort_signal(target: i32, sig: Signal) {
    let _ = kill(Pid::from_raw(target), sig);
}

async fn supervise(
    mut child: tokio::process::Child,
    stdout: tokio::process::ChildStdout,
    ev_tx: mpsc::Sender<ProtocolEvent>,
    mut kill_rx: mpsc::Receiver<()>,
    stderr_task: JoinHandle<()>,
    timeouts: SupervisionTimeouts,
) {
    let mut lines = BufReader::new(stdout);
    let mut tool_tracker = ClaudeToolTracker::new();
    let pid = child.id();
    let signal_target = pid.and_then(|p| i32::try_from(p).ok()).map(|p| -p);

    'outer: loop {
        tokio::select! {
            line = read_bounded_line(&mut lines, MAX_STDOUT_LINE_BYTES) => {
                match line {
                    Ok(BoundedLineRead::Line(line)) => {
                        for ev in tool_tracker.observe_line(&line) {
                            if ev_tx.send(ev).await.is_err() {
                                break 'outer;
                            }
                        }
                    }
                    Ok(BoundedLineRead::TooLong) => {
                        let _ = ev_tx
                            .send(ProtocolEvent::TurnFailed {
                                error: format!(
                                    "claude stdout line exceeded {} bytes",
                                    MAX_STDOUT_LINE_BYTES
                                ),
                            })
                            .await;
                        let code = escalate(&mut child, signal_target, timeouts).await;
                        let _ = ev_tx.send(ProtocolEvent::ProcessExited { code }).await;
                        break;
                    }
                    Ok(BoundedLineRead::Eof) | Err(_) => {
                        let code = escalate(&mut child, signal_target, timeouts).await;
                        let _ = ev_tx.send(ProtocolEvent::ProcessExited { code }).await;
                        break;
                    }
                }
            }
            msg = kill_rx.recv() => {
                let _ = msg;
                let code = escalate(&mut child, signal_target, timeouts).await;
                let _ = ev_tx.send(ProtocolEvent::ProcessExited { code }).await;
                break;
            }
        }
    }

    let _ = wait_for_exit(&mut child, timeouts.kill_after).await;
    stderr_task.abort();
}

async fn escalate(
    child: &mut tokio::process::Child,
    signal_target: Option<i32>,
    t: SupervisionTimeouts,
) -> Option<i32> {
    if let Some(target) = signal_target {
        best_effort_signal(target, Signal::SIGINT);
    }
    if let Some(code) = wait_for_exit(child, t.interrupt_ack).await {
        return code;
    }
    if let Some(target) = signal_target {
        best_effort_signal(target, Signal::SIGTERM);
    }
    if let Some(code) = wait_for_exit(child, t.term_after).await {
        return code;
    }
    // Stage 3: SIGKILL (the hard stop). Signal the whole process GROUP (not
    // just the leader, §4.9 defense-in-depth) so a grandchild that also
    // ignored SIGINT/SIGTERM cannot survive as an orphan; `start_kill()` is
    // kept as a direct-child backstop and to let tokio's own bookkeeping know
    // a kill was issued.
    if let Some(target) = signal_target {
        best_effort_signal(target, Signal::SIGKILL);
    }
    let _ = child.start_kill();
    if let Some(code) = wait_for_exit(child, t.kill_after).await {
        return code;
    }
    wait_for_exit(child, t.kill_after).await.flatten()
}

async fn wait_for_exit(child: &mut tokio::process::Child, bound: Duration) -> Option<Option<i32>> {
    match tokio::time::timeout(bound, child.wait()).await {
        Ok(Ok(status)) => Some(status.code()),
        Ok(Err(_)) => Some(None),
        Err(_) => None,
    }
}

fn parse_line_value(line: &str) -> Option<Value> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    serde_json::from_str(trimmed).ok()
}

fn message_content_array(v: &Value) -> Option<&[Value]> {
    v.get("message")
        .and_then(|m| m.get("content"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
}

fn tool_use_entries(content: &[Value]) -> impl Iterator<Item = (&str, &str)> {
    content.iter().filter_map(|block| {
        if block.get("type").and_then(Value::as_str) != Some("tool_use") {
            return None;
        }
        let id = block.get("id").and_then(Value::as_str)?;
        let name = block.get("name").and_then(Value::as_str)?;
        Some((id, name))
    })
}

fn tool_result_blocks(content: &[Value]) -> impl Iterator<Item = &Value> {
    content
        .iter()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("tool_result"))
}

fn map_claude_value(v: &Value) -> Vec<ProtocolEvent> {
    let Some(kind) = v.get("type").and_then(Value::as_str) else {
        return vec![];
    };

    match kind {
        "system" => {
            let subtype = v.get("subtype").and_then(Value::as_str);
            if subtype == Some("init") {
                if let Some(session_id) = v.get("session_id").and_then(Value::as_str) {
                    return vec![ProtocolEvent::SessionReady {
                        native: Some(NativeSessionId(session_id.to_string())),
                    }];
                }
            }
            vec![]
        }
        "assistant" => {
            let Some(content) = message_content_array(v) else {
                return vec![];
            };
            let mut out = Vec::new();
            for block in content {
                match block.get("type").and_then(Value::as_str) {
                    Some("text") => {
                        if let Some(text) = block.get("text").and_then(Value::as_str) {
                            out.push(ProtocolEvent::AssistantDelta {
                                text: text.to_string(),
                            });
                        }
                    }
                    Some("tool_use") => {
                        if let Some(name) = block.get("name").and_then(Value::as_str) {
                            out.push(ProtocolEvent::ToolStarted {
                                name: name.to_string(),
                            });
                        }
                    }
                    _ => {}
                }
            }
            out
        }
        "result" => {
            if v.get("is_error").and_then(Value::as_bool).unwrap_or(false) {
                let error = v
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("claude result reported is_error=true with no error field")
                    .to_string();
                return vec![ProtocolEvent::TurnFailed { error }];
            }
            if let Some(reply) = v.get("result").and_then(Value::as_str) {
                return vec![ProtocolEvent::TurnCompleted {
                    reply: reply.to_string(),
                }];
            }
            vec![]
        }
        _ => vec![],
    }
}

/// PURE per-line map of claude stream-json lines.
///
/// This function intentionally carries no cross-line state. It can therefore map
/// `tool_use` (name is present in the same line) but not `tool_result`
/// (name absent; only `tool_use_id` is present).
pub fn map_claude_line(line: &str) -> Vec<ProtocolEvent> {
    let Some(v) = parse_line_value(line) else {
        return vec![];
    };
    map_claude_value(&v)
}

/// Stateful tool-call correlator for claude `stream-json`.
#[derive(Debug, Default)]
pub struct ClaudeToolTracker {
    tool_names_by_id: HashMap<String, String>,
}

impl ClaudeToolTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe_line(&mut self, line: &str) -> Vec<ProtocolEvent> {
        let Some(v) = parse_line_value(line) else {
            return vec![];
        };
        let Some(kind) = v.get("type").and_then(Value::as_str) else {
            return vec![];
        };

        if kind == "assistant" {
            if let Some(content) = message_content_array(&v) {
                for (id, name) in tool_use_entries(content) {
                    self.tool_names_by_id
                        .insert(id.to_string(), name.to_string());
                }
            }
            return map_claude_value(&v);
        }

        if kind == "user" {
            let Some(content) = message_content_array(&v) else {
                return vec![];
            };
            let mut out = Vec::new();
            for block in tool_result_blocks(content) {
                let Some(tool_use_id) = block.get("tool_use_id").and_then(Value::as_str) else {
                    continue;
                };
                let Some(name) = self.tool_names_by_id.remove(tool_use_id) else {
                    continue;
                };
                // Only successful `tool_result` rows are spike-captured so far.
                // We default to success unless `is_error: true` is present on the
                // block. A future failing capture may reveal additional error
                // marker fields this derivation should include.
                let ok = !block
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                out.push(ProtocolEvent::ToolFinished { name, ok });
            }
            return out;
        }

        if kind == "result" {
            // Turn boundary: drop any tool_use ids whose tool_result never
            // arrived (e.g. the turn was cancelled/crashed mid-tool-call).
            // Without this, an interrupted tool call would leak one small
            // entry per turn for the life of the session — the same class of
            // unbounded-growth concern this crate's own resource-bounds
            // hardening (event channel / line-read / accumulator caps)
            // closed elsewhere; a turn's own id namespace should not outlive
            // the turn.
            self.tool_names_by_id.clear();
        }

        map_claude_value(&v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(line: &str) -> Vec<ProtocolEvent> {
        map_claude_line(line)
    }

    #[test]
    fn maps_system_init_to_session_ready() {
        let line = r#"{"type":"system","subtype":"init","cwd":"/work/mewais/Wasl","session_id":"405bb438-3cd5-4f5e-b682-a5e336d6b0c1","tools":["Task","Read"],"model":"claude-sonnet-4.6[1m]","permissionMode":"default"}"#;
        assert_eq!(
            m(line),
            vec![ProtocolEvent::SessionReady {
                native: Some(NativeSessionId(
                    "405bb438-3cd5-4f5e-b682-a5e336d6b0c1".to_string()
                ))
            }]
        );
    }

    #[test]
    fn ignores_non_init_system_events() {
        let line = r#"{"type":"system","subtype":"thinking_tokens","estimated_tokens":123,"session_id":"405bb438-3cd5-4f5e-b682-a5e336d6b0c1"}"#;
        assert_eq!(m(line), vec![]);
    }

    #[test]
    fn ignores_assistant_thinking_blocks() {
        let line = r#"{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"The user wants..."}]},"session_id":"405bb438-3cd5-4f5e-b682-a5e336d6b0c1"}"#;
        assert_eq!(m(line), vec![]);
    }

    #[test]
    fn maps_assistant_text_blocks_to_deltas() {
        let line = r#"{"type":"assistant","message":{"content":[{"type":"text","text":"OK"}]},"session_id":"405bb438-3cd5-4f5e-b682-a5e336d6b0c1"}"#;
        assert_eq!(
            m(line),
            vec![ProtocolEvent::AssistantDelta {
                text: "OK".to_string()
            }]
        );
    }

    #[test]
    fn maps_all_text_blocks_in_one_assistant_message() {
        let line = r#"{"type":"assistant","message":{"content":[{"type":"text","text":"A"},{"type":"thinking","thinking":"..."},{"type":"text","text":"B"}]}}"#;
        assert_eq!(
            m(line),
            vec![
                ProtocolEvent::AssistantDelta {
                    text: "A".to_string()
                },
                ProtocolEvent::AssistantDelta {
                    text: "B".to_string()
                }
            ]
        );
    }

    #[test]
    fn tool_use_maps_but_pure_tool_result_stays_unmapped_without_state() {
        let tool_use = r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Task"}]},"session_id":"405bb438-3cd5-4f5e-b682-a5e336d6b0c1"}"#;
        let tool_result = r#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"toolu_01","content":"ok"}]},"session_id":"405bb438-3cd5-4f5e-b682-a5e336d6b0c1"}"#;
        assert_eq!(
            m(tool_use),
            vec![ProtocolEvent::ToolStarted {
                name: "Task".to_string()
            }]
        );
        assert_eq!(m(tool_result), vec![]);
    }

    #[test]
    fn maps_result_success_to_turn_completed() {
        let line = r#"{"type":"result","subtype":"success","is_error":false,"result":"OK","stop_reason":"end_turn","session_id":"405bb438-3cd5-4f5e-b682-a5e336d6b0c1"}"#;
        assert_eq!(
            m(line),
            vec![ProtocolEvent::TurnCompleted {
                reply: "OK".to_string()
            }]
        );
    }

    #[test]
    fn maps_result_failure_to_turn_failed_using_adapter_precedent() {
        // This row mirrors `wasl-adapter-claude::parse_reply`'s proven handling
        // of the same `result` envelope (is_error + optional error text).
        let line = r#"{"type":"result","is_error":true,"error":"boom","session_id":"405bb438-3cd5-4f5e-b682-a5e336d6b0c1"}"#;
        assert_eq!(
            m(line),
            vec![ProtocolEvent::TurnFailed {
                error: "boom".to_string()
            }]
        );
    }

    #[test]
    fn maps_result_failure_missing_error_to_fixed_fallback() {
        let line = r#"{"type":"result","is_error":true,"session_id":"405bb438-3cd5-4f5e-b682-a5e336d6b0c1"}"#;
        assert_eq!(
            m(line),
            vec![ProtocolEvent::TurnFailed {
                error: "claude result reported is_error=true with no error field".to_string()
            }]
        );
    }

    #[test]
    fn drops_malformed_unknown_and_blank_lines() {
        assert_eq!(m(""), vec![]);
        assert_eq!(m("   \t"), vec![]);
        assert_eq!(m("{not json]"), vec![]);
        assert_eq!(m(r#"{"foo":"bar"}"#), vec![]);
        assert_eq!(m(r#"{"type":"nonsense"}"#), vec![]);
    }

    #[test]
    fn session_uuid_is_stable_for_same_context() {
        let a = session_uuid("ctx-1");
        let b = session_uuid("ctx-1");
        let c = session_uuid("ctx-2");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    // =================================================================
    // Regression guard for the in-flight `next_wait` bug (the pure
    // wait-duration policy `pull_one` calls). The bug: while a turn was in
    // flight the wait was capped at the short `POLL_IDLE` (500ms) breather
    // REGARDLESS of the real hang-watchdog budget, so `pull_one` returned a
    // spurious `None` after 500ms; the generic R1 actor then parked on its
    // mailbox with no re-poll timer and a real event arriving later (e.g.
    // claude's `system/init`, 3-7s in) was LOST FOREVER. The fix waits the FULL
    // remaining watchdog budget while in flight. These table-driven cases pin
    // that policy; reverting `next_wait` to the old `POLL_IDLE.min(...)` body
    // makes the first case FAIL (proven fail-before/pass-after).
    // =================================================================

    const WATCHDOG: Duration = Duration::from_secs(60);
    const IDLE: Duration = Duration::from_millis(500);

    #[test]
    fn next_wait_in_flight_uses_full_watchdog_not_poll_idle() {
        // In flight, only 10ms elapsed of a 60s budget: the wait MUST be ~60s
        // (the full remaining budget), NOT capped at the 500ms POLL_IDLE. This
        // is the exact case the bug got wrong.
        let w = next_wait(true, Duration::from_millis(10), WATCHDOG, IDLE);
        assert_eq!(
            w,
            WATCHDOG - Duration::from_millis(10),
            "in-flight wait must be the full remaining watchdog budget, not POLL_IDLE"
        );
        assert!(
            w > IDLE,
            "the in-flight wait must exceed POLL_IDLE ({IDLE:?}); got {w:?} \
             (the old bug capped it at POLL_IDLE and lost slow real events)"
        );
    }

    #[test]
    fn next_wait_in_flight_near_watchdog_returns_small_remainder() {
        // 59.9s elapsed of a 60s budget → ~100ms remaining. Still the true
        // remainder (NOT POLL_IDLE.min(remainder), which the old buggy body
        // would ALSO have returned here — so this case alone does not
        // distinguish; it pins the boundary behavior correctly regardless).
        let elapsed = Duration::from_millis(59_900);
        let w = next_wait(true, elapsed, WATCHDOG, IDLE);
        assert_eq!(
            w,
            Duration::from_millis(100),
            "near the watchdog, the wait is the small true remainder"
        );
    }

    #[test]
    fn next_wait_in_flight_past_watchdog_clamps_to_one_millisecond() {
        // Elapsed >= watchdog → saturating_sub is zero, clamped up to 1ms so the
        // timeout still fires (and `pull_one` then trips the watchdog terminal).
        let w = next_wait(true, WATCHDOG + Duration::from_secs(5), WATCHDOG, IDLE);
        assert_eq!(w, Duration::from_millis(1));
    }

    #[test]
    fn next_wait_idle_is_exactly_poll_idle_regardless_of_elapsed() {
        // Not in flight → always exactly POLL_IDLE, no matter the elapsed value.
        assert_eq!(
            next_wait(false, Duration::from_millis(0), WATCHDOG, IDLE),
            IDLE
        );
        assert_eq!(
            next_wait(false, Duration::from_secs(120), WATCHDOG, IDLE),
            IDLE
        );
    }
}
