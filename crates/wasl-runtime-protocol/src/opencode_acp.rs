//! `OpenCodeAcpProtocol`  maps opencode `acp` JSON-RPC-over-stdio onto R1's
//! generic [`wasl_runtime::ProtocolEvent`].

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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use wasl_runtime::{
    ComposedPrompt, InteractiveProtocol, InteractiveTransport, NativeSessionId, ProtocolEvent,
    RuntimeCapabilities, RuntimeError, SpawnContext, SpawnOptions,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SupervisionTimeouts {
    interrupt_ack: Duration,
    term_after: Duration,
    kill_after: Duration,
    hang_watchdog: Duration,
    handshake_timeout: Duration,
    stdin_write: Duration,
}

struct SupervisionState {
    timeouts: SupervisionTimeouts,
    outstanding_request_id: Arc<AtomicU64>,
}

impl Default for SupervisionTimeouts {
    fn default() -> Self {
        SupervisionTimeouts {
            interrupt_ack: Duration::from_secs(2),
            term_after: Duration::from_secs(2),
            kill_after: Duration::from_secs(2),
            hang_watchdog: Duration::from_secs(60),
            handshake_timeout: Duration::from_secs(30),
            stdin_write: Duration::from_secs(15),
        }
    }
}

const POLL_IDLE: Duration = Duration::from_millis(500);
const MAX_STDOUT_LINE_BYTES: usize = 4 * 1024 * 1024;
const EVENT_CHANNEL_CAPACITY: usize = 4096;
const ACCUMULATED_REPLY_MAX_BYTES: usize = 4 * 1024 * 1024;
const TRUNCATION_MARKER: &str = "…[truncated]";

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

fn append_capped_reply(acc: &mut String, chunk: &str, truncated: &mut bool, max_bytes: usize) {
    if *truncated {
        return;
    }
    acc.push_str(chunk);
    if acc.len() > max_bytes {
        let mut boundary = max_bytes;
        while boundary > 0 && !acc.is_char_boundary(boundary) {
            boundary -= 1;
        }
        acc.truncate(boundary);
        acc.push_str(TRUNCATION_MARKER);
        *truncated = true;
    }
}

/// Compute how long `pull_one` should block on the next event.
///
/// While a turn is in flight, wait the FULL remaining hang-watchdog budget (so a
/// real but slow event — e.g. opencode's ACP updates on a cold invocation — is
/// always retrieved, never lost to the generic R1 actor's park-on-`None` race).
/// NEVER cap this at `poll_idle`: the bug this guards against was capping the
/// in-flight wait at the short `poll_idle` breather, so a `None` was returned
/// before the real event arrived; the actor then parked on its mailbox with no
/// re-poll timer and the event was lost forever. When idle (no turn), a
/// `poll_idle` breather is correct — nothing is coming until the next `send_turn`,
/// so the actor SHOULD park.
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

/// Build the ACP `session/new` or `session/load` params object.
///
/// * `session_method` — `"session/new"` or `"session/load"`.
/// * `requested_session_id` — the native session id for resume; `None` for new.
/// * `cwd` — the working directory to pass in the params.
///
/// The body is `todo!()` — Phase-0 scaffold; Phase-2 fills the real JSON
/// construction logic (currently inlined in `perform_handshake`).
pub fn opencode_session_params(
    session_method: &str,
    requested_session_id: Option<&str>,
    cwd: &str,
) -> serde_json::Value {
    if session_method == "session/load" {
        serde_json::json!({
            "sessionId": requested_session_id.unwrap_or_default(),
            "cwd": cwd,
            "mcpServers": []
        })
    } else {
        serde_json::json!({
            "cwd": cwd,
            "mcpServers": []
        })
    }
}

pub struct OpenCodeAcpProtocol {
    bin: PathBuf,
    timeouts: SupervisionTimeouts,
}

impl OpenCodeAcpProtocol {
    pub fn new(bin: impl Into<PathBuf>) -> Self {
        OpenCodeAcpProtocol {
            bin: bin.into(),
            timeouts: SupervisionTimeouts::default(),
        }
    }

    fn caps() -> RuntimeCapabilities {
        RuntimeCapabilities {
            persistent_process: true,
            persistent_conversation: true,
            structured_turns: true,
            streaming_output: true,
            attachable: true,
            interruptible: false,
            structured_provenance: true,
            resumable: true,
        }
    }

    async fn spawn_or_resume(
        &self,
        session_method: &str,
        requested_session_id: Option<&str>,
        context: &SpawnContext,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        let bin = context
            .bin
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| self.bin.clone());
        let mut cmd = Command::new(&bin);
        cmd.arg("acp")
            .env("OPENCODE_DISABLE_PROJECT_CONFIG", "1")
            .env("OPENCODE_DISABLE_AUTOCOMPACT", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(cwd) = &context.cwd {
            cmd.current_dir(cwd);
        }
        for (k, v) in &context.env {
            cmd.env(k, v);
        }
        #[cfg(unix)]
        {
            cmd.as_std_mut().process_group(0);
        }
        let mut child = cmd.spawn().map_err(|e| RuntimeError::SpawnFailed {
            detail: format!("spawn {} acp: {e}", bin.display()),
        })?;

        let pid = child.id();
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| RuntimeError::SpawnFailed {
                detail: "opencode acp: no stdin pipe".to_string(),
            })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| RuntimeError::SpawnFailed {
                detail: "opencode acp: no stdout pipe".to_string(),
            })?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| RuntimeError::SpawnFailed {
                detail: "opencode acp: no stderr pipe".to_string(),
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

        let mut stdout_reader = BufReader::new(stdout);
        let session_id = self
            .perform_handshake(
                &mut stdin,
                &mut stdout_reader,
                session_method,
                requested_session_id,
                context,
            )
            .await?;

        let (ev_tx, ev_rx) = mpsc::channel::<ProtocolEvent>(EVENT_CHANNEL_CAPACITY);
        let (kill_tx, kill_rx) = mpsc::channel::<()>(1);
        let outstanding_request_id = Arc::new(AtomicU64::new(0));
        let supervisor = tokio::spawn(supervise(
            child,
            stdout_reader,
            session_id.clone(),
            ev_tx,
            kill_rx,
            stderr_task,
            SupervisionState {
                timeouts: self.timeouts,
                outstanding_request_id: Arc::clone(&outstanding_request_id),
            },
        ));

        Ok(Box::new(OpenCodeAcpTransport {
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
            session_id,
            next_request_id: 3,
            outstanding_request_id,
            accumulated_reply: String::new(),
            accumulated_reply_truncated: false,
        }))
    }

    async fn perform_handshake(
        &self,
        stdin: &mut ChildStdin,
        reader: &mut BufReader<ChildStdout>,
        session_method: &str,
        requested_session_id: Option<&str>,
        context: &SpawnContext,
    ) -> Result<String, RuntimeError> {
        let deadline = Instant::now() + self.timeouts.handshake_timeout;

        let initialize = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": 1,
                "clientCapabilities": {},
                "clientInfo": {
                    "name": "wasl",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }
        });
        write_jsonrpc_line(
            stdin,
            &initialize,
            self.timeouts.stdin_write,
            deadline,
            "initialize",
        )
        .await?;

        let init_line = read_expected_handshake_line(reader, 1, "initialize", deadline).await?;
        let init = parse_handshake_line(&init_line, "initialize")?;
        ensure_no_jsonrpc_error(&init, "initialize")?;
        ensure_has_result(&init, "initialize")?;

        // Use context.cwd if provided; fall back to the process current dir.
        let cwd = match &context.cwd {
            Some(d) => d.clone(),
            None => std::env::current_dir()
                .map_err(|e| RuntimeError::SpawnFailed {
                    detail: format!("opencode acp handshake current_dir: {e}"),
                })?
                .to_string_lossy()
                .to_string(),
        };

        let session_params = if session_method == "session/load" {
            serde_json::json!({
                "sessionId": requested_session_id.unwrap_or_default(),
                "cwd": cwd,
                "mcpServers": []
            })
        } else {
            serde_json::json!({
                "cwd": cwd,
                "mcpServers": []
            })
        };

        let session_req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": session_method,
            "params": session_params
        });
        write_jsonrpc_line(
            stdin,
            &session_req,
            self.timeouts.stdin_write,
            deadline,
            session_method,
        )
        .await?;

        let session_line =
            read_expected_handshake_line(reader, 2, session_method, deadline).await?;
        let session_resp = parse_handshake_line(&session_line, session_method)?;
        ensure_no_jsonrpc_error(&session_resp, session_method)?;
        let result = ensure_has_result(&session_resp, session_method)?;
        let native = result
            .get("sessionId")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| requested_session_id.map(ToOwned::to_owned))
            .ok_or_else(|| RuntimeError::SpawnFailed {
                detail: format!(
                    "opencode acp {session_method} response missing result.sessionId and no fallback native id"
                ),
            })?;

        Ok(native)
    }
}

#[async_trait]
impl InteractiveProtocol for OpenCodeAcpProtocol {
    fn capabilities(&self) -> RuntimeCapabilities {
        Self::caps()
    }

    async fn spawn(
        &self,
        opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        self.spawn_or_resume("session/new", None, opts.context()).await
    }

    /// Resume is implemented with ACP `session/load` as the documented
    /// counterpart to `session/new`, but this exact wire shape is ASSUMED (not
    /// spike-captured): the Phase-0' probe captured resume capability
    /// advertisement only, not an exercised resume round-trip.
    async fn resume(
        &self,
        native: &NativeSessionId,
        opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        self.spawn_or_resume("session/load", Some(&native.0), opts.context()).await
    }
}

struct OpenCodeAcpTransport {
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
    session_id: String,
    next_request_id: u64,
    outstanding_request_id: Arc<AtomicU64>,
    accumulated_reply: String,
    accumulated_reply_truncated: bool,
}

impl OpenCodeAcpTransport {
    async fn fail_write_with_unknown_outcome(&mut self, detail: String) -> RuntimeError {
        // A timed-out `write_all` can leave a partial JSON-RPC frame. Treat the
        // transport as fatally desynced and trigger termination.
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
                        "opencode acp stdin write timed out after {:?}",
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
                    detail: format!("opencode acp stdin write: {e}"),
                });
            }
        }
        match tokio::time::timeout(self.timeouts.stdin_write, stdin.flush()).await {
            Err(_) => {
                let err = self
                    .fail_write_with_unknown_outcome(format!(
                        "opencode acp stdin flush timed out after {:?}",
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
                detail: format!("opencode acp stdin flush: {e}"),
            }),
        }
    }

    fn observe(&mut self, ev: &ProtocolEvent) {
        self.last_event_at = Instant::now();
        match ev {
            ProtocolEvent::TurnCompleted { .. } | ProtocolEvent::TurnFailed { .. } => {
                self.turn_in_flight = false;
                self.turn_terminal_seen = true;
                self.outstanding_request_id.store(0, Ordering::Release);
            }
            ProtocolEvent::ProcessExited { .. } => {
                self.turn_in_flight = false;
                self.ended = true;
                self.outstanding_request_id.store(0, Ordering::Release);
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
        // A real event that arrives AFTER the actor has parked (e.g. opencode's
        // ACP handshake updates, which can land seconds later on a cold
        // invocation — well past `POLL_IDLE`) would sit unretrieved in the
        // channel FOREVER. Waiting the full watchdog budget here means an
        // in-flight event is always delivered whenever it arrives (bounded by
        // the watchdog), never lost to that park race. When idle (no turn in
        // flight) a `POLL_IDLE` `None` breather is still correct: nothing is
        // coming until the next `send_turn`, so the actor SHOULD park. Tradeoff:
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
impl InteractiveTransport for OpenCodeAcpTransport {
    async fn send_turn(&mut self, prompt: &ComposedPrompt) -> Result<(), RuntimeError> {
        self.turn_in_flight = true;
        self.turn_terminal_seen = false;
        self.last_event_at = Instant::now();
        self.accumulated_reply.clear();
        self.accumulated_reply_truncated = false;

        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        self.outstanding_request_id
            .store(request_id, Ordering::Release);

        let line = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "session/prompt",
            "params": {
                "sessionId": self.session_id,
                "prompt": [
                    {
                        "type": "text",
                        "text": prompt.0
                    }
                ]
            }
        });
        let payload = serde_json::to_string(&line).map_err(|e| RuntimeError::Transport {
            detail: format!("opencode acp send_turn serialization: {e}"),
        })?;

        match self.write_stdin_line(&payload).await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.outstanding_request_id.store(0, Ordering::Release);
                Err(err)
            }
        }
    }

    async fn next_event(&mut self) -> Option<ProtocolEvent> {
        // Cancellation-safety audit: once `pull_one()` resolves with an event,
        // this method does not await again before returning it.
        loop {
            match self.pull_one().await {
                Some(ev) if self.is_stale_after_terminal(&ev) => continue,
                Some(ProtocolEvent::AssistantDelta { text }) => {
                    append_capped_reply(
                        &mut self.accumulated_reply,
                        &text,
                        &mut self.accumulated_reply_truncated,
                        ACCUMULATED_REPLY_MAX_BYTES,
                    );
                    let ev = ProtocolEvent::AssistantDelta { text };
                    self.observe(&ev);
                    return Some(ev);
                }
                Some(ProtocolEvent::TurnCompleted { .. }) => {
                    let ev = ProtocolEvent::TurnCompleted {
                        reply: std::mem::take(&mut self.accumulated_reply),
                    };
                    self.observe(&ev);
                    return Some(ev);
                }
                Some(ev @ ProtocolEvent::TurnFailed { .. }) => {
                    self.accumulated_reply.clear();
                    self.accumulated_reply_truncated = false;
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

impl Drop for OpenCodeAcpTransport {
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
    mut lines: BufReader<ChildStdout>,
    session_id: String,
    ev_tx: mpsc::Sender<ProtocolEvent>,
    mut kill_rx: mpsc::Receiver<()>,
    stderr_task: JoinHandle<()>,
    supervision: SupervisionState,
) {
    let pid = child.id();
    let signal_target = pid.and_then(|p| i32::try_from(p).ok()).map(|p| -p);

    if ev_tx
        .send(ProtocolEvent::SessionReady {
            native: Some(NativeSessionId(session_id)),
        })
        .await
        .is_err()
    {
        let _ = child.wait().await;
        stderr_task.abort();
        return;
    }

    'outer: loop {
        tokio::select! {
            line = read_bounded_line(&mut lines, MAX_STDOUT_LINE_BYTES) => {
                match line {
                    Ok(BoundedLineRead::Line(line)) => {
                        let expected_id = supervision.outstanding_request_id.load(Ordering::Acquire);
                        for ev in map_acp_line_correlated(&line, expected_id) {
                            if ev_tx.send(ev).await.is_err() {
                                break 'outer;
                            }
                        }
                    }
                    Ok(BoundedLineRead::TooLong) => {
                        let _ = ev_tx
                            .send(ProtocolEvent::TurnFailed {
                                error: format!(
                                    "opencode acp stdout line exceeded {} bytes",
                                    MAX_STDOUT_LINE_BYTES
                                ),
                            })
                            .await;
                        let code = escalate(&mut child, signal_target, supervision.timeouts).await;
                        let _ = ev_tx.send(ProtocolEvent::ProcessExited { code }).await;
                        break;
                    }
                    Ok(BoundedLineRead::Eof) | Err(_) => {
                        let code = escalate(&mut child, signal_target, supervision.timeouts).await;
                        let _ = ev_tx.send(ProtocolEvent::ProcessExited { code }).await;
                        break;
                    }
                }
            }
            msg = kill_rx.recv() => {
                let _ = msg;
                let code = escalate(&mut child, signal_target, supervision.timeouts).await;
                let _ = ev_tx.send(ProtocolEvent::ProcessExited { code }).await;
                break;
            }
        }
    }

    let _ = wait_for_exit(&mut child, supervision.timeouts.kill_after).await;
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

async fn write_jsonrpc_line(
    stdin: &mut ChildStdin,
    value: &Value,
    stdin_write_timeout: Duration,
    handshake_deadline: Instant,
    step: &str,
) -> Result<(), RuntimeError> {
    let payload = serde_json::to_string(value).map_err(|e| RuntimeError::SpawnFailed {
        detail: format!("opencode acp handshake serialize: {e}"),
    })?;
    let mut line = payload.into_bytes();
    line.push(b'\n');
    let write_bound = remaining_handshake_time(handshake_deadline, step)?.min(stdin_write_timeout);
    tokio::time::timeout(write_bound, stdin.write_all(&line))
        .await
        .map_err(|_| RuntimeError::SpawnFailed {
            detail: format!(
                "opencode acp handshake {step}: write timed out after {:?}",
                write_bound
            ),
        })?
        .map_err(|e| RuntimeError::SpawnFailed {
            detail: format!("opencode acp handshake {step}: write: {e}"),
        })?;
    let flush_bound = remaining_handshake_time(handshake_deadline, step)?.min(stdin_write_timeout);
    tokio::time::timeout(flush_bound, stdin.flush())
        .await
        .map_err(|_| RuntimeError::SpawnFailed {
            detail: format!(
                "opencode acp handshake {step}: flush timed out after {:?}",
                flush_bound
            ),
        })?
        .map_err(|e| RuntimeError::SpawnFailed {
            detail: format!("opencode acp handshake {step}: flush: {e}"),
        })
}

fn remaining_handshake_time(deadline: Instant, step: &str) -> Result<Duration, RuntimeError> {
    let now = Instant::now();
    if now >= deadline {
        return Err(RuntimeError::SpawnFailed {
            detail: format!("opencode acp handshake {step}: timed out"),
        });
    }
    Ok(deadline.saturating_duration_since(now))
}

async fn read_handshake_line(
    reader: &mut BufReader<ChildStdout>,
    deadline: Instant,
    step: &str,
) -> Result<String, RuntimeError> {
    let timeout = remaining_handshake_time(deadline, step)?;
    match tokio::time::timeout(timeout, read_bounded_line(reader, MAX_STDOUT_LINE_BYTES)).await {
        Ok(Ok(BoundedLineRead::Line(line))) => Ok(line),
        Ok(Ok(BoundedLineRead::Eof)) => Err(RuntimeError::SpawnFailed {
            detail: format!("opencode acp handshake {step}: EOF"),
        }),
        Ok(Ok(BoundedLineRead::TooLong)) => Err(RuntimeError::SpawnFailed {
            detail: format!(
                "opencode acp handshake {step}: line exceeded {} bytes",
                MAX_STDOUT_LINE_BYTES
            ),
        }),
        Ok(Err(e)) => Err(RuntimeError::SpawnFailed {
            detail: format!("opencode acp handshake {step}: read error: {e}"),
        }),
        Err(_) => Err(RuntimeError::SpawnFailed {
            detail: format!(
                "opencode acp handshake {step}: timed out after {:?} (remaining handshake budget)",
                timeout
            ),
        }),
    }
}

/// Cap an anomalous/error line embedded in a diagnostic message: the line
/// itself may legally be up to [`MAX_STDOUT_LINE_BYTES`] (4 MiB), and a
/// `RuntimeError::SpawnFailed` detail string that size is pure waste for an
/// error/log path nobody needs the full payload for.
fn truncate_for_diagnostic(line: &str) -> &str {
    const MAX_DIAGNOSTIC_CHARS: usize = 512;
    let mut boundary = line.len().min(MAX_DIAGNOSTIC_CHARS);
    while boundary > 0 && !line.is_char_boundary(boundary) {
        boundary -= 1;
    }
    &line[..boundary]
}

async fn read_expected_handshake_line(
    reader: &mut BufReader<ChildStdout>,
    expected_id: u64,
    step: &str,
    deadline: Instant,
) -> Result<String, RuntimeError> {
    loop {
        let line = read_handshake_line(reader, deadline, step).await?;
        match classify_handshake_line(&line, expected_id) {
            HandshakeLineClass::Notification => continue,
            HandshakeLineClass::ExpectedResponse => return Ok(line),
            HandshakeLineClass::Anomaly => {
                return Err(RuntimeError::SpawnFailed {
                    detail: format!(
                        "opencode acp handshake {step}: protocol anomaly while awaiting id \
                         {expected_id}: {}",
                        truncate_for_diagnostic(&line)
                    ),
                });
            }
        }
    }
}

fn parse_handshake_line(line: &str, step: &str) -> Result<Value, RuntimeError> {
    let v: Value = serde_json::from_str(line).map_err(|e| RuntimeError::SpawnFailed {
        detail: format!("opencode acp handshake {step}: malformed JSON: {e}"),
    })?;
    Ok(v)
}

fn ensure_no_jsonrpc_error(v: &Value, step: &str) -> Result<(), RuntimeError> {
    if let Some(error) = v.get("error") {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("unknown json-rpc error during handshake");
        return Err(RuntimeError::SpawnFailed {
            detail: format!("opencode acp handshake {step}: {message}"),
        });
    }
    Ok(())
}

fn ensure_has_result<'a>(v: &'a Value, step: &str) -> Result<&'a Value, RuntimeError> {
    v.get("result").ok_or_else(|| RuntimeError::SpawnFailed {
        detail: format!("opencode acp handshake {step}: missing result"),
    })
}

fn map_acp_line(line: &str) -> Vec<ProtocolEvent> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return vec![];
    }

    let v: Value = match serde_json::from_str(trimmed) {
        Ok(v) => v,
        Err(_) => return vec![],
    };

    if let Some(error) = v.get("error") {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("opencode acp json-rpc error with no message")
            .to_string();
        return vec![ProtocolEvent::TurnFailed { error: message }];
    }

    if let Some(result) = v.get("result") {
        if result.get("stopReason").is_some() {
            return vec![ProtocolEvent::TurnCompleted {
                reply: String::new(),
            }];
        }
        return vec![];
    }

    if v.get("method").and_then(Value::as_str) != Some("session/update") {
        return vec![];
    }

    let update = match v.get("params").and_then(|p| p.get("update")) {
        Some(update) => update,
        None => return vec![],
    };
    let kind = update.get("sessionUpdate").and_then(Value::as_str);
    if kind != Some("agent_message_chunk") {
        return vec![];
    }

    let text = match update
        .get("content")
        .and_then(|c| c.get("text"))
        .and_then(Value::as_str)
    {
        Some(text) => text,
        None => return vec![],
    };

    vec![ProtocolEvent::AssistantDelta {
        text: text.to_string(),
    }]
}

/// Classification of one JSON-RPC line read during the handshake, relative to
/// the request id currently being awaited (hardening-plan §4.6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandshakeLineClass {
    /// A JSON-RPC notification (has `method`, no `id`). A real ACP server may
    /// legally send one at any time, including mid-handshake; ignore it and
    /// keep reading — it is never the handshake response.
    Notification,
    /// The response we are waiting for: `id` matches `expected_id` AND the
    /// line carries `result` or `error`. Either satisfies the handshake step
    /// (an `error` still ends the read; the caller surfaces it separately).
    ExpectedResponse,
    /// Neither of the above: a response tagged with a DIFFERENT id, a
    /// malformed/non-JSON line, or a line with no `id` and no `method`. A real
    /// protocol anomaly to surface — never silently treated the same as a
    /// benign `Notification`.
    Anomaly,
}

/// Classify one handshake line against the request id currently awaited
/// (hardening-plan §4.6). Pure — no IO, no async. `perform_handshake` loops
/// reading lines (bounded by ONE overall handshake deadline, not a fresh
/// timeout per skipped line) and acts on the classification: skip
/// `Notification`s, accept `ExpectedResponse`, fail the handshake on
/// `Anomaly`.
pub fn classify_handshake_line(line: &str, expected_id: u64) -> HandshakeLineClass {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return HandshakeLineClass::Anomaly;
    }

    let v: Value = match serde_json::from_str(trimmed) {
        Ok(v) => v,
        Err(_) => return HandshakeLineClass::Anomaly,
    };

    if v.get("method").is_some() && v.get("id").is_none() {
        return HandshakeLineClass::Notification;
    }

    let Some(id) = v.get("id").and_then(Value::as_u64) else {
        return HandshakeLineClass::Anomaly;
    };

    if v.get("result").is_some() || v.get("error").is_some() {
        return if id == expected_id {
            HandshakeLineClass::ExpectedResponse
        } else {
            HandshakeLineClass::Anomaly
        };
    }

    HandshakeLineClass::Anomaly
}

/// Map one opencode ACP wire line to zero or more [`ProtocolEvent`]s, gating a
/// terminal (`result{stopReason}`/`error`) by JSON-RPC id (hardening-plan
/// §4.8): a terminal whose `id` does not match `expected_id` (or carries none
/// at all) is DROPPED (`vec![]`) rather than misrouted into the current turn
/// — the actor's single-writer invariant makes this structurally rare in
/// practice, but the id check is explicit defense-in-depth rather than an
/// unstated "at most one prompt outstanding" assumption. Id-less notifications
/// (streaming deltas) are unaffected by `expected_id`, since they carry no id
/// to correlate. Delegates to [`map_acp_line`] once the id gate passes.
pub fn map_acp_line_correlated(line: &str, expected_id: u64) -> Vec<ProtocolEvent> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return vec![];
    }

    let v: Value = match serde_json::from_str(trimmed) {
        Ok(v) => v,
        Err(_) => return vec![],
    };

    let is_terminal_response = v.get("error").is_some()
        || v.get("result")
            .and_then(|result| result.get("stopReason"))
            .is_some();

    if is_terminal_response {
        let Some(id) = v.get("id").and_then(Value::as_u64) else {
            return vec![];
        };
        if id != expected_id {
            return vec![];
        }
    }

    map_acp_line(trimmed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(line: &str) -> Vec<ProtocolEvent> {
        map_acp_line(line)
    }

    #[test]
    fn ignores_available_commands_update_from_spike_capture() {
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_079566635ffeaDOzjr6jJ5LKa7","update":{"sessionUpdate":"available_commands_update","availableCommands":[]}}}"#;
        assert_eq!(m(line), vec![]);
    }

    #[test]
    fn maps_agent_message_chunk_ok_from_spike_capture() {
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_079566635ffeaDOzjr6jJ5LKa7","update":{"sessionUpdate":"agent_message_chunk","messageId":"msg_f86a99a43001oJ18XtjzHIOWRO","content":{"type":"text","text":"OK"}}}}"#;
        assert_eq!(
            m(line),
            vec![ProtocolEvent::AssistantDelta {
                text: "OK".to_string()
            }]
        );
    }

    #[test]
    fn maps_agent_message_chunk_with_different_text_value() {
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_079566635ffeaDOzjr6jJ5LKa7","update":{"sessionUpdate":"agent_message_chunk","messageId":"msg_second","content":{"type":"text","text":"VIOLET-7F29"}}}}"#;
        assert_eq!(
            m(line),
            vec![ProtocolEvent::AssistantDelta {
                text: "VIOLET-7F29".to_string()
            }]
        );
    }

    #[test]
    fn two_chunk_lines_each_map_independently_without_accumulation() {
        let a = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_079566635ffeaDOzjr6jJ5LKa7","update":{"sessionUpdate":"agent_message_chunk","messageId":"msg_a","content":{"type":"text","text":"A"}}}}"#;
        let b = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_079566635ffeaDOzjr6jJ5LKa7","update":{"sessionUpdate":"agent_message_chunk","messageId":"msg_b","content":{"type":"text","text":"B"}}}}"#;
        assert_eq!(
            m(a),
            vec![ProtocolEvent::AssistantDelta {
                text: "A".to_string()
            }]
        );
        assert_eq!(
            m(b),
            vec![ProtocolEvent::AssistantDelta {
                text: "B".to_string()
            }]
        );
    }

    #[test]
    fn ignores_usage_update_from_spike_capture() {
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_079566635ffeaDOzjr6jJ5LKa7","update":{"sessionUpdate":"usage_update","used":3,"size":200000,"cost":{"amount":0,"currency":"USD"}}}}"#;
        assert_eq!(m(line), vec![]);
    }

    #[test]
    fn maps_prompt_result_stop_reason_to_empty_turn_completed_placeholder() {
        let line = r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","usage":{}}}"#;
        assert_eq!(
            m(line),
            vec![ProtocolEvent::TurnCompleted {
                reply: String::new()
            }]
        );
    }

    #[test]
    fn maps_jsonrpc_error_to_turn_failed_using_standard_jsonrpc_shape() {
        // Not spike-captured (the probe did not hit an error); this fixture uses
        // the standard JSON-RPC 2.0 error object shape as the documented form.
        let line = r#"{"jsonrpc":"2.0","id":3,"error":{"code":-32000,"message":"boom"}}"#;
        assert_eq!(
            m(line),
            vec![ProtocolEvent::TurnFailed {
                error: "boom".to_string()
            }]
        );
    }

    #[test]
    fn maps_jsonrpc_error_missing_message_to_fixed_fallback() {
        let line = r#"{"jsonrpc":"2.0","id":3,"error":{"code":-32000}}"#;
        assert_eq!(
            m(line),
            vec![ProtocolEvent::TurnFailed {
                error: "opencode acp json-rpc error with no message".to_string()
            }]
        );
    }

    #[test]
    fn ignores_malformed_blank_and_unknown_lines() {
        assert_eq!(m(""), vec![]);
        assert_eq!(m("   \t"), vec![]);
        assert_eq!(m("{not json]"), vec![]);
        assert_eq!(
            m(r#"{"jsonrpc":"2.0","id":99,"method":"initialize"}"#),
            vec![]
        );
    }

    #[test]
    fn tool_and_gate_shaped_updates_are_intentionally_unmapped_for_now() {
        // runtime-protocol.md 2.1/3.1 marks tool/gate update rows as ASSUMED
        // (not spike-captured yet), so Phase-5 intentionally ignores these
        // notifications until a real capture pins the concrete wire shape.
        let line = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_079566635ffeaDOzjr6jJ5LKa7","update":{"sessionUpdate":"tool_call_start","name":"shell"}}}"#;
        assert_eq!(m(line), vec![]);
    }

    #[test]
    fn accumulated_reply_cap_truncates_on_utf8_boundary_and_stops_growth() {
        let mut acc = String::new();
        let mut truncated = false;

        // 3-byte UTF-8 scalar repeated so a byte cap can bisect a codepoint.
        append_capped_reply(&mut acc, "界".repeat(8).as_str(), &mut truncated, 10);
        assert!(truncated, "accumulator should mark truncation once capped");
        assert_eq!(acc, "界界界…[truncated]");

        let len_after_cap = acc.len();
        append_capped_reply(&mut acc, "SHOULD-NOT-BE-APPENDED", &mut truncated, 10);
        assert_eq!(
            acc.len(),
            len_after_cap,
            "once truncated, accumulator must stop growing on later chunks"
        );
        assert_eq!(acc, "界界界…[truncated]");
    }

    // =================================================================
    // Regression guard for the in-flight `next_wait` bug (the pure
    // wait-duration policy `pull_one` calls). The bug: while a turn was in
    // flight the wait was capped at the short `POLL_IDLE` (500ms) breather
    // REGARDLESS of the real hang-watchdog budget, so `pull_one` returned a
    // spurious `None` after 500ms; the generic R1 actor then parked on its
    // mailbox with no re-poll timer and a real event arriving later was LOST
    // FOREVER. The fix waits the FULL remaining watchdog budget while in flight.
    // These table-driven cases pin that policy; reverting `next_wait` to the old
    // `POLL_IDLE.min(...)` body makes the first case FAIL.
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
