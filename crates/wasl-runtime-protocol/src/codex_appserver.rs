//! `CodexAppServerProtocol` — maps the real `codex app-server --stdio`
//! JSON-RPC stream onto R1's generic [`wasl_runtime::ProtocolEvent`].

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use crate::bounded_io::{
    discard_one_bounded_line, read_bounded_line, BoundedLineRead, EVENT_CHANNEL_CAPACITY,
    MAX_STDOUT_LINE_BYTES,
};
use crate::json_rpc_handshake::{classify_handshake_line, HandshakeLineClass};
use crate::process_supervision::{escalate, next_wait, wait_for_exit};
use async_trait::async_trait;
use serde_json::Value;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use wasl_runtime::{
    ComposedPrompt, InteractiveProtocol, InteractiveTransport, NativeSessionId, ProtocolEvent,
    RuntimeCapabilities, RuntimeError, SpawnOptions,
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
    active_turn_id: Arc<Mutex<Option<String>>>,
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

pub struct CodexAppServerProtocol {
    bin: PathBuf,
    timeouts: SupervisionTimeouts,
}

impl CodexAppServerProtocol {
    pub fn new(bin: impl Into<PathBuf>) -> Self {
        CodexAppServerProtocol {
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
            // Intentionally locked false until a live interrupt round-trip is
            // independently captured, even though the schema advertises it.
            interruptible: false,
            structured_provenance: true,
            resumable: true,
        }
    }

    async fn spawn_or_resume(
        &self,
        thread_method: &str,
        requested_thread_id: Option<&str>,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        let mut cmd = Command::new(&self.bin);
        cmd.arg("app-server")
            .arg("--stdio")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        {
            cmd.as_std_mut().process_group(0);
        }

        let mut child = cmd.spawn().map_err(|e| RuntimeError::SpawnFailed {
            detail: format!("spawn {} app-server --stdio: {e}", self.bin.display()),
        })?;

        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| RuntimeError::SpawnFailed {
                detail: "codex app-server: no stdin pipe".to_string(),
            })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| RuntimeError::SpawnFailed {
                detail: "codex app-server: no stdout pipe".to_string(),
            })?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| RuntimeError::SpawnFailed {
                detail: "codex app-server: no stderr pipe".to_string(),
            })?;

        // Spawn stderr drain BEFORE the handshake to avoid pipe backpressure
        // deadlocking the startup exchange.
        let stderr_task = tokio::spawn(async move {
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
                thread_method,
                requested_thread_id,
            )
            .await?;

        let (ev_tx, ev_rx) = mpsc::channel::<ProtocolEvent>(EVENT_CHANNEL_CAPACITY);
        let (kill_tx, kill_rx) = mpsc::channel::<()>(1);
        let outstanding_request_id = Arc::new(AtomicU64::new(0));
        let active_turn_id = Arc::new(Mutex::new(None));
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
                active_turn_id: Arc::clone(&active_turn_id),
            },
        ));

        Ok(Box::new(CodexAppServerTransport {
            stdin: Some(stdin),
            events: ev_rx,
            kill_tx: Some(kill_tx),
            supervisor: Some(supervisor),
            timeouts: self.timeouts,
            turn_in_flight: false,
            last_event_at: Instant::now(),
            ended: false,
            turn_terminal_seen: false,
            session_ready_seen: false,
            thread_id: session_id,
            next_request_id: 3,
            outstanding_request_id,
            active_turn_id,
        }))
    }

    async fn perform_handshake(
        &self,
        stdin: &mut ChildStdin,
        reader: &mut BufReader<ChildStdout>,
        thread_method: &str,
        requested_thread_id: Option<&str>,
    ) -> Result<String, RuntimeError> {
        let deadline = Instant::now() + self.timeouts.handshake_timeout;

        let initialize = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "clientInfo": {
                    "name": "wasl",
                    "version": env!("CARGO_PKG_VERSION")
                },
                "capabilities": {
                    "experimentalApi": true
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

        let cwd = std::env::current_dir()
            .map_err(|e| RuntimeError::SpawnFailed {
                detail: format!("codex app-server handshake current_dir: {e}"),
            })?
            .to_string_lossy()
            .to_string();

        let thread_params = if thread_method == "thread/resume" {
            serde_json::json!({
                "threadId": requested_thread_id.unwrap_or_default(),
                "cwd": cwd
            })
        } else {
            serde_json::json!({
                "cwd": cwd
            })
        };

        let thread_req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": thread_method,
            "params": thread_params
        });
        write_jsonrpc_line(
            stdin,
            &thread_req,
            self.timeouts.stdin_write,
            deadline,
            thread_method,
        )
        .await?;

        let thread_line = read_expected_handshake_line(reader, 2, thread_method, deadline).await?;
        let thread_resp = parse_handshake_line(&thread_line, thread_method)?;
        ensure_no_jsonrpc_error(&thread_resp, thread_method)?;
        let result = ensure_has_result(&thread_resp, thread_method)?;

        let native = result
            .get("thread")
            .and_then(|t| t.get("id"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| requested_thread_id.map(ToOwned::to_owned))
            .ok_or_else(|| RuntimeError::SpawnFailed {
                detail: format!(
                    "codex app-server {thread_method} response missing result.thread.id and no fallback native id"
                ),
            })?;

        Ok(native)
    }
}

#[async_trait]
impl InteractiveProtocol for CodexAppServerProtocol {
    fn capabilities(&self) -> RuntimeCapabilities {
        Self::caps()
    }

    async fn spawn(
        &self,
        _opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        self.spawn_or_resume("thread/start", None).await
    }

    /// Resume is implemented with `thread/resume`. VERIFIED, not ASSUMED: the
    /// request/response shape was confirmed against the authoritative schema
    /// (`codex app-server generate-json-schema`), and the actual cross-process
    /// round-trip — a thread started by one OS process, resumed by a genuinely
    /// FRESH, separate OS process after the original has fully exited, still
    /// recalling pre-exit conversational context — was proven live twice: once
    /// by a standalone probe (`spikes/live-substrate/
    /// codex_appserver_resume_probe.py`) and again as a permanent
    /// regression-guarded test (`tests/codex_appserver_resume_live.rs`,
    /// `resume_in_a_fresh_process_recalls_pre_exit_context`, passing against the
    /// real CLI).
    async fn resume(
        &self,
        native: &NativeSessionId,
        _opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        self.spawn_or_resume("thread/resume", Some(&native.0)).await
    }
}

struct CodexAppServerTransport {
    stdin: Option<ChildStdin>,
    events: mpsc::Receiver<ProtocolEvent>,
    kill_tx: Option<mpsc::Sender<()>>,
    supervisor: Option<JoinHandle<()>>,
    timeouts: SupervisionTimeouts,
    turn_in_flight: bool,
    last_event_at: Instant,
    ended: bool,
    turn_terminal_seen: bool,
    session_ready_seen: bool,
    thread_id: String,
    next_request_id: u64,
    outstanding_request_id: Arc<AtomicU64>,
    active_turn_id: Arc<Mutex<Option<String>>>,
}

impl CodexAppServerTransport {
    async fn fail_write_with_unknown_outcome(&mut self, detail: String) -> RuntimeError {
        self.stdin = None;
        self.turn_in_flight = false;
        self.outstanding_request_id.store(0, Ordering::Release);
        if let Ok(mut guard) = self.active_turn_id.lock() {
            *guard = None;
        }
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
                        "codex app-server stdin write timed out after {:?}",
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
                    detail: format!("codex app-server stdin write: {e}"),
                });
            }
        }
        match tokio::time::timeout(self.timeouts.stdin_write, stdin.flush()).await {
            Err(_) => {
                let err = self
                    .fail_write_with_unknown_outcome(format!(
                        "codex app-server stdin flush timed out after {:?}",
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
                detail: format!("codex app-server stdin flush: {e}"),
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
                if let Ok(mut guard) = self.active_turn_id.lock() {
                    *guard = None;
                }
            }
            ProtocolEvent::ProcessExited { .. } => {
                self.turn_in_flight = false;
                self.ended = true;
                self.outstanding_request_id.store(0, Ordering::Release);
                if let Ok(mut guard) = self.active_turn_id.lock() {
                    *guard = None;
                }
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
impl InteractiveTransport for CodexAppServerTransport {
    async fn send_turn(&mut self, prompt: &ComposedPrompt) -> Result<(), RuntimeError> {
        self.turn_in_flight = true;
        self.turn_terminal_seen = false;
        self.last_event_at = Instant::now();

        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        self.outstanding_request_id
            .store(request_id, Ordering::Release);
        if let Ok(mut guard) = self.active_turn_id.lock() {
            *guard = None;
        }

        let line = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "turn/start",
            "params": {
                "threadId": self.thread_id,
                "input": [
                    {
                        "type": "text",
                        "text": prompt.0
                    }
                ]
            }
        });

        let payload = serde_json::to_string(&line).map_err(|e| RuntimeError::Transport {
            detail: format!("codex app-server send_turn serialization: {e}"),
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
                Some(ProtocolEvent::SessionReady { .. }) if self.session_ready_seen => continue,
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
        let turn_id = self
            .active_turn_id
            .lock()
            .ok()
            .and_then(|g| g.clone())
            .ok_or_else(|| RuntimeError::Unsupported {
                operation: "codex turn/interrupt requested with no active turn id".to_string(),
            })?;

        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        self.outstanding_request_id
            .store(request_id, Ordering::Release);

        let line = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "turn/interrupt",
            "params": {
                "threadId": self.thread_id,
                "turnId": turn_id,
            }
        });

        let payload = serde_json::to_string(&line).map_err(|e| RuntimeError::Transport {
            detail: format!("codex app-server interrupt serialization: {e}"),
        })?;

        match self.write_stdin_line(&payload).await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.outstanding_request_id.store(0, Ordering::Release);
                Err(err)
            }
        }
    }
}

impl Drop for CodexAppServerTransport {
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

fn update_active_turn_id_from_wire(
    line: &str,
    expected_request_id: u64,
    active_turn_id: &Arc<Mutex<Option<String>>>,
) {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return;
    }

    let Ok(v) = serde_json::from_str::<Value>(trimmed) else {
        return;
    };

    if v.get("method").and_then(Value::as_str) == Some("turn/started") {
        if let Some(turn_id) = v
            .get("params")
            .and_then(|p| p.get("turn"))
            .and_then(|t| t.get("id"))
            .and_then(Value::as_str)
        {
            if let Ok(mut guard) = active_turn_id.lock() {
                *guard = Some(turn_id.to_string());
            }
        }
        return;
    }

    if v.get("method").and_then(Value::as_str) == Some("turn/completed") {
        if let Ok(mut guard) = active_turn_id.lock() {
            *guard = None;
        }
        return;
    }

    let response_id_matches = v
        .get("id")
        .and_then(Value::as_u64)
        .map(|id| id == expected_request_id)
        .unwrap_or(false);

    if response_id_matches {
        if let Some(turn_id) = v
            .get("result")
            .and_then(|r| r.get("turn"))
            .and_then(|t| t.get("id"))
            .and_then(Value::as_str)
        {
            if let Ok(mut guard) = active_turn_id.lock() {
                *guard = Some(turn_id.to_string());
            }
        }
    }
}

/// KNOWN GAP, not silently hidden — read before touching approval handling:
///
/// The real codex app-server schema (`codex app-server generate-json-schema`,
/// `ServerRequest.json`) defines several methods the SERVER can send as a
/// genuine JSON-RPC REQUEST (i.e. carrying BOTH `method` and `id`, requiring a
/// `{"id":..,"result":..}` response written back to stdin before codex will
/// proceed): `execCommandApproval`, `applyPatchApproval`,
/// `item/commandExecution/requestApproval`, `item/fileChange/requestApproval`,
/// `item/permissions/requestApproval`, `item/tool/requestUserInput`,
/// `mcpServer/elicitation/request`. These exist for interactive approval flows
/// (e.g. a stricter `approvalPolicy` requiring per-command confirmation before
/// exec/patch).
///
/// `supervise`'s read loop never writes anything back to stdin outside the
/// handshake and `send_turn`, so this driver cannot answer ANY of these (or
/// any future method the schema adds). Rather than silently drop such a line
/// (leaving codex blocked forever waiting for an answer that will never
/// come — a hang bounded only by the hang watchdog, not an instant/visible
/// failure), `supervise` detects the JSON-RPC REQUEST shape itself
/// (`unanswerable_server_request_method`: `method` AND `id` present, no
/// `result`/`error` — a structural discriminator, not a curated method-name
/// list, so it also catches any future method) and immediately fails the
/// turn with a clear diagnostic naming the method, then tears the process
/// down (mirroring the `TooLong` handling below) since it is now permanently
/// stuck and cannot serve another turn either. This is a SAFE, POLICY-FREE
/// fix: it does not approve or deny anything, it only refuses and reports
/// loudly instead of hanging silently.
///
/// Why this has NOT manifested in this crate's own live tests/spikes: neither
/// `thread/start` nor `thread/resume` sets `approvalPolicy`/`sandbox` in this
/// driver (both fields are optional and omitted), so codex falls back to
/// whatever policy `~/.codex/config.toml`/its own defaults apply under
/// `experimentalApi: true`. A real, live-captured shell-command tool call
/// (`spikes/live-substrate/out/codex_appserver/codex_toolcall_capture.jsonl`)
/// completed end-to-end WITHOUT any approval-request-shaped line ever
/// appearing on the wire — i.e. under the config this crate has actually
/// exercised, approvals are not being requested. This is empirical evidence
/// the gap is currently DORMANT, not proof it can never trigger (a different
/// `approvalPolicy`, a different codex CLI default in a future version, or a
/// deployment explicitly setting a stricter policy could all trigger it).
///
/// STILL an open product/security decision, NOT fixed here deliberately: an
/// immediate, clear failure (what's implemented) is always correct as a
/// SAFETY FLOOR, but it is not the same as actually SUPPORTING approval
/// flows — the correct richer response (auto-approve? auto-deny? surface as
/// a new `ProtocolEvent` gate the caller must answer, mirroring
/// `NativeGateRequested`?) is a real product/security decision — auto-approving
/// arbitrary shell/file-change requests on an agent's behalf has real safety
/// implications and should not be picked unilaterally inside a driver. Flagged
/// for an explicit decision before this driver is used with any
/// `approvalPolicy` stricter than whatever default made the current live
/// tests pass.
async fn supervise(
    mut child: tokio::process::Child,
    mut lines: BufReader<ChildStdout>,
    session_id: String,
    ev_tx: mpsc::Sender<ProtocolEvent>,
    mut kill_rx: mpsc::Receiver<()>,
    stderr_task: JoinHandle<()>,
    supervision: SupervisionState,
) {
    let mut tracker = CodexToolTracker::new();
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
                        let unanswerable_method = parse_line_value(&line)
                            .as_ref()
                            .and_then(unanswerable_server_request_method)
                            .map(ToOwned::to_owned);
                        if let Some(method) = unanswerable_method {
                            // A genuine server-initiated JSON-RPC REQUEST this
                            // driver has no handler for (e.g. an approval
                            // decision). Left unanswered, codex would block
                            // forever waiting for a response that will never
                            // come -- this transport is now permanently
                            // useless, so fail the turn immediately (rather
                            // than a silent hang bounded only by the hang
                            // watchdog) and tear the process down, mirroring
                            // the `TooLong` handling above.
                            let _ = ev_tx
                                .send(ProtocolEvent::TurnFailed {
                                    error: format!(
                                        "codex app-server sent an unanswerable request \
                                         ({method}) this driver does not support answering \
                                         -- the process cannot make progress and is being \
                                         terminated"
                                    ),
                                })
                                .await;
                            let code = escalate(
                                &mut child,
                                signal_target,
                                supervision.timeouts.interrupt_ack,
                                supervision.timeouts.term_after,
                                supervision.timeouts.kill_after,
                            )
                            .await;
                            let _ = ev_tx.send(ProtocolEvent::ProcessExited { code }).await;
                            break;
                        }
                        let expected_id = supervision.outstanding_request_id.load(Ordering::Acquire);
                        update_active_turn_id_from_wire(&line, expected_id, &supervision.active_turn_id);
                        for ev in tracker.observe_line(&line) {
                            if ev_tx.send(ev).await.is_err() {
                                break 'outer;
                            }
                        }
                    }
                    Ok(BoundedLineRead::TooLong) => {
                        let _ = ev_tx
                            .send(ProtocolEvent::TurnFailed {
                                error: format!(
                                    "codex app-server stdout line exceeded {} bytes",
                                    MAX_STDOUT_LINE_BYTES
                                ),
                            })
                            .await;
                        let code = escalate(
                            &mut child,
                            signal_target,
                            supervision.timeouts.interrupt_ack,
                            supervision.timeouts.term_after,
                            supervision.timeouts.kill_after,
                        )
                        .await;
                        let _ = ev_tx.send(ProtocolEvent::ProcessExited { code }).await;
                        break;
                    }
                    Ok(BoundedLineRead::Eof) | Err(_) => {
                        let code = escalate(
                            &mut child,
                            signal_target,
                            supervision.timeouts.interrupt_ack,
                            supervision.timeouts.term_after,
                            supervision.timeouts.kill_after,
                        )
                        .await;
                        let _ = ev_tx.send(ProtocolEvent::ProcessExited { code }).await;
                        break;
                    }
                }
            }
            msg = kill_rx.recv() => {
                let _ = msg;
                let code = escalate(
                    &mut child,
                    signal_target,
                    supervision.timeouts.interrupt_ack,
                    supervision.timeouts.term_after,
                    supervision.timeouts.kill_after,
                )
                .await;
                let _ = ev_tx.send(ProtocolEvent::ProcessExited { code }).await;
                break;
            }
        }
    }

    let _ = wait_for_exit(&mut child, supervision.timeouts.kill_after).await;
    stderr_task.abort();
}

async fn write_jsonrpc_line(
    stdin: &mut ChildStdin,
    value: &Value,
    stdin_write_timeout: Duration,
    handshake_deadline: Instant,
    step: &str,
) -> Result<(), RuntimeError> {
    let payload = serde_json::to_string(value).map_err(|e| RuntimeError::SpawnFailed {
        detail: format!("codex app-server handshake serialize: {e}"),
    })?;
    let mut line = payload.into_bytes();
    line.push(b'\n');

    let write_bound = remaining_handshake_time(handshake_deadline, step)?.min(stdin_write_timeout);
    tokio::time::timeout(write_bound, stdin.write_all(&line))
        .await
        .map_err(|_| RuntimeError::SpawnFailed {
            detail: format!(
                "codex app-server handshake {step}: write timed out after {:?}",
                write_bound
            ),
        })?
        .map_err(|e| RuntimeError::SpawnFailed {
            detail: format!("codex app-server handshake {step}: write: {e}"),
        })?;

    let flush_bound = remaining_handshake_time(handshake_deadline, step)?.min(stdin_write_timeout);
    tokio::time::timeout(flush_bound, stdin.flush())
        .await
        .map_err(|_| RuntimeError::SpawnFailed {
            detail: format!(
                "codex app-server handshake {step}: flush timed out after {:?}",
                flush_bound
            ),
        })?
        .map_err(|e| RuntimeError::SpawnFailed {
            detail: format!("codex app-server handshake {step}: flush: {e}"),
        })
}

fn remaining_handshake_time(deadline: Instant, step: &str) -> Result<Duration, RuntimeError> {
    let now = Instant::now();
    if now >= deadline {
        return Err(RuntimeError::SpawnFailed {
            detail: format!("codex app-server handshake {step}: timed out"),
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
            detail: format!("codex app-server handshake {step}: EOF"),
        }),
        Ok(Ok(BoundedLineRead::TooLong)) => Err(RuntimeError::SpawnFailed {
            detail: format!(
                "codex app-server handshake {step}: line exceeded {} bytes",
                MAX_STDOUT_LINE_BYTES
            ),
        }),
        Ok(Err(e)) => Err(RuntimeError::SpawnFailed {
            detail: format!("codex app-server handshake {step}: read error: {e}"),
        }),
        Err(_) => Err(RuntimeError::SpawnFailed {
            detail: format!(
                "codex app-server handshake {step}: timed out after {:?} (remaining handshake budget)",
                timeout
            ),
        }),
    }
}

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
                        "codex app-server handshake {step}: protocol anomaly while awaiting id \
                         {expected_id}: {}",
                        truncate_for_diagnostic(&line)
                    ),
                });
            }
        }
    }
}

fn parse_handshake_line(line: &str, step: &str) -> Result<Value, RuntimeError> {
    serde_json::from_str(line).map_err(|e| RuntimeError::SpawnFailed {
        detail: format!("codex app-server handshake {step}: malformed JSON: {e}"),
    })
}

fn ensure_no_jsonrpc_error(v: &Value, step: &str) -> Result<(), RuntimeError> {
    if let Some(error) = v.get("error") {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("unknown json-rpc error during handshake");
        return Err(RuntimeError::SpawnFailed {
            detail: format!("codex app-server handshake {step}: {message}"),
        });
    }
    Ok(())
}

fn ensure_has_result<'a>(v: &'a Value, step: &str) -> Result<&'a Value, RuntimeError> {
    v.get("result").ok_or_else(|| RuntimeError::SpawnFailed {
        detail: format!("codex app-server handshake {step}: missing result"),
    })
}

fn parse_line_value(line: &str) -> Option<Value> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    serde_json::from_str(trimmed).ok()
}

/// Detect a genuine JSON-RPC REQUEST sent BY THE SERVER (codex), as opposed to
/// a notification or a response to one of OUR OWN outgoing requests.
///
/// Per JSON-RPC 2.0: a Request has `method` AND `id` (no `result`/`error`); a
/// Notification has `method`, no `id`; a Response has `id` and EXACTLY ONE of
/// `result`/`error`, no `method`. This driver only ever sends `initialize`/
/// `thread/start`/`thread/resume`/`turn/start` requests itself, so any line
/// matching the Request shape is BY CONSTRUCTION the server asking THIS
/// DRIVER for something (e.g. an approval decision — `execCommandApproval`,
/// `applyPatchApproval`, `item/*/requestApproval`, `item/tool/requestUserInput`,
/// `mcpServer/elicitation/request`, or any future method the schema adds) that
/// this driver does not implement answering. Returns the method name for a
/// clear diagnostic; deliberately NOT a curated method-name list — matching
/// the Request SHAPE means this correctly generalizes to any such method,
/// present or future, without needing to be updated when codex adds new ones.
///
/// Deliberately does NOT inspect the `jsonrpc` field: codex's own real
/// notifications omit it (see `AGENTS.md`'s "notifications carry NO
/// `id`/`jsonrpc`"), so requiring it would be strictly LESS conservative, not
/// more — it could only ever narrow the match, never widen it into a false
/// positive.
fn unanswerable_server_request_method(v: &Value) -> Option<&str> {
    let method = v.get("method").and_then(Value::as_str)?;
    v.get("id")?; // a notification (no id) is not a request
    if v.get("result").is_some() || v.get("error").is_some() {
        return None; // malformed/theoretical: a line can't be both a request and a response
    }
    Some(method)
}

fn final_agent_message_text(turn: &Value) -> Option<String> {
    turn.get("items")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|items| items.iter())
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("agentMessage"))
        .filter_map(|item| item.get("text").and_then(Value::as_str))
        .next_back()
        .map(ToOwned::to_owned)
}

fn map_codex_value(v: &Value) -> Vec<ProtocolEvent> {
    match v.get("method").and_then(Value::as_str) {
        Some("thread/started") => {
            let native = v
                .get("params")
                .and_then(|p| p.get("thread"))
                .and_then(|t| t.get("id"))
                .and_then(Value::as_str)
                .map(|id| NativeSessionId(id.to_string()));
            match native {
                Some(native) => vec![ProtocolEvent::SessionReady {
                    native: Some(native),
                }],
                None => vec![],
            }
        }
        Some("item/agentMessage/delta") => v
            .get("params")
            .and_then(|p| p.get("delta"))
            .and_then(Value::as_str)
            .map(|delta| {
                vec![ProtocolEvent::AssistantDelta {
                    text: delta.to_string(),
                }]
            })
            .unwrap_or_default(),
        Some("turn/completed") => {
            let turn = match v.get("params").and_then(|p| p.get("turn")) {
                Some(turn) => turn,
                None => return vec![],
            };
            match turn.get("status").and_then(Value::as_str) {
                Some("completed") => vec![ProtocolEvent::TurnCompleted {
                    reply: final_agent_message_text(turn).unwrap_or_default(),
                }],
                Some("failed") => {
                    let error = turn
                        .get("error")
                        .and_then(|e| e.get("message"))
                        .and_then(Value::as_str)
                        .unwrap_or("codex turn failed with no error.message")
                        .to_string();
                    vec![ProtocolEvent::TurnFailed { error }]
                }
                Some("interrupted") => vec![ProtocolEvent::TurnFailed {
                    error: "codex turn interrupted".to_string(),
                }],
                _ => vec![],
            }
        }
        Some("item/started") => {
            let item = match v.get("params").and_then(|p| p.get("item")) {
                Some(item) => item,
                None => return vec![],
            };
            if item.get("type").and_then(Value::as_str) != Some("commandExecution") {
                return vec![];
            }
            item.get("command")
                .and_then(Value::as_str)
                .map(|name| {
                    vec![ProtocolEvent::ToolStarted {
                        name: name.to_string(),
                    }]
                })
                .unwrap_or_default()
        }
        Some("item/completed") => {
            let item = match v.get("params").and_then(|p| p.get("item")) {
                Some(item) => item,
                None => return vec![],
            };
            if item.get("type").and_then(Value::as_str) == Some("commandExecution") {
                return vec![];
            }
            vec![]
        }
        _ => vec![],
    }
}

/// PURE per-line codex map: no cross-line state.
pub fn map_codex_line(line: &str) -> Vec<ProtocolEvent> {
    let Some(v) = parse_line_value(line) else {
        return vec![];
    };
    map_codex_value(&v)
}

/// Stateful commandExecution correlator for codex app-server lines.
#[derive(Debug, Default)]
pub struct CodexToolTracker {
    command_by_item_id: HashMap<String, String>,
}

impl CodexToolTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe_line(&mut self, line: &str) -> Vec<ProtocolEvent> {
        let Some(v) = parse_line_value(line) else {
            return vec![];
        };

        let method = v.get("method").and_then(Value::as_str);
        let item = v.get("params").and_then(|p| p.get("item"));

        if matches!(method, Some("item/started" | "item/completed"))
            && item.and_then(|i| i.get("type")).and_then(Value::as_str) == Some("commandExecution")
        {
            let item = item.expect("checked above");
            let item_id = item.get("id").and_then(Value::as_str);
            match method {
                Some("item/started") => {
                    let Some(id) = item_id else {
                        return vec![];
                    };
                    let Some(command) = item.get("command").and_then(Value::as_str) else {
                        return vec![];
                    };
                    self.command_by_item_id
                        .insert(id.to_string(), command.to_string());
                    return vec![ProtocolEvent::ToolStarted {
                        name: command.to_string(),
                    }];
                }
                Some("item/completed") => {
                    let Some(id) = item_id else {
                        return vec![];
                    };
                    let Some(name) = self.command_by_item_id.remove(id) else {
                        return vec![];
                    };
                    let ok = item.get("exitCode").and_then(Value::as_i64) == Some(0);
                    return vec![ProtocolEvent::ToolFinished { name, ok }];
                }
                _ => {}
            }
        }

        if method == Some("turn/completed") {
            // Turn boundary: drop any commandExecution item ids whose
            // `item/completed` never arrived (e.g. the turn was
            // cancelled/failed mid-tool-call). Without this, an interrupted
            // tool call would leak one small entry per turn for the life of
            // the session — the same unbounded-growth concern this crate's
            // resource-bounds hardening (event channel / line-read /
            // accumulator caps) closed elsewhere, and the same fix already
            // applied to `ClaudeToolTracker` on its own turn boundary
            // (`result`): a turn's own id namespace should not outlive the
            // turn.
            //
            // Deliberately keyed on the METHOD, not `params.turn.status`:
            // the real captured wire (spike, `events.jsonl`) confirms all
            // terminal outcomes — `completed`/`failed`/`interrupted` — ride
            // the SAME `turn/completed` notification, just with a different
            // embedded `status`; there is no distinct `turn/failed` method
            // in the real capture (only `completed` was actually observed
            // live; `failed`/`interrupted` are ASSUMED to share this method
            // per the schema, not yet captured). If a future live capture
            // ever shows a genuinely separate terminal method, this clear
            // must be extended to it too.
            self.command_by_item_id.clear();
        }

        map_codex_value(&v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unanswerable_server_request_detects_id_plus_method_shape() {
        // A genuine server-initiated REQUEST: has both `method` and `id`.
        let approval = r#"{"jsonrpc":"2.0","id":42,"method":"execCommandApproval","params":{"command":"rm -rf /"}}"#;
        assert_eq!(
            unanswerable_server_request_method(&serde_json::from_str(approval).unwrap()),
            Some("execCommandApproval")
        );

        let patch_approval =
            r#"{"jsonrpc":"2.0","id":7,"method":"item/fileChange/requestApproval","params":{}}"#;
        assert_eq!(
            unanswerable_server_request_method(&serde_json::from_str(patch_approval).unwrap()),
            Some("item/fileChange/requestApproval")
        );

        // A notification: `method` but no `id` -- NOT a request.
        let notification = r#"{"method":"turn/completed","params":{}}"#;
        assert_eq!(
            unanswerable_server_request_method(&serde_json::from_str(notification).unwrap()),
            None
        );

        // A response to OUR OWN request: `id` + `result`, no `method` -- NOT a request.
        let our_response = r#"{"id":3,"result":{"turn":{"id":"t1"}}}"#;
        assert_eq!(
            unanswerable_server_request_method(&serde_json::from_str(our_response).unwrap()),
            None
        );

        // A response carrying an error to OUR OWN request -- also NOT a request.
        let our_error_response = r#"{"id":3,"error":{"message":"boom"}}"#;
        assert_eq!(
            unanswerable_server_request_method(&serde_json::from_str(our_error_response).unwrap()),
            None
        );

        // Malformed/theoretical: `method` + `id` + `result` all present at
        // once -- not a real JSON-RPC shape (a line can't be a request AND a
        // response), but the helper must not misclassify it as an
        // unanswerable request either.
        let malformed_both = r#"{"id":9,"method":"execCommandApproval","result":{}}"#;
        assert_eq!(
            unanswerable_server_request_method(&serde_json::from_str(malformed_both).unwrap()),
            None
        );
    }

    #[test]
    fn pure_map_ignores_ack_and_maps_terminal_reply_from_turn_items() {
        let ack = r#"{"id":3,"result":{"turn":{"id":"t1","status":"inProgress"}}}"#;
        assert_eq!(map_codex_line(ack), vec![]);

        let completed = r#"{"method":"turn/completed","params":{"threadId":"x","turn":{"id":"t1","items":[{"type":"agentMessage","text":"A"},{"type":"agentMessage","text":"B"}],"status":"completed","error":null}}}"#;
        assert_eq!(
            map_codex_line(completed),
            vec![ProtocolEvent::TurnCompleted {
                reply: "B".to_string()
            }]
        );
    }

    #[test]
    fn tool_tracker_uses_item_id_correlation_and_ignores_unknown_completed() {
        let started = r#"{"method":"item/started","params":{"item":{"type":"commandExecution","id":"call_1","command":"echo hi"}}}"#;
        let completed = r#"{"method":"item/completed","params":{"item":{"type":"commandExecution","id":"call_1","exitCode":0}}}"#;
        let foreign = r#"{"method":"item/completed","params":{"item":{"type":"commandExecution","id":"call_2","exitCode":0}}}"#;

        let mut tracker = CodexToolTracker::new();
        assert_eq!(
            tracker.observe_line(started),
            vec![ProtocolEvent::ToolStarted {
                name: "echo hi".to_string()
            }]
        );
        assert_eq!(tracker.observe_line(foreign), vec![]);
        assert_eq!(
            tracker.observe_line(completed),
            vec![ProtocolEvent::ToolFinished {
                name: "echo hi".to_string(),
                ok: true
            }]
        );
    }

    #[test]
    fn tool_tracker_clears_pending_ids_on_turn_completed_boundary() {
        let started = r#"{"method":"item/started","params":{"item":{"type":"commandExecution","id":"call_1","command":"echo hi"}}}"#;
        let completed_turn = r#"{"method":"turn/completed","params":{"threadId":"x","turn":{"id":"t1","items":[],"status":"completed","error":null}}}"#;
        let late_completed = r#"{"method":"item/completed","params":{"item":{"type":"commandExecution","id":"call_1","exitCode":0}}}"#;

        let mut tracker = CodexToolTracker::new();
        assert_eq!(
            tracker.observe_line(started),
            vec![ProtocolEvent::ToolStarted {
                name: "echo hi".to_string()
            }]
        );

        tracker.observe_line(completed_turn);

        assert_eq!(
            tracker.observe_line(late_completed),
            vec![],
            "a commandExecution id from a prior turn must not leak past turn/completed"
        );
    }

    #[test]
    fn handshake_classifier_requires_method_for_idless_notification_shape() {
        assert_eq!(
            classify_handshake_line(r#"{"method":"thread/started","params":{}}"#, 2),
            HandshakeLineClass::Notification
        );
        assert_eq!(
            classify_handshake_line(r#"{"result":{}}"#, 2),
            HandshakeLineClass::Anomaly
        );
    }
}
