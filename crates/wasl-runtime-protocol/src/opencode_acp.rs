//! `OpenCodeAcpProtocol`  maps opencode `acp` JSON-RPC-over-stdio onto R1's
//! generic [`wasl_runtime::ProtocolEvent`].

use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
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
}

impl Default for SupervisionTimeouts {
    fn default() -> Self {
        SupervisionTimeouts {
            interrupt_ack: Duration::from_secs(2),
            term_after: Duration::from_secs(2),
            kill_after: Duration::from_secs(2),
            hang_watchdog: Duration::from_secs(60),
            handshake_timeout: Duration::from_secs(30),
        }
    }
}

const POLL_IDLE: Duration = Duration::from_millis(500);

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
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        let mut child = Command::new(&self.bin)
            .arg("acp")
            .env("OPENCODE_DISABLE_PROJECT_CONFIG", "1")
            .env("OPENCODE_DISABLE_AUTOCOMPACT", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| RuntimeError::SpawnFailed {
                detail: format!("spawn {} acp: {e}", self.bin.display()),
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

        let mut lines = BufReader::new(stdout).lines();
        let session_id = self
            .perform_handshake(&mut stdin, &mut lines, session_method, requested_session_id)
            .await?;

        let stderr_task = tokio::spawn(async move {
            let mut err_lines = BufReader::new(stderr).lines();
            while let Ok(Some(_line)) = err_lines.next_line().await {}
        });

        let (ev_tx, ev_rx) = mpsc::unbounded_channel::<ProtocolEvent>();
        let (kill_tx, kill_rx) = mpsc::channel::<()>(1);
        let supervisor = tokio::spawn(supervise(
            child,
            lines,
            session_id.clone(),
            ev_tx,
            kill_rx,
            stderr_task,
            self.timeouts,
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
            accumulated_reply: String::new(),
        }))
    }

    async fn perform_handshake(
        &self,
        stdin: &mut ChildStdin,
        lines: &mut Lines<BufReader<ChildStdout>>,
        session_method: &str,
        requested_session_id: Option<&str>,
    ) -> Result<String, RuntimeError> {
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
        write_jsonrpc_line(stdin, &initialize).await?;

        let init_line =
            read_handshake_line(lines, self.timeouts.handshake_timeout, "initialize").await?;
        let init = parse_handshake_line(&init_line, 1, "initialize")?;
        ensure_no_jsonrpc_error(&init, "initialize")?;
        ensure_has_result(&init, "initialize")?;

        let cwd = std::env::current_dir()
            .map_err(|e| RuntimeError::SpawnFailed {
                detail: format!("opencode acp handshake current_dir: {e}"),
            })?
            .to_string_lossy()
            .to_string();

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
        write_jsonrpc_line(stdin, &session_req).await?;

        let session_line =
            read_handshake_line(lines, self.timeouts.handshake_timeout, session_method).await?;
        let session_resp = parse_handshake_line(&session_line, 2, session_method)?;
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
        _opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        self.spawn_or_resume("session/new", None).await
    }

    /// Resume is implemented with ACP `session/load` as the documented
    /// counterpart to `session/new`, but this exact wire shape is ASSUMED (not
    /// spike-captured): the Phase-0' probe captured resume capability
    /// advertisement only, not an exercised resume round-trip.
    async fn resume(
        &self,
        native: &NativeSessionId,
        _opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        self.spawn_or_resume("session/load", Some(&native.0)).await
    }
}

struct OpenCodeAcpTransport {
    stdin: Option<ChildStdin>,
    events: mpsc::UnboundedReceiver<ProtocolEvent>,
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
    accumulated_reply: String,
}

impl OpenCodeAcpTransport {
    async fn write_stdin_line(&mut self, text: &str) -> Result<(), RuntimeError> {
        let Some(stdin) = self.stdin.as_mut() else {
            return Ok(());
        };

        let mut line = text.as_bytes().to_vec();
        line.push(b'\n');
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
                    detail: format!("opencode acp stdin write: {e}"),
                });
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

        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);

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

        self.write_stdin_line(&payload).await
    }

    async fn next_event(&mut self) -> Option<ProtocolEvent> {
        loop {
            match self.pull_one().await {
                Some(ev) if self.is_stale_after_terminal(&ev) => continue,
                Some(ProtocolEvent::AssistantDelta { text }) => {
                    self.accumulated_reply.push_str(&text);
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

fn best_effort_signal(pid: u32, sig: &str) {
    let _ = std::process::Command::new("kill")
        .arg(sig)
        .arg(pid.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}

async fn supervise(
    mut child: tokio::process::Child,
    mut lines: Lines<BufReader<ChildStdout>>,
    session_id: String,
    ev_tx: mpsc::UnboundedSender<ProtocolEvent>,
    mut kill_rx: mpsc::Receiver<()>,
    stderr_task: JoinHandle<()>,
    timeouts: SupervisionTimeouts,
) {
    let pid = child.id();

    if ev_tx
        .send(ProtocolEvent::SessionReady {
            native: Some(NativeSessionId(session_id)),
        })
        .is_err()
    {
        let _ = child.wait().await;
        stderr_task.abort();
        return;
    }

    'outer: loop {
        tokio::select! {
            line = lines.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        for ev in map_acp_line(&line) {
                            if ev_tx.send(ev).is_err() {
                                break 'outer;
                            }
                        }
                    }
                    Ok(None) | Err(_) => {
                        let code = child.wait().await.ok().and_then(|s| s.code());
                        let _ = ev_tx.send(ProtocolEvent::ProcessExited { code });
                        break;
                    }
                }
            }
            msg = kill_rx.recv() => {
                let _ = msg;
                let code = escalate(&mut child, pid, timeouts).await;
                let _ = ev_tx.send(ProtocolEvent::ProcessExited { code });
                break;
            }
        }
    }

    let _ = child.wait().await;
    stderr_task.abort();
}

async fn escalate(
    child: &mut tokio::process::Child,
    pid: Option<u32>,
    t: SupervisionTimeouts,
) -> Option<i32> {
    if let Some(pid) = pid {
        best_effort_signal(pid, "-INT");
    }
    if let Some(code) = wait_for_exit(child, t.interrupt_ack).await {
        return code;
    }
    if let Some(pid) = pid {
        best_effort_signal(pid, "-TERM");
    }
    if let Some(code) = wait_for_exit(child, t.term_after).await {
        return code;
    }
    let _ = child.start_kill();
    if let Some(code) = wait_for_exit(child, t.kill_after).await {
        return code;
    }
    child.wait().await.ok().and_then(|s| s.code())
}

async fn wait_for_exit(child: &mut tokio::process::Child, bound: Duration) -> Option<Option<i32>> {
    match tokio::time::timeout(bound, child.wait()).await {
        Ok(Ok(status)) => Some(status.code()),
        Ok(Err(_)) => Some(None),
        Err(_) => None,
    }
}

async fn write_jsonrpc_line(stdin: &mut ChildStdin, value: &Value) -> Result<(), RuntimeError> {
    let payload = serde_json::to_string(value).map_err(|e| RuntimeError::SpawnFailed {
        detail: format!("opencode acp handshake serialize: {e}"),
    })?;
    let mut line = payload.into_bytes();
    line.push(b'\n');
    stdin
        .write_all(&line)
        .await
        .map_err(|e| RuntimeError::SpawnFailed {
            detail: format!("opencode acp handshake write: {e}"),
        })?;
    stdin.flush().await.map_err(|e| RuntimeError::SpawnFailed {
        detail: format!("opencode acp handshake flush: {e}"),
    })
}

async fn read_handshake_line(
    lines: &mut Lines<BufReader<ChildStdout>>,
    timeout: Duration,
    step: &str,
) -> Result<String, RuntimeError> {
    match tokio::time::timeout(timeout, lines.next_line()).await {
        Ok(Ok(Some(line))) => Ok(line),
        Ok(Ok(None)) => Err(RuntimeError::SpawnFailed {
            detail: format!("opencode acp handshake {step}: EOF"),
        }),
        Ok(Err(e)) => Err(RuntimeError::SpawnFailed {
            detail: format!("opencode acp handshake {step}: read error: {e}"),
        }),
        Err(_) => Err(RuntimeError::SpawnFailed {
            detail: format!(
                "opencode acp handshake {step}: timed out after {}s",
                timeout.as_secs()
            ),
        }),
    }
}

fn parse_handshake_line(line: &str, expected_id: i64, step: &str) -> Result<Value, RuntimeError> {
    let v: Value = serde_json::from_str(line).map_err(|e| RuntimeError::SpawnFailed {
        detail: format!("opencode acp handshake {step}: malformed JSON: {e}"),
    })?;
    let got_id = v.get("id").and_then(Value::as_i64);
    if got_id != Some(expected_id) {
        return Err(RuntimeError::SpawnFailed {
            detail: format!(
                "opencode acp handshake {step}: expected id {expected_id}, got {:?}",
                got_id
            ),
        });
    }
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
