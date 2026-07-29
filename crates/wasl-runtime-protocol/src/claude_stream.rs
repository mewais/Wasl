//! `ClaudeStreamProtocol` — maps the real claude `stream-json` duplex onto R1's
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
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use uuid::Uuid;
use wasl_runtime::{
    ComposedPrompt, InteractiveProtocol, InteractiveTransport, NativeSessionId, ProtocolEvent,
    RuntimeCapabilities, RuntimeError, SpawnOptions,
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
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        let mut cmd = Command::new(&self.bin);
        cmd.arg("-p")
            .arg("--input-format")
            .arg("stream-json")
            .arg("--output-format")
            .arg("stream-json")
            .arg("--verbose")
            .arg("--session-id")
            .arg(session_id)
            .stdin(Stdio::piped())
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
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(_line)) = lines.next_line().await {}
        });

        let (ev_tx, ev_rx) = mpsc::unbounded_channel::<ProtocolEvent>();
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
        self.spawn_child(&native).await
    }

    async fn resume(
        &self,
        native: &NativeSessionId,
        _opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        self.spawn_child(&native.0).await
    }
}

struct ClaudeStreamTransport {
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
    ev_tx: mpsc::UnboundedSender<ProtocolEvent>,
    mut kill_rx: mpsc::Receiver<()>,
    stderr_task: JoinHandle<()>,
    timeouts: SupervisionTimeouts,
) {
    let mut lines = BufReader::new(stdout).lines();
    let pid = child.id();
    let signal_target = pid.and_then(|p| i32::try_from(p).ok()).map(|p| -p);

    'outer: loop {
        tokio::select! {
            line = lines.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        for ev in map_claude_line(&line) {
                            if ev_tx.send(ev).is_err() {
                                break 'outer;
                            }
                        }
                    }
                    Ok(None) | Err(_) => {
                        let code = escalate(&mut child, signal_target, timeouts).await;
                        let _ = ev_tx.send(ProtocolEvent::ProcessExited { code });
                        break;
                    }
                }
            }
            msg = kill_rx.recv() => {
                let _ = msg;
                let code = escalate(&mut child, signal_target, timeouts).await;
                let _ = ev_tx.send(ProtocolEvent::ProcessExited { code });
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

fn map_claude_line(line: &str) -> Vec<ProtocolEvent> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return vec![];
    }
    let v: Value = match serde_json::from_str(trimmed) {
        Ok(v) => v,
        Err(_) => return vec![],
    };
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
            let Some(content) = v
                .get("message")
                .and_then(|m| m.get("content"))
                .and_then(Value::as_array)
            else {
                return vec![];
            };
            let mut out = Vec::new();
            for block in content {
                if block.get("type").and_then(Value::as_str) == Some("text") {
                    if let Some(text) = block.get("text").and_then(Value::as_str) {
                        out.push(ProtocolEvent::AssistantDelta {
                            text: text.to_string(),
                        });
                    }
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
    fn tool_use_and_tool_result_are_intentionally_unmapped_for_now() {
        // runtime-protocol.md §2.1 marks both rows as ASSUMED (not spike-captured
        // yet), so Phase-4 intentionally does not map them until a real tool/gate
        // capture pins the concrete wire shape.
        let tool_use = r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Task"}]},"session_id":"405bb438-3cd5-4f5e-b682-a5e336d6b0c1"}"#;
        let tool_result = r#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"toolu_01","content":"ok"}]},"session_id":"405bb438-3cd5-4f5e-b682-a5e336d6b0c1"}"#;
        assert_eq!(m(tool_use), vec![]);
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
