//! The receiving side: custody of delivered tasks, one FIFO per receiving session, and the
//! worker that turns a task into a turn of that session and derives progress and the result
//! from the session's events.

use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;
use wasit_core::{AgentEvent, Command, EndReason, Usage};
use wasit_runtime::wire::{Phase, SessionEventKind, SessionId};
use wasit_runtime::StreamEnd;
use wasl_proto::{
    codes, Ack, CancelRemoteRequest, Progress, ResultStatus, Task, TaskId, TaskMode, TaskResult,
    TaskState, UpdateRequest,
};

use crate::node::{err, finished_audit, now_ms, truncate, Node};
use crate::state::{InPersist, InRec, TurnMark};

/// Marker that identifies the prompt of a task in the session's input events.
fn marker(id: &TaskId) -> String {
    format!("[wasl task {id}]")
}

/// The turn prompt: a short provenance header, then the instruction.
pub(crate) fn prompt(task: &Task, from_name: &str) -> String {
    let who = if from_name.is_empty() {
        task.from.to_string()
    } else {
        format!("{from_name} ({})", task.from)
    };
    let ask = match task.mode {
        TaskMode::Consult => {
            "Consultation: answer the question below. Do not modify files or run side-effecting commands."
        }
        TaskMode::Command => "Task: carry out the instruction below.",
        TaskMode::Message => "Message for you; no reply is expected.",
    };
    let mut s = format!(
        "{} {} from {who}\n{ask}\nYour final message in this turn is returned to the sender as the result; keep it self-contained.\n---\n{}",
        marker(&task.id),
        task.mode.as_str(),
        task.instruction
    );
    if !task.attachments.is_empty() {
        s.push_str("\n---\nAttachments: ");
        s.push_str(&task.attachments.join(", "));
    }
    s
}

/// Resolve with the withdrawal once one is set (`None` if the record went away).
async fn wait_cancel(
    rx: &mut watch::Receiver<Option<CancelRemoteRequest>>,
) -> Option<CancelRemoteRequest> {
    match rx.wait_for(|c| c.is_some()).await {
        Ok(c) => c.clone(),
        Err(_) => None,
    }
}

fn usage_delta(before: &Usage, after: &Usage) -> Usage {
    Usage {
        input_tokens: after.input_tokens.saturating_sub(before.input_tokens),
        output_tokens: after.output_tokens.saturating_sub(before.output_tokens),
        cache_read_tokens: after
            .cache_read_tokens
            .saturating_sub(before.cache_read_tokens),
        cache_creation_tokens: after
            .cache_creation_tokens
            .saturating_sub(before.cache_creation_tokens),
        cost_usd: (after.cost_usd - before.cost_usd).max(0.0),
    }
}

impl Node {
    // ── node-to-node handlers (receiving side) ──────────────────────────────

    /// `wasl.node.deliver`: take custody (idempotent).
    pub async fn node_deliver(&self, task: Task) -> Result<Ack, wasit_control::RpcError> {
        if task.to.host != self.host_id() {
            return Err(Node::not_local(&task.to.host));
        }
        let session = task.to.session.clone();
        let ack = {
            let mut st = self.lock();
            if st.inbound.contains_key(&task.id) {
                return Ok(Ack {});
            }
            let known =
                SessionId::parse(&session).is_some_and(|id| self.sessions().snapshot(&id).is_ok());
            let state = if !known {
                TaskState::Failed {
                    error: format!("no session {session} on host {}", self.host_id()),
                }
            } else if task.deadline_ms <= now_ms() {
                TaskState::Expired
            } else {
                TaskState::Queued
            };
            let id = task.id.clone();
            let queued = state == TaskState::Queued;
            st.inbound.insert(
                id.clone(),
                InRec {
                    p: InPersist {
                        task,
                        state,
                        updated_ms: now_ms(),
                        reported: false,
                        turn: None,
                        cancel: None,
                    },
                    rev: 0,
                    cancel: watch::channel(None).0,
                },
            );
            if queued {
                st.queues
                    .entry(session.clone())
                    .or_default()
                    .fifo
                    .push_back(id.clone());
            }
            self.persist_in(&mut st, &id)
        };
        if let Some(a) = ack {
            crate::journal::durable(a)
                .await
                .map_err(|e| err(codes::IO, e.to_string()))?;
        }
        self.ensure_worker(&session);
        self.inner.deadlines.notify_one();
        self.inner.kick.notify_one();
        Ok(Ack {})
    }

    /// `wasl.node.update`: the receiver reports a state (idempotent; terminal is final).
    pub async fn node_update(&self, req: UpdateRequest) -> Result<Ack, wasit_control::RpcError> {
        // Audit a terminal state before it becomes visible to waiters.
        let audit = {
            let st = self.lock();
            let Some(r) = st.out.get(&req.id) else {
                return Err(err(codes::UNKNOWN_TASK, format!("no task {}", req.id)));
            };
            if r.p.state.is_terminal() || r.p.state == req.state {
                return Ok(Ack {});
            }
            req.state
                .is_terminal()
                .then(|| finished_audit(&r.p.task, &req.state))
        };
        if let Some(a) = audit {
            // Logged by `record_audit`; the receiver's report is applied regardless.
            let _ = self.record_audit(a).await;
        }
        let ack = {
            let mut st = self.lock();
            let Some(r) = st.out.get_mut(&req.id) else {
                return Ok(Ack {});
            };
            if r.p.state.is_terminal() {
                return Ok(Ack {});
            }
            r.p.state = req.state.clone();
            r.p.delivered = true;
            let ws = r.p.task.workstream.clone();
            if let TaskState::Completed { result } = &req.state {
                if let Some(u) = &result.usage {
                    *st.spent.entry(ws).or_default() += u.cost_usd;
                }
            }
            self.persist_out(&mut st, &req.id)
        };
        self.settle(ack).await;
        Ok(Ack {})
    }

    /// `wasl.node.cancel`: the sender withdrew a task.
    pub async fn node_cancel(
        &self,
        req: CancelRemoteRequest,
    ) -> Result<Ack, wasit_control::RpcError> {
        let ack = {
            let mut st = self.lock();
            let Some(r) = st.inbound.get_mut(&req.id) else {
                // Never delivered here (or long gone): nothing to stop.
                return Ok(Ack {});
            };
            if r.p.state.is_terminal() {
                return Ok(Ack {});
            }
            let session = r.p.task.to.session.clone();
            let running = st.queues.get(&session).and_then(|q| q.running.as_ref()) == Some(&req.id);
            let Some(r) = st.inbound.get_mut(&req.id) else {
                return Ok(Ack {});
            };
            if running {
                // The worker interrupts the turn and reports the final state.
                r.p.cancel = Some(req.clone());
                r.cancel.send_replace(Some(req.clone()));
            } else {
                r.p.state = if req.expired {
                    TaskState::Expired
                } else {
                    TaskState::Cancelled {
                        reason: req.reason.clone(),
                    }
                };
                r.p.reported = false;
                if let Some(q) = st.queues.get_mut(&session) {
                    q.fifo.retain(|x| *x != req.id);
                }
            }
            self.persist_in(&mut st, &req.id)
        };
        self.settle(ack).await;
        self.inner.kick.notify_one();
        Ok(Ack {})
    }

    // ── workers ─────────────────────────────────────────────────────────────

    /// Start the worker of `session` unless it runs.
    pub(crate) fn ensure_worker(&self, session: &str) {
        {
            let mut st = self.lock();
            let Some(q) = st.queues.get_mut(session) else {
                return;
            };
            if q.worker || (q.fifo.is_empty() && q.running.is_none()) {
                return;
            }
            q.worker = true;
        }
        let node = self.clone();
        let session = session.to_string();
        tokio::spawn(async move { node.worker(session).await });
    }

    async fn worker(self, session: String) {
        loop {
            let next = {
                let mut st = self.lock();
                let st = &mut *st;
                let Some(q) = st.queues.get_mut(&session) else {
                    return;
                };
                let next = match q.running.clone() {
                    Some(id) => Some(id),
                    None => loop {
                        match q.fifo.pop_front() {
                            Some(id)
                                if st
                                    .inbound
                                    .get(&id)
                                    .is_some_and(|r| r.p.state == TaskState::Queued) =>
                            {
                                break Some(id)
                            }
                            Some(_) => continue,
                            None => break None,
                        }
                    },
                };
                match next {
                    Some(id) => {
                        q.running = Some(id.clone());
                        id
                    }
                    None => {
                        st.queues.remove(&session);
                        return;
                    }
                }
            };
            let state = self.run_task(&session, &next).await;
            let ack = {
                let mut st = self.lock();
                if let Some(q) = st.queues.get_mut(&session) {
                    q.running = None;
                }
                match st.inbound.get_mut(&next) {
                    Some(r) if !r.p.state.is_terminal() => {
                        r.p.state = state;
                        r.p.reported = false;
                        self.persist_in(&mut st, &next)
                    }
                    _ => None,
                }
            };
            self.settle(ack).await;
            self.inner.kick.notify_one();
        }
    }

    /// Set the inbound state (reported to the sender by the pump).
    async fn set_in_state(&self, id: &TaskId, state: TaskState, turn: Option<TurnMark>) {
        let ack = {
            let mut st = self.lock();
            let Some(r) = st.inbound.get_mut(id) else {
                return;
            };
            if r.p.state.is_terminal() {
                return;
            }
            r.p.state = state;
            r.p.reported = false;
            if turn.is_some() {
                r.p.turn = turn;
            }
            self.persist_in(&mut st, id)
        };
        self.settle(ack).await;
        self.inner.kick.notify_one();
    }

    /// Run one task as a turn of `session`; returns its terminal state.
    async fn run_task(&self, session: &str, id: &TaskId) -> TaskState {
        let Some(sid) = SessionId::parse(session) else {
            return TaskState::Failed {
                error: format!("not a session id: {session}"),
            };
        };
        let (task, mark, mut cancel, from_name) = {
            let st = self.lock();
            let Some(r) = st.inbound.get(id) else {
                return TaskState::Failed {
                    error: "task record vanished".into(),
                };
            };
            (
                r.p.task.clone(),
                r.p.turn.clone(),
                r.cancel.subscribe(),
                st.name_of(&r.p.task.from),
            )
        };
        let deadline = {
            let left = task.deadline_ms.saturating_sub(now_ms());
            Instant::now() + Duration::from_millis(left)
        };
        let cancelled_state = |c: &CancelRemoteRequest| {
            if c.expired {
                TaskState::Expired
            } else {
                TaskState::Cancelled {
                    reason: c.reason.clone(),
                }
            }
        };
        let (mark, resubmit_check) = match mark {
            Some(m) => {
                // Recovery: was the prompt sent before the restart?
                // The head of the session's journal, read from the journal itself: right after
                // a daemon restart the session's snapshot is still blank (cursor 0) or only
                // partly replayed, and trusting it would resubmit a prompt that did go out.
                let now_cursor = match self.sessions().history(&sid, None, 1).await {
                    Ok(p) => p.events.last().map(|e| e.cursor),
                    Err(_) => None,
                }
                .unwrap_or(0);
                (m, Some(now_cursor))
            }
            None => {
                // Wait for the session to be free (bounded by the deadline and by a cancel).
                match self.ready(&sid, deadline, &mut cancel).await {
                    Ok(()) => {}
                    Err(state) => return state,
                }
                let snap = match self.sessions().snapshot(&sid) {
                    Ok(s) => s,
                    Err(e) => {
                        return TaskState::Failed {
                            error: e.to_string(),
                        }
                    }
                };
                let m = TurnMark {
                    cursor: snap.cursor,
                    usage_before: snap.usage.clone(),
                };
                self.set_in_state(
                    id,
                    TaskState::Running {
                        progress: Progress::default(),
                    },
                    Some(m.clone()),
                )
                .await;
                if let Err(e) = self
                    .sessions()
                    .command(
                        &sid,
                        Command::Prompt {
                            text: prompt(&task, &from_name),
                        },
                    )
                    .await
                {
                    return TaskState::Failed {
                        error: format!("the receiving session did not accept the task: {e}"),
                    };
                }
                (m, None)
            }
        };
        self.observe(
            &sid,
            id,
            &task,
            &from_name,
            mark,
            resubmit_check,
            deadline,
            cancel,
        )
        .await
        .unwrap_or_else(|c| cancelled_state(&c))
    }

    /// Wait until `sid` can take a prompt (continuing it if its process ended).
    async fn ready(
        &self,
        sid: &SessionId,
        deadline: Instant,
        cancel: &mut watch::Receiver<Option<CancelRemoteRequest>>,
    ) -> Result<(), TaskState> {
        let snap = self
            .sessions()
            .snapshot(sid)
            .map_err(|e| TaskState::Failed {
                error: e.to_string(),
            })?;
        if !snap.phase.is_live() {
            self.sessions()
                .continue_session(sid)
                .await
                .map_err(|e| TaskState::Failed {
                    error: format!(
                        "the receiving session is not running and could not be continued: {e}"
                    ),
                })?;
        }
        let mut w = self.sessions().watch(sid).map_err(|e| TaskState::Failed {
            error: e.to_string(),
        })?;
        let ready = w.wait_for(|s| matches!(s.phase, Phase::Idle) || !s.phase.is_live());
        tokio::select! {
            r = ready => match r {
                Ok(s) if s.phase == Phase::Idle => Ok(()),
                Ok(s) => Err(TaskState::Failed {
                    error: format!("the receiving session is {}", s.phase.name()),
                }),
                Err(_) => Err(TaskState::Failed { error: "the receiving session was deleted".into() }),
            },
            _ = tokio::time::sleep_until(deadline) => Err(TaskState::Expired),
            c = wait_cancel(cancel) => Err(match c {
                Some(c) if c.expired => TaskState::Expired,
                Some(c) => TaskState::Cancelled { reason: c.reason },
                None => TaskState::Cancelled { reason: "cancelled".into() },
            }),
        }
    }

    /// Follow the session's events from the turn mark to the end of the task's turn.
    /// `Err(cancel)` when the turn ended because the task was withdrawn.
    #[allow(clippy::too_many_arguments)]
    async fn observe(
        &self,
        sid: &SessionId,
        id: &TaskId,
        task: &Task,
        from_name: &str,
        mark: TurnMark,
        mut resubmit_check: Option<u64>,
        deadline: Instant,
        mut cancel: watch::Receiver<Option<CancelRemoteRequest>>,
    ) -> Result<TaskState, CancelRemoteRequest> {
        let mk = marker(id);
        let mut from = Some(mark.cursor);
        let mut seen_input = false;
        let mut in_turn = false;
        let mut progress = Progress::default();
        let mut text = String::new();
        let mut last_message: Option<String> = None;
        let mut last_report = Instant::now() - self.inner.cfg.progress_interval;
        let mut interrupted: Option<CancelRemoteRequest> = None;
        // Recovery with nothing journaled after the mark: the prompt never went out.
        if let Some(c) = resubmit_check {
            if c <= mark.cursor {
                resubmit_check = None;
                if let Err(e) = self
                    .sessions()
                    .command(
                        sid,
                        Command::Prompt {
                            text: prompt(task, from_name),
                        },
                    )
                    .await
                {
                    return Ok(TaskState::Failed {
                        error: format!("the receiving session did not accept the task: {e}"),
                    });
                }
            }
        }
        'outer: loop {
            let mut stream = match self.sessions().subscribe(sid, from).await {
                Ok(s) => s,
                Err(e) => {
                    return Ok(TaskState::Failed {
                        error: format!("cannot follow the receiving session: {e}"),
                    })
                }
            };
            loop {
                let item = tokio::select! {
                    item = stream.next() => item,
                    _ = tokio::time::sleep_until(deadline), if interrupted.is_none() => {
                        interrupted = Some(CancelRemoteRequest { id: id.clone(), reason: "deadline passed".into(), expired: true });
                        self.interrupt(sid).await;
                        continue;
                    }
                    c = wait_cancel(&mut cancel), if interrupted.is_none() => {
                        let c = c.unwrap_or(CancelRemoteRequest { id: id.clone(), reason: "cancelled".into(), expired: false });
                        interrupted = Some(c);
                        self.interrupt(sid).await;
                        continue;
                    }
                };
                let ev = match item {
                    Some(Ok(ev)) => ev,
                    Some(Err(StreamEnd::Lagged)) => continue 'outer,
                    Some(Err(StreamEnd::Error(e))) => {
                        tracing::warn!(error = %e, "task observer: stream failed; resubscribing");
                        continue 'outer;
                    }
                    None => {
                        return Ok(TaskState::Failed {
                            error: "the receiving session was deleted".into(),
                        })
                    }
                };
                from = Some(ev.cursor);
                match &ev.kind {
                    SessionEventKind::Input {
                        command: Command::Prompt { text: p },
                        status,
                    } if p.starts_with(&mk) => {
                        if matches!(
                            status,
                            wasit_runtime::wire::CommandStatus::Sent
                                | wasit_runtime::wire::CommandStatus::Queued
                        ) {
                            seen_input = true;
                        }
                    }
                    SessionEventKind::Agent { event } if seen_input => match event {
                        AgentEvent::TurnStarted { .. } => in_turn = true,
                        AgentEvent::AssistantDelta {
                            text: t,
                            message_id,
                        } if in_turn => {
                            // A new assistant message replaces the narration before it.
                            if message_id.is_some() && *message_id != last_message {
                                text.clear();
                                last_message = message_id.clone();
                            }
                            text.push_str(t);
                            progress.last_text = Some(truncate(text.trim(), 200));
                        }
                        AgentEvent::ToolCallStarted { name, .. } if in_turn => {
                            progress.tool_calls += 1;
                            progress.last_tool = Some(name.clone());
                            // The result is the final message: text before a tool call is
                            // narration.
                            text.clear();
                            if last_report.elapsed() >= self.inner.cfg.progress_interval {
                                last_report = Instant::now();
                                self.set_in_state(
                                    id,
                                    TaskState::Running {
                                        progress: progress.clone(),
                                    },
                                    None,
                                )
                                .await;
                            }
                        }
                        AgentEvent::TurnCompleted {
                            end_reason,
                            was_interrupted,
                            usage,
                            reply,
                            ..
                        } if in_turn => {
                            if let Some(c) = interrupted.take() {
                                return Err(c);
                            }
                            if *was_interrupted || *end_reason == Some(EndReason::Cancelled) {
                                return Ok(TaskState::Failed {
                                    error: "the receiver's turn was interrupted".into(),
                                });
                            }
                            let summary = match reply.as_deref().map(str::trim) {
                                Some(r) if !r.is_empty() => r.to_string(),
                                _ => text.trim().to_string(),
                            };
                            let usage = (*usage != Usage::default())
                                .then(|| usage_delta(&mark.usage_before, usage));
                            return Ok(TaskState::Completed {
                                result: TaskResult {
                                    status: if end_reason.is_some() {
                                        ResultStatus::Error
                                    } else {
                                        ResultStatus::Success
                                    },
                                    summary,
                                    artifacts: Vec::new(),
                                    usage,
                                },
                            });
                        }
                        _ => {}
                    },
                    SessionEventKind::Exited { code, signal } => {
                        if let Some(c) = interrupted.take() {
                            return Err(c);
                        }
                        return Ok(TaskState::Failed {
                            error: format!(
                                "the receiving session's process exited (code {code:?}, signal {signal:?}) before finishing the task"
                            ),
                        });
                    }
                    SessionEventKind::Lost { reason } => {
                        return Ok(TaskState::Failed {
                            error: format!("the receiving session was lost: {reason}"),
                        })
                    }
                    _ => {}
                }
                if let Some(c) = resubmit_check {
                    if ev.cursor >= c {
                        resubmit_check = None;
                        if !seen_input {
                            if let Err(e) = self
                                .sessions()
                                .command(
                                    sid,
                                    Command::Prompt {
                                        text: prompt(task, from_name),
                                    },
                                )
                                .await
                            {
                                return Ok(TaskState::Failed {
                                    error: format!(
                                        "the receiving session did not accept the task: {e}"
                                    ),
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    async fn interrupt(&self, sid: &SessionId) {
        if let Err(e) = self.sessions().command(sid, Command::Interrupt).await {
            tracing::warn!(error = %e, session = %sid, "interrupting a withdrawn task's turn failed");
        }
    }
}
