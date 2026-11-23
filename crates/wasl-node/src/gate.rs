//! Watching an external gate (handoff or spawn): the answer arrives as an `input` event on the
//! raising session's event stream (the runtime journals answers to external gates), so the
//! watcher subscribes from the cursor recorded right before the raise — after a daemon restart
//! the same replay finds an answer given while nobody watched.
//!
//! A gate lives as long as the request that raised it. It is withdrawn — taken down on the
//! session (`gate_resolved`, with the reason the request ended: an [`wasl_proto::EndCode`] and
//! the sentence the agent is told; the runtime then refuses a late answer with
//! `gate_withdrawn`) and its request settled without acting — when that request is gone:
//!
//! - handoff (the task): the task is cancelled or expires, the sending session's process
//!   ends, or the turn that delegated is interrupted;
//! - spawn (the blocked `spawn_agent` call): the turn that called it ends or is interrupted,
//!   the call is cancelled or its MCP client disconnects (no `wasl.spawn` caller waits any
//!   more), the session's process ends, or its deadline passes before approval.

use wasit_core::{AgentEvent, Command, GateDecision, GateReason};
use wasit_runtime::wire::{SessionEventKind, SessionId};
use wasit_runtime::StreamEnd;
use wasl_proto::diag::{emit, task, WaslEvent, Watcher, WithdrawCause};
use wasl_proto::{codes, EndCode, HandoffDecision, TaskId, TaskState};

use crate::diag::{rpc_error, session, stream_error};
use crate::node::Node;
use crate::state::GateInfo;

/// How a gate ended.
pub(crate) enum Answer {
    Decided(GateDecision),
    /// Answered as if it were a question (not a valid answer to a choice gate).
    AsQuestion,
    /// Withdrawn, or no longer wanted (`still` returned false).
    Gone,
    /// The session's process ended (or was lost or stopped) before the gate was answered.
    SessionEnded(String),
    /// The session was deleted, or its events cannot be followed any more.
    SessionGone(String),
    /// The asking turn ended after the raise (only for gates raised during a turn, and only
    /// for the turn ends the caller said end the request).
    TurnEnded {
        interrupted: bool,
    },
}

impl Node {
    /// Follow `sid`'s events from `gate.cursor` until the gate is answered or withdrawn, the
    /// session ends, `still()` turns false, or (for a gate raised during a turn) that turn
    /// ends in a way `ends_request(interrupted)` accepts.
    pub(crate) async fn await_answer(
        &self,
        sid: &SessionId,
        gate: &GateInfo,
        still: impl Fn() -> bool,
        ends_request: impl Fn(bool) -> bool,
    ) -> Answer {
        let mut from = Some(gate.cursor);
        // Turn ends before the raise belong to earlier turns; only the first one after it is
        // the asking turn's.
        let mut raised = false;
        let mut bound = gate.turn;
        'outer: loop {
            let mut stream = match self.sessions().subscribe(sid, from).await {
                Ok(s) => s,
                Err(e) => return Answer::SessionGone(format!("the session is gone: {e}")),
            };
            while let Some(item) = stream.next().await {
                let ev = match item {
                    Ok(ev) => ev,
                    Err(StreamEnd::Lagged) => continue 'outer,
                    Err(StreamEnd::Error(e)) => {
                        emit(WaslEvent::StreamResubscribe {
                            watcher: Watcher::Gate,
                            session: session(sid),
                            id: wasit_diag::Ident::new(&gate.request_id),
                            error: stream_error(&e),
                        });
                        continue 'outer;
                    }
                };
                from = Some(ev.cursor);
                match &ev.kind {
                    SessionEventKind::Input {
                        command:
                            Command::AnswerGate {
                                request_id,
                                decision,
                            },
                        ..
                    } if *request_id == gate.request_id => {
                        return Answer::Decided(decision.clone());
                    }
                    SessionEventKind::Input {
                        command: Command::AnswerQuestion { request_id, .. },
                        ..
                    } if *request_id == gate.request_id => return Answer::AsQuestion,
                    SessionEventKind::Agent {
                        event: AgentEvent::GateResolved { request_id, reason },
                    } if *request_id == gate.request_id => {
                        // Taken down by us (the request is settled), or by the runtime because
                        // the process ended or the session was stopped (its exit follows).
                        let ended = reason.as_ref().is_some_and(|r| {
                            r.code == GateReason::PROCESS_ENDED
                                || r.code == GateReason::SESSION_STOPPED
                        });
                        return if ended {
                            Answer::SessionEnded(
                                "the session ended before the gate was answered".into(),
                            )
                        } else {
                            Answer::Gone
                        };
                    }
                    SessionEventKind::Agent {
                        event: AgentEvent::GateRequested { gate: g },
                    } if g.request_id() == gate.request_id => raised = true,
                    SessionEventKind::Agent {
                        event:
                            AgentEvent::TurnCompleted {
                                was_interrupted, ..
                            },
                    } if raised && bound => {
                        if ends_request(*was_interrupted) {
                            return Answer::TurnEnded {
                                interrupted: *was_interrupted,
                            };
                        }
                        bound = false;
                    }
                    SessionEventKind::Exited { .. } | SessionEventKind::Lost { .. } => {
                        return Answer::SessionEnded(
                            "the session ended before the gate was answered".into(),
                        );
                    }
                    _ => {}
                }
                if !still() {
                    return Answer::Gone;
                }
            }
            // The stream ended: the session was deleted.
            return Answer::SessionGone("the session was deleted".into());
        }
    }

    pub(crate) fn spawn_gate_watcher(&self, id: TaskId, gate: GateInfo) {
        let node = self.clone();
        tokio::spawn(async move { node.watch_gate(id, gate).await });
    }

    fn still_pending(&self, id: &TaskId) -> bool {
        self.lock()
            .out
            .get(id)
            .is_some_and(|r| matches!(r.p.state, TaskState::PendingApproval { .. }))
    }

    async fn watch_gate(self, id: TaskId, gate: GateInfo) {
        let sid = {
            let st = self.lock();
            st.out
                .get(&id)
                .and_then(|r| SessionId::parse(&r.p.task.from.session))
        };
        let Some(sid) = sid else { return };
        // The task outlives a turn that ends normally (its result can be awaited later); an
        // interrupted turn takes its pending handoffs with it.
        let answer = self
            .await_answer(
                &sid,
                &gate,
                || self.still_pending(&id),
                |interrupted| interrupted,
            )
            .await;
        let decision = match answer {
            // The runtime refuses answers that pick no offered choice, so this only fails for a
            // gate raised with other choices: treat it as a denial.
            Answer::Decided(d) => HandoffDecision::from_gate(&d).unwrap_or(HandoffDecision::Deny {
                reason: Some(format!("unknown handoff choice {:?}", d.choice)),
            }),
            Answer::AsQuestion => HandoffDecision::Deny {
                reason: Some("answered as a question; handoffs need allow or deny".into()),
            },
            Answer::Gone => {
                // Taken down by someone else while the task still waits: settle it.
                if self.still_pending(&id) {
                    self.finish_out(
                        &id,
                        TaskState::Cancelled {
                            reason: "withdrawn: the handoff gate was taken down before anyone \
                                     answered"
                                .into(),
                        },
                    )
                    .await;
                }
                return;
            }
            Answer::TurnEnded { .. } => {
                let reason = "withdrawn: the turn that delegated was interrupted before the \
                              handoff was approved"
                    .to_string();
                if let Err(e) = self.withdraw(&id, EndCode::TurnInterrupted, reason).await {
                    emit(WaslEvent::TaskWithdrawFailed {
                        task: task(&id),
                        cause: WithdrawCause::TurnInterrupted,
                        error: rpc_error(&e),
                    });
                }
                return;
            }
            Answer::SessionEnded(reason) | Answer::SessionGone(reason) => {
                // The runtime already withdrew the gate with the process (or the session is
                // gone, with its gates).
                if self.still_pending(&id) {
                    self.finish_out(
                        &id,
                        TaskState::Cancelled {
                            reason: format!("withdrawn: the sending session ended: {reason}"),
                        },
                    )
                    .await;
                }
                return;
            }
        };
        // After the human clicked: a failure here leaves the task pending with no gate, so it
        // is always recorded. An I/O failure (the audit trail or the journal) is an error; a
        // task that moved on meanwhile (cancelled, expired) is a refusal.
        if let Err(e) = self.decide(&id, decision, "gate").await {
            let (task, error) = (task(&id), rpc_error(&e));
            emit(if e.code == codes::IO {
                WaslEvent::GateDecideFailed { task, error }
            } else {
                WaslEvent::GateDecideRefused { task, error }
            });
        }
    }
}
