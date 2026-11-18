//! Watching an external gate (handoff or spawn): the answer arrives as an `input` event on the
//! raising session's event stream (the runtime journals answers to external gates), so the
//! watcher subscribes from the cursor recorded right before the raise — after a daemon restart
//! the same replay finds an answer given while nobody watched.

use wasit_core::{AgentEvent, Command, GateDecision};
use wasit_runtime::wire::{SessionEventKind, SessionId};
use wasit_runtime::StreamEnd;
use wasl_proto::{HandoffDecision, TaskId, TaskState};

use crate::node::Node;
use crate::state::GateInfo;

/// How a gate ended.
pub(crate) enum Answer {
    Decided(GateDecision),
    /// Answered as if it were a question (not a valid answer to a choice gate).
    AsQuestion,
    /// Withdrawn, or no longer wanted (`still` returned false).
    Gone,
    SessionEnded(String),
}

impl Node {
    /// Follow `sid`'s events from `gate.cursor` until the gate is answered or withdrawn, the
    /// session ends, or `still()` turns false.
    pub(crate) async fn await_answer(
        &self,
        sid: &SessionId,
        gate: &GateInfo,
        still: impl Fn() -> bool,
    ) -> Answer {
        let mut from = Some(gate.cursor);
        'outer: loop {
            let mut stream = match self.sessions().subscribe(sid, from).await {
                Ok(s) => s,
                Err(e) => return Answer::SessionEnded(format!("the session is gone: {e}")),
            };
            while let Some(item) = stream.next().await {
                let ev = match item {
                    Ok(ev) => ev,
                    Err(StreamEnd::Lagged) => continue 'outer,
                    Err(StreamEnd::Error(e)) => {
                        tracing::warn!(error = %e, "gate watcher: stream failed; resubscribing");
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
                        event: AgentEvent::GateResolved { request_id, .. },
                    } if *request_id == gate.request_id => return Answer::Gone,
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
            return Answer::SessionEnded("the session was deleted".into());
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
        let answer = self
            .await_answer(&sid, &gate, || self.still_pending(&id))
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
            Answer::Gone => return,
            Answer::SessionEnded(reason) => {
                if self.still_pending(&id) {
                    self.finish_out(
                        &id,
                        TaskState::Cancelled {
                            reason: format!("the sending session ended: {reason}"),
                        },
                    )
                    .await;
                }
                return;
            }
        };
        if let Err(e) = self.decide(&id, decision, "gate").await {
            tracing::debug!(error = %e, task = %id, "handoff decision not applied");
        }
    }
}
