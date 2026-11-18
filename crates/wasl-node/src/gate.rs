//! Watching a handoff gate: the answer arrives as an `input` event on the sending session's
//! event stream (the runtime journals answers to external gates), so the watcher subscribes
//! from the cursor recorded right before the raise — after a daemon restart the same replay
//! finds an answer given while nobody watched.

use wasit_core::{AgentEvent, Command};
use wasit_runtime::wire::{SessionEventKind, SessionId};
use wasit_runtime::StreamEnd;
use wasl_proto::{HandoffDecision, TaskId, TaskState};

use crate::node::Node;
use crate::state::GateInfo;

enum Outcome {
    Decided(HandoffDecision),
    /// Withdrawn by the node, or the task is no longer pending.
    Gone,
    SessionEnded(String),
}

impl Node {
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
        let mut from = Some(gate.cursor);
        let outcome = 'outer: loop {
            let mut stream = match self.sessions().subscribe(&sid, from).await {
                Ok(s) => s,
                Err(e) => break Outcome::SessionEnded(format!("the sending session is gone: {e}")),
            };
            while let Some(item) = stream.next().await {
                let ev = match item {
                    Ok(ev) => ev,
                    Err(StreamEnd::Lagged) => continue 'outer,
                    Err(StreamEnd::Error(e)) => {
                        tracing::warn!(error = %e, "handoff gate watcher: stream failed; resubscribing");
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
                        // The runtime refuses answers that pick no offered choice, so this
                        // only fails for a gate raised with other choices: treat it as a denial.
                        break 'outer Outcome::Decided(
                            HandoffDecision::from_gate(decision).unwrap_or(HandoffDecision::Deny {
                                reason: Some(format!(
                                    "unknown handoff choice {:?}",
                                    decision.choice
                                )),
                            }),
                        );
                    }
                    SessionEventKind::Input {
                        command: Command::AnswerQuestion { request_id, .. },
                        ..
                    } if *request_id == gate.request_id => {
                        break 'outer Outcome::Decided(HandoffDecision::Deny {
                            reason: Some(
                                "answered as a question; handoffs need allow or deny".into(),
                            ),
                        });
                    }
                    SessionEventKind::Agent {
                        event: AgentEvent::GateResolved { request_id },
                    } if *request_id == gate.request_id => break 'outer Outcome::Gone,
                    SessionEventKind::Exited { .. } | SessionEventKind::Lost { .. } => {
                        break 'outer Outcome::SessionEnded(
                            "the sending session ended before the handoff was decided".into(),
                        );
                    }
                    _ => {}
                }
                if !self.still_pending(&id) {
                    break 'outer Outcome::Gone;
                }
            }
            // The stream ended: the session was deleted.
            break Outcome::SessionEnded("the sending session was deleted".into());
        };
        match outcome {
            Outcome::Decided(d) => {
                if let Err(e) = self.decide(&id, d, "gate").await {
                    tracing::debug!(error = %e, task = %id, "handoff decision not applied");
                }
            }
            Outcome::Gone => {}
            Outcome::SessionEnded(reason) => {
                if self.still_pending(&id) {
                    self.finish_out(&id, TaskState::Cancelled { reason }).await;
                }
            }
        }
    }
}
