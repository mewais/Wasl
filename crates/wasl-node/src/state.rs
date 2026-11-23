//! The node's in-memory state (owned behind one `std::sync::Mutex`, never held across an
//! await) and the persisted shapes of its records.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use wasit_core::Usage;
use wasl_proto::{
    AgentId, CancelRemoteRequest, EdgePolicy, End, EndCode, PeerCard, SpawnDecision, SpawnEnded,
    SpawnLaunched, SpawnPlan, SpawnReply, SpawnSpec, Task, TaskBrief, TaskId, TaskState, TaskView,
    Topology,
};

/// A handoff gate raised on the sending session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateInfo {
    pub request_id: String,
    /// The raise was journaled by the session (else it must be raised again on recovery).
    pub raised: bool,
    /// The session's event cursor right before the raise (the watcher replays from it).
    pub cursor: u64,
}

/// A task this node's session delegated.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutPersist {
    pub task: Task,
    pub state: TaskState,
    pub updated_ms: u64,
    #[serde(default)]
    pub to_name: String,
    /// The receiving host has taken custody.
    #[serde(default)]
    pub delivered: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate: Option<GateInfo>,
    /// A withdrawal still to be forwarded to the receiving host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel: Option<CancelRemoteRequest>,
}

/// Where the receiver's turn for a task starts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnMark {
    /// The session's event cursor right before the prompt was sent.
    pub cursor: u64,
    pub usage_before: Usage,
}

/// A task this node's session works on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InPersist {
    pub task: Task,
    pub state: TaskState,
    pub updated_ms: u64,
    /// The sending host acknowledged the current state.
    #[serde(default)]
    pub reported: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn: Option<TurnMark>,
    /// The sender withdrew the task while it was running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel: Option<CancelRemoteRequest>,
}

pub struct OutRec {
    pub p: OutPersist,
    pub rev: u64,
    pub watch: watch::Sender<TaskState>,
}

impl OutRec {
    pub fn brief(&self) -> TaskBrief {
        TaskBrief {
            id: self.p.task.id.clone(),
            to: self.p.task.to.clone(),
            to_name: self.p.to_name.clone(),
            mode: self.p.task.mode,
            state: self.p.state.clone(),
        }
    }

    pub fn view(&self) -> TaskView {
        TaskView {
            task: self.p.task.clone(),
            state: self.p.state.clone(),
            updated_ms: self.p.updated_ms,
        }
    }
}

pub struct InRec {
    pub p: InPersist,
    pub rev: u64,
    /// Signals the worker that `p.cancel` was set.
    pub cancel: watch::Sender<Option<CancelRemoteRequest>>,
}

impl InRec {
    pub fn view(&self) -> TaskView {
        TaskView {
            task: self.p.task.clone(),
            state: self.p.state.clone(),
            updated_ms: self.p.updated_ms,
        }
    }
}

/// Where a spawn request stands.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum SpawnPhase {
    /// A spawn gate is (being) raised on the calling session.
    Asking {
        gate: GateInfo,
    },
    /// Approved; the supervisor is asked to launch it.
    Launching {
        decision: SpawnDecision,
    },
    /// Launched; the first task (if any) is still to be delegated.
    Launched {
        decision: SpawnDecision,
        launched: SpawnLaunched,
    },
    Done {
        reply: SpawnReply,
    },
    /// Ended without a launch. `code` and `message` are what the asking agent's call fails
    /// with; `end` says why, from Wasl's fixed vocabulary (and `message` is its detail). A
    /// record written before endings were recorded has no `end`: it reads as unrecorded.
    Failed {
        code: String,
        message: String,
        #[serde(default = "End::unrecorded")]
        end: End,
    },
}

impl SpawnPhase {
    pub fn is_terminal(&self) -> bool {
        matches!(self, SpawnPhase::Done { .. } | SpawnPhase::Failed { .. })
    }

    /// A failure for `end`: the agent's error code follows from it, `message` is its detail.
    pub fn failed(end: End, message: impl Into<String>) -> SpawnPhase {
        SpawnPhase::Failed {
            code: end.rpc_code(),
            message: message.into(),
            end,
        }
    }
}

/// A spawn request of one of this node's sessions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpawnPersist {
    pub request_id: String,
    pub from: AgentId,
    pub spec: SpawnSpec,
    /// What the supervisor resolved the request into; none when it refused to plan one.
    #[serde(default)]
    pub plan: Option<SpawnPlan>,
    pub phase: SpawnPhase,
    pub updated_ms: u64,
    /// The inbound task the caller was working on when it asked: the spawn is for it, so it
    /// is refused once that task is no longer running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub for_task: Option<TaskId>,
    /// The spawn's deadline (ms since the epoch), fixed at request time: the first task's
    /// deadline, capped by `for_task`'s. Approval after it is refused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u64>,
}

impl SpawnPersist {
    /// The request's ending, once it has reached a final state.
    pub fn ended(&self) -> Option<SpawnEnded> {
        let (end, detail) = match &self.phase {
            SpawnPhase::Done { .. } => (End::new(EndCode::Launched), None),
            SpawnPhase::Failed { message, end, .. } => (end.clone(), wasl_proto::bounded(message)),
            _ => return None,
        };
        Some(SpawnEnded {
            request_id: self.request_id.clone(),
            from: self.from.clone(),
            name: self.spec.name.clone(),
            ended_ms: self.updated_ms,
            end,
            detail,
        })
    }
}

pub struct SpawnRec {
    pub p: SpawnPersist,
    pub watch: watch::Sender<SpawnPhase>,
    /// `wasl.spawn` calls waiting for this request right now (not journaled: after a restart
    /// the caller re-sends the call). When the last one goes away while the request still
    /// asks the human, the request is withdrawn unless one comes back within
    /// `NodeConfig::spawn_reattach_grace`.
    pub waiters: watch::Sender<usize>,
}

/// Per receiving session: FIFO of queued tasks and the one being worked on.
#[derive(Default)]
pub struct SessionQueue {
    pub fifo: VecDeque<TaskId>,
    pub running: Option<TaskId>,
    pub worker: bool,
}

/// One unit of outbound network work.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Work {
    Deliver(TaskId),
    Cancel(TaskId),
    Update(TaskId),
}

#[derive(Default)]
pub struct State {
    pub rev: u64,
    pub topo: BTreeMap<String, Topology>,
    pub out: HashMap<TaskId, OutRec>,
    pub inbound: HashMap<TaskId, InRec>,
    pub queues: HashMap<String, SessionQueue>,
    /// Reported cost of finished outbound tasks per workstream.
    pub spent: HashMap<String, f64>,
    pub inflight: HashSet<Work>,
    /// Earliest next attempt and attempt count of work that failed retryably.
    pub retry: HashMap<Work, (tokio::time::Instant, u32)>,
    /// Spawn requests by request id.
    pub spawns: HashMap<String, SpawnRec>,
}

impl State {
    pub fn next_rev(&mut self) -> u64 {
        self.rev += 1;
        self.rev
    }

    /// The card of `agent` in any workstream.
    pub fn card(&self, agent: &AgentId) -> Option<&PeerCard> {
        self.topo
            .values()
            .flat_map(|t| t.peers.iter())
            .find(|p| p.agent == *agent)
    }

    pub fn name_of(&self, agent: &AgentId) -> String {
        self.card(agent).map(|c| c.name.clone()).unwrap_or_default()
    }

    /// Outgoing edges of `from`: (workstream, to, policy).
    pub fn edges_from(&self, from: &AgentId) -> Vec<(String, AgentId, EdgePolicy)> {
        let mut v = Vec::new();
        for t in self.topo.values() {
            for e in t.edges.iter().filter(|e| e.from == *from) {
                if !v.iter().any(|(_, to, _)| *to == e.to) {
                    v.push((t.workstream.clone(), e.to.clone(), e.policy));
                }
            }
        }
        v
    }

    /// The inbound task `session` is working on right now, if any.
    pub fn running_inbound(&self, session: &str) -> Option<&InRec> {
        let id = self.queues.get(session)?.running.as_ref()?;
        self.inbound.get(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasl_proto::{codes, EndCode};

    fn persist(phase: SpawnPhase) -> SpawnPersist {
        SpawnPersist {
            request_id: "r1".into(),
            from: AgentId::new("h", "s"),
            spec: SpawnSpec {
                name: "w".into(),
                role: "r".into(),
                ..Default::default()
            },
            plan: None,
            phase,
            updated_ms: 7,
            for_task: None,
            deadline_ms: None,
        }
    }

    #[test]
    fn only_a_final_state_is_an_ending_and_an_old_failure_reads_as_unrecorded() {
        let asking = SpawnPhase::Asking {
            gate: GateInfo {
                request_id: "g".into(),
                raised: false,
                cursor: 0,
            },
        };
        assert_eq!(persist(asking).ended(), None);
        let denied = SpawnPhase::failed(End::new(EndCode::Denied), "the human denied spawning w");
        let SpawnPhase::Failed { code, .. } = &denied else {
            panic!("a failure")
        };
        assert_eq!(code, codes::SPAWN_DENIED);
        let e = persist(denied).ended().unwrap();
        assert_eq!(e.end, End::new(EndCode::Denied));
        assert_eq!(e.detail.as_deref(), Some("the human denied spawning w"));
        assert_eq!((e.ended_ms, e.name.as_str()), (7, "w"));

        // A record journaled before endings were recorded has a code and a message only.
        let old: SpawnPhase = serde_json::from_value(serde_json::json!({
            "phase": "failed", "code": "spawn_withdrawn", "message": "gone"
        }))
        .unwrap();
        let e = persist(old).ended().unwrap();
        assert_eq!(e.end, End::new(EndCode::Unrecorded));
        assert_eq!(e.detail.as_deref(), Some("gone"));
        // Nor does one have a plan field.
        let mut v = serde_json::to_value(persist(SpawnPhase::failed(
            End::new(EndCode::Cancelled),
            "x",
        )))
        .unwrap();
        v.as_object_mut().unwrap().remove("plan");
        assert!(serde_json::from_value::<SpawnPersist>(v)
            .unwrap()
            .plan
            .is_none());
    }
}
