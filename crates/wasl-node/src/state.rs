//! The node's in-memory state (owned behind one `std::sync::Mutex`, never held across an
//! await) and the persisted shapes of its records.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use wasit_core::Usage;
use wasl_proto::{
    AgentId, CancelRemoteRequest, EdgePolicy, PeerCard, Task, TaskBrief, TaskId, TaskState,
    TaskView, Topology,
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
