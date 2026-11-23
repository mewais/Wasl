//! [`NodeService`]: the node's `wasl.*` methods and topics as a [`wasit_control::Service`].

use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::broadcast;
use wasit_control::{CallContext, CloseReason, Cursor, EventStream, RpcError, Service};
use wasit_diag::{Code, Ms};
use wasl_proto::diag::{emit, WaslEvent};
use wasl_proto::{
    methods, topics, CancelRemoteRequest, CancelRequest, DecideRequest, DelegateRequest,
    DeliverRequest, DismissAsk, DismissRequest, MessageRequest, PeerStatusReply, PeerStatusRequest,
    PeersRequest, Side, SpawnAsk, SpawnCancel, SpawnLaunch, SpawnRequest, SpawnableAsk, TaskEvent,
    TaskIds, TaskView, Topology, UpdateRequest, WaitRequest,
};

use crate::journal::{Entry, Rec};
use crate::node::Node;

/// Serves a [`Node`] (register under [`wasl_proto::ROUTES`]).
#[derive(Clone)]
pub struct NodeService {
    node: Node,
}

impl NodeService {
    pub fn new(node: Node) -> Self {
        NodeService { node }
    }

    pub fn node(&self) -> &Node {
        &self.node
    }
}

fn params<T: DeserializeOwned>(p: Value) -> Result<T, RpcError> {
    serde_json::from_value(p).map_err(|e| RpcError::invalid_params(e.to_string()))
}

fn ok<T: Serialize>(v: T) -> Result<Value, RpcError> {
    serde_json::to_value(v).map_err(|e| RpcError::internal(e.to_string()))
}

/// The topic payload of a journal entry, if it belongs to `topic`.
fn project(topic: &str, e: &Entry) -> Option<Value> {
    match (topic, &e.rec) {
        (topics::TASKS, Rec::Out(o)) => serde_json::to_value(TaskEvent {
            side: Side::Outbound,
            view: TaskView {
                task: o.task.clone(),
                state: o.state.clone(),
                updated_ms: o.updated_ms,
            },
        })
        .ok(),
        (topics::TASKS, Rec::In(i)) => serde_json::to_value(TaskEvent {
            side: Side::Inbound,
            view: TaskView {
                task: i.task.clone(),
                state: i.state.clone(),
                updated_ms: i.updated_ms,
            },
        })
        .ok(),
        (topics::EDGES, Rec::Edge(ev)) => serde_json::to_value(ev.as_ref()).ok(),
        // Only a request that reached its final state is an event of this topic.
        (topics::SPAWNS, Rec::Spawn(s)) => s.ended().and_then(|e| serde_json::to_value(e).ok()),
        _ => None,
    }
}

impl NodeService {
    async fn dispatch(&self, method: &str, p: Value) -> Result<Value, RpcError> {
        let n = &self.node;
        match method {
            methods::TOPOLOGY_SET => ok(n.set_topology(params::<Topology>(p)?).await?),
            methods::TOPOLOGY_GET => ok(n.topologies()),
            methods::PEERS_LIST => ok(n.peers(&params::<PeersRequest>(p)?.from).await?),
            methods::DELEGATE => ok(n.delegate(params::<DelegateRequest>(p)?).await?),
            methods::MESSAGE => ok(n.message(params::<MessageRequest>(p)?).await?),
            methods::TASK_STATUS => ok(n.status(&params::<TaskIds>(p)?.ids)),
            methods::TASK_WAIT => {
                let r: WaitRequest = params(p)?;
                ok(n.wait(&r.ids, Duration::from_millis(r.timeout_ms), r.any)
                    .await)
            }
            methods::TASK_CANCEL => {
                let r: CancelRequest = params(p)?;
                ok(n.cancel(&r.id, r.reason).await?)
            }
            methods::TASK_DECIDE => {
                let r: DecideRequest = params(p)?;
                ok(n.decide(&r.id, r.decision, "elicitation").await?)
            }
            methods::TASKS_LIST => ok(n.tasks()),
            methods::NODE_DELIVER => ok(n.node_deliver(params::<DeliverRequest>(p)?.task).await?),
            methods::NODE_UPDATE => ok(n.node_update(params::<UpdateRequest>(p)?).await?),
            methods::NODE_CANCEL => ok(n.node_cancel(params::<CancelRemoteRequest>(p)?).await?),
            methods::NODE_PEER_STATUS => {
                let r: PeerStatusRequest = params(p)?;
                ok(PeerStatusReply {
                    statuses: n.peer_status_local(&r.sessions),
                })
            }
            methods::SPAWN => ok(n.spawn(params::<SpawnRequest>(p)?).await?),
            methods::SPAWN_CANCEL => ok(n.cancel_spawn(params::<SpawnCancel>(p)?).await?),
            methods::DISMISS => ok(n.dismiss(params::<DismissRequest>(p)?).await?),
            methods::SUPERVISOR_SPAWN_PLAN => {
                ok(n.serving_authority()?.plan(params::<SpawnAsk>(p)?).await?)
            }
            methods::SUPERVISOR_SPAWN_LAUNCH => ok(n
                .serving_authority()?
                .launch(params::<SpawnLaunch>(p)?)
                .await?),
            methods::SUPERVISOR_DISMISS => ok(n
                .serving_authority()?
                .dismiss(params::<DismissAsk>(p)?)
                .await?),
            methods::SUPERVISOR_SPAWNABLE => ok(n
                .serving_authority()?
                .spawnable(params::<SpawnableAsk>(p)?)
                .await?),
            other => Err(RpcError::method_not_found(other)),
        }
    }
}

/// Requests that are reads, long polls or frequent reports: recorded only when they fail.
const QUIET: &[&str] = &[
    methods::TOPOLOGY_GET,
    methods::PEERS_LIST,
    methods::TASK_STATUS,
    methods::TASK_WAIT,
    methods::TASKS_LIST,
    methods::NODE_PEER_STATUS,
    methods::NODE_UPDATE,
    methods::SUPERVISOR_SPAWNABLE,
];

/// Records how a request ended: its method and time, and on failure its error code (never the
/// message, which can name what an agent asked for).
fn record(method: &str, r: &Result<Value, RpcError>, began: Instant) {
    let ms = Ms::of(began.elapsed());
    match r {
        Ok(_) if QUIET.contains(&method) => {}
        Ok(_) => emit(WaslEvent::Request {
            method: Code::new(method),
            ms,
        }),
        Err(e) => emit(WaslEvent::RequestFailed {
            method: Code::new(method),
            code: Code::new(&e.code),
            ms,
        }),
    }
}

#[async_trait]
impl Service for NodeService {
    async fn call(&self, _ctx: CallContext, method: &str, p: Value) -> Result<Value, RpcError> {
        let began = Instant::now();
        let r = self.dispatch(method, p).await;
        record(method, &r, began);
        r
    }

    async fn subscribe(
        &self,
        _ctx: CallContext,
        topic: &str,
        _params: Value,
        from: Option<Cursor>,
    ) -> Result<EventStream, RpcError> {
        if ![topics::TASKS, topics::EDGES, topics::SPAWNS].contains(&topic) {
            return Err(RpcError::topic_not_found(topic));
        }
        let from = match &from {
            None => None,
            Some(c) => Some(c.as_u64().ok_or_else(|| {
                RpcError::invalid_params(format!("bad cursor {c}: expected a number"))
            })?),
        };
        let (backlog, live) = self.node.inner.journal.subscribe(from);
        let topic = topic.to_string();
        let t2 = topic.clone();
        let head = futures::stream::iter(
            backlog
                .into_iter()
                .filter_map(move |e| project(&t2, &e).map(|v| Ok((Cursor::from_u64(e.seq), v))))
                .collect::<Vec<_>>(),
        );
        let tail = futures::stream::unfold((live, topic), |(mut rx, topic)| async move {
            loop {
                match rx.recv().await {
                    Ok(e) => {
                        if let Some(v) = project(&topic, &e) {
                            return Some((Ok((Cursor::from_u64(e.seq), v)), (rx, topic)));
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        return Some((Err(CloseReason::Lagged), (rx, topic)))
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        });
        // A lagged stream ends after its `Lagged` item.
        let mut ended = false;
        let tail = tail.take_while(move |item| {
            let go = !ended;
            if item.is_err() {
                ended = true;
            }
            futures::future::ready(go)
        });
        Ok(head.chain(tail).boxed())
    }
}
