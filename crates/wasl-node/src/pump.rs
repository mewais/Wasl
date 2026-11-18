//! Store-and-forward: the outbox pump sends what the journal says is owed (deliveries of
//! approved tasks, withdrawals, state reports back to the sender) and the deadline timer
//! expires overdue tasks.
//!
//! Each unit of work runs as its own task (fan-out never serialises behind a slow host);
//! retryable failures are retried on an exponential backoff schedule and immediately whenever
//! the transport signals a route change.

use std::sync::Weak;
use std::time::Duration;

use serde_json::Value;
use tokio::time::Instant;
use wasl_proto::{methods, CancelRemoteRequest, DeliverRequest, TaskId, TaskState, UpdateRequest};

use crate::node::{now_ms, Inner, Node};
use crate::state::Work;
use crate::transport::TransportError;

enum Job {
    Deliver(DeliverRequest, String),
    Cancel(CancelRemoteRequest, String),
    Update(UpdateRequest, String, u64),
}

impl Node {
    /// Work owed right now that is neither in flight nor backing off.
    fn owed(&self) -> (Vec<(Work, Job)>, Option<Instant>) {
        let mut st = self.lock();
        let now = Instant::now();
        let mut jobs = Vec::new();
        let mut next: Option<Instant> = None;
        let mut candidates: Vec<(Work, Job)> = Vec::new();
        for r in st.out.values() {
            let id = r.p.task.id.clone();
            if r.p.state == TaskState::Queued && !r.p.delivered {
                candidates.push((
                    Work::Deliver(id.clone()),
                    Job::Deliver(
                        DeliverRequest {
                            task: r.p.task.clone(),
                        },
                        r.p.task.to.host.clone(),
                    ),
                ));
            }
            if let Some(c) = &r.p.cancel {
                candidates.push((
                    Work::Cancel(id),
                    Job::Cancel(c.clone(), r.p.task.to.host.clone()),
                ));
            }
        }
        for r in st.inbound.values() {
            if !r.p.reported {
                candidates.push((
                    Work::Update(r.p.task.id.clone()),
                    Job::Update(
                        UpdateRequest {
                            id: r.p.task.id.clone(),
                            state: r.p.state.clone(),
                        },
                        r.p.task.from.host.clone(),
                        r.rev,
                    ),
                ));
            }
        }
        for (w, j) in candidates {
            if st.inflight.contains(&w) {
                continue;
            }
            if let Some((at, _)) = st.retry.get(&w) {
                if *at > now {
                    next = Some(next.map_or(*at, |n| n.min(*at)));
                    continue;
                }
            }
            st.inflight.insert(w.clone());
            jobs.push((w, j));
        }
        (jobs, next)
    }

    async fn send(&self, host: &str, method: &str, params: Value) -> Result<(), TransportError> {
        let local = host == self.host_id();
        let r = if local {
            self.local_call(method, params).await
        } else {
            match self.transport() {
                Some(t) => t.call(host, method, params).await.map(|_| ()),
                None => Err(TransportError {
                    message: format!("no route to host {host} (no uplink attached)"),
                    retryable: true,
                }),
            }
        };
        r
    }

    async fn local_call(&self, method: &str, params: Value) -> Result<(), TransportError> {
        let bad = |e: serde_json::Error| TransportError {
            message: e.to_string(),
            retryable: false,
        };
        let r = match method {
            methods::NODE_DELIVER => {
                let req: DeliverRequest = serde_json::from_value(params).map_err(bad)?;
                self.node_deliver(req.task).await
            }
            methods::NODE_UPDATE => {
                let req: UpdateRequest = serde_json::from_value(params).map_err(bad)?;
                self.node_update(req).await
            }
            methods::NODE_CANCEL => {
                let req: CancelRemoteRequest = serde_json::from_value(params).map_err(bad)?;
                self.node_cancel(req).await
            }
            other => Err(wasit_control::RpcError::method_not_found(other)),
        };
        r.map(|_| ()).map_err(|e| TransportError {
            retryable: e.code == wasl_proto::codes::IO,
            message: e.to_string(),
        })
    }

    async fn run_job(&self, work: Work, job: Job) {
        let (host, method, params) = match &job {
            Job::Deliver(r, h) => (h.clone(), methods::NODE_DELIVER, serde_json::to_value(r)),
            Job::Cancel(r, h) => (h.clone(), methods::NODE_CANCEL, serde_json::to_value(r)),
            Job::Update(r, h, _) => (h.clone(), methods::NODE_UPDATE, serde_json::to_value(r)),
        };
        let result = match params {
            Ok(p) => self.send(&host, method, p).await,
            Err(e) => Err(TransportError {
                message: e.to_string(),
                retryable: false,
            }),
        };
        let mut failed_final: Option<(TaskId, String)> = None;
        let ack = {
            let mut st = self.lock();
            st.inflight.remove(&work);
            match result {
                Ok(()) => {
                    st.retry.remove(&work);
                    match &job {
                        Job::Deliver(r, _) => match st.out.get_mut(&r.task.id) {
                            Some(rec) if !rec.p.delivered => {
                                rec.p.delivered = true;
                                // Withdrawn while the delivery was in flight: forward it.
                                if rec.p.cancel.is_none() {
                                    match &rec.p.state {
                                        TaskState::Cancelled { reason } => {
                                            rec.p.cancel = Some(CancelRemoteRequest {
                                                id: r.task.id.clone(),
                                                reason: reason.clone(),
                                                expired: false,
                                            })
                                        }
                                        TaskState::Expired => {
                                            rec.p.cancel = Some(CancelRemoteRequest {
                                                id: r.task.id.clone(),
                                                reason: "deadline passed".into(),
                                                expired: true,
                                            })
                                        }
                                        _ => {}
                                    }
                                }
                                self.persist_out(&mut st, &r.task.id)
                            }
                            _ => None,
                        },
                        Job::Cancel(r, _) => match st.out.get_mut(&r.id) {
                            Some(rec) if rec.p.cancel.is_some() => {
                                rec.p.cancel = None;
                                self.persist_out(&mut st, &r.id)
                            }
                            _ => None,
                        },
                        Job::Update(r, _, rev) => match st.inbound.get_mut(&r.id) {
                            Some(rec) if rec.rev == *rev => {
                                rec.p.reported = true;
                                self.persist_in(&mut st, &r.id)
                            }
                            _ => None,
                        },
                    }
                }
                Err(e) if e.retryable => {
                    let attempt = st.retry.get(&work).map(|(_, a)| a + 1).unwrap_or(0);
                    let cfg = &self.inner.cfg;
                    let delay = cfg
                        .retry_initial
                        .saturating_mul(1u32 << attempt.min(16))
                        .min(cfg.retry_max);
                    tracing::debug!(?work, error = %e, ?delay, "node-to-node send failed; will retry");
                    st.retry
                        .insert(work.clone(), (Instant::now() + delay, attempt));
                    None
                }
                Err(e) => {
                    st.retry.remove(&work);
                    tracing::warn!(?work, error = %e, "node-to-node send failed permanently");
                    match &job {
                        Job::Deliver(r, _) => {
                            failed_final = Some((r.task.id.clone(), e.message.clone()));
                            None
                        }
                        Job::Cancel(r, _) => match st.out.get_mut(&r.id) {
                            Some(rec) => {
                                rec.p.cancel = None;
                                self.persist_out(&mut st, &r.id)
                            }
                            None => None,
                        },
                        // The sender does not know the task (any more): stop reporting.
                        Job::Update(r, _, _) => match st.inbound.get_mut(&r.id) {
                            Some(rec) => {
                                rec.p.reported = true;
                                self.persist_in(&mut st, &r.id)
                            }
                            None => None,
                        },
                    }
                }
            }
        };
        self.settle(ack).await;
        if let Some((id, msg)) = failed_final {
            self.finish_out(
                &id,
                TaskState::Failed {
                    error: format!("the task could not be delivered: {msg}"),
                },
            )
            .await;
        }
        self.inner.kick.notify_one();
    }

    /// Expire every non-terminal task past its deadline; returns the next deadline.
    async fn expire_overdue(&self) -> Option<u64> {
        let now = now_ms();
        let (out_ids, in_reqs, next) = {
            let st = self.lock();
            let mut next: Option<u64> = None;
            let mut out_ids = Vec::new();
            for r in st.out.values().filter(|r| !r.p.state.is_terminal()) {
                if r.p.task.deadline_ms <= now {
                    out_ids.push(r.p.task.id.clone());
                } else {
                    next = Some(next.map_or(r.p.task.deadline_ms, |n| n.min(r.p.task.deadline_ms)));
                }
            }
            let mut in_reqs = Vec::new();
            for r in st
                .inbound
                .values()
                .filter(|r| !r.p.state.is_terminal() && r.p.cancel.is_none())
            {
                if r.p.task.deadline_ms <= now {
                    in_reqs.push(CancelRemoteRequest {
                        id: r.p.task.id.clone(),
                        reason: "deadline passed".into(),
                        expired: true,
                    });
                } else {
                    next = Some(next.map_or(r.p.task.deadline_ms, |n| n.min(r.p.task.deadline_ms)));
                }
            }
            (out_ids, in_reqs, next)
        };
        for id in out_ids {
            if let Err(e) = self.withdraw(&id, "deadline passed".into(), true).await {
                tracing::debug!(error = %e, "expiring a task");
            }
        }
        for req in in_reqs {
            if let Err(e) = self.node_cancel(req).await {
                tracing::debug!(error = %e, "expiring an inbound task");
            }
        }
        next
    }
}

/// The outbox pump (ends when the node is dropped).
pub(crate) async fn run(inner: Weak<Inner>) {
    let mut gen_rx = match inner.upgrade() {
        Some(i) => i.transport_gen.subscribe(),
        None => return,
    };
    loop {
        let Some(i) = inner.upgrade() else { return };
        let node = Node { inner: i };
        let (jobs, next) = node.owed();
        for (w, j) in jobs {
            let n = node.clone();
            tokio::spawn(async move { n.run_job(w, j).await });
        }
        let mut changes = node.transport().and_then(|t| t.changes());
        let kick = node.inner.clone();
        drop(node);
        let route_changed = async {
            match changes.as_mut() {
                Some(c) => {
                    if !c.changed().await {
                        std::future::pending::<()>().await
                    }
                }
                None => std::future::pending::<()>().await,
            }
        };
        let backoff = async {
            match next {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            _ = kick.kick.notified() => {}
            _ = gen_rx.changed() => {}
            _ = backoff => {}
            _ = route_changed => {
                // A route came back (or went): retry everything now.
                kick.state.lock().unwrap_or_else(|e| e.into_inner()).retry.clear();
            }
        }
        drop(kick);
    }
}

/// The deadline timer (ends when the node is dropped).
pub(crate) async fn deadlines(inner: Weak<Inner>) {
    loop {
        let Some(i) = inner.upgrade() else { return };
        let node = Node { inner: i };
        let next = node.expire_overdue().await;
        let wake = node.inner.clone();
        drop(node);
        let sleep = async {
            match next {
                Some(ms) => {
                    let left = ms.saturating_sub(now_ms());
                    tokio::time::sleep(Duration::from_millis(left)).await
                }
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            _ = wake.deadlines.notified() => {}
            _ = sleep => {}
        }
        drop(wake);
    }
}
