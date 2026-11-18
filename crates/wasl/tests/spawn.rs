//! Spawning workers on demand: the node's spawn flow against a test supervisor (the spawn
//! authority served by the in-test hub), real daemons and the fake CLI.

mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common::*;
use serde_json::{json, Value};
use wasit_control::{CallContext, RpcError, Service};
use wasit_core::GateRequest;
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasit_runtime::wire::CreateSession;
use wasit_runtime::RuntimeClient;
use wasl_proto::*;

/// What the test supervisor knows about a host.
struct HostEntry {
    client: RuntimeClient,
    dir: std::path::PathBuf,
}

#[derive(Default)]
struct SupState {
    topo: Option<Topology>,
    /// Launched requests (request id → agent): the idempotency record.
    launched: BTreeMap<String, AgentId>,
    launch_calls: u32,
    spawned_by: BTreeMap<AgentId, AgentId>,
    always: bool,
    always_handoffs: bool,
}

/// A spawn authority in the test: plans (limit `max`), launches on a daemon, keeps the
/// topology and pushes it to every daemon.
struct Supervisor {
    hosts: BTreeMap<String, HostEntry>,
    max: usize,
    st: Mutex<SupState>,
}

fn rpc(code: &str, msg: impl Into<String>) -> RpcError {
    RpcError::new(code, msg)
}

impl Supervisor {
    fn new(daemons: &[&Daemon], topo: Topology, max: usize) -> Arc<Supervisor> {
        let hosts = daemons
            .iter()
            .map(|d| {
                (
                    d.host.clone(),
                    HostEntry {
                        client: d.client.clone(),
                        dir: d.dir.clone(),
                    },
                )
            })
            .collect();
        Arc::new(Supervisor {
            hosts,
            max,
            st: Mutex::new(SupState {
                topo: Some(topo),
                ..Default::default()
            }),
        })
    }

    fn st(&self) -> std::sync::MutexGuard<'_, SupState> {
        self.st.lock().unwrap()
    }

    fn plan(&self, ask: SpawnAsk) -> Result<SpawnPlan, RpcError> {
        let st = self.st();
        if st.spawned_by.len() >= self.max {
            return Err(rpc(
                codes::SPAWN_LIMIT,
                format!(
                    "this workstream may have at most {} spawned agents",
                    self.max
                ),
            ));
        }
        let host = ask.spec.host.clone().unwrap_or(ask.parent.host.clone());
        if !self.hosts.contains_key(&host) {
            return Err(rpc(
                codes::SPAWN_REFUSED,
                format!("host {host} is not allowed"),
            ));
        }
        let topo = st.topo.as_ref().unwrap();
        let from = topo
            .peers
            .iter()
            .find(|p| p.agent == ask.parent)
            .map(|p| p.name.clone())
            .unwrap_or_default();
        Ok(SpawnPlan {
            approval: if st.always {
                SpawnApproval::Allowed
            } else {
                SpawnApproval::Ask
            },
            from,
            name: ask.spec.name,
            role: ask.spec.role,
            host,
            driver: "Fake claude".into(),
            model: ask.spec.model,
            cwd: "/w".into(),
            mode: ask.spec.mode,
            workstream: "ws".into(),
            new_workstream: false,
            details: json!({"workstream": "ws"}),
        })
    }

    async fn launch(&self, l: SpawnLaunch) -> Result<SpawnLaunched, RpcError> {
        {
            let mut st = self.st();
            st.launch_calls += 1;
            if let Some(a) = st.launched.get(&l.request_id) {
                let topo = st.topo.clone().unwrap();
                return Ok(SpawnLaunched {
                    agent: a.clone(),
                    name: l.spec.name,
                    workstream: topo.workstream.clone(),
                    topology: topo,
                });
            }
        }
        let host = l.spec.host.clone().unwrap_or(l.parent.host.clone());
        let h = self.hosts.get(&host).unwrap();
        let path = h.dir.join(format!("worker-{}.json", l.request_id));
        Scenario::builder()
            .turn(Turn::builder().say("The answer is 42."))
            .build()
            .write_to(&path)
            .unwrap();
        let mut req = CreateSession::new("claude");
        req.cwd = Some(h.dir.to_string_lossy().into_owned());
        req.config.env = vec![(
            "WASIT_FAKE_SCENARIO".into(),
            path.to_string_lossy().into_owned(),
        )];
        let id = h
            .client
            .create(req)
            .await
            .map_err(|e| rpc("internal", e.to_string()))?
            .id;
        let agent = AgentId::new(host, id.0);
        let topo = {
            let mut st = self.st();
            match &l.decision {
                SpawnDecision::AlwaysAllow => st.always = true,
                SpawnDecision::AlwaysAllowWithHandoffs => {
                    st.always = true;
                    st.always_handoffs = true;
                }
                _ => {}
            }
            let policy = if st.always_handoffs {
                EdgePolicy::AlwaysAllow
            } else {
                EdgePolicy::Ask
            };
            st.launched.insert(l.request_id.clone(), agent.clone());
            st.spawned_by.insert(agent.clone(), l.parent.clone());
            let t = st.topo.as_mut().unwrap();
            t.version += 1;
            t.peers.push(card(&agent, &l.spec.name));
            t.edges.push(Edge {
                from: l.parent.clone(),
                to: agent.clone(),
                policy,
            });
            t.clone()
        };
        for h in self.hosts.values() {
            let _: TopologyApplied = h
                .client
                .link()
                .request(
                    methods::TOPOLOGY_SET,
                    &topo,
                    wasit_control::RequestOpts::new(std::time::Duration::from_secs(20)),
                    std::time::Duration::from_secs(20),
                )
                .await
                .map_err(|e| rpc("internal", e.to_string()))?;
        }
        Ok(SpawnLaunched {
            agent,
            name: l.spec.name,
            workstream: topo.workstream.clone(),
            topology: topo,
        })
    }

    async fn dismiss(&self, d: DismissAsk) -> Result<DismissReply, RpcError> {
        let (agent, name) = {
            let st = self.st();
            let topo = st.topo.as_ref().unwrap();
            let Some(p) = topo
                .peers
                .iter()
                .find(|p| p.name == d.peer || p.agent.to_string() == d.peer)
            else {
                return Err(rpc(codes::UNKNOWN_PEER, format!("no agent {}", d.peer)));
            };
            if st.spawned_by.get(&p.agent) != Some(&d.parent) {
                return Err(rpc(
                    codes::NOT_SPAWNED_BY_CALLER,
                    format!("{} was not spawned by you", p.name),
                ));
            }
            (p.agent.clone(), p.name.clone())
        };
        let h = self.hosts.get(&agent.host).unwrap();
        h.client
            .stop(&sid(&agent))
            .await
            .map_err(|e| rpc("internal", e.to_string()))?;
        Ok(DismissReply { agent, name })
    }
}

struct SupervisorService(Arc<Supervisor>);

fn reply<T: serde::Serialize>(r: Result<T, RpcError>) -> Result<Value, RpcError> {
    r.map(|v| serde_json::to_value(v).unwrap())
}

#[async_trait]
impl Service for SupervisorService {
    async fn call(&self, _ctx: CallContext, method: &str, p: Value) -> Result<Value, RpcError> {
        let s = &self.0;
        match method {
            methods::SUPERVISOR_SPAWN_PLAN => reply(s.plan(serde_json::from_value(p).unwrap())),
            methods::SUPERVISOR_SPAWN_LAUNCH => {
                reply(s.launch(serde_json::from_value(p).unwrap()).await)
            }
            methods::SUPERVISOR_DISMISS => {
                reply(s.dismiss(serde_json::from_value(p).unwrap()).await)
            }
            methods::SUPERVISOR_SPAWNABLE => reply(Ok(Spawnable {
                available: true,
                note: "every spawn asks the human".into(),
                hosts: s
                    .hosts
                    .keys()
                    .map(|h| SpawnHost {
                        id: h.clone(),
                        name: h.clone(),
                        drivers: vec![SpawnDriver {
                            id: "claude".into(),
                            name: "Fake claude".into(),
                            models: vec![],
                        }],
                    })
                    .collect(),
                remaining: Some((s.max - s.st().spawned_by.len()) as u32),
            })),
            other => Err(RpcError::method_not_found(other)),
        }
    }
}

fn spec(name: &str, host: Option<&str>, instruction: Option<&str>) -> SpawnSpec {
    SpawnSpec {
        name: name.into(),
        role: "answers arithmetic".into(),
        host: host.map(Into::into),
        instruction: instruction.map(Into::into),
        ..Default::default()
    }
}

fn spawn_call(d: &Daemon, from: &AgentId, rid: &str, spec: SpawnSpec) -> Value {
    let _ = d;
    json!(SpawnRequest {
        from: from.session.clone(),
        request_id: rid.into(),
        spec,
    })
}

async fn spawn_gate(d: &Daemon, on: &AgentId, rid: &str) -> GateRequest {
    let want = spawn_request_id(rid);
    let s = d
        .wait_snapshot(&sid(on), "spawn gate", |s| {
            s.pending_gates.iter().any(|g| g.gate.request_id() == want)
        })
        .await;
    s.pending_gates
        .into_iter()
        .find(|g| g.gate.request_id() == want)
        .unwrap()
        .gate
}

struct Setup {
    _s: Scratch,
    a: Daemon,
    b: Daemon,
    hub: TestHub,
    sup: Arc<Supervisor>,
    orch: AgentId,
}

async fn setup(max: usize) -> Setup {
    setup_args(max, &[]).await
}

async fn setup_args(max: usize, a_args: &[&str]) -> Setup {
    let s = Scratch::new();
    let a = Daemon::start_args(&s, "a", a_args).await;
    let b = Daemon::start(&s, "b").await;
    let orch = a.session(&Scenario::builder().build()).await;
    let t = topology(1, &[(&orch, "orchestrator")], &[], Limits::default());
    a.set_topology(&t).await;
    b.set_topology(&t).await;
    let sup = Supervisor::new(&[&a, &b], t, max);
    let hub = TestHub::start_with(&[&a, &b], Arc::new(SupervisorService(sup.clone()))).await;
    Setup {
        _s: s,
        a,
        b,
        hub,
        sup,
        orch,
    }
}

/// A daemon with no supervisor answers "spawning is not available" (and says so in
/// `list_peers`).
#[tokio::test]
async fn standalone_daemon_refuses_spawning() {
    let s = Scratch::new();
    let a = Daemon::start_args(&s, "a", &["--spawn-wait-ms", "300"]).await;
    let orch = a.session(&Scenario::builder().build()).await;
    let r = a
        .try_call::<SpawnReply>(
            methods::SPAWN,
            spawn_call(&a, &orch, "r1", spec("w", None, None)),
        )
        .await;
    match r {
        Err(wasit_control::CallError::Rpc(e)) => {
            assert_eq!(e.code, codes::SPAWN_UNAVAILABLE, "{e:?}");
            assert!(e.message.contains("not available"), "{}", e.message);
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    let peers: PeerList = a
        .call(methods::PEERS_LIST, json!({"from": orch.session}))
        .await;
    let sp = peers.spawnable.expect("spawnable");
    assert!(!sp.available);
    assert!(sp.note.contains("no supervisor"), "{}", sp.note);
}

/// The gate appears on the calling session with the plan; "always allow spawning and handoffs"
/// launches the worker on another host, the first task goes through without a handoff gate and
/// its result returns; the next spawn needs no gate; dismissing works only for one's own.
#[tokio::test]
async fn spawn_on_another_host_with_always_allow_then_delegate_and_dismiss() {
    let x = setup(8).await;
    let call = spawn_call(
        &x.a,
        &x.orch,
        "r1",
        spec("calc", Some("b"), Some("what is 6*7?")),
    );
    let a2 = x.a.client.clone();
    let pending = tokio::spawn(async move {
        a2.link()
            .request::<SpawnReply>(
                methods::SPAWN,
                &call,
                wasit_control::RequestOpts::new(std::time::Duration::from_secs(120)),
                std::time::Duration::from_secs(30),
            )
            .await
    });
    let gate = spawn_gate(&x.a, &x.orch, "r1").await;
    match &gate {
        GateRequest::Spawn { spawn, choices, .. } => {
            assert_eq!(spawn.from, "orchestrator");
            assert_eq!(spawn.name, "calc");
            assert_eq!(spawn.role, "answers arithmetic");
            assert_eq!(spawn.host, "b");
            assert_eq!(spawn.instruction.as_deref(), Some("what is 6*7?"));
            assert_eq!(choices.len(), 4);
        }
        other => panic!("not a spawn gate: {other:?}"),
    }
    x.a.answer(
        &x.orch,
        gate.request_id(),
        SpawnDecision::AlwaysAllowWithHandoffs.to_gate(),
    )
    .await;
    let r = within("spawn reply", pending).await.unwrap().unwrap();
    assert_eq!(r.name, "calc");
    assert_eq!(r.agent.host, "b");
    // (Checked after the first spawn: by then the hub's link into the daemon is registered.)
    let peers: PeerList =
        x.a.call(methods::PEERS_LIST, json!({"from": x.orch.session}))
            .await;
    let sp = peers.spawnable.expect("spawnable");
    assert!(sp.available, "{sp:?}");
    assert_eq!(sp.hosts.len(), 2);
    assert_eq!(sp.remaining, Some(7));
    assert!(peers.peers.iter().any(|p| p.card.name == "calc"));
    let task = r.task.expect("first task");
    assert_eq!(
        task.state,
        TaskState::Queued,
        "always-allow edge: no handoff gate"
    );
    let done = x.a.wait_done(&task.id).await;
    assert_eq!(result_summary(&done), "The answer is 42.");

    // Spawning is now always allowed: no gate.
    let r2: SpawnReply =
        x.a.call(
            methods::SPAWN,
            spawn_call(&x.a, &x.orch, "r2", spec("calc2", None, None)),
        )
        .await;
    assert_eq!(r2.agent.host, "a");
    assert!(r2.task.is_none());

    // Dismiss: only an agent you spawned.
    let worker2 = x.b.session(&Scenario::builder().build()).await;
    let bad =
        x.b.try_call::<DismissReply>(
            methods::DISMISS,
            json!(DismissRequest {
                from: worker2.session.clone(),
                peer: "calc".into()
            }),
        )
        .await;
    assert!(
        matches!(&bad, Err(wasit_control::CallError::Rpc(e)) if e.code == codes::NOT_SPAWNED_BY_CALLER),
        "{bad:?}"
    );
    let d: DismissReply =
        x.a.call(
            methods::DISMISS,
            json!(DismissRequest {
                from: x.orch.session.clone(),
                peer: "calc".into()
            }),
        )
        .await;
    assert_eq!(d.agent, r.agent);
    x.b.wait_snapshot(&sid(&r.agent), "worker stopped", |s| {
        s.phase.name() == "exited"
    })
    .await;
    x.hub.shutdown().await;
}

/// Deny with a reason: the agent gets `spawn_denied` with the reason; nothing is launched.
/// Allow once on the same host keeps the next spawn gated; the limit is reported.
#[tokio::test]
async fn deny_allow_once_and_limits() {
    let x = setup(1).await;
    let call = spawn_call(&x.a, &x.orch, "r1", spec("w1", None, None));
    let a2 = x.a.client.clone();
    let pending = tokio::spawn(async move {
        a2.link()
            .request::<SpawnReply>(
                methods::SPAWN,
                &call,
                wasit_control::RequestOpts::new(std::time::Duration::from_secs(120)),
                std::time::Duration::from_secs(30),
            )
            .await
    });
    let gate = spawn_gate(&x.a, &x.orch, "r1").await;
    x.a.answer(
        &x.orch,
        gate.request_id(),
        SpawnDecision::Deny {
            reason: Some("not today".into()),
        }
        .to_gate(),
    )
    .await;
    match within("denied", pending).await.unwrap() {
        Err(wasit_control::CallError::Rpc(e)) => {
            assert_eq!(e.code, codes::SPAWN_DENIED);
            assert!(e.message.contains("not today"), "{}", e.message);
        }
        other => panic!("expected a denial, got {other:?}"),
    }
    assert_eq!(x.sup.st().launch_calls, 0);

    let call = spawn_call(&x.a, &x.orch, "r2", spec("w2", None, None));
    let a2 = x.a.client.clone();
    let pending = tokio::spawn(async move {
        a2.link()
            .request::<SpawnReply>(
                methods::SPAWN,
                &call,
                wasit_control::RequestOpts::new(std::time::Duration::from_secs(120)),
                std::time::Duration::from_secs(30),
            )
            .await
    });
    let gate = spawn_gate(&x.a, &x.orch, "r2").await;
    x.a.answer(
        &x.orch,
        gate.request_id(),
        SpawnDecision::AllowOnce.to_gate(),
    )
    .await;
    let r = within("allowed", pending).await.unwrap().unwrap();
    assert_eq!(r.agent.host, "a");
    // The edge to it asks (allow once does not extend to handoffs).
    let peers: PeerList =
        x.a.call(methods::PEERS_LIST, json!({"from": x.orch.session}))
            .await;
    let p = peers.peers.iter().find(|p| p.card.name == "w2").unwrap();
    assert_eq!(p.policy, EdgePolicy::Ask);
    // Limit of one spawned agent reached.
    let over =
        x.a.try_call::<SpawnReply>(
            methods::SPAWN,
            spawn_call(&x.a, &x.orch, "r3", spec("w3", None, None)),
        )
        .await;
    match over {
        Err(wasit_control::CallError::Rpc(e)) => {
            assert_eq!(e.code, codes::SPAWN_LIMIT);
            assert!(e.message.contains("at most 1"), "{}", e.message);
        }
        other => panic!("expected the limit, got {other:?}"),
    }
    x.hub.shutdown().await;
}

/// The calling daemon is killed while the gate waits; after the restart a retry with the same
/// request id waits for the same gate, and answering it launches exactly one agent.
#[tokio::test]
async fn restart_while_asking_does_not_spawn_twice() {
    let mut x = setup(8).await;
    let call = spawn_call(&x.a, &x.orch, "r1", spec("w", Some("b"), None));
    let a2 = x.a.client.clone();
    let c2 = call.clone();
    let first = tokio::spawn(async move {
        a2.link()
            .request::<SpawnReply>(
                methods::SPAWN,
                &c2,
                wasit_control::RequestOpts::new(std::time::Duration::from_secs(120)),
                std::time::Duration::from_secs(1),
            )
            .await
    });
    let gate = spawn_gate(&x.a, &x.orch, "r1").await;
    x.a.restart().await;
    // The first call lost its daemon.
    assert!(within("first call ends", first).await.unwrap().is_err());
    x.hub.connected(&[&x.a, &x.b]).await;
    // The gate survived the restart.
    let again = spawn_gate(&x.a, &x.orch, "r1").await;
    assert_eq!(again.request_id(), gate.request_id());
    let a2 = x.a.client.clone();
    let retry = tokio::spawn(async move {
        a2.link()
            .request::<SpawnReply>(
                methods::SPAWN,
                &call,
                wasit_control::RequestOpts::new(std::time::Duration::from_secs(120)),
                std::time::Duration::from_secs(30),
            )
            .await
    });
    x.a.answer(
        &x.orch,
        gate.request_id(),
        SpawnDecision::AllowOnce.to_gate(),
    )
    .await;
    let r = within("retry reply", retry).await.unwrap().unwrap();
    assert_eq!(r.agent.host, "b");
    assert_eq!(x.sup.st().launched.len(), 1);
    assert_eq!(x.b.client.list().await.unwrap().len(), 1);
    x.hub.shutdown().await;
}

fn pending_spawn(
    d: &Daemon,
    from: &AgentId,
    rid: &str,
    spec: SpawnSpec,
) -> tokio::task::JoinHandle<Result<SpawnReply, wasit_control::CallError>> {
    let call = spawn_call(d, from, rid, spec);
    let link = d.client.clone();
    tokio::spawn(async move {
        link.link()
            .request::<SpawnReply>(
                methods::SPAWN,
                &call,
                wasit_control::RequestOpts::new(std::time::Duration::from_secs(120)),
                std::time::Duration::from_secs(30),
            )
            .await
    })
}

async fn refused(
    what: &str,
    call: tokio::task::JoinHandle<Result<SpawnReply, wasit_control::CallError>>,
) -> RpcError {
    match within(what, call).await.unwrap() {
        Err(wasit_control::CallError::Rpc(e)) => e,
        other => panic!("{what}: expected a refusal, got {other:?}"),
    }
}

/// A spawn gate raised from a turn is withdrawn when that turn is interrupted: the call fails
/// with `spawn_withdrawn`, the gate leaves the session, a late "allow" is refused and starts
/// nothing.
#[tokio::test]
async fn spawn_gate_is_withdrawn_when_the_asking_turn_is_interrupted() {
    let x = setup(8).await;
    let busy = x.a.session(&busy_until_interrupted()).await;
    x.a.start_turn(&busy).await;
    let call = pending_spawn(&x.a, &busy, "r1", spec("w", None, None));
    let gate = spawn_gate(&x.a, &busy, "r1").await;
    x.a.interrupt(&busy).await;
    let e = refused("withdrawn", call).await;
    assert_eq!(e.code, codes::SPAWN_WITHDRAWN, "{e:?}");
    assert!(e.message.contains("interrupted"), "{}", e.message);
    x.a.assert_withdrawn(&busy, gate.request_id(), "allow_once")
        .await;
    assert_eq!(x.sup.st().launch_calls, 0);
    x.hub.shutdown().await;
}

/// The `spawn_agent` MCP call is cancelled by its client, or its client disconnects: the
/// request is withdrawn either way and a late answer starts nothing.
#[tokio::test]
async fn spawn_gate_is_withdrawn_when_the_mcp_call_is_cancelled_or_its_client_disconnects() {
    use rmcp::model::{CallToolRequestParam, ClientRequest, Request};
    use rmcp::service::PeerRequestOptions;
    use rmcp::ServiceExt;

    let x = setup_args(8, &["--spawn-reattach-ms", "200"]).await;
    let mcp = || async {
        let mut cmd = tokio::process::Command::new(wasl_bin());
        cmd.args([
            "mcp",
            "--socket",
            &x.a.socket().to_string_lossy(),
            "--session",
            &x.orch.session,
        ]);
        let transport = rmcp::transport::TokioChildProcess::new(cmd).expect("spawn wasl mcp");
        within("mcp initialize", ().serve(transport))
            .await
            .expect("mcp client")
    };
    let spawn_req = |name: &str| {
        ClientRequest::CallToolRequest(Request::new(CallToolRequestParam {
            name: "spawn_agent".into(),
            arguments: json!({"name": name, "role": "answers arithmetic"})
                .as_object()
                .cloned(),
        }))
    };
    let first_gate = |snap: &wasit_runtime::wire::SessionSnapshot| {
        snap.pending_gates
            .iter()
            .find(|g| matches!(g.gate, GateRequest::Spawn { .. }))
            .map(|g| g.gate.request_id().to_string())
    };

    // Cancelled (`notifications/cancelled`).
    let client = mcp().await;
    let handle = client
        .peer()
        .send_cancellable_request(spawn_req("w1"), PeerRequestOptions::no_options())
        .await
        .unwrap();
    let snap =
        x.a.wait_snapshot(&sid(&x.orch), "spawn gate", |s| first_gate(s).is_some())
            .await;
    let rid = first_gate(&snap).unwrap();
    handle
        .cancel(Some("turn interrupted".into()))
        .await
        .unwrap();
    x.a.assert_withdrawn(&x.orch, &rid, "allow_once").await;

    // Disconnected (the MCP server process goes away with its stdio).
    let handle = client
        .peer()
        .send_cancellable_request(spawn_req("w2"), PeerRequestOptions::no_options())
        .await
        .unwrap();
    let snap =
        x.a.wait_snapshot(&sid(&x.orch), "second spawn gate", |s| {
            first_gate(s).is_some()
        })
        .await;
    let rid = first_gate(&snap).unwrap();
    drop(handle);
    within("mcp client closes", client.cancel()).await.unwrap();
    x.a.assert_withdrawn(&x.orch, &rid, "allow_once").await;
    assert_eq!(x.sup.st().launch_calls, 0);
    x.hub.shutdown().await;
}

/// A spawn whose first task's deadline already passed is refused at once; one whose deadline
/// passes while it waits for the human is withdrawn (`spawn_expired`) and a late answer starts
/// nothing.
#[tokio::test]
async fn spawn_past_its_deadline_is_refused_or_withdrawn() {
    let x = setup(8).await;
    let mut late = spec("w0", None, Some("what is 6*7?"));
    late.deadline_secs = Some(0);
    let e = refused("expired", pending_spawn(&x.a, &x.orch, "r0", late)).await;
    assert_eq!(e.code, codes::SPAWN_EXPIRED, "{e:?}");
    assert!(e.message.contains("deadline"), "{}", e.message);

    let mut soon = spec("w1", None, Some("what is 6*7?"));
    soon.deadline_secs = Some(1);
    let call = pending_spawn(&x.a, &x.orch, "r1", soon);
    let gate = spawn_gate(&x.a, &x.orch, "r1").await;
    let e = refused("expired while asking", call).await;
    assert_eq!(e.code, codes::SPAWN_EXPIRED, "{e:?}");
    x.a.assert_withdrawn(&x.orch, gate.request_id(), "allow_once")
        .await;
    assert_eq!(x.sup.st().launch_calls, 0);
    x.hub.shutdown().await;
}
