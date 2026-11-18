//! Test harness: real `wasl serve` daemons over Unix sockets, sessions running the scriptable
//! fake CLI, and an in-test hub (wasit-control `Hub`) linking the daemons through each one's
//! `wasl bridge`. Nothing waits on sleeps: tests await topic events, snapshots and long-polls,
//! each under a bound that only turns a hang into a failure.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tokio::io::AsyncBufReadExt;
use wasit_control::{
    local_endpoint, CommandConnector, DurableOpts, DurableSubscription, Hello, HostSpec, Hub,
    HubConfig, LinkState, NoService, RequestOpts, RoutingService, Service,
};
use wasit_core::{AgentEvent, Command, GateDecision, GateRequest};
use wasit_fake_cli::scenario::Scenario;
use wasit_runtime::wire::{CreateSession, SessionEventKind, SessionId, SessionSnapshot};
use wasit_runtime::RuntimeClient;
use wasl_proto::*;

pub const BOUND: Duration = Duration::from_secs(60);

pub async fn within<T>(what: &str, fut: impl std::future::Future<Output = T>) -> T {
    match tokio::time::timeout(BOUND, fut).await {
        Ok(v) => v,
        Err(_) => panic!("timed out: {what}"),
    }
}

pub fn wasl_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_wasl"))
}

/// The fake CLI binary, building it once if needed.
pub fn fake() -> PathBuf {
    static P: OnceLock<PathBuf> = OnceLock::new();
    P.get_or_init(|| {
        if let Some(p) = wasit_fake_cli::fake_cli_path() {
            return p;
        }
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
        let st = std::process::Command::new(cargo)
            .args([
                "build",
                "-q",
                "-p",
                "wasit-fake-cli",
                "--bin",
                "wasit-fake-cli",
            ])
            .status()
            .expect("run cargo build for the fake CLI");
        assert!(st.success(), "building wasit-fake-cli failed");
        wasit_fake_cli::fake_cli_path().expect("fake CLI built")
    })
    .clone()
}

/// A short scratch directory (Unix socket paths are limited to ~104 bytes).
pub struct Scratch(pub PathBuf);

impl Scratch {
    pub fn new() -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let p = PathBuf::from(format!(
            "/tmp/wl-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        Scratch(p)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        kill_session_processes(&self.0);
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Kill the CLI processes of every session under `root` (their shims then exit).
fn kill_session_processes(root: &Path) {
    let Ok(hosts) = std::fs::read_dir(root) else {
        return;
    };
    for h in hosts.flatten() {
        let Ok(rd) = std::fs::read_dir(h.path().join("sessions")) else {
            continue;
        };
        for ent in rd.flatten() {
            let Ok(text) = std::fs::read_to_string(ent.path().join("journal.ndjson")) else {
                continue;
            };
            for line in text.lines() {
                let Ok(v) = serde_json::from_str::<Value>(line) else {
                    continue;
                };
                if v["kind"] == "spawned" {
                    if let Some(pid) = v["data"]["pid"].as_u64() {
                        let _ = std::process::Command::new("kill")
                            .args(["-9", &pid.to_string()])
                            .stderr(Stdio::null())
                            .status();
                    }
                }
            }
        }
    }
}

/// One `wasl serve` process.
pub struct Daemon {
    pub host: String,
    pub dir: PathBuf,
    child: Option<tokio::process::Child>,
    pub client: RuntimeClient,
    real: bool,
}

impl Daemon {
    pub fn socket(&self) -> PathBuf {
        self.dir.join("wasl.sock")
    }

    fn args(host: &str, dir: &Path, real: bool) -> Vec<String> {
        if real {
            return vec![
                "--data".into(),
                dir.to_string_lossy().into_owned(),
                "--host-id".into(),
                host.into(),
            ];
        }
        let fake = fake().to_string_lossy().into_owned();
        vec![
            "--data".into(),
            dir.to_string_lossy().into_owned(),
            "--host-id".into(),
            host.into(),
            "--driver-bin".into(),
            format!("claude={fake}"),
            "--driver-env".into(),
            "claude=WASIT_FAKE_PERSONA=claude".into(),
            "--handshake-timeout-ms".into(),
            "20000".into(),
            "--stop-grace-ms".into(),
            "500".into(),
        ]
    }

    pub async fn start(scratch: &Scratch, host: &str) -> Daemon {
        Self::start_with(scratch, host, false).await
    }

    /// A daemon running the real CLIs (live tests).
    pub async fn start_real(scratch: &Scratch, host: &str) -> Daemon {
        Self::start_with(scratch, host, true).await
    }

    async fn start_with(scratch: &Scratch, host: &str, real: bool) -> Daemon {
        let dir = scratch.0.join(host);
        std::fs::create_dir_all(&dir).unwrap();
        let child = Self::spawn(host, &dir, real).await;
        let client = RuntimeClient::connect_endpoint(
            local_endpoint(&dir.join("wasl.sock")),
            Hello::new("test", "test", "test"),
        )
        .with_deadlines(Duration::from_secs(30), Duration::from_secs(30));
        Daemon {
            host: host.into(),
            dir,
            child: Some(child),
            client,
            real,
        }
    }

    async fn spawn(host: &str, dir: &Path, real: bool) -> tokio::process::Child {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("serve.log"))
            .unwrap();
        let mut child = tokio::process::Command::new(wasl_bin())
            .arg("serve")
            .args(Self::args(host, dir, real))
            .env("WASL_LOG", "info,wasl_node=debug")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(log))
            .kill_on_drop(true)
            .spawn()
            .expect("spawn wasl serve");
        let stdout = child.stdout.take().unwrap();
        let mut line = String::new();
        within(
            "daemon readiness",
            tokio::io::BufReader::new(stdout).read_line(&mut line),
        )
        .await
        .unwrap();
        assert!(
            line.contains("\"ready\""),
            "daemon {host} did not start: {line:?}"
        );
        child
    }

    /// SIGKILL the daemon (sessions keep running in their shims).
    pub async fn kill(&mut self) {
        if let Some(mut c) = self.child.take() {
            c.kill().await.unwrap();
        }
    }

    pub async fn restart(&mut self) {
        self.kill().await;
        self.child = Some(Self::spawn(&self.host, &self.dir, self.real).await);
    }

    pub async fn call<T: DeserializeOwned>(&self, method: &str, params: Value) -> T {
        match self.try_call(method, params.clone()).await {
            Ok(v) => v,
            Err(e) => panic!("{method} {params} on {}: {e}", self.host),
        }
    }

    pub async fn try_call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Value,
    ) -> Result<T, wasit_control::CallError> {
        self.client
            .link()
            .request(
                method,
                params,
                RequestOpts::new(Duration::from_secs(120)),
                Duration::from_secs(30),
            )
            .await
    }

    /// Create a session running `scenario`.
    pub async fn session(&self, scenario: &Scenario) -> AgentId {
        static N: AtomicU64 = AtomicU64::new(0);
        let path = self.dir.join(format!(
            "scenario-{}.json",
            N.fetch_add(1, Ordering::Relaxed)
        ));
        scenario.write_to(&path).unwrap();
        let mut req = CreateSession::new("claude");
        req.cwd = Some(self.dir.to_string_lossy().into_owned());
        req.config.env = vec![(
            "WASIT_FAKE_SCENARIO".into(),
            path.to_string_lossy().into_owned(),
        )];
        let id = self.client.create(req).await.unwrap().id;
        self.wait_snapshot(&id, "idle", |s| s.phase.name() == "idle")
            .await;
        AgentId::new(self.host.clone(), id.0)
    }

    /// Wait for a session snapshot matching `pred` (signalled by the session's events).
    pub async fn wait_snapshot(
        &self,
        id: &SessionId,
        what: &str,
        pred: impl Fn(&SessionSnapshot) -> bool,
    ) -> SessionSnapshot {
        within(what, async {
            let mut sub = self.client.subscribe_session(id, None);
            loop {
                let s = self.client.snapshot(id).await.unwrap();
                if pred(&s) {
                    return s;
                }
                // Next event, then look again.
                sub.next().await.expect("session stream").unwrap();
            }
        })
        .await
    }

    /// Wait for the pending handoff gate of `task` on session `on`.
    pub async fn handoff_gate(&self, on: &AgentId, task: &TaskId) -> GateRequest {
        let rid = handoff_request_id(task);
        let s = self
            .wait_snapshot(&sid(on), "handoff gate", |s| {
                s.pending_gates.iter().any(|g| g.gate.request_id() == rid)
            })
            .await;
        s.pending_gates
            .into_iter()
            .find(|g| g.gate.request_id() == rid)
            .unwrap()
            .gate
    }

    pub async fn answer(&self, on: &AgentId, request_id: &str, decision: GateDecision) {
        self.client
            .command(
                &sid(on),
                Command::AnswerGate {
                    request_id: request_id.into(),
                    decision,
                },
            )
            .await
            .unwrap();
    }

    /// Answer the next native (CLI) permission gate of session `on` with approve.
    pub async fn approve_native_gate(&self, on: &AgentId) {
        let s = self
            .wait_snapshot(&sid(on), "native gate", |s| {
                s.pending_gates
                    .iter()
                    .any(|g| matches!(g.gate, GateRequest::Permission { .. }))
            })
            .await;
        let rid = s
            .pending_gates
            .iter()
            .find(|g| matches!(g.gate, GateRequest::Permission { .. }))
            .unwrap()
            .gate
            .request_id()
            .to_string();
        self.answer(on, &rid, GateDecision::approve()).await;
    }

    pub async fn set_topology(&self, t: &Topology) {
        let _: TopologyApplied = self.call(methods::TOPOLOGY_SET, json!(t)).await;
    }

    pub async fn delegate(
        &self,
        from: &AgentId,
        to: &[&AgentId],
        mode: TaskMode,
        instruction: &str,
        deadline_secs: Option<u64>,
    ) -> DelegateReply {
        let req = DelegateRequest {
            from: from.session.clone(),
            to: to.iter().map(|a| a.to_string()).collect(),
            mode,
            instruction: instruction.into(),
            deadline_secs,
            attachments: vec![],
        };
        self.call(methods::DELEGATE, json!(req)).await
    }

    /// Delegate to one peer and return the task (panics on a rejection).
    pub async fn delegate1(&self, from: &AgentId, to: &AgentId, instruction: &str) -> TaskBrief {
        let r = self
            .delegate(from, &[to], TaskMode::Consult, instruction, None)
            .await;
        r.results[0]
            .task
            .clone()
            .unwrap_or_else(|| panic!("delegation rejected: {:?}", r.results[0].error))
    }

    pub async fn wait(&self, ids: &[&TaskId], timeout_ms: u64) -> TaskStates {
        let req = WaitRequest {
            ids: ids.iter().map(|i| (*i).clone()).collect(),
            timeout_ms,
            any: false,
        };
        self.call(methods::TASK_WAIT, json!(req)).await
    }

    pub async fn wait_done(&self, id: &TaskId) -> TaskState {
        let s = self.wait(&[id], 55_000).await;
        assert!(!s.timed_out, "task {id} did not finish: {:?}", s.tasks);
        s.tasks[0].state.clone()
    }

    pub async fn status(&self, id: &TaskId) -> TaskState {
        let s: TaskStates = self
            .call(
                methods::TASK_STATUS,
                json!(TaskIds {
                    ids: vec![id.clone()]
                }),
            )
            .await;
        s.tasks[0].state.clone()
    }

    /// The `wasl.tasks` topic of this daemon.
    pub fn tasks_topic(&self) -> DurableSubscription {
        self.client
            .link()
            .subscribe_durable(topics::TASKS, DurableOpts::default())
    }

    pub fn edges_topic(&self) -> DurableSubscription {
        self.client
            .link()
            .subscribe_durable(topics::EDGES, DurableOpts::default())
    }
}

pub fn sid(a: &AgentId) -> SessionId {
    SessionId(a.session.clone())
}

/// Next task event matching `pred` (bounded).
pub async fn next_task_event(
    sub: &mut DurableSubscription,
    what: &str,
    pred: impl Fn(&TaskEvent) -> bool,
) -> TaskEvent {
    within(what, async {
        loop {
            let (_, v) = sub.next().await.expect("topic").expect("topic item");
            let e: TaskEvent = serde_json::from_value(v).unwrap();
            if pred(&e) {
                return e;
            }
        }
    })
    .await
}

pub fn card(a: &AgentId, name: &str) -> PeerCard {
    PeerCard {
        agent: a.clone(),
        name: name.into(),
        driver: "claude".into(),
        role: String::new(),
        description: String::new(),
        status: PeerStatus::Unknown,
    }
}

/// A topology with `edges` (from, to, policy); every agent named after its index.
pub fn topology(
    version: u64,
    agents: &[(&AgentId, &str)],
    edges: &[(&AgentId, &AgentId, EdgePolicy)],
    limits: Limits,
) -> Topology {
    Topology {
        workstream: "ws".into(),
        version,
        peers: agents.iter().map(|(a, n)| card(a, n)).collect(),
        edges: edges
            .iter()
            .map(|(f, t, p)| Edge {
                from: (*f).clone(),
                to: (*t).clone(),
                policy: *p,
            })
            .collect(),
        limits,
    }
}

/// An in-test hub linked to each daemon through `wasl bridge --no-start`.
pub struct TestHub {
    pub hub: Hub,
}

impl TestHub {
    pub async fn start(daemons: &[&Daemon]) -> TestHub {
        let mut cfg = HubConfig::new(Hello::new("test-hub", "hub", "hub"));
        cfg.supervise.backoff.initial = Duration::from_millis(50);
        cfg.supervise.backoff.max = Duration::from_millis(400);
        cfg.forward_timeout = Duration::from_secs(60);
        let hub = Hub::new(cfg, Arc::new(CommandConnector), None);
        let local: Arc<dyn Service> = Arc::new(NoService);
        hub.set_inbound_service(Arc::new(RoutingService::new(hub.clone(), local)));
        let h = TestHub { hub };
        for d in daemons {
            h.link(d);
        }
        h.connected(daemons).await;
        h
    }

    pub fn link(&self, d: &Daemon) {
        let argv = vec![
            wasl_bin().to_string_lossy().into_owned(),
            "bridge".into(),
            "--data".into(),
            d.dir.to_string_lossy().into_owned(),
            "--host-id".into(),
            d.host.clone(),
            "--no-start".into(),
        ];
        self.hub
            .add_link(HostSpec::new(d.host.clone(), argv))
            .unwrap();
    }

    /// Wait until every daemon's link is connected.
    pub async fn connected(&self, daemons: &[&Daemon]) {
        let hosts: Vec<String> = daemons.iter().map(|d| d.host.clone()).collect();
        let mut st = self.hub.states();
        within("hub links connected", async {
            loop {
                {
                    let m: BTreeMap<_, _> = st.borrow_and_update().clone();
                    if hosts.iter().all(|h| {
                        m.get(&h.as_str().into())
                            .is_some_and(LinkState::is_connected)
                    }) {
                        return;
                    }
                }
                st.changed().await.unwrap();
            }
        })
        .await
    }

    pub async fn shutdown(&self) {
        self.hub.shutdown().await;
    }
}

/// A worker that answers after `ms` of "work".
pub fn slow_worker(answer: &str, ms: u64) -> Scenario {
    use wasit_fake_cli::scenario::Turn;
    let mut b = Scenario::builder();
    for _ in 0..4 {
        b = b.turn(
            Turn::builder()
                .say("working on it")
                .sleep_ms(ms)
                .say(answer),
        );
    }
    b.build()
}

pub fn result_summary(s: &TaskState) -> String {
    match s {
        TaskState::Completed { result } => result.summary.clone(),
        other => panic!("expected completed, got {other:?}"),
    }
}

pub fn is_gate_event(e: &SessionEventKind) -> bool {
    matches!(
        e,
        SessionEventKind::Agent {
            event: AgentEvent::GateRequested { .. }
        }
    )
}
