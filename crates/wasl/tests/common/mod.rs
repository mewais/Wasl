//! Test harness: real `wasl serve` daemons over local endpoints (Unix sockets / Windows named pipes), sessions running the scriptable
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
use wasit_core::{AgentEvent, Command, GateDecision, GateReason, GateRequest};
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
            .args(["build", "-q", "-p", "wasl-fake-cli"])
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
        #[cfg(unix)]
        let root = PathBuf::from("/tmp");
        #[cfg(not(unix))]
        let root = std::env::temp_dir();
        let p = root.join(format!(
            "wl-{}-{}",
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
        // Daemons and session shims (with their CLIs) started anywhere below this directory
        // outlive their starters on purpose: end them before their files go.
        wasit_fake_cli::harness::stop_all_under(&self.0);
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// One `wasl serve` process.
pub struct Daemon {
    pub host: String,
    pub dir: PathBuf,
    child: Option<tokio::process::Child>,
    pub client: RuntimeClient,
    real: bool,
    extra: Vec<String>,
    /// Started without the `WASL_LOG` developer override: stderr then holds only what the
    /// daemon itself prints.
    quiet: bool,
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
        Self::start_with(scratch, host, false, &[]).await
    }

    /// A daemon without the `WASL_LOG` developer override (its stderr capture is then what a
    /// user's would be).
    pub async fn start_quiet(scratch: &Scratch, host: &str) -> Daemon {
        Self::start_inner(scratch, host, false, &[], true).await
    }

    /// A daemon with extra `serve` options.
    pub async fn start_args(scratch: &Scratch, host: &str, extra: &[&str]) -> Daemon {
        Self::start_with(scratch, host, false, extra).await
    }

    /// A daemon running the real CLIs (live tests).
    pub async fn start_real(scratch: &Scratch, host: &str) -> Daemon {
        Self::start_with(scratch, host, true, &[]).await
    }

    async fn start_with(scratch: &Scratch, host: &str, real: bool, extra: &[&str]) -> Daemon {
        Self::start_inner(scratch, host, real, extra, false).await
    }

    async fn start_inner(
        scratch: &Scratch,
        host: &str,
        real: bool,
        extra: &[&str],
        quiet: bool,
    ) -> Daemon {
        let dir = scratch.0.join(host);
        std::fs::create_dir_all(&dir).unwrap();
        let extra: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
        let child = Self::spawn(host, &dir, real, &extra, quiet).await;
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
            extra,
            quiet,
        }
    }

    async fn spawn(
        host: &str,
        dir: &Path,
        real: bool,
        extra: &[String],
        quiet: bool,
    ) -> tokio::process::Child {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("serve.log"))
            .unwrap();
        let mut cmd = tokio::process::Command::new(wasl_bin());
        cmd.arg("serve")
            .args(Self::args(host, dir, real))
            .args(extra);
        if quiet {
            cmd.env_remove("WASL_LOG");
        } else {
            cmd.env("WASL_LOG", "info,wasl_node=debug");
        }
        let mut child = cmd
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
        self.child =
            Some(Self::spawn(&self.host, &self.dir, self.real, &self.extra, self.quiet).await);
    }

    /// Stop the daemon the way a service manager does (SIGTERM) and wait for it to exit: it
    /// writes what it recorded before it goes (sessions keep running in their shims).
    #[cfg(unix)]
    pub async fn terminate(&mut self) {
        if let Some(mut c) = self.child.take() {
            let pid = c.id().expect("the daemon runs");
            // SAFETY: plain syscall on the child this harness started.
            unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
            let status = within("daemon exit", c.wait()).await.unwrap();
            assert!(
                status.success(),
                "daemon {} exited with {status}",
                self.host
            );
        }
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

    /// The gate `request_id` was withdrawn on session `on`: it leaves the snapshot, the
    /// session's events say `gate_resolved` with reason code `code` (returned, with its detail),
    /// and a late answer is refused with `gate_withdrawn` (it does nothing).
    pub async fn assert_withdrawn(
        &self,
        on: &AgentId,
        request_id: &str,
        choice: &str,
        code: &str,
    ) -> GateReason {
        self.wait_snapshot(&sid(on), "gate withdrawn", |s| {
            !s.pending_gates
                .iter()
                .any(|g| g.gate.request_id() == request_id)
        })
        .await;
        let events = self
            .client
            .history(&sid(on), None, 1000)
            .await
            .expect("history");
        let reason = events
            .events
            .iter()
            .find_map(|e| match &e.kind {
                SessionEventKind::Agent {
                    event:
                        AgentEvent::GateResolved {
                            request_id: r,
                            reason: Some(reason),
                        },
                } if r == request_id => Some(reason.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("no gate_resolved with a reason for {request_id}"));
        assert_eq!(
            reason.code, code,
            "why {request_id} was withdrawn: {reason:?}"
        );
        let late = self
            .client
            .command(
                &sid(on),
                Command::AnswerGate {
                    request_id: request_id.into(),
                    decision: GateDecision::choose(choice),
                },
            )
            .await;
        assert_eq!(
            late,
            Err(wasit_runtime::ClientError::Command(
                wasit_runtime::wire::CommandError::GateWithdrawn {
                    request_id: request_id.into()
                }
            ))
        );
        reason
    }

    /// The order the session itself guarantees for a gate bound to its asking turn: in `on`'s
    /// events the gate `request_id` is resolved before the turn it was raised in completes
    /// (the first completion after the raise) and, when that turn was `interrupted`, right
    /// behind the interrupt's `input` event, ahead of anything the CLI said about it.
    pub async fn assert_gate_resolved_before_its_turn_completed(
        &self,
        on: &AgentId,
        request_id: &str,
        interrupted: bool,
    ) {
        self.wait_snapshot(&sid(on), "the turn completed", |s| {
            s.turns_completed > 0 && s.turn.is_none()
        })
        .await;
        let events = self
            .client
            .history(&sid(on), None, 1000)
            .await
            .expect("history")
            .events;
        let find = |from: usize, f: &dyn Fn(&SessionEventKind) -> bool| {
            events[from..]
                .iter()
                .position(|e| f(&e.kind))
                .map(|i| i + from)
        };
        let requested = find(0, &|k| {
            matches!(k, SessionEventKind::Agent {
                event: AgentEvent::GateRequested { gate }
            } if gate.request_id() == request_id)
        })
        .expect("the gate was raised");
        let resolved = find(requested, &|k| {
            matches!(k, SessionEventKind::Agent {
                event: AgentEvent::GateResolved { request_id: r, .. }
            } if r == request_id)
        })
        .expect("the gate was resolved");
        let completed = find(requested, &|k| {
            matches!(
                k,
                SessionEventKind::Agent {
                    event: AgentEvent::TurnCompleted { .. }
                }
            )
        })
        .expect("the turn completed");
        assert!(
            resolved < completed,
            "the gate must be resolved before its turn completes: resolved at {resolved}, \
             completed at {completed}"
        );
        if interrupted {
            let interrupt = find(requested, &|k| {
                matches!(
                    k,
                    SessionEventKind::Input {
                        command: Command::Interrupt,
                        ..
                    }
                )
            })
            .expect("the interrupt is an event");
            assert_eq!(
                resolved,
                interrupt + 1,
                "the gate is resolved right behind the interrupt"
            );
        }
    }

    /// Start a turn on `on` that runs until it is interrupted (the scenario must have one).
    pub async fn start_turn(&self, on: &AgentId) {
        self.client
            .command(&sid(on), Command::prompt("go"))
            .await
            .unwrap();
        self.wait_snapshot(&sid(on), "turn running", |s| s.turn.is_some())
            .await;
    }

    pub async fn interrupt(&self, on: &AgentId) {
        self.client
            .command(&sid(on), Command::Interrupt)
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
        let gate = s
            .pending_gates
            .iter()
            .find(|g| matches!(g.gate, GateRequest::Permission { .. }))
            .unwrap()
            .gate
            .clone();
        let decision = GateDecision::by_kind(&gate, &[wasit_core::GateChoiceKind::AllowOnce])
            .expect("an allow choice");
        self.answer(on, gate.request_id(), decision).await;
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

    /// The `wasl.spawns` topic of this daemon (every spawn request that ended).
    pub fn spawns_topic(&self) -> DurableSubscription {
        self.client
            .link()
            .subscribe_durable(topics::SPAWNS, DurableOpts::default())
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

/// Next ended spawn request matching `pred` (bounded).
pub async fn next_spawn_ended(
    sub: &mut DurableSubscription,
    what: &str,
    pred: impl Fn(&SpawnEnded) -> bool,
) -> SpawnEnded {
    within(what, async {
        loop {
            let (_, v) = sub.next().await.expect("topic").expect("topic item");
            let e: SpawnEnded = serde_json::from_value(v).unwrap();
            if pred(&e) {
                return e;
            }
        }
    })
    .await
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
        paused: false,
    }
}

/// An in-test hub linked to each daemon through `wasl bridge --no-start`.
pub struct TestHub {
    pub hub: Hub,
}

impl TestHub {
    pub async fn start(daemons: &[&Daemon]) -> TestHub {
        Self::start_with(daemons, Arc::new(NoService)).await
    }

    /// A hub whose own requests (`host: None`, e.g. `wasl.supervisor.*`) are served by `local`.
    pub async fn start_with(daemons: &[&Daemon], local: Arc<dyn Service>) -> TestHub {
        let mut cfg = HubConfig::new(Hello::new("test-hub", "hub", "hub"));
        cfg.supervise.backoff.initial = Duration::from_millis(50);
        cfg.supervise.backoff.max = Duration::from_millis(400);
        cfg.forward_timeout = Duration::from_secs(60);
        let hub = Hub::new(cfg, Arc::new(CommandConnector), None);
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

/// A session whose turns run until interrupted.
pub fn busy_until_interrupted() -> Scenario {
    use wasit_fake_cli::scenario::Turn;
    let mut b = Scenario::builder();
    for _ in 0..4 {
        b = b.turn(Turn::builder().say("thinking").wait_for_interrupt());
    }
    b.build()
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
