//! wasl-mcp — the stdio MCP server an agent CLI launches to talk to other agents.
//!
//! It is a thin client of the local daemon's Wasl node: every tool is one `wasl.*` request over
//! the daemon's control socket (a supervised connection: a daemon restart mid-call reconnects
//! and read-only calls such as `task_wait` resume). The session it speaks for is fixed at
//! injection time ([`mcp_server_config`]).
//!
//! Tools: `list_peers`, `delegate`, `task_status`, `task_wait`, `task_cancel`, `consult`,
//! `message`. Results are compact JSON.
//!
//! Handoff approval normally happens on the sending session as a gate the supervisor shows.
//! Without a supervisor the node marks a task `pending_approval` via `elicitation`; this server
//! then asks its client (MCP elicitation) and reports the answer, or denies when the client
//! cannot ask.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CallToolResult, Content, CreateElicitationRequestParam, ElicitationAction, ElicitationSchema,
    Implementation, ProtocolVersion, ServerCapabilities, ServerInfo,
};
use rmcp::schemars::{self, JsonSchema};
use rmcp::{
    tool, tool_handler, tool_router, ErrorData as McpError, Peer, RoleServer, ServerHandler,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use wasit_control::{
    connector, local_endpoint, CallError, ClientOptions, ControlClient, Hello, RequestOpts,
    Supervised, SupervisedOptions,
};
use wasit_core::{McpServerConfig, McpTransport};
use wasl_proto::{
    methods, ApprovalVia, CancelRequest, DecideRequest, DelegateReply, DelegateRequest,
    HandoffDecision, MessageRequest, PeerList, PeersRequest, TaskBrief, TaskId, TaskMode,
    TaskState, TaskStates, WaitRequest,
};

/// Name of the MCP server as agents see it (tools appear as `mcp__wasl__…` in claude).
pub const SERVER_NAME: &str = "wasl";

const BUILD: &str = concat!("wasl-mcp/", env!("CARGO_PKG_VERSION"));
/// How long a call waits for the daemon to come (back) up.
const WAIT_CONNECTED: Duration = Duration::from_secs(30);
const DEADLINE: Duration = Duration::from_secs(30);
const DEFAULT_WAIT_SECS: u64 = 120;
const MAX_WAIT_SECS: u64 = 600;

/// Where the server connects and which session it speaks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpArgs {
    /// The daemon's socket path (on Windows the path its named pipe is derived from).
    pub socket: PathBuf,
    pub session: String,
}

impl McpArgs {
    /// `--socket <path> --session <id>`; falls back to `WASL_SOCKET` / `WASL_SESSION`.
    pub fn parse(args: &[String]) -> Result<McpArgs, String> {
        let mut socket = std::env::var_os("WASL_SOCKET").map(PathBuf::from);
        let mut session = std::env::var("WASL_SESSION").ok();
        let mut it = args.iter();
        while let Some(a) = it.next() {
            let mut val = || it.next().cloned().ok_or(format!("{a} needs a value"));
            match a.as_str() {
                "--socket" => socket = Some(PathBuf::from(val()?)),
                "--session" => session = Some(val()?),
                other => return Err(format!("unknown option {other}")),
            }
        }
        Ok(McpArgs {
            socket: socket.ok_or("--socket <path> (or WASL_SOCKET) is required")?,
            session: session.ok_or("--session <id> (or WASL_SESSION) is required")?,
        })
    }
}

/// The MCP server item that injects this server into a session's launch config
/// (`AgentConfig::mcp_servers`). `program` + `prefix` is how to start it (e.g. the daemon
/// binary and `["mcp"]`).
pub fn mcp_server_config(
    program: &Path,
    prefix: &[String],
    socket: &Path,
    session: &str,
) -> McpServerConfig {
    let mut args: Vec<String> = prefix.to_vec();
    args.extend([
        "--socket".to_string(),
        socket.to_string_lossy().into_owned(),
        "--session".to_string(),
        session.to_string(),
    ]);
    McpServerConfig {
        name: SERVER_NAME.into(),
        transport: McpTransport::Stdio {
            command: program.to_string_lossy().into_owned(),
            args,
            env: Default::default(),
        },
    }
}

// ── tool arguments ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, JsonSchema, Default)]
pub struct NoArgs {}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct DelegateArgs {
    /// Peer names or ids from list_peers (one or many).
    pub to: Vec<String>,
    /// What the peer should do; self-contained (the peer does not see your conversation).
    pub instruction: String,
    /// "command" (do the work; default) or "consult" (answer only, change nothing).
    #[serde(default)]
    pub mode: Option<String>,
    /// Give up after this many seconds (default: the workstream's).
    #[serde(default)]
    pub deadline_secs: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct IdsArgs {
    /// Task ids returned by delegate.
    pub ids: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct WaitArgs {
    /// Task ids returned by delegate.
    pub ids: Vec<String>,
    /// Max seconds to wait (default 120, max 600).
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Return as soon as any one task finishes.
    #[serde(default)]
    pub any: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct CancelArgs {
    pub id: String,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ConsultArgs {
    /// Peer names or ids from list_peers.
    pub to: Vec<String>,
    /// The question; self-contained.
    pub question: String,
    /// Max seconds to wait for the answers (default 120, max 600).
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct MessageArgs {
    /// Peer names or ids from list_peers.
    pub to: Vec<String>,
    pub text: String,
}

// ── compact results ─────────────────────────────────────────────────────────

/// A task as an agent sees it.
#[derive(Debug, Serialize)]
struct TaskOut {
    id: String,
    to: String,
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    progress: Option<Value>,
}

fn task_out(b: &TaskBrief) -> TaskOut {
    let to = if b.to_name.is_empty() {
        b.to.to_string()
    } else {
        b.to_name.clone()
    };
    let (result, error, progress) = match &b.state {
        TaskState::Completed { result } => (Some(result.summary.clone()), None, None),
        TaskState::Failed { error } => (None, Some(error.clone()), None),
        TaskState::Cancelled { reason } | TaskState::Denied { reason } => {
            (None, Some(reason.clone()), None)
        }
        TaskState::Expired => (None, Some("deadline passed".into()), None),
        TaskState::Running { progress }
            if progress.tool_calls > 0 || progress.last_text.is_some() =>
        {
            (None, None, serde_json::to_value(progress).ok())
        }
        _ => (None, None, None),
    };
    TaskOut {
        id: b.id.to_string(),
        to,
        state: b.state.name(),
        result,
        error,
        progress,
    }
}

fn json_result(v: Value) -> CallToolResult {
    CallToolResult::success(vec![Content::text(v.to_string())])
}

fn tool_error(msg: impl Into<String>) -> CallToolResult {
    CallToolResult::error(vec![Content::text(msg.into())])
}

fn ids(v: &[String]) -> Vec<TaskId> {
    v.iter().map(|s| TaskId(s.clone())).collect()
}

fn wait_secs(t: Option<u64>) -> u64 {
    t.unwrap_or(DEFAULT_WAIT_SECS).clamp(1, MAX_WAIT_SECS)
}

// ── the server ──────────────────────────────────────────────────────────────

/// The MCP handler.
#[derive(Clone)]
pub struct WaslMcp {
    link: Supervised,
    session: String,
    tool_router: ToolRouter<Self>,
}

impl WaslMcp {
    /// A handler talking to the daemon at `args.socket` (the connection is made lazily and
    /// re-made after a daemon restart).
    pub fn new(args: &McpArgs) -> Self {
        let endpoint = local_endpoint(&args.socket);
        let hello = Hello::new(BUILD, "wasl-mcp", "wasl-mcp");
        let link = Supervised::spawn(
            connector(move || {
                let endpoint = endpoint.clone();
                let opts = ClientOptions::new(hello.clone());
                async move { ControlClient::connect_endpoint(&endpoint, opts).await }
            }),
            SupervisedOptions::default(),
        );
        WaslMcp {
            link,
            session: args.session.clone(),
            tool_router: Self::tool_router(),
        }
    }

    /// One request. `retry`: the call is idempotent, so it is re-sent if the daemon restarts
    /// while it is in flight.
    async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Value,
        deadline: Duration,
        retry: bool,
    ) -> Result<T, String> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            let r: Result<T, CallError> = self
                .link
                .request(method, &params, RequestOpts::new(deadline), WAIT_CONNECTED)
                .await;
            match r {
                Ok(v) => return Ok(v),
                Err(CallError::ConnectionLost(_) | CallError::NotConnected)
                    if retry && attempts < 5 =>
                {
                    continue
                }
                Err(CallError::Rpc(e)) => return Err(e.message),
                Err(CallError::NotConnected) => {
                    return Err("the local Wasl daemon is not reachable".into())
                }
                Err(e) => return Err(e.to_string()),
            }
        }
    }

    /// Standalone approval: ask the client for tasks that wait for an elicitation.
    async fn approve_pending(&self, peer: &Peer<RoleServer>, tasks: &mut [TaskBrief]) {
        let can_ask = peer
            .peer_info()
            .is_some_and(|c| c.capabilities.elicitation.is_some());
        for t in tasks.iter_mut() {
            if t.state
                != (TaskState::PendingApproval {
                    via: ApprovalVia::Elicitation,
                })
            {
                continue;
            }
            let to = if t.to_name.is_empty() {
                t.to.to_string()
            } else {
                t.to_name.clone()
            };
            let decision = if can_ask {
                let param = CreateElicitationRequestParam {
                    message: format!(
                        "Allow this agent to hand a {} task to {to}? Accept to allow once.",
                        t.mode.as_str()
                    ),
                    requested_schema: ElicitationSchema::builder()
                        .build()
                        .unwrap_or_else(|_| ElicitationSchema::builder().build_unchecked()),
                };
                match peer.create_elicitation(param).await {
                    Ok(r) if r.action == ElicitationAction::Accept => HandoffDecision::AllowOnce,
                    Ok(_) => HandoffDecision::Deny {
                        reason: Some("declined by the user".into()),
                    },
                    Err(e) => HandoffDecision::Deny {
                        reason: Some(format!("could not ask the user: {e}")),
                    },
                }
            } else {
                HandoffDecision::Deny {
                    reason: Some(
                        "no supervisor is attached and this client cannot ask the user".into(),
                    ),
                }
            };
            let req = DecideRequest {
                id: t.id.clone(),
                decision,
            };
            match self
                .call::<TaskBrief>(methods::TASK_DECIDE, json!(req), DEADLINE, false)
                .await
            {
                Ok(b) => *t = b,
                Err(e) => tracing::warn!(error = %e, "reporting a handoff decision failed"),
            }
        }
    }

    async fn do_delegate(
        &self,
        peer: &Peer<RoleServer>,
        req: DelegateRequest,
        method: &str,
    ) -> Result<(Vec<TaskBrief>, Vec<Value>), String> {
        let reply: DelegateReply = self.call(method, json!(req), DEADLINE, false).await?;
        let mut tasks = Vec::new();
        let mut errors = Vec::new();
        for r in reply.results {
            match (r.task, r.error) {
                (Some(t), _) => tasks.push(t),
                (None, Some(e)) => errors.push(json!({"to": r.to, "error": e.message})),
                (None, None) => {}
            }
        }
        self.approve_pending(peer, &mut tasks).await;
        Ok((tasks, errors))
    }

    async fn do_wait(&self, ids: Vec<TaskId>, secs: u64, any: bool) -> Result<TaskStates, String> {
        let req = WaitRequest {
            ids,
            timeout_ms: secs * 1000,
            any,
        };
        self.call(
            methods::TASK_WAIT,
            json!(req),
            Duration::from_secs(secs + 30),
            true,
        )
        .await
    }
}

fn states_json(s: &TaskStates) -> Value {
    let tasks: Vec<TaskOut> = s.tasks.iter().map(task_out).collect();
    let mut v = json!({"tasks": tasks});
    if s.timed_out {
        v["timed_out"] = json!(true);
    }
    if !s.unknown.is_empty() {
        v["unknown"] = json!(s.unknown);
    }
    v
}

#[tool_router]
impl WaslMcp {
    #[tool(
        description = "List the agents you can delegate to: name, id, role, live status, approval policy. Empty when this session has no connections."
    )]
    async fn list_peers(
        &self,
        Parameters(_a): Parameters<NoArgs>,
    ) -> Result<CallToolResult, McpError> {
        let req = PeersRequest {
            from: self.session.clone(),
        };
        match self
            .call::<PeerList>(methods::PEERS_LIST, json!(req), DEADLINE, true)
            .await
        {
            Ok(l) if l.peers.is_empty() => Ok(json_result(json!({
                "peers": [],
                "note": "no connections are configured for this session; there is nobody to delegate to"
            }))),
            Ok(l) => {
                let peers: Vec<Value> = l
                    .peers
                    .iter()
                    .map(|p| {
                        let mut v = json!({
                            "name": p.card.name,
                            "id": p.card.agent,
                            "status": p.card.status,
                            "policy": p.policy,
                        });
                        for (k, s) in [
                            ("role", &p.card.role),
                            ("description", &p.card.description),
                            ("driver", &p.card.driver),
                        ] {
                            if !s.is_empty() {
                                v[k] = json!(s);
                            }
                        }
                        v
                    })
                    .collect();
                Ok(json_result(json!({ "peers": peers })))
            }
            Err(e) => Ok(tool_error(e)),
        }
    }

    #[tool(
        description = "Hand a task to one or more peers. Returns task ids at once; peers work in parallel. Collect results with task_wait (delegate to many, then wait once). mode: command (default, do work) or consult (answer only). A human may have to approve the handoff first."
    )]
    async fn delegate(
        &self,
        Parameters(a): Parameters<DelegateArgs>,
        peer: Peer<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let mode = match a.mode.as_deref() {
            None | Some("command") => TaskMode::Command,
            Some("consult") => TaskMode::Consult,
            Some(other) => {
                return Ok(tool_error(format!(
                    "unknown mode {other:?}; use \"command\" or \"consult\""
                )))
            }
        };
        let req = DelegateRequest {
            from: self.session.clone(),
            to: a.to,
            mode,
            instruction: a.instruction,
            deadline_secs: a.deadline_secs,
            attachments: Vec::new(),
        };
        match self.do_delegate(&peer, req, methods::DELEGATE).await {
            Ok((tasks, errors)) => {
                let out: Vec<TaskOut> = tasks.iter().map(task_out).collect();
                let mut v = json!({ "tasks": out });
                if !errors.is_empty() {
                    v["errors"] = json!(errors);
                }
                if tasks.is_empty() {
                    return Ok(CallToolResult::error(vec![Content::text(v.to_string())]));
                }
                Ok(json_result(v))
            }
            Err(e) => Ok(tool_error(e)),
        }
    }

    #[tool(
        description = "Current state of tasks (pending_approval, queued, running+progress, completed+result, failed, cancelled, denied, expired). Does not wait."
    )]
    async fn task_status(
        &self,
        Parameters(a): Parameters<IdsArgs>,
    ) -> Result<CallToolResult, McpError> {
        let req = wasl_proto::TaskIds { ids: ids(&a.ids) };
        match self
            .call::<TaskStates>(methods::TASK_STATUS, json!(req), DEADLINE, true)
            .await
        {
            Ok(s) => Ok(json_result(states_json(&s))),
            Err(e) => Ok(tool_error(e)),
        }
    }

    #[tool(
        description = "Wait until the tasks finish (all, or any=true for the first) or timeout_secs passes (default 120, max 600); returns states and results. Call again if timed_out."
    )]
    async fn task_wait(
        &self,
        Parameters(a): Parameters<WaitArgs>,
    ) -> Result<CallToolResult, McpError> {
        match self
            .do_wait(
                ids(&a.ids),
                wait_secs(a.timeout_secs),
                a.any.unwrap_or(false),
            )
            .await
        {
            Ok(s) => Ok(json_result(states_json(&s))),
            Err(e) => Ok(tool_error(e)),
        }
    }

    #[tool(description = "Cancel a task you delegated (stops it if running).")]
    async fn task_cancel(
        &self,
        Parameters(a): Parameters<CancelArgs>,
    ) -> Result<CallToolResult, McpError> {
        let req = CancelRequest {
            id: TaskId(a.id),
            reason: a.reason,
        };
        match self
            .call::<TaskBrief>(methods::TASK_CANCEL, json!(req), DEADLINE, true)
            .await
        {
            Ok(b) => Ok(json_result(json!(task_out(&b)))),
            Err(e) => Ok(tool_error(e)),
        }
    }

    #[tool(
        description = "Ask peers a question and wait for their answers (consult-mode delegate + task_wait). For long work use delegate + task_wait."
    )]
    async fn consult(
        &self,
        Parameters(a): Parameters<ConsultArgs>,
        peer: Peer<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let req = DelegateRequest {
            from: self.session.clone(),
            to: a.to,
            mode: TaskMode::Consult,
            instruction: a.question,
            deadline_secs: None,
            attachments: Vec::new(),
        };
        let (tasks, errors) = match self.do_delegate(&peer, req, methods::DELEGATE).await {
            Ok(r) => r,
            Err(e) => return Ok(tool_error(e)),
        };
        if tasks.is_empty() {
            return Ok(tool_error(json!({ "errors": errors }).to_string()));
        }
        let wait_ids: Vec<TaskId> = tasks.iter().map(|t| t.id.clone()).collect();
        match self
            .do_wait(wait_ids, wait_secs(a.timeout_secs), false)
            .await
        {
            Ok(s) => {
                let mut v = states_json(&s);
                if !errors.is_empty() {
                    v["errors"] = json!(errors);
                }
                Ok(json_result(v))
            }
            Err(e) => Ok(tool_error(e)),
        }
    }

    #[tool(description = "Send peers a note. Fire-and-forget: no reply is expected.")]
    async fn message(
        &self,
        Parameters(a): Parameters<MessageArgs>,
        peer: Peer<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let req = MessageRequest {
            from: self.session.clone(),
            to: a.to.clone(),
            text: a.text.clone(),
        };
        let reply: Result<DelegateReply, String> = self
            .call(methods::MESSAGE, json!(req), DEADLINE, false)
            .await;
        match reply {
            Ok(r) => {
                let mut tasks: Vec<TaskBrief> = Vec::new();
                let mut errors = Vec::new();
                for o in r.results {
                    match (o.task, o.error) {
                        (Some(t), _) => tasks.push(t),
                        (None, Some(e)) => errors.push(json!({"to": o.to, "error": e.message})),
                        _ => {}
                    }
                }
                self.approve_pending(&peer, &mut tasks).await;
                let sent: Vec<TaskOut> = tasks.iter().map(task_out).collect();
                let mut v = json!({ "sent": sent });
                if !errors.is_empty() {
                    v["errors"] = json!(errors);
                }
                if tasks.is_empty() {
                    return Ok(CallToolResult::error(vec![Content::text(v.to_string())]));
                }
                Ok(json_result(v))
            }
            Err(e) => Ok(tool_error(e)),
        }
    }
}

#[tool_handler]
impl ServerHandler for WaslMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: SERVER_NAME.into(),
                version: env!("CARGO_PKG_VERSION").into(),
                ..Implementation::from_build_env()
            },
            instructions: Some(
                "Delegate work to other agents. list_peers shows who you can reach; delegate returns task ids immediately (fan out to several peers, then task_wait once); consult asks and waits."
                    .into(),
            ),
        }
    }
}

/// Serve MCP over this process's stdio until the client disconnects.
pub async fn serve(args: McpArgs) -> Result<(), String> {
    use rmcp::ServiceExt;
    let handler = WaslMcp::new(&args);
    let running = handler
        .serve(rmcp::transport::io::stdio())
        .await
        .map_err(|e| format!("cannot start the MCP server: {e}"))?;
    running
        .waiting()
        .await
        .map(|_| ())
        .map_err(|e| format!("MCP server ended with an error: {e}"))
}

/// The `mcp` subcommand: parse `args` and serve (builds its own runtime).
pub fn run(args: &[String]) -> std::process::ExitCode {
    let parsed = match McpArgs::parse(args) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("wasl mcp: {e}");
            return std::process::ExitCode::from(2);
        }
    };
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("wasl mcp: cannot start the runtime: {e}");
            return std::process::ExitCode::from(1);
        }
    };
    match rt.block_on(serve(parsed)) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("wasl mcp: {e}");
            std::process::ExitCode::from(1)
        }
    }
}
