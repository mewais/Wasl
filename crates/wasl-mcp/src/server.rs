//! wasl-mcp `server` — the rmcp 0.9 transport/runtime shell.
//!
//! [`WaslMcp`] implements an rmcp [`ServerHandler`] exposing the four Wasl tools
//! (`consult`/`command`/`message`/`transfer`). Each `#[tool]` method:
//!
//! 1. builds the secret-free gate request (`wasl_gate::build_request`) and picks
//!    the mode (`wasl_gate::choose_mode`),
//! 2. surfaces it to the human over MCP `elicitation/create` via the client peer,
//!    mapping accept/decline/cancel → [`GateOutcome`] (a client that cannot be
//!    elicited is **blocked**, never silently sent), and
//! 3. calls the async heart [`resolve_and_dispatch`] to audit + (only on approval)
//!    dispatch over REAL A2A, then renders the [`ToolReply`] into a
//!    [`CallToolResult`].
//!
//! No decision or routing logic lives here — it is all in the tested cores and
//! the [`crate::handle`] heart. State is held behind `Arc<Mutex<…>>` because rmcp
//! handlers are `&self` + async.

use std::sync::Arc;

use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CallToolResult, Content, CreateElicitationRequestParam, ElicitationAction, ElicitationSchema,
    Implementation, ProtocolVersion, ServerCapabilities, ServerInfo,
};
use rmcp::{
    tool, tool_handler, tool_router, ErrorData as McpError, Peer, RoleServer, ServerHandler,
};
use tokio::sync::Mutex;

use wasl_audit::{AuditLog, Sink};
use wasl_gate::{build_request, choose_mode, ClientCaps, GateOutcome};
use wasl_proto::{
    AgentId, CommandParams, ConsultParams, GateKind, HostId, MessageParams, TransferParams,
};
use wasl_registry::PeerRegistry;
use wasl_transfer::{Runner, Tool};

use crate::handle::{resolve_and_dispatch, HandleError, ToolCall, ToolCtx, ToolReply};

// ---------------------------------------------------------------------
// 0. Tool argument schemas
// ---------------------------------------------------------------------
//
// rmcp's `Parameters<T>` requires `T: JsonSchema`, which the pure `wasl_proto`
// params (deliberately serde-only) do not derive. These thin wrappers add the
// schema at the transport edge and convert into the proto types — no logic, just
// the IO-shell adapter. `schemars` comes re-exported from rmcp.

use rmcp::schemars::{self, JsonSchema};
use serde::Deserialize;

/// `consult` tool arguments.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ConsultArgs {
    /// Peer agent ids to consult.
    pub peers: Vec<String>,
    /// The question to ask.
    pub question: String,
}

impl From<ConsultArgs> for ConsultParams {
    fn from(a: ConsultArgs) -> Self {
        ConsultParams {
            peers: a.peers.into_iter().map(AgentId).collect(),
            question: a.question,
        }
    }
}

/// `command` tool arguments.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct CommandArgs {
    /// Peer agent ids to command.
    pub peers: Vec<String>,
    /// The task to run.
    pub task: String,
}

impl From<CommandArgs> for CommandParams {
    fn from(a: CommandArgs) -> Self {
        CommandParams {
            peers: a.peers.into_iter().map(AgentId).collect(),
            task: a.task,
        }
    }
}

/// `message` tool arguments.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct MessageArgs {
    /// Peer agent ids to message.
    pub peers: Vec<String>,
    /// The message payload.
    pub payload: String,
}

impl From<MessageArgs> for MessageParams {
    fn from(a: MessageArgs) -> Self {
        MessageParams {
            peers: a.peers.into_iter().map(AgentId).collect(),
            payload: a.payload,
        }
    }
}

/// `transfer` tool arguments.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct TransferArgs {
    /// Source host id.
    pub src_host: String,
    /// Destination host ids.
    pub dst_hosts: Vec<String>,
    /// The file path to transfer (same on source and destinations).
    pub path: String,
}

impl From<TransferArgs> for TransferParams {
    fn from(a: TransferArgs) -> Self {
        TransferParams {
            src_host: HostId(a.src_host),
            dst_hosts: a.dst_hosts.into_iter().map(HostId).collect(),
            path: a.path,
        }
    }
}

// ---------------------------------------------------------------------
// 1. Mutable state (behind the async lock)
// ---------------------------------------------------------------------

/// Everything the handler mutates per call: the audit log, the transfer runner,
/// and a monotonic sequence counter for deterministic audit/envelope ids. The
/// A2A dispatch seam is a shared `reqwest::Client` (cheap `Arc`-backed clone).
pub struct WaslState<K: Sink, R: Runner> {
    pub audit: AuditLog<K>,
    pub http: reqwest::Client,
    pub runner: R,
    pub seq: u64,
}

impl<K: Sink, R: Runner> WaslState<K, R> {
    pub fn new(audit: AuditLog<K>, http: reqwest::Client, runner: R) -> Self {
        Self {
            audit,
            http,
            runner,
            seq: 0,
        }
    }

    fn next_seq(&mut self) -> u64 {
        let s = self.seq;
        self.seq += 1;
        s
    }
}

// ---------------------------------------------------------------------
// 2. The handler
// ---------------------------------------------------------------------

/// The rmcp MCP server handler for Wasl. Cloneable (shared state behind `Arc`),
/// as rmcp requires for the tool router.
#[derive(Clone)]
pub struct WaslMcp<K: Sink, R: Runner> {
    self_id: AgentId,
    self_host: HostId,
    registry: Arc<PeerRegistry>,
    state: Arc<Mutex<WaslState<K, R>>>,
    tool_router: ToolRouter<Self>,
}

impl<K, R> WaslMcp<K, R>
where
    K: Sink + Send + Sync + 'static,
    R: Runner + Send + Sync + 'static,
{
    pub fn new(
        self_id: AgentId,
        self_host: HostId,
        registry: PeerRegistry,
        state: WaslState<K, R>,
    ) -> Self {
        Self {
            self_id,
            self_host,
            registry: Arc::new(registry),
            state: Arc::new(Mutex::new(state)),
            tool_router: Self::tool_router(),
        }
    }

    /// Surface a gate to the human via the client's elicitation, mapping the
    /// action → outcome. A client without elicitation capability, or a transport
    /// error, is treated as a **block** (auto-deny) — a client that cannot be
    /// prompted must never silently send.
    async fn surface_gate(
        peer: &Peer<RoleServer>,
        kind: GateKind,
        from: &AgentId,
        targets: &[String],
        preview: String,
    ) -> GateOutcome {
        let req = build_request("gate".to_string(), kind, from.clone(), targets, preview);
        // `choose_mode` selects the tier; this shell wires Tier-1 (elicitation)
        // only — the caps we report are elicitation-capable, and the actual
        // client-support check happens at the peer below.
        let caps = ClientCaps {
            elicitation: true,
            external: false,
        };
        let _mode = choose_mode(&caps);
        let message = wasl_gate::elicitation_for(&req).message;

        if !peer.supports_elicitation() {
            return GateOutcome::Denied {
                reason: "client cannot be prompted (no elicitation capability)".to_string(),
            };
        }

        // Empty object schema: this is a message-only approve/deny/cancel prompt.
        let schema = ElicitationSchema::builder()
            .build()
            .unwrap_or_else(|_| ElicitationSchema::builder().build_unchecked());
        let param = CreateElicitationRequestParam {
            message,
            requested_schema: schema,
        };
        match peer.create_elicitation(param).await {
            Ok(result) => match result.action {
                ElicitationAction::Accept => GateOutcome::Approved,
                ElicitationAction::Decline => GateOutcome::Denied {
                    reason: "declined".to_string(),
                },
                ElicitationAction::Cancel => GateOutcome::Dismissed,
            },
            Err(err) => GateOutcome::Denied {
                reason: format!("elicitation transport error: {err}"),
            },
        }
    }

    /// Run the full per-call flow: surface the gate, then audit + dispatch via the
    /// sync heart, then render the reply. Shared by all four tools.
    async fn gated_call(
        &self,
        peer: Peer<RoleServer>,
        kind: GateKind,
        target_strs: Vec<String>,
        preview: String,
        make_call: impl FnOnce() -> ToolCall,
    ) -> Result<CallToolResult, McpError> {
        let outcome = Self::surface_gate(&peer, kind, &self.self_id, &target_strs, preview).await;

        let mut state = self.state.lock().await;
        let seq = state.next_seq();
        let base_id = format!("mcp-{seq}");
        let ctx = ToolCtx {
            self_id: &self.self_id,
            self_host: &self.self_host,
            registry: &self.registry,
        };

        // Split the borrow of `state`: the A2A seam is a `&` shared client (cheap
        // Arc-backed clone), while the audit sink and transfer runner are `&mut`.
        let WaslState {
            audit,
            http,
            runner,
            ..
        } = &mut *state;
        let http = http.clone();

        let reply = resolve_and_dispatch(
            &ctx,
            &outcome,
            make_call(),
            &base_id,
            seq,
            &http,
            runner,
            audit,
        )
        .await
        .map_err(handle_error_to_mcp)?;

        Ok(reply_to_result(reply))
    }
}

// ---------------------------------------------------------------------
// 3. Tools
// ---------------------------------------------------------------------

#[tool_router]
impl<K, R> WaslMcp<K, R>
where
    K: Sink + Send + Sync + 'static,
    R: Runner + Send + Sync + 'static,
{
    #[tool(
        description = "Consult one or more peer agents with a question and collect their replies"
    )]
    async fn consult(
        &self,
        Parameters(args): Parameters<ConsultArgs>,
        peer: Peer<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let params: ConsultParams = args.into();
        let targets = peer_targets(&params.peers);
        let preview = truncate(&params.question);
        self.gated_call(peer, GateKind::Consult, targets, preview, || {
            ToolCall::Consult(params)
        })
        .await
    }

    #[tool(description = "Command one or more peer agents to run a task and collect their replies")]
    async fn command(
        &self,
        Parameters(args): Parameters<CommandArgs>,
        peer: Peer<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let params: CommandParams = args.into();
        let targets = peer_targets(&params.peers);
        let preview = truncate(&params.task);
        self.gated_call(peer, GateKind::Command, targets, preview, || {
            ToolCall::Command(params)
        })
        .await
    }

    #[tool(description = "Send a one-way message to one or more peer agents")]
    async fn message(
        &self,
        Parameters(args): Parameters<MessageArgs>,
        peer: Peer<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let params: MessageParams = args.into();
        let targets = peer_targets(&params.peers);
        let preview = truncate(&params.payload);
        self.gated_call(peer, GateKind::Message, targets, preview, || {
            ToolCall::Message(params)
        })
        .await
    }

    #[tool(description = "Transfer a file from a source host to one or more destination hosts")]
    async fn transfer(
        &self,
        Parameters(args): Parameters<TransferArgs>,
        peer: Peer<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let params: TransferParams = args.into();
        let targets: Vec<String> = params
            .dst_hosts
            .iter()
            .map(|h| h.as_str().to_string())
            .collect();
        let preview = format!("path={}", truncate(&params.path));
        self.gated_call(peer, GateKind::Transfer, targets, preview, || {
            ToolCall::Transfer {
                tool: Tool::Scp,
                params,
            }
        })
        .await
    }
}

// ---------------------------------------------------------------------
// 4. ServerHandler
// ---------------------------------------------------------------------

#[tool_handler]
impl<K, R> ServerHandler for WaslMcp<K, R>
where
    K: Sink + Send + Sync + 'static,
    R: Runner + Send + Sync + 'static,
{
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation::from_build_env(),
            instructions: Some(
                "Wasl agent-to-agent MCP server: consult/command/message peers and transfer \
                 files. Every call is gated by a permission-before-send elicitation and audited."
                    .to_string(),
            ),
        }
    }
}

// ---------------------------------------------------------------------
// 5. Rendering helpers
// ---------------------------------------------------------------------

fn peer_targets(peers: &[AgentId]) -> Vec<String> {
    peers.iter().map(|a| a.as_str().to_string()).collect()
}

/// A short, secret-free preview: the tool call content is summarized to at most
/// 120 chars for the human-facing gate.
fn truncate(s: &str) -> String {
    const MAX: usize = 120;
    if s.chars().count() <= MAX {
        s.to_string()
    } else {
        let head: String = s.chars().take(MAX).collect();
        format!("{head}…")
    }
}

fn handle_error_to_mcp(err: HandleError) -> McpError {
    McpError::internal_error(err.to_string(), None)
}

/// Render a [`ToolReply`] into an MCP tool result: a JSON content block for the
/// dispatched results, or an error-content result for a block.
fn reply_to_result(reply: ToolReply) -> CallToolResult {
    match reply {
        ToolReply::Consult(r) => json_ok(&r),
        ToolReply::Command(r) => json_ok(&r),
        ToolReply::Message(r) => json_ok(&r),
        ToolReply::Transfer(r) => json_ok(&r),
        ToolReply::Blocked { reason } => {
            CallToolResult::error(vec![Content::text(format!("blocked: {reason}"))])
        }
    }
}

fn json_ok<T: serde::Serialize>(value: &T) -> CallToolResult {
    match serde_json::to_string(value) {
        Ok(json) => CallToolResult::success(vec![Content::text(json)]),
        Err(err) => CallToolResult::error(vec![Content::text(format!("serialize error: {err}"))]),
    }
}
