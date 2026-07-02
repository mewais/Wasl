//! wasl-mcp `server` — the rmcp 0.9 transport/runtime shell.
//!
//! [`WaslMcp`] implements an rmcp [`ServerHandler`] exposing the five Wasl tools
//! (`consult`/`command`/`message`/`transfer`/`commit`). Each gated `#[tool]` method:
//!
//! 1. builds the secret-free gate request (`wasl_gate::build_request`) and picks
//!    the tier for THIS call from the configured [`GateAuthority`] + the REAL
//!    client capabilities (`crate::gating::{detect_caps, select_mode}`),
//! 2. surfaces (Tier-1 elicitation / Tier-3 external) or defers (Tier-2
//!    prepare-commit) the gate accordingly — a client that cannot be prompted
//!    and has no other authority is **blocked**, never silently sent, and
//! 3. calls the async heart [`resolve_and_dispatch`] to audit + (only on approval)
//!    dispatch over REAL A2A, then renders the [`ToolReply`] into a
//!    [`CallToolResult`].
//!
//! No decision or routing logic lives here — it is all in the tested cores, the
//! pure seams in [`crate::gating`], and the [`crate::handle`] heart. State is
//! held behind `Arc<Mutex<…>>` because rmcp handlers are `&self` + async.
//! See `docs/specs/mcp-gating.md`.

use std::collections::HashMap;
use std::sync::Arc;

use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CallToolResult, Content, CreateElicitationRequestParam, ElicitationSchema, Implementation,
    ProtocolVersion, ServerCapabilities, ServerInfo,
};
use rmcp::{
    tool, tool_handler, tool_router, ErrorData as McpError, Peer, RoleServer, ServerHandler,
};
use tokio::sync::Mutex;

use wasl_audit::{AuditLog, Sink};
use wasl_gate::{build_request, outcome_of, GateError, GateLedger, GateMode, GateOutcome};
use wasl_operator::{DecidedBy, OperatorHub, OperatorSurfacer};
use wasl_proto::{
    AgentId, ApprovalToken, CommandParams, ConsultParams, ElicitationResponse, GateDecision,
    GateKind, HostId, MessageParams, TransferParams,
};
use wasl_registry::PeerRegistry;
use wasl_server::Surfacer;
use wasl_transfer::{Runner, Tool};

use crate::gating::{detect_caps, ledger_decision_of, proto_action, select_mode, GateAuthority};
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
use serde::{Deserialize, Serialize};

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

/// `commit` tool arguments.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct CommitArgs {
    /// The approval token returned by a pending tool call.
    pub token: String,
}

// ---------------------------------------------------------------------
// 1. Mutable state (behind the async lock)
// ---------------------------------------------------------------------

/// Everything the handler mutates per call: the audit log, the transfer runner,
/// a monotonic sequence counter for deterministic audit/envelope ids, and the
/// Tier-2 prepare->commit tables. The A2A dispatch seam is a shared
/// `reqwest::Client` (cheap `Arc`-backed clone).
pub struct WaslState<K: Sink, R: Runner> {
    pub audit: AuditLog<K>,
    pub http: reqwest::Client,
    pub runner: R,
    pub seq: u64,
    /// Tier-2 token validity (`docs/specs/gate.md` §4).
    pub ledger: GateLedger,
    /// The remembered call for each still-pending token, in lockstep with
    /// `ledger`: inserted together at prepare, removed together at commit.
    pub pending_calls: HashMap<ApprovalToken, ToolCall>,
    /// Human decisions that have arrived for a prepared token but not yet
    /// been redeemed by `commit`. First decision wins.
    pub decisions: HashMap<ApprovalToken, GateOutcome>,
}

impl<K: Sink, R: Runner> WaslState<K, R> {
    pub fn new(audit: AuditLog<K>, http: reqwest::Client, runner: R) -> Self {
        Self {
            audit,
            http,
            runner,
            seq: 0,
            ledger: GateLedger::new(),
            pending_calls: HashMap::new(),
            decisions: HashMap::new(),
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
    /// The configured gate authority (which tier(s) may decide a call).
    authority: GateAuthority,
    /// The operator surface, if `[operator] bind` is configured. `Some` is
    /// the only source of `ClientCaps::external = true` (`detect_caps`).
    hub: Option<OperatorHub>,
}

impl<K, R> WaslMcp<K, R>
where
    K: Sink + Send + Sync + 'static,
    R: Runner + Send + Sync + 'static,
{
    /// `new(..) == with_gating(.., GateAuthority::Elicitation, None)` — the
    /// pre-W4 constructor, preserved byte-for-byte for backward compatibility.
    pub fn new(
        self_id: AgentId,
        self_host: HostId,
        registry: PeerRegistry,
        state: WaslState<K, R>,
    ) -> Self {
        Self::with_gating(
            self_id,
            self_host,
            registry,
            state,
            GateAuthority::default(),
            None,
        )
    }

    /// The full W4 constructor: a configured [`GateAuthority`] and an
    /// optional attached [`OperatorHub`].
    pub fn with_gating(
        self_id: AgentId,
        self_host: HostId,
        registry: PeerRegistry,
        state: WaslState<K, R>,
        authority: GateAuthority,
        hub: Option<OperatorHub>,
    ) -> Self {
        Self {
            self_id,
            self_host,
            registry: Arc::new(registry),
            state: Arc::new(Mutex::new(state)),
            tool_router: Self::tool_router(),
            authority,
            hub,
        }
    }

    /// Tier-1: surface the gate via MCP `elicitation/create`. Mirrored
    /// observe-only on the operator feed when a hub is attached — the
    /// mirrored gate is never decidable (`GateRequested{decidable: false}`;
    /// never enters `/v1/gates`; a `POST …/decision` on it 404s) — the human
    /// decision comes ONLY from the elicitation response
    /// (`docs/specs/mcp-gating.md` §4).
    async fn surface_elicitation(
        &self,
        peer: &Peer<RoleServer>,
        kind: GateKind,
        targets: &[String],
        preview: String,
    ) -> GateOutcome {
        let req = build_request(
            "gate".to_string(),
            kind,
            self.self_id.clone(),
            targets,
            preview,
        );

        // Broadcast the mirror BEFORE the elicitation goes out (per spec).
        let gate_id = self
            .hub
            .as_ref()
            .map(|hub| hub.announce(GateMode::Elicitation, &req));

        let message = wasl_gate::elicitation_for(&req).message;
        let schema = ElicitationSchema::builder()
            .build()
            .unwrap_or_else(|_| ElicitationSchema::builder().build_unchecked());
        let param = CreateElicitationRequestParam {
            message,
            requested_schema: schema,
        };

        // Type conversion only (`proto_action`); the tested core
        // (`wasl_gate::outcome_of`) owns the accept/decline/cancel SEMANTICS —
        // the dedup this block requires (`docs/specs/mcp-gating.md` §6).
        let outcome = match peer.create_elicitation(param).await {
            Ok(result) => outcome_of(&ElicitationResponse {
                action: proto_action(result.action),
            }),
            Err(err) => GateOutcome::Denied {
                reason: format!("elicitation transport error: {err}"),
            },
        };

        // Mirror the resolution for EVERY outcome, including a transport
        // error deny.
        if let (Some(hub), Some(gate_id)) = (&self.hub, &gate_id) {
            hub.resolve_announced(gate_id, &outcome, DecidedBy::Elicitation);
        }

        outcome
    }

    /// Tier-3: surface the gate to the operator and block for a decision.
    /// `External` is only ever selected when `caps.external == true`, and
    /// `caps.external` is exactly `self.hub.is_some()` by construction
    /// (`detect_caps`) — so `self.hub` is always `Some` here; `External` with
    /// `hub == None` is unreachable.
    ///
    /// The surfacer blocks its thread for up to the hub's `decision_timeout`
    /// (`OperatorSurfacer`'s normative embedder contract), so it MUST run on
    /// the blocking pool, never a plain async worker: a fresh surfacer is
    /// built per gate inside `spawn_blocking`.
    async fn surface_external(
        &self,
        kind: GateKind,
        targets: &[String],
        preview: String,
    ) -> GateOutcome {
        let req = build_request(
            "gate".to_string(),
            kind,
            self.self_id.clone(),
            targets,
            preview,
        );
        let hub = self
            .hub
            .clone()
            .expect("GateMode::External is unreachable without an attached hub (see detect_caps)");
        tokio::task::spawn_blocking(move || {
            let mut surfacer = OperatorSurfacer::new(hub);
            surfacer.surface(GateMode::External, &req)
        })
        .await
        // A panicked/cancelled waiter never approves.
        .unwrap_or(GateOutcome::Dismissed)
    }

    /// Tier-2: prepare the ledger entry, remember the call, and (if an
    /// operator is bound) spawn the decision waiter. Returns the pinned
    /// `Pending` reply WITHOUT calling `resolve_and_dispatch` — nothing is
    /// audited at prepare (`docs/specs/mcp-gating.md` §5).
    async fn prepare_call(
        &self,
        kind: GateKind,
        targets: Vec<String>,
        preview: String,
        make_call: impl FnOnce() -> ToolCall,
    ) -> CallToolResult {
        let (token, req) = {
            let mut state = self.state.lock().await;
            let seq = state.next_seq();
            let token = ApprovalToken(format!("tok-mcp-{seq}"));
            let req = build_request(
                token.0.clone(),
                kind,
                self.self_id.clone(),
                &targets,
                preview,
            );
            // Lockstep: `ledger.prepare` and `pending_calls.insert` are
            // inserted (and later removed, in `commit`) together.
            state.ledger.prepare(token.clone(), req.clone());
            state.pending_calls.insert(token.clone(), make_call());
            (token, req)
        };

        if let Some(hub) = &self.hub {
            // Decidable on the operator surface: open the gate, then spawn a
            // background waiter so this call returns `Pending` immediately.
            let w = hub.open_gate(GateMode::PrepareCommit, &req);
            let hub2 = hub.clone();
            let state2 = self.state.clone();
            let tok2 = token.clone();
            tokio::spawn(async move {
                let outcome = tokio::task::spawn_blocking(move || hub2.wait(w))
                    .await
                    .unwrap_or(GateOutcome::Dismissed);
                // First decision wins.
                state2.lock().await.decisions.entry(tok2).or_insert(outcome);
            });
        }
        // If no operator is bound, approval lands only via `resolve_pending`
        // (the embedder API) — Tier-2 waits are then unbounded.

        pending_result(&token)
    }

    /// The embedder's decision inlet for a prepared token, used when no
    /// operator surface is bound (`docs/specs/mcp-gating.md` §5). First
    /// decision wins.
    pub async fn resolve_pending(
        &self,
        token: &ApprovalToken,
        outcome: GateOutcome,
    ) -> Result<(), GateError> {
        let mut state = self.state.lock().await;
        if state.ledger.is_pending(token) {
            if state.decisions.contains_key(token) {
                return Err(GateError::AlreadyResolved);
            }
            state.decisions.insert(token.clone(), outcome);
            Ok(())
        } else {
            Err(classify_not_pending(&mut state.ledger, token))
        }
    }

    /// Run the full per-call flow: detect the REAL client caps, select the
    /// tier for THIS call, surface (or defer) the gate, then audit + dispatch
    /// via the sync heart and render the reply. Shared by the four
    /// non-`commit` tools.
    async fn gated_call(
        &self,
        peer: Peer<RoleServer>,
        kind: GateKind,
        target_strs: Vec<String>,
        preview: String,
        make_call: impl FnOnce() -> ToolCall,
    ) -> Result<CallToolResult, McpError> {
        let caps = detect_caps(peer.peer_info(), self.hub.is_some());

        let outcome = match select_mode(self.authority, &caps) {
            None => GateOutcome::Denied {
                reason: "client cannot be prompted (no elicitation capability)".to_string(),
            },
            Some(GateMode::Elicitation) => {
                self.surface_elicitation(&peer, kind, &target_strs, preview)
                    .await
            }
            Some(GateMode::External) => self.surface_external(kind, &target_strs, preview).await,
            Some(GateMode::PrepareCommit) => {
                return Ok(self
                    .prepare_call(kind, target_strs, preview, make_call)
                    .await);
            }
        };

        self.finish_call(&outcome, make_call()).await
    }

    /// Record `outcome` and (only on approval) dispatch `call`, then render
    /// the reply — the tail shared by every non-deferred tier AND by
    /// `commit`'s freshly-decided path.
    async fn finish_call(
        &self,
        outcome: &GateOutcome,
        call: ToolCall,
    ) -> Result<CallToolResult, McpError> {
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

        let reply = resolve_and_dispatch(&ctx, outcome, call, &base_id, seq, &http, runner, audit)
            .await
            .map_err(handle_error_to_mcp)?;

        Ok(reply_to_result(reply))
    }
}

/// Classify a `commit`/`resolve_pending` on a token that is neither
/// freshly-decided nor still pending: probe a commit with a throwaway
/// `Approve` decision — side-effect-free on a non-pending token, because
/// `GateLedger::commit` only ever succeeds when the token IS still pending —
/// and return its error.
fn classify_not_pending(ledger: &mut GateLedger, token: &ApprovalToken) -> GateError {
    match ledger.commit(token, GateDecision::Approve) {
        Err(err) => err,
        // Verified-unreachable: this helper is only called when
        // `ledger.is_pending(token) == false`, and `GateLedger::commit`
        // succeeds only when the token IS still pending — the two can never
        // both hold. Documented (not gamed), the `wasl-filesink` precedent
        // for a structurally-unreachable branch (see its AGENTS.md).
        Ok(_) => unreachable!("classify_not_pending: token unexpectedly still pending"),
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

    #[tool(
        description = "Redeem a prepare token: report the human decision for a pending gated \
                       call, dispatching it if approved"
    )]
    async fn commit(
        &self,
        Parameters(args): Parameters<CommitArgs>,
        _peer: Peer<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let token = ApprovalToken(args.token);
        let mut state = self.state.lock().await;

        if let Some(outcome) = state.decisions.remove(&token) {
            // The ledger needs SOME decision to retire the token; the AUDIT
            // decision (recorded below, inside `finish_call`) is
            // `outcome.to_decision()`, which correctly records `None` for a
            // dismissal.
            state
                .ledger
                .commit(&token, ledger_decision_of(&outcome))
                .expect(
                    "ledger in lockstep with decisions: a decided token is always still pending",
                );
            let call = state
                .pending_calls
                .remove(&token)
                .expect("pending_calls in lockstep with the ledger");
            drop(state);
            // The ONE audit record for this gate is written here, at commit —
            // not at prepare. No `resolve_announced`: when the operator is on,
            // the hub already broadcast `GateDecided` at POST/timeout time.
            return self.finish_call(&outcome, call).await;
        }

        if state.ledger.is_pending(&token) {
            // The human has not decided yet: idempotent poll.
            return Ok(pending_result(&token));
        }

        let err = classify_not_pending(&mut state.ledger, &token);
        Ok(reply_to_result(ToolReply::Blocked {
            reason: err.to_string(),
        }))
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
                 files. Every call is gated by a permission-before-send tier (elicitation, an \
                 operator, or prepare/commit) and audited; a `Pending` reply carries a token to \
                 redeem later with the `commit` tool."
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

/// The pinned Tier-2 "pending" JSON body (exact field order):
/// `{"status":"pending","token":"tok-mcp-0","hint":"…"}`
/// (`docs/specs/mcp-gating.md` §5). A SUCCESS content block — Tier-2 deferral
/// is not an error, the agent's next move (call `commit`) is well-defined.
#[derive(Serialize)]
struct PendingBody<'a> {
    status: &'static str,
    token: &'a str,
    hint: &'static str,
}

fn pending_result(token: &ApprovalToken) -> CallToolResult {
    json_ok(&PendingBody {
        status: "pending",
        token: token.0.as_str(),
        hint: "awaiting human approval; call the commit tool with this token",
    })
}

/// Render a [`ToolReply`] into an MCP tool result: a JSON content block for the
/// dispatched results, an error-content result for a block, or the pinned
/// success content block for a Tier-2 deferral.
fn reply_to_result(reply: ToolReply) -> CallToolResult {
    match reply {
        ToolReply::Consult(r) => json_ok(&r),
        ToolReply::Command(r) => json_ok(&r),
        ToolReply::Message(r) => json_ok(&r),
        ToolReply::Transfer(r) => json_ok(&r),
        ToolReply::Blocked { reason } => {
            CallToolResult::error(vec![Content::text(format!("blocked: {reason}"))])
        }
        ToolReply::Pending { token } => pending_result(&token),
    }
}

fn json_ok<T: serde::Serialize>(value: &T) -> CallToolResult {
    match serde_json::to_string(value) {
        Ok(json) => CallToolResult::success(vec![Content::text(json)]),
        Err(err) => CallToolResult::error(vec![Content::text(format!("serialize error: {err}"))]),
    }
}
