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
use std::sync::atomic::{AtomicU64, Ordering};
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

/// `list_peers` tool arguments — none (the whole roster is returned).
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
pub struct ListPeersArgs {}

/// One entry in the `list_peers` reply — the human-and-machine roster the inner
/// agent uses to map a name (e.g. `"Writer"`) to the `agent_id` the send tools
/// (`consult`/`command`/`message`) actually address. Serialized as the tool's
/// JSON content.
#[derive(Debug, Clone, Serialize)]
pub struct PeerListEntry {
    /// The peer's USER-facing display name (empty when unknown).
    pub name: String,
    /// The peer's opaque host id (e.g. `"host-16"`).
    pub host: String,
    /// The peer's USER-facing host display name (empty when unknown).
    pub host_name: String,
    /// The peer's opaque agent id — what the send tools address.
    pub agent_id: String,
    /// The peer's A2A endpoint URL, if known.
    pub url: Option<String>,
}

/// The full `list_peers` reply: the peer roster plus this node's own identity so
/// the inner agent knows who IT is by name too.
#[derive(Debug, Clone, Serialize)]
pub struct PeerListReply {
    pub peers: Vec<PeerListEntry>,
    #[serde(rename = "self")]
    pub self_identity: SelfIdentity,
}

/// This node's own identity in the `list_peers` reply.
#[derive(Debug, Clone, Serialize)]
pub struct SelfIdentity {
    pub agent_id: String,
    pub host: String,
    pub host_name: String,
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
// 1.5 TokenIds — Tier-2 approval-token minting (unguessable in production)
// ---------------------------------------------------------------------
//
// A Tier-2 approval token is a bearer credential: whoever presents it to
// `commit` redeems the human's decision, and `WaslMcp` is `Clone` over a
// shared `Arc<Mutex<WaslState>>`, so nothing else stops a second session
// from calling `commit` with a token it merely guessed. The old
// `ApprovalToken(format!("tok-mcp-{seq}"))` scheme minted them off the same
// monotonic counter `finish_call` uses for audit/envelope ids — sequential
// and therefore enumerable (see `docs/specs/mcp-gating.md` §5, now
// superseded by this seam). `TokenIds` (mirrors `wasl_operator::GateIds`)
// decouples "how a token is minted" from the rest of the flow so production
// can use an unguessable source while tests stay deterministic.

/// Injectable source of Tier-2 approval-token ids. Production MUST use an
/// unguessable id ([`RandomTokenIds`], the default via [`WaslMcp::new`] /
/// [`WaslMcp::with_gating`]); a deterministic sequence ([`CounterTokenIds`])
/// is for tests only, via [`WaslMcp::with_seams`].
pub trait TokenIds: Send + Sync {
    fn next(&self) -> String;
}

/// Draw a `u64` straight from the operating system's cryptographically secure
/// pseudo-random number generator (OS CSPRNG) via the `getrandom` crate: eight
/// bytes of OS entropy assembled little-endian. This is genuine cryptographic
/// randomness — the output is unpredictable to any external observer and has no
/// derivable relationship to any other value this process has drawn.
///
/// `getrandom` is fail-closed: if the OS CSPRNG is unavailable this `.expect`s
/// and aborts token minting rather than falling back to a weaker source. A
/// machine that cannot supply cryptographic entropy must not mint Tier-2
/// approval tokens at all (a bearer credential minted from a degraded source
/// would be a silent security regression), so failing loud here is deliberate.
fn random_u64() -> u64 {
    let mut buf = [0u8; 8];
    getrandom::getrandom(&mut buf).expect("OS CSPRNG (getrandom) must not fail");
    u64::from_le_bytes(buf)
}

/// Production default: a 128-bit opaque, unguessable token drawn from the OS
/// CSPRNG (two independent [`random_u64`] draws via `getrandom`, rendered
/// lowercase hex) — no sequential counter, no cross-session predictability, and
/// no reliance on wall-clock timing. 128 bits of OS-provided cryptographic
/// randomness make the token infeasible to guess.
#[derive(Debug, Default, Clone, Copy)]
pub struct RandomTokenIds;

impl TokenIds for RandomTokenIds {
    fn next(&self) -> String {
        format!("tok-mcp-{:016x}{:016x}", random_u64(), random_u64())
    }
}

/// Deterministic ids for tests: `"tok-mcp-0"`, `"tok-mcp-1"`, ... (the
/// pre-fix sequence). NEVER use in production — the whole point of this
/// block is that production tokens must not be predictable.
#[derive(Debug, Default)]
pub struct CounterTokenIds(AtomicU64);

impl TokenIds for CounterTokenIds {
    fn next(&self) -> String {
        let n = self.0.fetch_add(1, Ordering::Relaxed);
        format!("tok-mcp-{n}")
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
    /// Shared, runtime-mutable peer registry (D3). The SAME `Arc` handed to the
    /// runtime-control `/v1/peers` endpoint via [`WaslMcp::registry_handle`], so
    /// a peer Marsad adds/removes at runtime is immediately visible to the send
    /// path (`consult`/`command`/`message`), which reads a per-dispatch snapshot
    /// (see [`WaslMcp::registry_snapshot`]) — never holding the lock across an
    /// `.await`.
    registry: Arc<std::sync::RwLock<PeerRegistry>>,
    state: Arc<Mutex<WaslState<K, R>>>,
    tool_router: ToolRouter<Self>,
    /// The configured gate authority (which tier(s) may decide a call).
    authority: GateAuthority,
    /// The operator surface, if `[operator] bind` is configured. `Some` is
    /// the only source of `ClientCaps::external = true` (`detect_caps`).
    hub: Option<OperatorHub>,
    /// Tier-2 approval-token minting (`RandomTokenIds` in production; a
    /// deterministic sequence only under `with_seams`, for tests).
    token_ids: Arc<dyn TokenIds>,
    /// The USER-facing display name of THIS node's host (from config
    /// `identity.host_name`). Empty when unknown. Surfaced as the self identity
    /// in `list_peers`. Cheaply cloneable `Arc<str>`.
    self_host_name: Arc<str>,
    /// USER-facing peer names keyed by `agent_id` (config `peers[].name` /
    /// `peers[].host_name`), the human-name side-table `list_peers` returns and
    /// the send tools use to resolve a NAME alias to its `agent_id`. Behind an
    /// `Arc<RwLock<..>>` to mirror the runtime-mutable peer registry; wired at
    /// construction from config, additive to the routing registry.
    peer_names: Arc<std::sync::RwLock<std::collections::BTreeMap<String, crate::config::PeerName>>>,
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
    /// optional attached [`OperatorHub`]. Always mints unguessable Tier-2
    /// tokens ([`RandomTokenIds`]) — see [`WaslMcp::with_seams`] for the
    /// deterministic-for-tests variant.
    pub fn with_gating(
        self_id: AgentId,
        self_host: HostId,
        registry: PeerRegistry,
        state: WaslState<K, R>,
        authority: GateAuthority,
        hub: Option<OperatorHub>,
    ) -> Self {
        Self::with_seams(
            self_id,
            self_host,
            registry,
            state,
            authority,
            hub,
            Arc::new(RandomTokenIds),
        )
    }

    /// The full seam constructor: everything `with_gating` takes, plus an
    /// injectable [`TokenIds`] source. Production code should use
    /// [`WaslMcp::new`]/[`WaslMcp::with_gating`] (unguessable tokens by
    /// default); this exists so tests can pin deterministic token literals
    /// (`CounterTokenIds`) without weakening the production default.
    pub fn with_seams(
        self_id: AgentId,
        self_host: HostId,
        registry: PeerRegistry,
        state: WaslState<K, R>,
        authority: GateAuthority,
        hub: Option<OperatorHub>,
        token_ids: Arc<dyn TokenIds>,
    ) -> Self {
        Self {
            self_id,
            self_host,
            registry: Arc::new(std::sync::RwLock::new(registry)),
            state: Arc::new(Mutex::new(state)),
            tool_router: Self::tool_router(),
            authority,
            hub,
            token_ids,
            self_host_name: Arc::from(""),
            peer_names: Arc::new(std::sync::RwLock::new(std::collections::BTreeMap::new())),
        }
    }

    /// Attach the USER-facing name surface (Feature A): the peer name side-table
    /// and this node's own host display name, both from the wasl-mcp config.
    /// Builder-style (consumes + returns `self`) so it composes onto any of the
    /// constructors above without changing their signatures — every existing
    /// call site that does not care about names is unaffected (names default to
    /// empty). `run()`/`build_handler` calls this with the config-derived values
    /// so `list_peers` can report the roster by name.
    pub fn with_names(
        self,
        self_host_name: impl Into<String>,
        peer_names: std::collections::BTreeMap<String, crate::config::PeerName>,
    ) -> Self {
        let self_host_name: String = self_host_name.into();
        Self {
            self_host_name: Arc::from(self_host_name.as_str()),
            peer_names: Arc::new(std::sync::RwLock::new(peer_names)),
            ..self
        }
    }

    /// The shared peer-registry handle (D3). Register/unregister peers on this
    /// at runtime and the change is immediately visible to the send path — it is
    /// the SAME `Arc` this handler reads. The runtime-control `/v1/peers`
    /// endpoint is wired to this handle in `wasl_mcp::run`.
    pub fn registry_handle(&self) -> Arc<std::sync::RwLock<PeerRegistry>> {
        self.registry.clone()
    }

    /// A point-in-time clone of the peer registry — exactly what each dispatch
    /// reads to build its `ToolCtx`. Cloning (rather than borrowing the guard)
    /// keeps the `std::sync::RwLock` read lock from being held across the
    /// dispatch `.await`.
    pub fn registry_snapshot(&self) -> PeerRegistry {
        self.registry
            .read()
            .expect("peer registry lock poisoned")
            .clone()
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
            // Unguessable, not the sequential seq counter — see `TokenIds`.
            let token = ApprovalToken(self.token_ids.next());
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
        // Snapshot the shared registry (D3): a plain clone so no read lock is
        // held across the dispatch `.await` below; the snapshot reflects any
        // peer added/removed at runtime via `registry_handle`.
        let registry_snapshot = self.registry_snapshot();
        let ctx = ToolCtx {
            self_id: &self.self_id,
            self_host: &self.self_host,
            registry: &registry_snapshot,
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
        description = "Consult one or more peer agents with a question and collect their replies. \
                       Each entry in `peers` may be a peer's agent_id OR its display name (as \
                       shown by `list_peers`); names are resolved to agent_ids before sending."
    )]
    async fn consult(
        &self,
        Parameters(args): Parameters<ConsultArgs>,
        peer: Peer<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let mut params: ConsultParams = args.into();
        params.peers = self.resolve_peer_aliases(params.peers);
        let targets = peer_targets(&params.peers);
        let preview = truncate(&params.question);
        self.gated_call(peer, GateKind::Consult, targets, preview, || {
            ToolCall::Consult(params)
        })
        .await
    }

    #[tool(
        description = "Command one or more peer agents to run a task and collect their replies. \
                       Each entry in `peers` may be a peer's agent_id OR its display name (see \
                       `list_peers`); names are resolved to agent_ids before sending."
    )]
    async fn command(
        &self,
        Parameters(args): Parameters<CommandArgs>,
        peer: Peer<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let mut params: CommandParams = args.into();
        params.peers = self.resolve_peer_aliases(params.peers);
        let targets = peer_targets(&params.peers);
        let preview = truncate(&params.task);
        self.gated_call(peer, GateKind::Command, targets, preview, || {
            ToolCall::Command(params)
        })
        .await
    }

    #[tool(
        description = "Send a one-way message to one or more peer agents. Each entry in `peers` \
                       may be a peer's agent_id OR its display name (see `list_peers`); names are \
                       resolved to agent_ids before sending."
    )]
    async fn message(
        &self,
        Parameters(args): Parameters<MessageArgs>,
        peer: Peer<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let mut params: MessageParams = args.into();
        params.peers = self.resolve_peer_aliases(params.peers);
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

        // `decisions.remove` is the single-winner gate: at most one
        // concurrent `commit` call can walk away with `Some(outcome)` for a
        // given token (a second caller sees `None` below and, since the
        // ledger is still pending until this call finishes, gets the
        // idempotent poll reply instead of racing the dispatch).
        let outcome = match state.decisions.remove(&token) {
            Some(outcome) => outcome,
            None => {
                if state.ledger.is_pending(&token) {
                    // The human has not decided yet: idempotent poll.
                    return Ok(pending_result(&token));
                }
                let err = classify_not_pending(&mut state.ledger, &token);
                return Ok(reply_to_result(ToolReply::Blocked {
                    reason: err.to_string(),
                }));
            }
        };
        // Durability-first, like every other tier: do NOT touch the ledger
        // or `pending_calls` yet. `finish_call` -> `resolve_and_dispatch`
        // records the audit entry BEFORE it dispatches anything, and an
        // audit-sink failure fails the whole call (handle.rs's own
        // contract) — if we had already consumed the ledger token here (the
        // old behavior), that failure would strand the human's approval:
        // the token could never be re-committed, and the decision would
        // vanish with no audit trace. Peeking the remembered call (cheap —
        // `ToolCall` is `Clone`) instead of removing it keeps the token
        // re-committable across a transient failure.
        let call = state
            .pending_calls
            .get(&token)
            .expect("pending_calls in lockstep with decisions: a decided token's call is still remembered")
            .clone();
        drop(state);

        // The ONE audit record for this gate is written here, at commit —
        // not at prepare. No `resolve_announced`: when the operator is on,
        // the hub already broadcast `GateDecided` at POST/timeout time.
        match self.finish_call(&outcome, call).await {
            Ok(reply) => {
                // The audit record is durably written (and dispatch, if
                // approved, already ran) — NOW it is safe to irreversibly
                // retire the token.
                let mut state = self.state.lock().await;
                state.pending_calls.remove(&token);
                state
                    .ledger
                    .commit(&token, ledger_decision_of(&outcome))
                    .expect(
                    "ledger in lockstep with decisions: a decided token is always still pending",
                );
                Ok(reply)
            }
            Err(err) => {
                // The audit write failed before anything was dispatched
                // (`resolve_and_dispatch` is durability-first) — restore the
                // decision so the human's approval is not lost: the ledger
                // token and `pending_calls` entry are still exactly as they
                // were before this call, so a retry of `commit` (once the
                // transient sink failure clears) redeems it exactly once.
                // `or_insert` (not a plain insert) preserves "first decision
                // wins" if a fresh decision somehow landed in the meantime
                // (the same idiom `prepare_call`'s background waiter uses).
                let mut state = self.state.lock().await;
                state.decisions.entry(token).or_insert(outcome);
                Err(err)
            }
        }
    }

    #[tool(
        description = "List the A2A peer agents this node can reach — each with its display name, \
                       agent_id, host, host display name, and endpoint — plus this node's own \
                       identity. Use it to map a peer's human name (e.g. \"Writer\") to the \
                       agent_id that consult/command/message address."
    )]
    async fn list_peers(
        &self,
        Parameters(_args): Parameters<ListPeersArgs>,
        _peer: Peer<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        // Join the runtime-mutable routing registry (host/endpoint/agent_id) with
        // the config-delivered name side-table (name/host_name). The registry is
        // the authoritative set of reachable peers; a name is attached when known.
        let registry = self.registry_snapshot();
        let names = self.peer_names.read().expect("peer_names lock poisoned");
        let mut peers: Vec<PeerListEntry> = registry
            .list()
            .into_iter()
            .map(|card| {
                let n = names.get(card.id.as_str()).cloned().unwrap_or_default();
                PeerListEntry {
                    name: n.name,
                    host: card.host.as_str().to_string(),
                    host_name: n.host_name,
                    agent_id: card.id.as_str().to_string(),
                    url: card.endpoint.clone(),
                }
            })
            .collect();
        // Deterministic order (registry.list already sorts by id, but be explicit).
        peers.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
        drop(names);

        let reply = PeerListReply {
            peers,
            self_identity: SelfIdentity {
                agent_id: self.self_id.as_str().to_string(),
                host: self.self_host.as_str().to_string(),
                host_name: self.self_host_name.to_string(),
            },
        };
        let json = serde_json::to_string(&reply)
            .unwrap_or_else(|_| "{\"peers\":[],\"self\":{}}".to_string());
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }
}

impl<K, R> WaslMcp<K, R>
where
    K: Sink + Send + Sync + 'static,
    R: Runner + Send + Sync + 'static,
{
    /// Resolve each incoming peer identifier to a routable `agent_id`: if the
    /// identifier matches a KNOWN peer's display name (from the config name
    /// side-table) AND is not itself already a known agent_id, rewrite it to that
    /// peer's agent_id. An identifier that is already an agent_id, or matches no
    /// name, passes through verbatim (the registry then decides reachability, so
    /// an unknown target still surfaces the existing "unknown peer" outcome — no
    /// silent drop). Case-sensitive exact match. Pure over a name-table snapshot.
    fn resolve_peer_aliases(&self, peers: Vec<AgentId>) -> Vec<AgentId> {
        let names = self.peer_names.read().expect("peer_names lock poisoned");
        if names.is_empty() {
            return peers;
        }
        // Build name -> agent_id once (skip empty names). A name that collides
        // with a real agent_id never shadows it (the agent_id passthrough wins).
        let mut by_name: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
        for (agent_id, pn) in names.iter() {
            if !pn.name.is_empty() {
                by_name.entry(pn.name.as_str()).or_insert(agent_id.as_str());
            }
        }
        peers
            .into_iter()
            .map(|p| {
                // Already a known agent_id → keep as-is (agent_id wins over name).
                if names.contains_key(p.as_str()) {
                    return p;
                }
                match by_name.get(p.as_str()) {
                    Some(agent_id) => AgentId((*agent_id).to_string()),
                    None => p,
                }
            })
            .collect()
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
                 files. Call `list_peers` first to see the reachable peers by their display \
                 names and map a name to its agent_id (consult/command/message also accept a \
                 name directly and resolve it). Every call is gated by a permission-before-send \
                 tier (elicitation, an operator, or prepare/commit) and audited; a `Pending` \
                 reply carries a token to redeem later with the `commit` tool."
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
