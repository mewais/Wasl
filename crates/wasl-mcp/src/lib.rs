//! wasl-mcp — MCP server facade for Wasl (the IO shell over the tested cores).
//!
//! Exposes the four Wasl agent-to-agent tools (consult / command / message /
//! transfer) plus the Tier-2 `commit` redemption tool as MCP tools. Each call
//! runs the real three-tier permission-before-send gate (`crate::gating`
//! picks Tier-1 elicitation / Tier-3 external / Tier-2 prepare-commit per the
//! configured [`GateAuthority`] and the REAL client capabilities, reusing the
//! tested `wasl_gate` logic) and, only on approval, dispatches over REAL
//! async A2A (`wasl_dispatch` for the messaging verbs; `wasl_transfer` for
//! transfer), recording every decision to `wasl_audit` (and, when an operator
//! is bound, mirroring it to `/v1/events`). Transport/runtime is this crate's
//! concern; all decision + routing logic is the tested cores.
//! See `docs/specs/mcp-gating.md` (current) and `docs/specs/mcp-a2a.md`.

pub mod config;
pub mod gating;
pub mod handle;
pub mod server;

// Needed for the managed-runtime / runtime-control wiring in `run()`.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use wasl_runtime::{ManagedConfig, MonotonicClock, ProtocolRoutingProvider, RuntimeProvider,
    WaslManagedRuntimeProvider};
use wasl_runtime_protocol::{ClaudeStreamProtocol, OpenCodeAcpProtocol};

use wasl_audit::{AuditEntry, AuditError, AuditLog, Sink};
use wasl_filesink::FileSink;
use wasl_operator::{HubConfig, NodeInfo, OperatorHub, TeeSink};
use wasl_procrunner::ProcessRunner;

pub use config::{
    build_adapter, registry_from_peers, A2aConfig, AdapterConfig, AdapterKind, AuditConfig, Config,
    ConfigError, EnvOverrides, GateConfig, IdentityConfig, NodeAdapter, OperatorConfig, PeerEntry,
    RuntimeConfig, RuntimeKind,
};
pub use gating::{detect_caps, ledger_decision_of, proto_action, select_mode, GateAuthority};
pub use handle::{resolve_and_dispatch, HandleError, ToolCall, ToolCtx, ToolReply};
pub use server::{CounterTokenIds, RandomTokenIds, TokenIds, WaslMcp, WaslState};

// ---------------------------------------------------------------------
// 1. Default IO seams for `run()`
// ---------------------------------------------------------------------

/// An in-memory audit sink: keeps every recorded entry. Simple and infallible —
/// used by the tests; `run()` uses the durable `wasl_filesink::FileSink`.
#[derive(Debug, Default, Clone)]
pub struct MemSink {
    pub entries: Vec<AuditEntry>,
}

impl MemSink {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Sink for MemSink {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
        self.entries.push(entry.clone());
        Ok(())
    }
}

/// The concrete audit sink `run()` uses: the durable `FileSink` normally, or an
/// in-memory fallback if the audit file can't be opened. An enum (not a boxed
/// trait object) so it satisfies `K: Sink` with one concrete type.
pub enum DefaultSink {
    File(FileSink),
    Mem(MemSink),
}

impl Sink for DefaultSink {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
        match self {
            DefaultSink::File(sink) => sink.append(entry),
            DefaultSink::Mem(sink) => sink.append(entry),
        }
    }
}

/// The concrete sink `run()`'s handler is built over: the plain [`DefaultSink`]
/// when no operator is bound, or [`TeeSink`]-wrapped (durable append first,
/// then a broadcast `Audit` frame fan-out to `/v1/events`) when one is. One
/// concrete enum (not a boxed trait object) so `build_handler` stays a single
/// `WaslMcp<RunSink, ProcessRunner>` type regardless of `[operator] bind`.
pub enum RunSink {
    Plain(DefaultSink),
    Teed(TeeSink<DefaultSink>),
}

impl Sink for RunSink {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
        match self {
            RunSink::Plain(sink) => sink.append(entry),
            RunSink::Teed(tee) => tee.append(entry),
        }
    }
}

// ---------------------------------------------------------------------
// 1b. build_managed_provider — IO SHELL, UNTESTED HERE
// ---------------------------------------------------------------------

/// Construct a per-protocol [`WaslManagedRuntimeProvider`] for each of the
/// configured protocols (`"claude"`, `"opencode-acp"`) and wrap them in a
/// [`ProtocolRoutingProvider`] so one Wasl node can serve both protocols.
///
/// The clock is [`MonotonicClock`] (the same default the node uses throughout).
/// The audit sink is a fresh [`MemSink`] — a non-durable in-memory sink, which
/// is correct here: the managed provider's actor uses the sink only for
/// runtime-layer audit entries; the durable per-session audit trail is
/// separately written by the MCP handler via the `AuditLog`/`FileSink` already
/// built in [`build_handler`]. A shared durable sink could be wired in a
/// future phase; for now the default matches the crate's existing pattern.
fn build_managed_provider(cfg: &Config) -> Arc<dyn RuntimeProvider> {
    let clock: Arc<dyn wasl_runtime::RuntimeClock> = Arc::new(MonotonicClock);
    // Each sub-provider gets its own sink handle (the actor's sole writer).
    let claude_bin = cfg
        .runtime
        .claude_bin
        .clone()
        .unwrap_or_else(|| "claude".to_string());
    let opencode_bin = cfg
        .runtime
        .opencode_bin
        .clone()
        .unwrap_or_else(|| "opencode".to_string());

    let claude_sink: Arc<Mutex<dyn wasl_audit::Sink + Send>> =
        Arc::new(Mutex::new(MemSink::new()));
    let opencode_sink: Arc<Mutex<dyn wasl_audit::Sink + Send>> =
        Arc::new(Mutex::new(MemSink::new()));

    let claude_provider: Arc<dyn RuntimeProvider> = Arc::new(
        WaslManagedRuntimeProvider::with_seams(
            ClaudeStreamProtocol::new(claude_bin),
            ManagedConfig::default(),
            clock.clone(),
            claude_sink,
        ),
    );
    let opencode_provider: Arc<dyn RuntimeProvider> = Arc::new(
        WaslManagedRuntimeProvider::with_seams(
            OpenCodeAcpProtocol::new(opencode_bin),
            ManagedConfig::default(),
            clock,
            opencode_sink,
        ),
    );

    let mut providers = BTreeMap::new();
    providers.insert("claude".to_string(), claude_provider);
    providers.insert("opencode-acp".to_string(), opencode_provider);

    Arc::new(ProtocolRoutingProvider::new(
        providers,
        cfg.runtime.default_protocol.clone(),
    ))
}

// ---------------------------------------------------------------------
// 2. build_handler
// ---------------------------------------------------------------------

/// Open the durable audit sink at `path`, falling back to an in-memory sink
/// (LOUDLY, on stderr) if the file cannot be opened — the server still
/// starts, but the operator is told the trail is not durable rather than it
/// being dropped silently.
fn open_audit(path: &str) -> DefaultSink {
    match FileSink::open(path) {
        Ok(sink) => DefaultSink::File(sink),
        Err(err) => {
            eprintln!(
                "wasl-mcpd: WARNING: could not open durable audit log at {path:?}: {err}. \
                 Falling back to a NON-DURABLE in-memory audit sink for this run."
            );
            DefaultSink::Mem(MemSink::new())
        }
    }
}

/// Build the default handler wired to the durable/real seams: identity +
/// peers + audit path from `cfg`, a `ProcessRunner` for transfer, a shared
/// `reqwest::Client` for A2A, the configured [`GateAuthority`], and — when
/// `hub` is `Some` (i.e. `[operator] bind` was set and parsed) — the durable
/// sink teed to the operator's `/v1/events` feed.
fn build_handler(cfg: &Config, hub: Option<OperatorHub>) -> WaslMcp<RunSink, ProcessRunner> {
    let self_id = wasl_proto::AgentId(cfg.identity.agent_id.clone());
    let self_host = wasl_proto::HostId(cfg.identity.host_id.clone());
    let registry = registry_from_peers(&cfg.peers);
    let sink = open_audit(&cfg.audit.path);
    let run_sink = match &hub {
        Some(hub) => RunSink::Teed(TeeSink::new(sink, hub.clone())),
        None => RunSink::Plain(sink),
    };
    let audit = AuditLog::new(run_sink);
    let http = reqwest::Client::new();
    let state = WaslState::new(audit, http, ProcessRunner::new());
    WaslMcp::with_gating(self_id, self_host, registry, state, cfg.gate.authority, hub)
}

// ---------------------------------------------------------------------
// 3. run()
// ---------------------------------------------------------------------

/// Run the stdio MCP server (blocking until the transport closes).
///
/// Loads the config (`Config::load()`) FIRST and fails loud — before any IO
/// starts, in particular before the stdio MCP transport would ever block on
/// stdin — if it is invalid: prints `wasl-mcpd: config error: {e}` to stderr
/// and exits with status 2.
///
/// On success: builds the handler (identity/registry/audit path from the
/// config, durable `FileSink` audit + `ProcessRunner` transfer + shared A2A
/// `reqwest::Client` + the configured gate authority), starts a tokio
/// runtime, serves over stdio, and blocks until the client disconnects.
///
/// If `cfg.a2a.bind` is set (post-env-overlay), it ALSO spawns this node's
/// A2A RECEIVE server concurrently (see the UNTESTED-HERE shell below). If
/// `cfg.operator.bind` is set, it ALSO spawns the operator surface
/// (`wasl_operator::serve`) and tees the audit sink to it — see the
/// UNTESTED-HERE shell just below the A2A one.
pub fn run() {
    let cfg = match Config::load() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("wasl-mcpd: config error: {err}");
            std::process::exit(2);
        }
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    runtime.block_on(async {
        use rmcp::transport::io::stdio;
        use rmcp::ServiceExt;

        // D1 (live-runtime-plan §3): under `runtime.kind = managed`, build the
        // managed provider ONCE and share it between the A2A receive server (so a
        // peer's task lands in the node's live managed session) and the
        // runtime-control hub below (so a session Marsad creates via
        // runtime-control is the SAME target the A2A receiver delivers into).
        let managed_provider: Option<std::sync::Arc<dyn wasl_runtime::RuntimeProvider>> =
            if matches!(cfg.runtime.kind, RuntimeKind::Managed) {
                Some(build_managed_provider(&cfg))
            } else {
                None
            };

        // ── A2A RECEIVE wiring — IO SHELL, UNTESTED HERE ──────────────────────
        //
        // This composes only already-tested pieces (`wasl_node::receiver` →
        // `A2aServer` over a real `AdapterExecutor` wrapping the config-selected
        // `NodeAdapter` — `ClaudeAdapter`, `CodexAdapter` (W7), since
        // W8'b `AntigravityAdapter`, or, since W9, `OpencodeAdapter`), but the
        // LIVE serve loop and driving a REAL `claude`/`codex`/`agy`/`opencode`
        // CLI are NOT exercised by any test in this crate (none of the four
        // binaries is in the build env and the socket serve loop is never
        // entered under test).
        // Compile-only shell.
        if let Some(bind) = cfg.a2a.bind.clone() {
            match bind.parse::<std::net::SocketAddr>() {
                Ok(addr) => {
                    let card = wasl_a2a_types::AgentCard {
                        name: cfg.identity.agent_id.clone(),
                        description: "Wasl node A2A receive endpoint".to_string(),
                        url: format!("http://{addr}/"),
                        version: env!("CARGO_PKG_VERSION").to_string(),
                        capabilities: wasl_a2a_types::AgentCapabilities {
                            streaming: false,
                            push_notifications: false,
                        },
                        default_input_modes: vec!["text/plain".to_string()],
                        default_output_modes: vec!["text/plain".to_string()],
                        skills: vec![],
                    };
                    // Managed mode → drive inbound A2A into the persistent managed
                    // session via the shared provider (D1); otherwise the one-shot
                    // adapter path.
                    let server = std::sync::Arc::new(match &managed_provider {
                        Some(provider) => {
                            wasl_node::receiver_runtime(card, std::sync::Arc::clone(provider))
                        }
                        None => wasl_node::receiver(card, build_adapter(&cfg.adapter)),
                    });
                    // Spawn the receive serve loop concurrently with the stdio MCP
                    // server. UNTESTED HERE: this future's body never runs in tests.
                    tokio::spawn(async move {
                        if let Err(err) = server.serve(addr).await {
                            eprintln!("wasl-mcpd: A2A receive server terminated: {err}");
                        }
                    });
                }
                Err(err) => {
                    eprintln!(
                        "wasl-mcpd: WARNING: WASL_A2A_BIND={bind:?} is not a valid socket \
                         address ({err}); A2A receive server not started."
                    );
                }
            }
        }
        // ── end A2A RECEIVE shell ─────────────────────────────────────────────

        // ── OPERATOR wiring — IO SHELL, UNTESTED HERE ──────────────────────────
        //
        // `wasl_operator::router` (the axum app this binds) IS loopback-tested
        // (`wasl-operator`'s own suite, plus `wasl-mcp`'s `gating_loopback.rs`
        // driving it end to end over a REAL bind). What is NOT exercised by any
        // test in this crate is THIS live `serve()` bind inside `run()` — same
        // compile-only-shell status as the A2A receive loop above.
        let hub = match cfg.operator.bind.clone() {
            Some(bind) => match bind.parse::<std::net::SocketAddr>() {
                Ok(addr) => {
                    let info = NodeInfo {
                        agent_id: wasl_proto::AgentId(cfg.identity.agent_id.clone()),
                        host_id: wasl_proto::HostId(cfg.identity.host_id.clone()),
                        version: env!("CARGO_PKG_VERSION").to_string(),
                    };
                    let hub = OperatorHub::new(info, HubConfig::default());
                    let serve_hub = hub.clone();
                    tokio::spawn(async move {
                        if let Err(err) = wasl_operator::serve(serve_hub, addr).await {
                            eprintln!("wasl-mcpd: operator server terminated: {err}");
                        }
                    });
                    Some(hub)
                }
                Err(err) => {
                    eprintln!(
                        "wasl-mcpd: WARNING: operator.bind={bind:?} is not a valid socket \
                         address ({err}); operator surface not started."
                    );
                    None
                }
            },
            None => None,
        };
        // ── end OPERATOR shell ──────────────────────────────────────────────────

        // ── RUNTIME-CONTROL wiring — IO SHELL, UNTESTED HERE ──────────────────
        //
        // When `runtime.kind = managed` AND `runtime.control_bind` is set, spin
        // up the runtime-control HTTP API (wasl_runtime_control::serve) backed
        // by a ProtocolRoutingProvider that delegates to per-protocol managed
        // WaslManagedRuntimeProvider instances. The serve body and the managed-
        // provider constructor are COMPILE-ONLY SHELLS (`todo!()` inside
        // `build_managed_provider`); the actual logic lands in Phase-2.
        if matches!(cfg.runtime.kind, RuntimeKind::Managed) {
            if let Some(bind) = cfg.runtime.control_bind.clone() {
                match bind.parse::<std::net::SocketAddr>() {
                    Ok(addr) => {
                        // Reuse the shared managed provider built above (D1) so the
                        // runtime-control API and the A2A receiver serve the SAME
                        // sessions. Guaranteed Some here (kind == Managed).
                        let provider = managed_provider
                            .clone()
                            .expect("managed provider built when runtime.kind = managed");
                        let hub = wasl_runtime_control::RuntimeControlHub::new(
                            provider,
                            cfg.identity.agent_id.clone(),
                            cfg.identity.host_id.clone(),
                            env!("CARGO_PKG_VERSION").to_string(),
                            wasl_runtime_control::RuntimeKind::Managed,
                        );
                        tokio::spawn(async move {
                            if let Err(err) = wasl_runtime_control::serve(hub, addr).await {
                                eprintln!(
                                    "wasl-mcpd: runtime-control server terminated: {err}"
                                );
                            }
                        });
                    }
                    Err(err) => {
                        eprintln!(
                            "wasl-mcpd: WARNING: runtime.control_bind={bind:?} is not a valid \
                             socket address ({err}); runtime-control API not started."
                        );
                    }
                }
            }
        }
        // ── end RUNTIME-CONTROL shell ─────────────────────────────────────────

        let handler = build_handler(&cfg, hub);
        match handler.serve(stdio()).await {
            Ok(running) => {
                if let Err(err) = running.waiting().await {
                    eprintln!("wasl-mcpd: server terminated with error: {err}");
                }
            }
            Err(err) => {
                eprintln!("wasl-mcpd: failed to start MCP server: {err}");
            }
        }
    });
}
