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

use wasl_audit::{AuditEntry, AuditError, AuditLog, Sink};
use wasl_filesink::FileSink;
use wasl_operator::{HubConfig, NodeInfo, OperatorHub, TeeSink};
use wasl_procrunner::ProcessRunner;

pub use config::{
    build_adapter, registry_from_peers, A2aConfig, AdapterConfig, AdapterKind, AuditConfig, Config,
    ConfigError, EnvOverrides, GateConfig, IdentityConfig, NodeAdapter, OperatorConfig, PeerEntry,
};
pub use gating::{detect_caps, ledger_decision_of, proto_action, select_mode, GateAuthority};
pub use handle::{resolve_and_dispatch, HandleError, ToolCall, ToolCtx, ToolReply};
pub use server::{WaslMcp, WaslState};

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

        // ── A2A RECEIVE wiring — IO SHELL, UNTESTED HERE ──────────────────────
        //
        // This composes only already-tested pieces (`wasl_node::receiver` →
        // `A2aServer` over a real `AdapterExecutor` wrapping the config-selected
        // `NodeAdapter` — `ClaudeAdapter`, `CodexAdapter` (W7), or, since
        // W8'b, `AntigravityAdapter`), but the LIVE serve loop and driving a
        // REAL `claude`/`codex`/`agy` CLI are NOT exercised by any test in
        // this crate (none of the three binaries is in the build env and the
        // socket serve loop is never entered under test).
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
                    let adapter = build_adapter(&cfg.adapter);
                    let server = std::sync::Arc::new(wasl_node::receiver(card, adapter));
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
