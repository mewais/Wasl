//! wasl-mcp — MCP server facade for Wasl (the IO shell over the tested cores).
//!
//! Exposes the four Wasl agent-to-agent tools (consult / command / message /
//! transfer) as MCP tools. Each call runs the permission-before-send gate
//! (surfaced to the client via MCP `elicitation/create`, reusing the tested
//! `wasl_gate` logic) and, only on approval, dispatches over REAL async A2A
//! (`wasl_dispatch` for the messaging verbs; `wasl_transfer` for transfer),
//! recording every decision to `wasl_audit`. Transport/runtime is this crate's
//! concern; all decision + routing logic is the tested cores.
//! See `docs/specs/mcp-a2a.md`.

pub mod config;
pub mod handle;
pub mod server;

use wasl_audit::{AuditEntry, AuditError, AuditLog, Sink};
use wasl_filesink::FileSink;
use wasl_procrunner::ProcessRunner;

pub use config::{
    build_adapter, registry_from_peers, A2aConfig, AdapterConfig, AdapterKind, AuditConfig, Config,
    ConfigError, EnvOverrides, IdentityConfig, OperatorConfig, PeerEntry,
};
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

// ---------------------------------------------------------------------
// 2. build_handler
// ---------------------------------------------------------------------

/// Open the durable audit sink at `path`, falling back to an in-memory sink
/// (LOUDLY, on stderr) if the file cannot be opened — the server still
/// starts, but the operator is told the trail is not durable rather than it
/// being dropped silently.
fn open_audit(path: &str) -> AuditLog<DefaultSink> {
    match FileSink::open(path) {
        Ok(sink) => AuditLog::new(DefaultSink::File(sink)),
        Err(err) => {
            eprintln!(
                "wasl-mcpd: WARNING: could not open durable audit log at {path:?}: {err}. \
                 Falling back to a NON-DURABLE in-memory audit sink for this run."
            );
            AuditLog::new(DefaultSink::Mem(MemSink::new()))
        }
    }
}

/// Build the default handler wired to the durable/real seams: identity +
/// peers + audit path from `cfg`, a `ProcessRunner` for transfer, and a
/// shared `reqwest::Client` for A2A.
fn build_handler(cfg: &Config) -> WaslMcp<DefaultSink, ProcessRunner> {
    let self_id = wasl_proto::AgentId(cfg.identity.agent_id.clone());
    let self_host = wasl_proto::HostId(cfg.identity.host_id.clone());
    let registry = registry_from_peers(&cfg.peers);
    let audit = open_audit(&cfg.audit.path);
    let http = reqwest::Client::new();
    let state = WaslState::new(audit, http, ProcessRunner::new());
    WaslMcp::new(self_id, self_host, registry, state)
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
/// `reqwest::Client`), starts a tokio runtime, serves over stdio, and blocks
/// until the client disconnects.
///
/// If `cfg.a2a.bind` is set (post-env-overlay), it ALSO spawns this node's
/// A2A RECEIVE server concurrently (see the UNTESTED-HERE shell below).
/// `cfg.operator` is read and validated at load time, then deliberately
/// ignored here — W3 consumes it.
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
        // `A2aServer` over a real `AdapterExecutor` wrapping `ClaudeAdapter`), but
        // the LIVE serve loop and driving a REAL `claude` CLI are NOT exercised by
        // any test in this crate (there is no `claude` binary in the build env and
        // the socket serve loop is never entered under test). Compile-only shell.
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

        let handler = build_handler(&cfg);
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
