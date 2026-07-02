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

pub mod handle;
pub mod server;

use wasl_audit::{AuditEntry, AuditError, AuditLog, Sink};
use wasl_filesink::FileSink;
use wasl_procrunner::ProcessRunner;
use wasl_proto::{AgentId, HostId};

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
// 2. Config
// ---------------------------------------------------------------------

/// Self identity for the server, read from env (`WASL_AGENT_ID`, `WASL_HOST_ID`)
/// with sensible defaults so the server always starts.
fn self_identity() -> (AgentId, HostId) {
    let id = std::env::var("WASL_AGENT_ID").unwrap_or_else(|_| "wasl-local".to_string());
    let host = std::env::var("WASL_HOST_ID").unwrap_or_else(|_| "localhost".to_string());
    (AgentId(id), HostId(host))
}

/// The audit file path (`WASL_AUDIT_PATH`, default `"wasl-audit.jsonl"`).
fn audit_path() -> String {
    std::env::var("WASL_AUDIT_PATH").unwrap_or_else(|_| "wasl-audit.jsonl".to_string())
}

/// Open the durable audit sink, falling back to an in-memory sink (LOUDLY, on
/// stderr) if the file cannot be opened — the server still starts, but the
/// operator is told the trail is not durable rather than it being dropped
/// silently.
fn open_audit() -> AuditLog<DefaultSink> {
    let path = audit_path();
    match FileSink::open(&path) {
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

/// Build the default handler wired to the durable/real seams: a `FileSink` audit
/// log, a `ProcessRunner` for transfer, and a shared `reqwest::Client` for A2A.
fn build_handler() -> WaslMcp<DefaultSink, ProcessRunner> {
    let (self_id, self_host) = self_identity();
    let registry = wasl_registry::PeerRegistry::new();
    let audit = open_audit();
    let http = reqwest::Client::new();
    let state = WaslState::new(audit, http, ProcessRunner::new());
    WaslMcp::new(self_id, self_host, registry, state)
}

// ---------------------------------------------------------------------
// 3. run()
// ---------------------------------------------------------------------

/// Run the stdio MCP server (blocking until the transport closes).
///
/// Builds the handler (self id/host from env, empty registry, durable `FileSink`
/// audit + `ProcessRunner` transfer + shared A2A `reqwest::Client`), starts a
/// tokio runtime, serves over stdio, and blocks until the client disconnects.
///
/// If `WASL_A2A_BIND` is set, it ALSO spawns this node's A2A RECEIVE server
/// concurrently (see the UNTESTED-HERE shell below).
pub fn run() {
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
        if let Ok(bind) = std::env::var("WASL_A2A_BIND") {
            match bind.parse::<std::net::SocketAddr>() {
                Ok(addr) => {
                    let (self_id, _self_host) = self_identity();
                    let card = wasl_a2a_types::AgentCard {
                        name: self_id.as_str().to_string(),
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
                    let adapter = wasl_adapter_claude::ClaudeAdapter::new(
                        wasl_adapter_claude::ProcessCliInvoker,
                    );
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

        let handler = build_handler();
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
