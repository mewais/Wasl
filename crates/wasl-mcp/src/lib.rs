//! wasl-mcp — MCP server facade for Wasl (the IO shell over the tested cores).
//!
//! Exposes the four Wasl agent-to-agent tools (consult / command / message /
//! transfer) as MCP tools. Each call runs the permission-before-send gate
//! (surfaced to the client via MCP `elicitation/create`, reusing the tested
//! `wasl_gate` logic) and, only on approval, dispatches over `wasl_daemon`,
//! recording every decision to `wasl_audit`. Transport/runtime is this crate's
//! concern; all decision + routing logic is the tested cores.
//! See `docs/specs/mcp.md`.

pub mod handle;
pub mod server;

use wasl_audit::{AuditEntry, AuditError, AuditLog, Sink};
use wasl_daemon::Mesh;
use wasl_proto::{AgentId, Envelope, HostId};
use wasl_registry::Resolved;
use wasl_transfer::Runner;

pub use handle::{resolve_and_dispatch, HandleError, ToolCall, ToolCtx, ToolReply};
pub use server::{WaslMcp, WaslState};

// ---------------------------------------------------------------------
// 1. Default IO seams for `run()`
// ---------------------------------------------------------------------

/// An in-memory audit sink: keeps every recorded entry. Simple and infallible —
/// a file-backed sink can drop in later behind the same trait.
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

/// A local-only mesh: delivers same-host peers in-process (acking) and errors on
/// any remote route. The real cross-host socket loop is a later daemon shell;
/// until then a remote target fails loudly rather than silently succeeding.
#[derive(Debug, Default, Clone)]
pub struct LocalMesh;

impl Mesh for LocalMesh {
    fn deliver(&mut self, resolved: &Resolved, env: &Envelope) -> Result<String, String> {
        match resolved {
            Resolved::Local { peer } => Ok(format!("ack:{}", peer.as_str())),
            Resolved::Remote { peer, host } => Err(format!(
                "remote delivery not wired: peer {} on host {} (envelope {})",
                peer.as_str(),
                host.as_str(),
                env.id
            )),
        }
    }
}

/// A no-op transfer runner: cross-host process exec is out of scope for the
/// stdio shell, so every transfer job errors (never silently "succeeds").
#[derive(Debug, Default, Clone)]
pub struct NoopRunner;

impl Runner for NoopRunner {
    fn run(&mut self, _argv: &[String]) -> Result<(), String> {
        Err("transfer runner not wired in the stdio shell".to_string())
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

/// Build the default handler wired to the in-memory/local seams.
fn build_handler() -> WaslMcp<MemSink, LocalMesh, NoopRunner> {
    let (self_id, self_host) = self_identity();
    let registry = wasl_registry::PeerRegistry::new();
    let audit = AuditLog::new(MemSink::new());
    let state = WaslState::new(audit, LocalMesh, NoopRunner);
    WaslMcp::new(self_id, self_host, registry, state)
}

// ---------------------------------------------------------------------
// 3. run()
// ---------------------------------------------------------------------

/// Run the stdio MCP server (blocking until the transport closes).
///
/// Builds the handler (self id/host from env, empty registry, local mesh +
/// in-memory audit sink), starts a tokio runtime, serves over stdio, and blocks
/// until the client disconnects.
pub fn run() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    runtime.block_on(async {
        use rmcp::transport::io::stdio;
        use rmcp::ServiceExt;

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
