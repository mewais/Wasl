//! wasl-mcp — MCP server facade for Wasl (the IO shell over the tested cores).
//!
//! Exposes the four Wasl agent-to-agent tools (consult / command / message /
//! transfer) as MCP tools. Each call runs the permission-before-send gate
//! (surfaced to the client via MCP `elicitation/create`, reusing the tested
//! `wasl_gate`/`wasl_server` logic) and, only on approval, dispatches over
//! `wasl_daemon`, recording every decision to `wasl_audit`. Transport/runtime is
//! this crate's concern; all decision + routing logic is the tested cores.
//! See `docs/specs/mcp.md`.

/// Run the stdio MCP server (blocking). Implemented by the MCP block.
pub fn run() {
    unimplemented!("wasl-mcp: implemented by the MCP server block")
}
