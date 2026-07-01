//! wasl-mcpd — the Wasl MCP server binary entrypoint (thin shell).
//!
//! Wires the tested Wasl cores (gate + dispatch + audit) behind an rmcp MCP
//! server over stdio. See `docs/specs/mcp.md`.

fn main() {
    wasl_mcp::run();
}
