# MCP — `wasl-mcp` (`wasl-mcpd`) — working contract
**Purpose:** the MCP server facade — expose consult/command/message/transfer as MCP tools, gate each via
elicitation, dispatch over the tested Wasl cores, audit every decision. IO shell; NO new decision logic.
**Spec:** `../../docs/specs/mcp.md`
- Wires rmcp 0.9 (server + transport-io) + tokio over the tested crates (proto/gate/audit/registry/mailbox/transfer/server/daemon).
- Done: `cargo build -p wasl-mcp` + the `wasl-mcpd` bin compile; the `handle`/`resolve_and_dispatch` core is unit-tested green (fake Mesh/Runner/Sink); `clippy -p wasl-mcp --all-targets -D warnings` + fmt clean; no unsafe; no leftover `unimplemented!`/`todo!` in shipped paths; the gate is never bypassed (non-Approved never dispatches).
