# MCP — `wasl-mcp` (`wasl-mcpd`) — working contract
**Purpose:** the MCP server facade — expose consult/command/message/transfer as MCP tools, gate each via
elicitation, dispatch over the tested Wasl cores, audit every decision. IO shell; NO new decision logic.
**Spec (current):** `../../docs/specs/mcp-a2a.md` (F1 — dispatch over REAL A2A). Original: `../../docs/specs/mcp.md`.
- Wires rmcp 0.9 (server + transport-io) + tokio over the tested crates. **v2:** the SEND path dispatches
  over REAL A2A (`wasl-dispatch` + `reqwest`) instead of the retired canned `LocalMesh`; audit defaults to the
  durable `wasl-filesink::FileSink`; transfer uses the real `wasl-procrunner::ProcessRunner`; `run()` also
  wires the A2A RECEIVE server (`wasl_node::receiver` over a `wasl_adapter_claude::ClaudeAdapter`).
- **code-agent:** ONLY `src/**` + `Cargo.toml`. **test-agent:** ONLY `tests/**`.
- Done: `cargo build -p wasl-mcp` + the `wasl-mcpd` bin compile; the async `handle`/`resolve_and_dispatch`
  heart is integration-tested green over **loopback A2A** (real a2a-server + real AdapterExecutor + fake
  adapter — NOT fakes-only); `clippy -p wasl-mcp --all-targets -D warnings` + fmt clean; no unsafe; no
  leftover `unimplemented!`/`todo!`; the gate is never bypassed (non-Approved never dispatches).
- **Testability honesty:** ✅ the async send heart end-to-end over loopback A2A (gate→audit→A2A→receive→reply)
  + block/deny/dismiss + audit-failure abort + transfer. ❌ UNTESTED HERE: the rmcp stdio serve loop, the A2A
  receive serve loop in `run()`, driving a real Claude CLI, and true cross-host over SSH/WAN. Do NOT claim a
  coverage % for the IO shell (rmcp/stdio + serve wiring) — it is verified by compile + the rmcp smoke test.
