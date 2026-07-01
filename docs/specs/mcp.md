# MCP — `wasl-mcp` (`wasl-mcpd`) — MCP server facade — spec (authoritative)

**Tier:** L2 (IO shell over tested cores). **Deps:** `rmcp` (0.9, `server` + `transport-io`), `tokio`, all
Wasl logic crates (`proto`, `gate`, `audit`, `registry`, `mailbox`, `transfer`, `server`, `daemon`),
`serde`/`serde_json`. This crate is the **transport/runtime shell** — it adds NO new decision or routing
logic; every gate/dispatch/audit decision comes from the already-tested cores. Its own testable surface (the
pure request→reply mapping) is unit-tested; the async rmcp/stdio plumbing is verified by **compilation +
clippy + fmt** and a smoke test, mirroring how the other IO edges (Runner/Sink/Mesh) are trait-mocked.

## Goal
Make Wasl a real MCP server: expose the four agent-to-agent tools so any MCP-client CLI (Claude/Codex/Gemini)
can consult/command/message peers and transfer files, with a **permission-before-send** gate surfaced to the
human via MCP `elicitation/create`, and a durable audit of every decision.

## Server shape (rmcp 0.9)
Research the exact rmcp 0.9 API (docs.rs/rmcp, the repo's `examples/servers`) — it uses a
`#[tool_router]`/`#[tool]` macro pattern on a handler struct implementing `ServerHandler`, served over
`stdio()` with `serve_server`. Implement:

- `struct WaslMcp` holding shared state behind the appropriate sync primitive (rmcp handlers are `&self` +
  async, so use `Arc<Mutex<…>>` / `tokio::sync::Mutex` as needed): `self_id: AgentId`, `self_host: HostId`,
  `caps: ClientCaps`, `registry: PeerRegistry`, an `AuditLog<S>` (a simple in-memory/file `Sink`), a seq
  counter, and the delivery seams (a `Mesh` for consult/command/message dispatch + a `Runner` for transfer).
- Four `#[tool]` async methods — `consult`, `command`, `message`, `transfer` — taking the corresponding
  `wasl_proto::*Params` (as the tool's JSON args) and returning the `*Result` (as tool content).

## Per-call flow (each tool) — compose the tested cores, do NOT reimplement
1. Build the gate request: `wasl_gate::build_request(id, kind, from=self_id, &target_strs, preview)` where
   `preview` is a secret-free summary of the call.
2. Pick the mode: `wasl_gate::choose_mode(&caps)`.
3. Surface it:
   - **Elicitation (Tier-1):** call the rmcp peer's elicitation API (`context.peer().create_elicitation(...)`
     — confirm the 0.9 method/param names) with `wasl_gate::elicitation_for(&req).message`; map the
     client's accept/decline/cancel → `wasl_gate::GateOutcome` (accept→Approved, decline→Denied, cancel→
     Dismissed). If the client lacks elicitation, fall back to auto-deny with a clear message (Tier-2/Tier-3
     wiring — prepare/commit token round-trips and the Marsad connector — is a follow-on; a client that
     can't be prompted must NOT silently send).
4. Record the decision to the audit log (`AuditEntry{ kind: Decision, actor: self_id, targets, decision:
   outcome.to_decision(), … }`) — durability-first (`AuditLog::record`); an audit failure fails the call.
5. **Only on `Approved`:** dispatch — `wasl_daemon::{consult|command|message}(&registry, &self_host,
   base_id, &self_id, &params, &mut mesh)` (→ the `*Result`), or `wasl_daemon::transfer(tool, &params,
   &mut runner)` for transfers. On `Denied`/`Dismissed`, return an MCP tool result that reports the block
   (an error-content result), and do NOT dispatch.

## Testable core (factor this out; unit-test it)
Put the pure request→reply logic in a `handle` module independent of rmcp/tokio:
```rust
pub struct ToolCtx<'a> { pub self_id: &'a AgentId, pub self_host: &'a HostId, pub registry: &'a PeerRegistry }
pub enum ToolReply { Consult(ConsultResult), Command(CommandResult), Message(MessageResult),
                     Transfer(TransferResult), Blocked { reason: String } }
/// Given an already-decided GateOutcome (the async elicitation happens in the rmcp layer), record the
/// decision to `audit` and, only on Approved, dispatch over `mesh`/`runner`; return the reply. This is the
/// synchronous heart — fully unit-testable with fake Mesh/Runner/Sink (reuse the daemon/e2e fakes' style).
pub fn resolve_and_dispatch(...) -> Result<ToolReply, ServerLikeError>;
```
The rmcp `#[tool]` methods become: build request → (async) surface elicitation → `resolve_and_dispatch(...)`.

## `pub fn run()`
Build the `WaslMcp` (self id/host + caps + an empty registry seeded from env/args or empty; a local `Mesh`
that errors on remote and delivers locally; a file/in-mem audit sink), start a tokio runtime, and
`serve_server(handler, stdio())`, blocking until the transport closes. Config (self id/host, peers) may come
from env vars or argv — keep it simple and documented.

## Verification bar (Done)
- `cargo build -p wasl-mcp` and the `wasl-mcpd` bin compile.
- `cargo clippy -p wasl-mcp --all-targets -- -D warnings` clean; `cargo fmt -p wasl-mcp` clean.
- `tests/`: unit-test the `handle`/`resolve_and_dispatch` core (approved→dispatched with the right result;
  denied/dismissed→Blocked, no dispatch; audit failure→error; transfer path) with fake Mesh/Runner/Sink —
  real `#[test]`s, concrete assertions, green. (The async rmcp handlers themselves are covered by
  compilation + a smoke test if rmcp offers an in-memory/test transport; if not, note it.)
- No `unsafe`. No `unimplemented!`/`todo!` left in shipped paths. The gate is never bypassed: a call whose
  outcome is not `Approved` MUST NOT dispatch.

If the rmcp 0.9 API differs from assumptions here (method/type names), follow the REAL API (research it) —
the spec's intent (four tools, elicitation gate, dispatch over the tested cores, audit) is what must hold.
