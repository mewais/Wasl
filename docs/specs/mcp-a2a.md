# F1 — `wasl-mcp` v2 — dispatch over REAL A2A (retire the canned `LocalMesh`) — spec (authoritative)

**Why:** the v1 `wasl-mcp` dispatched over `wasl_daemon`'s `Mesh` seam, and the shipped `run()` wired a
`LocalMesh` that returned a **canned `"ack:{peer}"`** for local peers and errored on remote — i.e. it never
actually delivered to any agent. This block replaces that with the REAL A2A send path (`wasl-dispatch` over
HTTP), so an MCP tool call actually reaches a peer's A2A server and returns its real reply. The gate + audit
spine is unchanged; only the dispatch backend changes (sync `Mesh` → async A2A).

**Scope of THIS block:** the SEND path (handle heart + rmcp server + `run()` defaults). The A2A RECEIVE
server (serving this node's own agent behind an `AgentAdapter`) is added to `run()` as clearly-labeled
compile-only IO wiring. Retiring the `wasl-daemon` CRATE (still used by `wasl-e2e`) is a FOLLOW-ON block.

## Deps change
- Remove `wasl-daemon` from `wasl-mcp`'s dependencies.
- Add: `wasl-dispatch`, `reqwest` (client), and for the durable/real defaults `wasl-filesink` (E1) +
  `wasl-procrunner` (E2). For the receive-side wiring in `run()`: `wasl-a2a-server`, `wasl-a2a-types`,
  `wasl-adapter`, `wasl-adapter-claude`. `tokio` already present.
- Dev: `wasl-a2a-server`, `wasl-a2a-types`, `wasl-adapter`, `async-trait`, `axum`, `tokio`, `tempfile` (for
  the loopback heart tests + a temp FileSink).

## The heart (`handle.rs`) — now ASYNC over A2A
`resolve_and_dispatch` keeps its contract (record decision durability-first for EVERY outcome; dispatch ONLY
on `Approved`; non-approve → `Blocked`, seams untouched) but becomes `async` and dispatches over A2A:

```rust
#[allow(clippy::too_many_arguments)]
pub async fn resolve_and_dispatch<K: Sink>(
    ctx: &ToolCtx<'_>,
    outcome: &GateOutcome,
    call: ToolCall,
    base_id: &str,
    seq: u64,
    http: &reqwest::Client,       // was: mesh: &mut dyn Mesh
    runner: &mut dyn Runner,      // transfer still uses a Runner (now the REAL ProcessRunner in run())
    audit: &mut AuditLog<K>,
) -> Result<ToolReply, HandleError>;
```
- audit.record(entry)? unchanged (durability-first; audit failure → `Err(HandleError::Audit)`, no dispatch).
- On `Approved`, `dispatch_approved` becomes `async`:
  - `ToolCall::Consult(p)` → `wasl_dispatch::consult(ctx.registry, ctx.self_id, base_id, &p, http).await`
    → `ToolReply::Consult(result)`. (Same for command/message; note dispatch takes `from = ctx.self_id`
    and NO host arg — drop `self_host` from the dispatch call; `ToolCtx.self_host` stays for identity.)
  - `ToolCall::Transfer { tool, params }` → `wasl_transfer::execute(tool, &params, runner)` (the daemon's
    transfer was a thin passthrough to this): `Ok((result,_)) → Transfer(result)`;
    `Err(TransferError::NoDestinations) → Transfer(TransferResult{delivered:vec![]})`;
    `Err(other) → Blocked{reason}` — UNCHANGED behavior.
- Drop the `wasl_daemon` import; `ToolCtx`/`ToolReply`/`ToolCall`/`HandleError` types are otherwise unchanged.

## The rmcp server (`server.rs`)
- `WaslState<K, R>` drops the `M: Mesh` param and the `mesh` field; add `http: reqwest::Client`. `new` becomes
  `WaslState::new(audit, http, runner)`. `WaslMcp<K, R>` likewise drops `M`.
- `gated_call` `.await`s the async heart, passing `&state.http` instead of `&mut state.mesh`. (The borrow
  split changes: `http` is `&` shared, `runner`/`audit` are `&mut`.) Everything else (elicitation surfacing,
  rendering) is unchanged.

## `run()` defaults (`lib.rs`) — real seams + receive wiring
- DELETE `LocalMesh` (the canned-ack seam) and `NoopRunner`.
- Audit default: durable `wasl_filesink::FileSink` opened at `WASL_AUDIT_PATH` (default `"wasl-audit.jsonl"`).
  Keep `MemSink` available (used by tests) but `run()` uses `FileSink`.
- Transfer default: `wasl_procrunner::ProcessRunner` (real).
- Dispatch: a shared `reqwest::Client`.
- Receive side (IO shell, **UNTESTED HERE**): if `WASL_A2A_BIND` is set, build an `AgentCard` for this node's
  agent and `tokio::spawn` `wasl_a2a_server` from `wasl_node::receiver(card, wasl_adapter_claude::ClaudeAdapter::new(wasl_adapter_claude::ProcessCliInvoker))`
  serving on that bind addr, concurrently with the stdio MCP server. This composes only already-tested pieces;
  the actual serve loop + real Claude CLI are UNTESTED HERE and MUST be labeled as such in code + AGENTS.md.

## Test plan (`tests/handle.rs` rewritten to loopback A2A; `tests/smoke.rs` largely unchanged)
The heart is now genuinely integration-tested over loopback A2A (NOT fakes-only): spin a REAL
`wasl_a2a_server::A2aServer` on `127.0.0.1:0` running a REAL `wasl_adapter::AdapterExecutor` over a FAKE
`AgentAdapter` (records deliveries, scriptable reply/err) — exactly the wasl-dispatch test style. Register
the peer in a `PeerRegistry` with `endpoint = "http://127.0.0.1:{port}/"`. Use a recording/failing `Sink`
(the existing `MemSink` / a failing sink) for audit. `#[tokio::test]`.
- **approved consult** → `ToolReply::Consult` with the peer's reply; audit has one `Decision(Approve)`; the
  fake adapter SAW the delivery (`kind: Consult`). (Proves gate→audit→A2A→receive→reply→back.)
- **approved command / message** → analogous (`Command`/`Message` seen by the adapter).
- **approved transfer** over a fake/real `Runner` → `ToolReply::Transfer` with delivered hosts; a
  `NoDestinations` params → `Transfer{delivered:[]}`; a failing runner job → the failure bucket / `Blocked`
  per the unchanged contract. (Transfer does NOT use A2A.)
- **denied → Blocked**, and the peer's adapter recorded NOTHING (never dispatched). Audit has `Some(Deny)`.
- **dismissed → Blocked("dismissed")**, nothing dispatched, audit `decision: None`.
- **audit failure → Err(HandleError::Audit)** and NOTHING dispatched (adapter untouched) — durability-first.
- **unknown peer** (unregistered / no endpoint) → surfaces in the dispatch report bucket; a co-addressed
  known peer still succeeds (partial multicast). (Assert via the result/replies; report is internal to
  dispatch but the reply set proves it.)

Bind-before-spawn, short timeouts, abort/join server tasks so tests never hang. Do NOT weaken to unit-mocks
of the transport. `cargo test -p wasl-mcp` green; `cargo clippy -p wasl-mcp --all-targets -- -D warnings`
clean; `cargo fmt` clean. Coverage: the async heart's logic is covered by the loopback tests; the rmcp/stdio
`run()`/serve shell + the receive-side wiring are verified by **compile + the smoke test** and labeled
UNTESTED HERE for the live loop (mirroring v1's stance — do NOT claim a coverage % for the IO shell).

## Honesty
- ✅ verified here: the async send heart end-to-end over loopback A2A (gate → audit → A2A → receive → reply),
  block/deny/dismiss invariants, audit-failure abort, transfer over a Runner.
- ❌ UNTESTED HERE: the rmcp stdio serve loop, the A2A receive serve loop in `run()`, driving a real Claude
  CLI, and true cross-host over SSH/WAN (loopback different-ports is the stand-in, same code path).
