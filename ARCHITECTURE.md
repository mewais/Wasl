# Wasl — Architecture

**Wasl** (وصل, "to connect/link") is a **standalone MCP server** that lets AI coding-agent CLIs
(Claude Code, Codex, Gemini, …) **consult**, **command**, **message**, and **transfer data** with one
another — within a host and **across hosts** — with a **human-in-the-loop gate before every send**.

It is fully standalone. Wasl gates itself via MCP elicitation (or a tool-level `prepare`/`commit`
fallback) and needs no external system. Optionally, an **external supervisor/dashboard** can surface
Wasl's gates and observe all connections — but only by implementing Wasl's generic seams; Wasl names and
requires no specific external system.

## Locked design decisions
- **Independent MCP server.** No hard dependency on any external system. Integration with an external
  supervisor is *optional* — it just implements Wasl's generic `Surfacer` (gate authority) and audit
  `Sink` (observation) traits.
- **Three-tier gate** (permission-before-send):
  1. **MCP elicitation** (`elicitation/create`) when the calling client supports it (Claude Code, Codex).
  2. **Self-owned `prepare`→`commit`** handshake for clients without elicitation (Gemini) and whenever a
     real human sign-off must be guaranteed (elicitation can be auto-answered by client policy).
  3. **Optional external authority** (`GateMode::External`): surface the gate to an external supervisor via
     the generic `Surfacer` seam + emit an observation feed (the generic audit `Sink`) for EVERY connection
     (incl. auto-approved) so an external dashboard's view stays complete.
- **Capabilities:** consult, command, message, transfer; single host + cross-host; **multicast/broadcast**
  (one→many peers). Direct host-A→host-B transfer (`scp -3`/`rsync`), not routed-through-local.
- **Topology (v2, A2A):** each host runs a Wasl **node** that is BOTH an **A2A server** (HTTP + JSON-RPC 2.0
  + SSE — the RECEIVE side peers reach) and an **MCP server** (stdio — the SEND side local agent CLIs attach
  to). Local send = A2A over loopback, cross-host = A2A over the network, broadcast = concurrent fan-out.
  Because agent CLIs are MCP *clients* (not A2A servers), the node's `AgentAdapter` is the "last inch" that
  injects an incoming A2A task into the live local CLI and returns its reply. Self-contained (no external
  transport). The bespoke socket mesh (`wasl-mesh`) has been **retired** in favor of A2A over HTTP.
- **Stack:** Rust (100%-coverage / clippy / isolated-block test regime). A2A = JSON-RPC 2.0 over HTTP + SSE
  (axum server, reqwest client); MCP = JSON-RPC over stdio/HTTP with `elicitation/create` via the `rmcp` SDK.
- **Test-first isolation regime:** each block = a crate with a physically separate
  `tests/` dir compiled against the public API only; tests written before code; 100% line+region
  coverage gate; per-crate `AGENTS.md` contract.

## Block map (crates = blocks). Tiers: L0 pure · L1 trait-mocked · L2 integration-bound.

Status: ✅ implemented + tested (100% line+region where a coverage unit; green otherwise); 🔩 IO shell
(verified by compile + loopback/smoke, logic delegated to the tested cores below it).

| # | Crate | Tier | Status | Purpose |
|---|---|---|---|---|
| WP | `wasl-proto` | L0 | ✅ | Wire + domain types: agent cards, the 4 tool params/results (multicast peers), reply envelopes, gate request/decision/token, `elicitation/create` shapes. |
| WG | `wasl-gate` | L1 | ✅ | Three-tier gate: pick mode per client capability (`choose_mode`), build the secret-free request/elicitation, map outcome→decision, track `prepare`/`commit` tokens (`GateLedger`). |
| WR | `wasl-registry` | L1 | ✅ | Peer registry + agent cards + resolution (`resolve`/`resolve_many` → local vs remote `Resolved`). |
| WM | `wasl-mailbox` | L0 | ✅ | Message routing: multicast `fan_out` (`base:i` ids) + reply fan-in `Correlation` + per-owner `Mailbox`. Pure. |
| WX | `wasl-transfer` | L1 | ✅ | Transfer tool: pure `scp -3`/`rsync` argv builder (direct A→B, A→many) + `Runner` trait + `execute`. |
| WA | `wasl-audit` | L1 | ✅ | Durable, human-auditable interaction+decision log over an append-only `Sink` (durability-first). This IS the generic observation feed — an external observer just implements `Sink`. |
| WS | `wasl-server` | L2 | ✅ | Gate+audit **spine**: `Gatekeeper` runs the three-tier gate (Tier-1 elicitation / Tier-3 external-authority synchronous via `Surfacer`; Tier-2 prepare→commit via the ledger) wired to the audit log. |
| WD | `wasl-daemon` | L2 | ⚠️ | **Legacy** routing core over the retired sync `Mesh` seam. Superseded by the A2A path (WDISP + WNODE); still used by `wasl-mcp`/`wasl-e2e` pending their migration, then to be retired. |
| A1 | `wasl-a2a-types` | L0 | ✅ | Faithful A2A wire types: AgentCard, Message, Part (Text/File/Data), Task, TaskState, TaskStatus, Artifact, JSON-RPC params/results, SSE update events. serde matches the A2A JSON (camelCase). |
| B1 | `wasl-a2a-server` | L2 | ✅ | axum A2A HTTP server: accepts JSON-RPC, drives a `TaskExecutor`, manages Task lifecycle, returns results + SSE. Loopback-tested (real HTTP). |
| B2 | `wasl-a2a-client` | L2 | ✅ | reqwest A2A client: `send_message`/`get_task`/`fetch_agent_card` (+ streaming). Loopback-tested against B1. Line-100, documented HTTP/serde region residual. |
| C1 | `wasl-adapter` | L1 | ✅ | The "last inch" trait `AgentAdapter` (deliver→reply) + `delivery_from_message` (reads `metadata.wasl` verb/provenance) + `AdapterExecutor` (plugs an adapter into B1). Trait+extraction fully tested; concrete CLI adapters are E3. |
| D1 | `wasl-dispatch` | L2 | ✅ | Send-side fan-out over A2A: resolve peers → build A2A `Message` (Wasl verb in `metadata.wasl`) → concurrent send via B2 → collect replies. Loopback-tested against real B1+AdapterExecutor. |
| D2 | `wasl-node` | L2 | ✅ | The per-host send/receive **unit**: gate → dispatch (send) + `receiver()` (A2A server over an adapter). Proven by a two-node loopback e2e (send→gate→A2A→receive→reply→back). |
| E1 | `wasl-filesink` | L1 | ✅ | Durable append-JSONL `wasl_audit::Sink` (audit survives restart). Genuine 100%. |
| E2 | `wasl-procrunner` | L1 | ✅ | Real `wasl_transfer::Runner`: runs a transfer argv as a child process. Plumbing genuine 100% via stand-in commands; real scp/rsync across hosts UNTESTED HERE. |
| E3 | `wasl-adapter-claude` | L2 | ✅ | Concrete `AgentAdapter` driving the Claude Code CLI. Pure core + wiring + process plumbing genuine 100%; the real `claude` CLI contract (flags/JSON/session) is an explicit ASSUMPTION, UNTESTED HERE. |
| E2E | `wasl-e2e` | — | ✅ | Cross-crate scenario suite: gate(server) → dispatch(daemon) → audit (external authority via `Surfacer`), composed with fakes; proves seam agreement. |
| MCP | `wasl-mcp` (`wasl-mcpd`) | L2 | 🔩 | **The MCP server.** Exposes the 4 tools over rmcp 0.9/stdio; gates each via `elicitation/create`, audits — sync `resolve_and_dispatch` core unit-tested, async rmcp/stdio shell + client smoke test. Currently dispatches over WD; migration to the A2A node (WNODE) is pending. |

**Dependency DAG (acyclic):** `proto ← gate, registry, mailbox, transfer, audit`;
`proto,gate,audit ← server`; `a2a-types ← a2a-server, a2a-client, adapter`;
`proto,a2a-types,a2a-client,registry ← dispatch`; `server,dispatch,a2a-server,adapter,gate ← node`;
`audit ← filesink`; `transfer ← procrunner`; `adapter ← adapter-claude`;
`proto,registry,mailbox,transfer ← daemon (legacy)`; `server,daemon,… ← mcp`; all ← `e2e`.

**Testability honesty:** loopback on `127.0.0.1` (different ports) stands in for cross-host — same code path.
What is genuinely proven here vs. UNTESTED HERE (no 2nd host / no real CLIs) is enumerated in
`docs/specs/REDESIGN-A2A.md` (§Testability honesty matrix) and each crate's `AGENTS.md`. No block claims
end-to-end "it works" on the strength of fakes alone.

**Milestones:**
- **M1 — standalone gate/audit core:** ✅ WP → WG · WR · WM · WX · WA → WS, and the `wasl-mcpd` MCP server
  (Tier-1 elicitation + Tier-2 prepare/commit; Tier-3 external-authority `Surfacer` seam present).
- **M2 — A2A send/receive:** ✅ A1 → B1 · B2 → C1 → D1 → D2 (the two-node loopback e2e proves send+receive
  over same/different "hosts"), plus the real edges E1 (durable audit) · E2 (process runner) · E3 (Claude
  adapter). Remaining runtime wiring: a runnable node binary tying MCP-stdio + A2A-serve + peering around
  `wasl-node`, and migrating `wasl-mcp` off the legacy `wasl-daemon`. Real cross-host (SSH/WAN), real scp,
  and driving a real CLI are UNTESTED HERE.
- **M3 — external-supervisor integration (optional):** an external supervisor/dashboard implements the
  generic `Surfacer` (Tier-3 gate authority) and audit `Sink` (observation feed) seams — no Wasl-side crate
  needed; the live wiring lives entirely on the external system's side.

**Remaining (runtime/integration, gated on real systems — not unit-buildable in a headless sandbox):** the
per-host daemon binary + daemon↔daemon peering loop over WMESH; Tier-2/Tier-3 surfacing wired end-to-end
through `wasl-mcpd` (prepare/commit token round-trips + an external authority as a live `Surfacer`).
