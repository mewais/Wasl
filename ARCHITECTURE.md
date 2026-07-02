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
- **Topology:** a **per-host daemon mesh** — each host runs a Wasl daemon; daemons peer; local send =
  intra-daemon, cross-host = daemon→daemon, broadcast = fan-out. Self-contained (no reliance on an external
  system's transport).
- **Stack:** Rust (single static binary per host for the mesh; 100%-coverage / clippy / isolated-block test
  regime). MCP is JSON-RPC over stdio/HTTP; `elicitation/create` via the `rmcp` SDK.
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
| WS | `wasl-server` | L2 | ✅ | Gate+audit **spine**: `Gatekeeper` runs the three-tier gate (Tier-1 elicitation / Tier-3 external-authority synchronous via `Surfacer`; Tier-2 prepare→commit via the ledger) wired to the audit log. Routing deferred to WD. |
| WD | `wasl-daemon` | L2 | ✅ | Routing **core**: resolve peers + fan-out envelopes + dispatch consult/command/message over a `Mesh` seam (partial multicast) + transfer delegate. Socket IO is WMESH. |
| E2E | `wasl-e2e` | — | ✅ | Cross-crate scenario suite: gate(server) → dispatch(daemon) → audit (external authority via `Surfacer`), composed with fakes; proves seam agreement. |
| MCP | `wasl-mcp` (`wasl-mcpd`) | L2 | 🔩 | **The MCP server.** Exposes the 4 tools over rmcp 0.9/stdio; gates each via `elicitation/create`, dispatches over WD, audits — sync `resolve_and_dispatch` core unit-tested, async rmcp/stdio shell + client smoke test. |
| WMESH | `wasl-mesh` | L2 | 🔩 | Concrete `std::net` `Mesh`: `TcpMesh` client (deliver envelope→reply) + `serve`/`Handler` side; framing/addr unit-tested, socket roundtrip loopback-tested. |

**Dependency DAG (acyclic):** `proto ← gate, registry, mailbox, transfer, audit`;
`proto,gate,audit ← server`; `proto,registry,mailbox,transfer ← daemon`;
`server,daemon,audit,… ← mcp`; `daemon,registry ← mesh`; all ← `e2e`.

**Milestones:**
- **M1 — standalone core:** ✅ WP → WG · WR · WM · WX · WA → WS → WD → E2E, and the runnable `wasl-mcpd`
  MCP server (Tier-1 elicitation + Tier-2 prepare/commit; Tier-3 external-authority `Surfacer` seam present).
- **M2 — mesh:** WMESH ✅ provides the socket transport (`TcpMesh` + `serve`); the per-host daemon **binary**
  that runs `serve` + peers daemons and wires `TcpMesh` into `wasl-mcpd` for cross-host delivery is the
  remaining runtime wiring (composes only ✅ pieces).
- **M3 — external-supervisor integration (optional):** an external supervisor/dashboard implements the
  generic `Surfacer` (Tier-3 gate authority) and audit `Sink` (observation feed) seams — no Wasl-side crate
  needed; the live wiring lives entirely on the external system's side.

**Remaining (runtime/integration, gated on real systems — not unit-buildable in a headless sandbox):** the
per-host daemon binary + daemon↔daemon peering loop over WMESH; Tier-2/Tier-3 surfacing wired end-to-end
through `wasl-mcpd` (prepare/commit token round-trips + an external authority as a live `Surfacer`).
