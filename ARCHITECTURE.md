# Wasl — Architecture

**Wasl** (وصل, "to connect/link") is a **standalone MCP server** that lets AI coding-agent CLIs
(Claude Code, Codex, Gemini, …) **consult**, **command**, **message**, and **transfer data** with one
another — within a host and **across hosts** — with a **human-in-the-loop gate before every send**.

It is usable by anyone, with or without Marsad. When Marsad is present it can surface Wasl's gates in
its dashboard and observe all connections; when absent, Wasl gates itself via MCP elicitation (or a
tool-level `prepare`/`commit` fallback) and remains fully functional.

## Locked design decisions
- **Independent MCP server.** No hard dependency on Marsad. Marsad integration is an *optional* connector.
- **Three-tier gate** (permission-before-send):
  1. **MCP elicitation** (`elicitation/create`) when the calling client supports it (Claude Code, Codex).
  2. **Self-owned `prepare`→`commit`** handshake for clients without elicitation (Gemini) and whenever a
     real human sign-off must be guaranteed (elicitation can be auto-answered by client policy).
  3. **Optional direct-to-Marsad** connector: surface the gate to Marsad + emit an observation feed for
     EVERY connection (incl. auto-approved) so Marsad's graph stays complete.
- **Capabilities:** consult, command, message, transfer; single host + cross-host; **multicast/broadcast**
  (one→many peers). Direct host-A→host-B transfer (`scp -3`/`rsync`), not routed-through-local.
- **Topology:** a **per-host daemon mesh** — each host runs a Wasl daemon; daemons peer; local send =
  intra-daemon, cross-host = daemon→daemon, broadcast = fan-out. Keeps Wasl independent of Marsad's SSH.
- **Stack:** Rust (single static binary per host for the mesh; reuses the project's 100%-coverage /
  clippy / isolated-block test regime; clean Marsad integration). MCP is JSON-RPC over stdio/HTTP;
  `elicitation/create` can be sent via the SDK or hand-rolled.
- **Test-first isolation regime** identical to Marsad: each block = a crate with a physically separate
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
| WA | `wasl-audit` | L1 | ✅ | Durable, human-auditable interaction+decision log over an append-only `Sink` (durability-first). |
| WC | `wasl-marsad` | L1 | ✅ | Optional Tier-3 Marsad connector: surface gates → decision, notify auto-approved, observation feed, over a `MarsadLink` seam. |
| WS | `wasl-server` | L2 | ✅ | Gate+audit **spine**: `Gatekeeper` runs the three-tier gate (Tier-1 elicitation / Tier-3 marsad synchronous via `Surfacer`; Tier-2 prepare→commit via the ledger) wired to the audit log. Routing deferred to WD. |
| WD | `wasl-daemon` | L2 | ✅ | Routing **core**: resolve peers + fan-out envelopes + dispatch consult/command/message over a `Mesh` seam (partial multicast) + transfer delegate. Socket IO is WMESH. |
| E2E | `wasl-e2e` | — | ✅ | Cross-crate scenario suite: gate(server) → dispatch(daemon) → audit → marsad, composed with fakes; proves seam agreement. |
| MCP | `wasl-mcp` (`wasl-mcpd`) | L2 | 🔩 | **The MCP server.** Exposes the 4 tools over rmcp 0.9/stdio; gates each via `elicitation/create`, dispatches over WD, audits — sync `resolve_and_dispatch` core unit-tested, async rmcp/stdio shell + client smoke test. |
| WMESH | `wasl-mesh` | L2 | 🔩 | Concrete `std::net` `Mesh`: `TcpMesh` client (deliver envelope→reply) + `serve`/`Handler` side; framing/addr unit-tested, socket roundtrip loopback-tested. |

**Dependency DAG (acyclic):** `proto ← gate, registry, mailbox, transfer, audit`;
`proto,gate,audit ← server`; `proto,registry,mailbox,transfer ← daemon`; `gate ← marsad`;
`server,daemon,audit,… ← mcp`; `daemon,registry ← mesh`; all ← `e2e`.

**Milestones:**
- **M1 — standalone core:** ✅ WP → WG · WR · WM · WX · WA · WC → WS → WD → E2E, and the runnable `wasl-mcpd`
  MCP server (Tier-1 elicitation + Tier-2 prepare/commit; Tier-3 marsad connector present).
- **M2 — mesh:** WMESH ✅ provides the socket transport (`TcpMesh` + `serve`); the per-host daemon **binary**
  that runs `serve` + peers daemons and wires `TcpMesh` into `wasl-mcpd` for cross-host delivery is the
  remaining runtime wiring (composes only ✅ pieces).
- **M3 — Marsad integration:** WC ✅ (Tier-3 connector; observation feed) — the live wiring pairs with
  Marsad's Phase-3 work.

**Remaining (runtime/integration, gated on real systems — not unit-buildable in a headless sandbox):** the
per-host daemon binary + daemon↔daemon peering loop over WMESH; Tier-2/Tier-3 surfacing wired end-to-end
through `wasl-mcpd` (prepare/commit token round-trips + the Marsad connector as a live `Surfacer`).
