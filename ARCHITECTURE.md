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

| # | Crate | Tier | Purpose |
|---|---|---|---|
| WP | `wasl-proto` | L0 | Wire + domain types: agent cards, the 4 tool params/results (multicast peers), reply envelopes, gate request/decision/token, `elicitation/create` shapes. |
| WG | `wasl-gate` | L1 | Three-tier gate state machine: pick mode per client capability, track pending approvals + `prepare`/`commit` tokens. `Surfacer` trait (elicitation / prepare-commit / marsad impl it). |
| WR | `wasl-registry` | L1 | Peer registry + agent cards + mesh routing table (who's on which host, how to reach). `MeshTransport` trait. |
| WM | `wasl-mailbox` | L0 | Message routing: inbox/outbox, multicast fan-out + reply fan-in, correlation. Pure. |
| WX | `wasl-transfer` | L1 | Transfer tool: pure `scp -3`/`rsync` argv builder (direct A→B, A→many) + `Runner` trait. |
| WA | `wasl-audit` | L1 | Durable, human-auditable interaction+decision log. `Backend` trait (mirrors marsad-store). |
| WS | `wasl-server` | L2 | MCP server facade: tools → gate → registry/mailbox/transfer → audit; stdio + HTTP transports. |
| WD | `wasl-daemon` | L2 | Per-host daemon binary + mesh networking (daemon↔daemon). |
| WC | `wasl-marsad` | L1 | Optional Tier-3 Marsad connector: surface gates + observation feed to Marsad. |

**Dependency DAG (acyclic):** `proto ← gate, registry, mailbox, transfer, audit`;
`proto,gate,registry,mailbox,transfer,audit ← server`; `server ← daemon`; `gate ← marsad`.

**Milestones:**
- **M1 — standalone core:** WP → WG · WR · WM · WX · WA → WS (Tier-1 elicitation + Tier-2 prepare/commit).
- **M2 — mesh:** WD (per-host daemon peering; cross-host + broadcast over the network).
- **M3 — Marsad integration:** WC (Tier-3 connector; observation feed) — pairs with Marsad's Phase-3 work.
