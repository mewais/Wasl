# Wasl — وصل

**A standalone MCP server for agent-to-agent connection** — lets AI coding-agent CLIs (Claude Code,
Codex, …) **consult**, **command**, **message**, and **transfer data** with each other, within a
host and **across hosts**, with a **human-in-the-loop gate before every send**.

Fully standalone. Wasl gates itself via MCP elicitation (or a tool-level `prepare`/`commit` fallback), and
can optionally surface its gates + an observation feed to any **external supervisor/dashboard** — which just
implements Wasl's generic `Surfacer` / audit `Sink` seams — but names or requires none. See
[`ARCHITECTURE.md`](./ARCHITECTURE.md) for the block map, the three-tier gate design, and the
per-host node mesh (each host runs a `wasl-mcpd` node; nodes peer over A2A). Built in Rust (official `rmcp` MCP SDK), test-first, 100%-coverage per block.

## Status
Greenfield. Standing up the block-decomposed core (M1) test-first.
