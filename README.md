# Wasl

Agent-to-agent orchestration on top of [Wasit](https://github.com/mewais/Wasit): coding-agent
sessions (Claude Code, Codex, OpenCode, Kilo Code) delegate work to each other across machines,
asynchronously, with human approval on handoffs. The authoritative design is §5 of the Wasit
architecture document (`ARCHITECTURE.md` in the Wasit repo); [ARCHITECTURE.md](ARCHITECTURE.md)
here describes how this repo implements it.

## Crates

| Crate | Role |
|---|---|
| `wasl-proto` | Wire types and `wasl.*` method/topic names: agents, peers, topology (incl. `paused`), tasks, results, handoff decisions. `ts` feature → TypeScript (ts-rs). |
| `wasl-node` | The embeddable node (one per host daemon): live topology, durable task engine, handoff gates, delivery, relay routing, safety limits, events. Serves `wasl.*` as a `wasit_control::Service`. |
| `wasl-mcp` | The stdio MCP server an agent CLI launches (`list_peers`, `delegate`, `task_status`, `task_wait`, `task_cancel`, `consult`, `message`); a thin client of the local daemon. `mcp_server_config()` produces the `AgentConfig` item that injects it. |
| `wasl-audit` | Audit trail (JSON lines). |
| `wasl` | Standalone binary: `serve` (Wasit runtime + Wasl node daemon), `bridge`, `mcp`, `shim`. |

## Using it without Marsad

```sh
wasl serve --data ~/.wasl &                       # one daemon per host
# create sessions with `sessions.create` (set `id` and inject
# wasl_mcp::mcp_server_config(...) into `config.mcp_servers`),
# push a topology with `wasl.topology.set`, answer handoff gates with `session.command`.
```

A hub (any process running `wasit_control::Hub` with links that run `wasl bridge --data <dir>`
on each host, usually over ssh) connects daemons on several machines. Without a supervisor
connected, handoffs over `ask` edges are approved through MCP elicitation, or denied when the
client cannot ask.

## Development

Wasit is a git dependency (branch `claude/awesome-euler-r7x45r`). To build against a sibling
checkout, create an untracked `.cargo/config.toml`:

```toml
[patch."https://github.com/mewais/Wasit"]
wasit-core = { path = "../Wasit/crates/wasit-core" }
wasit-control = { path = "../Wasit/crates/wasit-control" }
wasit-runtime = { path = "../Wasit/crates/wasit-runtime" }
wasit-shim = { path = "../Wasit/crates/wasit-shim" }
wasit-fake-cli = { path = "../Wasit/crates/wasit-fake-cli" }
wasit-claude = { path = "../Wasit/crates/wasit-claude" }
wasit-codex = { path = "../Wasit/crates/wasit-codex" }
wasit-opencode = { path = "../Wasit/crates/wasit-opencode" }
```

This works with cargo's `[patch]`-in-config support (cargo still fetches the git source once to
resolve). Optionally add `[build] target-dir = "../Wasit/target"` to share build artifacts.

```sh
cargo build --workspace --all-targets
cargo test --workspace            # real daemons + fake CLI (Unix); unit tests everywhere
WASL_LIVE=1 cargo test -p wasl --test live -- --ignored --nocapture   # real CLIs
```
