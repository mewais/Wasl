# Wasl + Marsad — Architecture & Status Handoff

**Purpose.** You are moving to an environment where you can actually run/test. This document is the
single source of truth for (1) what each system *is*, (2) what is **genuinely verified** vs. **untested in
the build sandbox**, and (3) the exact first steps to close the untested gaps on real hardware.

**Honesty legend used throughout:**
- ✅ **VERIFIED HERE** — real tests pass in the sandbox (unit / loopback / coverage-gated).
- 🟡 **UNTESTED HERE** — built and compiles (or is loopback-proxied), but the real edge (real CLI, 2nd
  host, GTK desktop, live serve loop) was never exercised because the sandbox lacks it.
- ❌ **NOT BUILT** — design exists (or doesn't); no implementation.

Both repos are on branch **`claude/marsad-review-upgrade-1bahy2`**.

---

## 0. The two systems and how they relate

- **Wasl** (وصل, "to connect") — a **standalone MCP server** that lets AI coding-agent CLIs (Claude Code,
  Codex, Gemini) **consult / command / message / transfer** with each other, on the same host or across
  hosts, with a **human-in-the-loop gate before every send** and a durable audit of every decision. The
  transport is **A2A** (Agent2Agent: JSON-RPC 2.0 over HTTP + SSE).
- **Marsad** (مرصد, "observatory") — a **desktop/mobile control tower** for running/observing many `claude`
  sessions across hosts (Tauri desktop + native iOS/Android over a shared Rust core + a detached supervisor
  daemon that owns the `claude` processes).
- **Relationship (optional, not built):** Marsad can act as Wasl's **Tier-3 external authority** — Wasl
  surfaces its gate to an external supervisor and emits an observation feed, both via **generic seams**
  (`Surfacer` + audit `Sink`). Wasl names no specific external system; Marsad would just implement those
  seams. The live Marsad↔Wasl connector is **❌ NOT BUILT** on either side.

---

# PART 1 — WASL

## 1.1 Architecture (v2, A2A)

Each host runs a Wasl **node** that is simultaneously:
- an **A2A server** (HTTP + JSON-RPC 2.0 + SSE) — the **RECEIVE** side peers reach; and
- an **MCP server** (stdio) each local agent CLI attaches to — the **SEND** side (how a local agent
  initiates a consult/command/message/transfer).

```
 SEND:  local agent A --MCP tool--> node A [gate + audit] --A2A client--> node B (HTTP)
 RECV:  node B [A2A server] --TaskExecutor--> AgentAdapter --inject--> agent B's live CLI session
                                                         --reply--> A2A artifact --> back to node A --> A's MCP result
```

Key design facts:
- Agent CLIs (claude/codex/gemini) are **MCP clients, not A2A servers**. So the node's **`AgentAdapter`** is
  the "last inch": it injects an incoming A2A task into the live local CLI and returns its reply. A stable
  A2A `contextId` maps to a stable long-lived session, so context persists across messages.
- **Every send is gated** (permission-before-send) and **audited**. The gate is three-tier:
  1. **MCP elicitation** (`elicitation/create`) when the client supports it;
  2. **prepare→commit** handshake for clients without elicitation (or when a human sign-off must be
     guaranteed);
  3. **external authority** (`GateMode::External`) — surface to an external supervisor via the generic
     `Surfacer` seam (this is the optional Marsad integration point).
- **Wire convention** (shared by sender + receiver): each A2A `Message` carries
  `metadata.wasl = {"kind": consult|command|message, "from": "<sender>"}`, body as a `Part::Text`,
  `contextId = base_id`.
- **Loopback = cross-host stand-in.** Two nodes on `127.0.0.1` at different ports exercise the *exact same
  code path* as two hosts; only real SSH/WAN latency/auth is unproven.

## 1.2 Crate-by-crate status

Coverage regime: per-crate `cargo llvm-cov --fail-under-lines 100` (regions too where pure); `clippy
--all-targets -D warnings`; `rustfmt`; physically separate `tests/` compiled against the public API only.

| Crate | Role | Status |
|---|---|---|
| `wasl-proto` | Domain/wire types, 4 tool params/results, gate req/decision/token | ✅ VERIFIED (100%) |
| `wasl-gate` | Three-tier gate: `choose_mode`, build request, outcome→decision, prepare/commit ledger | ✅ VERIFIED (100%) |
| `wasl-registry` | Peer registry + agent cards + resolution (A2A endpoints) | ✅ VERIFIED (100%) |
| `wasl-mailbox` | Multicast fan-out (`base:i` ids) + reply correlation | ✅ VERIFIED (100%) |
| `wasl-transfer` | scp -3 / rsync argv builder + `Runner` trait + `execute` | ✅ VERIFIED (100%) |
| `wasl-audit` | Durable interaction/decision log over an append-only `Sink` | ✅ VERIFIED (100%) |
| `wasl-server` | `Gatekeeper` gate+audit spine (all three tiers) | ✅ VERIFIED (100%) |
| `wasl-a2a-types` | A2A wire types (AgentCard, Message, Part, Task, TaskState, JSON-RPC, SSE) | ✅ VERIFIED (100%) |
| `wasl-a2a-server` | axum A2A HTTP server; drives a `TaskExecutor`; Task lifecycle + SSE | ✅ VERIFIED loopback (real HTTP) |
| `wasl-a2a-client` | reqwest A2A client (send/get/card/stream) | ✅ VERIFIED loopback; line-100, documented HTTP/serde region residual |
| `wasl-adapter` | `AgentAdapter` trait + `delivery_from_message` + `AdapterExecutor` | ✅ VERIFIED (100%) |
| `wasl-dispatch` | Send-side fan-out over A2A (resolve→build Message→concurrent send→collect) | ✅ VERIFIED loopback (real a2a-server + AdapterExecutor + fake adapter) |
| `wasl-node` | The per-host send/receive UNIT: gate→dispatch (send) + `receiver()` (A2A server over an adapter) | ✅ VERIFIED — **two-node loopback e2e** (send→gate→A2A→receive→reply→back) |
| `wasl-filesink` (E1) | Durable append-JSONL audit `Sink` (survives restart) | ✅ VERIFIED (line/func 100%; 1 documented unreachable-serialize region) |
| `wasl-procrunner` (E2) | Real `Runner` — spawns the transfer process | ✅ VERIFIED (100/100/100) via stand-in commands. 🟡 real scp/rsync between 2 hosts UNTESTED HERE |
| `wasl-adapter-claude` (E3) | Concrete `AgentAdapter` driving the **Claude CLI** | ✅ VERIFIED (100/100/100) pure core + wiring + process plumbing. 🟡 the **real `claude` CLI contract is an ASSUMPTION**, UNTESTED HERE |
| `wasl-mcp` (`wasl-mcpd`) | The MCP server facade — 4 tools, elicitation gate, **dispatch over REAL A2A**, audit | ✅ VERIFIED: 10 loopback A2A heart tests + rmcp smoke. 🟡 rmcp stdio serve loop + A2A receive serve loop in `run()` UNTESTED HERE |
| `wasl-daemon` | **LEGACY** sync routing over the retired `Mesh` seam | ⚠️ Superseded by A2A; NOT on the live path; kept only as `wasl-e2e`'s dispatch backend |
| `wasl-e2e` | Cross-crate suite: three-tier GATE × verbs, composed with dispatch+audit | ✅ VERIFIED (green). Dispatch half runs over the legacy `wasl-daemon` (FakeMesh), not the live path |

Retired this cycle: **`wasl-mesh`** (bespoke socket transport) — deleted, superseded by A2A over HTTP.

Whole workspace currently: **81 test binaries green, clippy clean, fmt clean.**

## 1.3 What Wasl can actually do today (proven) vs. not

✅ **Proven end-to-end over loopback** (the user's core goal, minus the real CLI last inch):
- Agent A gates + audits + sends a **consult / command / message** to agent B over real A2A; B's node
  receives it, runs its `AdapterExecutor`, the adapter replies, the reply flows back to A. Same-host and
  "different host" (different ports) — same code path. Multicast/partial-failure/unknown-peer all covered.
- Denied/dismissed never dispatch; audit-failure aborts before dispatch (durability-first). Tier-2
  prepare→commit works. All in `wasl-node`'s e2e and `wasl-mcp`'s loopback heart tests.

🟡 **UNTESTED HERE** (needs the target environment):
- Driving a **real `claude` CLI** (no `claude` binary in the sandbox). The claude flag/JSON/session contract
  is encoded as an explicit assumption in `wasl-adapter-claude::{build_argv, parse_reply}` and the stdin
  convention — **verify these first on a real machine.**
- **Codex / Gemini adapters are ❌ NOT BUILT** — only Claude. (They are MCP clients too; each needs its own
  `AgentAdapter` like E3.)
- **Real scp/rsync between two hosts** (no 2nd host / SSH in the sandbox).
- **True cross-host over SSH/WAN** (loopback different-ports is the stand-in).
- The **rmcp stdio serve loop** and the **A2A receive serve loop** in `wasl-mcpd`'s `run()` (IO shells,
  compile-only; they compose only tested pieces but the live loops never ran).
- Real **human elicitation** approval (tests drive a scripted client, not a human).

## 1.4 How to verify/close the Wasl gaps on a real machine (in order)

1. **Build + full test:** `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`,
   `cargo build --release -p wasl-mcp` (produces `wasl-mcpd`).
2. **Verify the Claude CLI contract (highest priority).** With a real `claude` installed, confirm:
   `claude -p --output-format json --session-id <id>` with the prompt on **stdin** returns
   `{"result": "...", "is_error": false, ...}`, and that reusing `--session-id` continues the same
   conversation. If the real flags/JSON differ, fix **only** `wasl-adapter-claude::build_argv` /
   `parse_reply` / the stdin convention — the rest is contract-agnostic. Add a test that runs the real
   `claude` through `ProcessCliInvoker` and asserts a real reply.
3. **Run a real two-node cross-host test.** Start `wasl-mcpd` on host A and host B (set `WASL_A2A_BIND`,
   `WASL_AGENT_ID`, `WASL_HOST_ID`, `WASL_AUDIT_PATH`), register B's A2A endpoint in A's registry, attach a
   real agent CLI to each node's MCP stdio, and drive a consult A→B. Confirm the gate elicitation surfaces
   to the human and the reply comes back. Then repeat over SSH/WAN.
4. **Real transfer:** exercise `wasl-procrunner` with a real `scp -3`/`rsync` between two hosts.
5. **Then** build the **Codex** and **Gemini** adapters (new crates mirroring E3), each verified against its
   real CLI's protocol before being offered.

## 1.5 Remaining Wasl work
- Codex + Gemini `AgentAdapter`s (❌ not built).
- (Optional cleanup) fully retire `wasl-daemon` by migrating `wasl-e2e`'s dispatch half onto A2A, then
  `git rm wasl-daemon`. Deliberately deferred: redundant with `wasl-node`'s e2e, high churn, low value.
- Config/peering ergonomics for `wasl-mcpd` (peer list from a file, richer AgentCards).
- Harden the `run()` serve loops once they can be exercised live.

---

# PART 2 — MARSAD

> Marsad was set aside earlier to focus on Wasl. Its status below is the honest inventory from
> `Marsad/docs/GAPS.md` (the authoritative readiness file) plus its `ARCHITECTURE.md`. **Nothing in Marsad
> changed this cycle** beyond that documentation.

## 2.1 Architecture

- **Desktop = Tauri** (Rust core + React/TS UI). **Phones = fully native** (iOS SwiftUI, Android Compose)
  over the shared Rust core via **UniFFI**. Native code is UI + thin glue only.
- **Never parse rendered terminal output.** Drive `claude` as a persistent process
  (`claude --print --input-format stream-json --output-format stream-json --verbose`, NDJSON over stdio) +
  hooks + the permission control protocol. All state/usage/gates come from these structured surfaces.
- **Session liveness is independent of the GUI.** A detached supervisor daemon (`marsad-agent`) owns every
  `claude` process; the GUI is a client. Liveness container: **tmux** `marsad-<id>` (Linux/macOS),
  **ConPTY** (Windows). On relaunch: re-adopt live container → else `claude --resume <session_id>` → else
  mark **Stale**. Session states: `starting·working·needs·error·done·stale·detached`.
- **Local-first, no cloud.** Continuous snapshots; export/import `.marsad.json`.

**Rust core crates** (`crates/`): `marsad-proto` (B01, wire/IPC types), `marsad-events` (B02, reducer →
`SessionState`/usage/gate), `marsad-gates` (B03), `marsad-diff` (B04), `marsad-format` (B05),
`marsad-store` (B06, persistence), `marsad-hosts` (B07, SSH config/health), `marsad-transport` (B08, NDJSON
line transport: LocalIpc/SshStdio/fake), `marsad-lifecycle` (B09, reconnect-or-resume + `Container` trait),
`marsad-notify` (B10), `marsad-viewmodel` (B11, dashboard aggregation — port of `renderVals()`),
`marsad-core` (B12, facade + UniFFI surface + client of the agent), `marsad-agent` (B13, **detached
supervisor daemon**). Plus the Workstreams upgrade crates: `marsad-workstream`, `marsad-store` ext,
`marsad-core` ext, `marsad-viewmodel` ext. (Also present: `marsad-winshim` for ConPTY.)

**App packages:** `apps/desktop/src-tauri` (D01, Tauri commands + event bridge over core),
`apps/desktop/src` (D02, React/TS UI), `apps/ios` (SwiftUI), `apps/android` (Compose).

## 2.2 Status (from `docs/GAPS.md`)

✅ **VERIFIED HERE (in the sandbox):**
- Rust core library crates incl. the Workstreams upgrade (`marsad-workstream`, `-store`, `-core`,
  `-viewmodel`): unit-tested, 100% line+region where gated. They compute correctly given inputs.
- Desktop **frontend against the MOCK backend**: Board (L1), GraphScreen (L2), graph layout, `useVm`,
  `api.ts`, app-wiring. **301 Vitest tests pass; `tsc --noEmit` clean.** The board→graph→session flow
  works **in the mock harness**.

🟡 **UNTESTED HERE (needs the real runtime):**
- **`apps/desktop/src-tauri` was NEVER COMPILED** (no GTK/webkit in the sandbox). The 10 Tauri commands +
  the `VmUpdate` envelope arms were validated only via a standalone harness crate. The real
  frontend→invoke→core→vm-update path has **never run** — there may be Tauri-level compile/wiring errors.
  "UI hooked to functionality" is TRUE for the mock path, UNVERIFIED for the real app.
- Live integration tests `tests/integration/{i03_lifecycle_real, i09_gui_agent_live, i10_core_e2e_live,
  i11_windows_lifecycle}` — spawn real tmux/`claude`; not runnable headless.
- Playwright e2e (`apps/desktop/tests/e2e/board-flow.spec.ts`) — written; `playwright.config.ts` webServer
  is a TODO, never run against a live dev server.

❌ **NOT BUILT:**
- **Codex / Gemini drivers (Phase 2).** Only claude is wired. Research-grounded plan exists in
  `docs/specs/phase2-drivers.md` (P2a–P2g). Do not offer a driver in the UI until its full chain is green
  against the real CLI.
- **Marsad↔Wasl live connector** (the Marsad-side endpoint that would receive Wasl's gate + observation
  feed as Wasl's Tier-3 authority).

## 2.3 First steps on a machine with the real runtime (from GAPS.md)
1. **Actually build `apps/desktop/src-tauri`** (`cargo build`, then `cargo tauri dev`); fix any real
   compile/wiring errors in the workstream commands / events envelope. **Highest-value unblock** — first
   time the real app path is exercised.
2. Run the live integration tests (i03/i09/i10/i11) and the Playwright e2e against a running dev server.
3. Then Phase-2 drivers per `docs/specs/phase2-drivers.md`, test-first against **captured real CLI
   transcripts**.

---

## 3. Methodology to keep using (the anti-cheat process)

Per block: **spec** (`docs/specs/*.md`, authoritative) → **isolated TEST-agent** writes `tests/` only
(black-box, compiled against the public API; must fail test-first; no `#[ignore]`/`todo!`/tautologies) →
**orchestrator gates the tests for cheats** → **CODE-agent** writes `src/` only → **orchestrator
INDEPENDENTLY re-runs** tests + real coverage **exit code** + clippy + fmt + `git diff` scope before
committing. Never trust an agent's "done"/coverage claim — re-run it yourself (a real bug and a false
"gate exits 0" were both caught this way). **If it can't be verified in your environment, label it UNTESTED
THERE — do not mark it done.**

## 4. What the testable environment needs
- **Rust** toolchain (see each repo's `rust-toolchain.toml`), `cargo-llvm-cov`, `clippy`, `rustfmt`.
- A real **`claude` CLI** (and later `codex`, `gemini`) for the last-inch / driver work.
- **Two hosts + SSH** (or two machines) for real cross-host A2A + scp/rsync.
- For Marsad desktop: **GTK/webkit + Tauri** prerequisites, Node/pnpm, and **tmux** (ConPTY on Windows).
- For Marsad live tests: a real `claude` + tmux; for Playwright: a live dev server (wire the webServer).

## 5. Quick status summary
- **Wasl:** the full **MCP→A2A send + A2A receive**, gated + audited, same/different host, is **built and
  proven end-to-end over loopback**. Untested here: the **real CLI last inch** (claude contract is an
  assumption; codex/gemini not built), **real cross-host/scp**, and the **live serve loops**.
- **Marsad:** **core logic + mock-backed desktop UI are verified**; the **real Tauri desktop app was never
  compiled**, live/agent tests never run, and **codex/gemini drivers not built**.
