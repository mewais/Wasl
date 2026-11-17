# Wasl v2 — MCP→A2A sender + A2A-wrapped receiver — master design

## Goal (non-negotiable)
Agents must be able to FULLY **send** (data, commands, consults, plain messages) AND **receive**, between
agents on the **same host or different hosts**. Wasl is an MCP tool on the sender side and an A2A server
wrapper on the receiver side. Every send is gated (permission-before-send) and audited.

## Runtime picture
Each host runs a **wasl node daemon**. A node is BOTH:
- an **A2A server** (HTTP + JSON-RPC 2.0, SSE streaming) — the endpoint peers reach; publishes an Agent
  Card per local agent; this is the RECEIVE side.
- an **MCP server** (stdio) each local agent CLI attaches to — exposes the tools (consult/command/message/
  transfer + reply); this is the SEND side (how a local agent initiates).

Send path:  local agent A --MCP tool--> node A [gate + audit] --A2A client--> node B (HTTP).
Recv path:  node B [A2A server] --TaskExecutor--> AgentAdapter --inject--> agent B's live session
            --reply--> A2A artifact --> streamed back to node A --> A's MCP tool result.

Because claude/codex are MCP clients, NOT A2A servers, the node's **AgentAdapter** is the "last inch"
that bridges an A2A Task to the real CLI (inject a turn, capture the reply). A stable A2A `contextId` maps to
a stable long-lived session, so context persists across messages.

## What we KEEP (already real + tested — reuse, do not rebuild)
`wasl-proto` (internal types), `wasl-gate` (gate logic), `wasl-registry` (peer resolution — extend with
A2A URLs), `wasl-mailbox` (multicast fan-out + reply correlation), `wasl-audit` (log over a `Sink`),
`wasl-transfer` (argv + `Runner`), `wasl-server` (`Gatekeeper` gate+audit spine), `wasl-daemon` (routing
core — its `Mesh` seam is exactly where A2A plugs in). An external supervisor (formerly a dedicated crate)
is now just any implementer of the generic `Surfacer` (gate authority) + audit `Sink` (observation) seams.

## What we RETIRE
`wasl-mesh` (bespoke socket transport) — superseded by A2A over HTTP. Remove after the A2A path lands.

## Block DAG (each block: spec → isolated test-agent → gate for cheats → code-agent → INDEPENDENT verify)

Phase A — protocol
- **A1 `wasl-a2a-types`** (L0 pure): faithful A2A wire types — AgentCard, Message, Part (Text/File/Data),
  Task, TaskState (submitted/working/input-required/completed/failed/canceled), TaskStatus, Artifact, and
  the JSON-RPC params/results for `message/send`, `message/stream`, `tasks/get`, `tasks/cancel`, plus the
  SSE update events. serde matches the A2A JSON shapes (camelCase: `contextId`, `taskId`, `messageId`).
  TESTABLE HERE: fully (serde round-trips, 100%).

Phase B — A2A transport (HTTP; loopback-testable on 127.0.0.1 — different ports stand in for different hosts)
- **B1 `wasl-a2a-server`** (L2 IO): axum HTTP server; accepts JSON-RPC; drives a `TaskExecutor` trait;
  manages Task lifecycle; returns results + SSE stream. Fake executor in tests.
  TESTABLE HERE: yes, over loopback (real HTTP roundtrip). The bind/serve loop shell is thin.
- **B2 `wasl-a2a-client`** (L2 IO): reqwest client; `send_message(url, msg) -> Task/result` (+ streaming).
  TESTABLE HERE: yes, against B1 over loopback.

Phase C — the last inch
- **C1 `wasl-adapter`**: the `AgentAdapter` trait (`deliver(context_id, kind, text) -> reply`) + a fake +
  the `TaskExecutor` that maps an A2A Task to an adapter call. TESTABLE HERE: the trait + fake + executor
  mapping (fully). The CONCRETE CLI-driving adapters (E3) are a separate block and are **UNTESTED HERE**.

Phase D — compose + prove
- **D1 `A2aMesh`** (impl of `wasl_daemon::Mesh` over the B2 client): so the tested daemon routing dispatches
  over A2A (reuse multicast/correlation). TESTABLE HERE: yes, against a loopback B1 server.
- **D2 `wasl-node`** (binary + composition + THE loopback e2e): compose A2A server (executor=adapter) + MCP
  handler (send over A2aMesh) + gate + audit(FileSink) + registry. The e2e spins TWO nodes on
  127.0.0.1:pA / :pB and drives a real consult / command / message / data-share A→B with a FAKE adapter on
  B, asserting the reply flows through the whole stack, gate approves, audit records — proving send+receive
  over same/diff "hosts". TESTABLE HERE: the composition + loopback e2e (yes). The binary's real stdio/serve
  main loop is a shell (compile-only).

Phase E — real edges (build; mark verification honestly)
- **E1 `FileSink`** (durable append-JSONL audit `Sink`). TESTABLE HERE: yes (temp files).
- **E2 `ProcessRunner`** (real `Runner`: spawn scp/rsync). TESTABLE HERE: the process-drive plumbing via a
  stand-in command (e.g. a script); the REAL scp between two hosts is **UNTESTED HERE** (no 2nd host/scp).
- **E3 real `AgentAdapter`s** (claude via stream-json/tmux; later codex). Build to
  compile + unit-test any pure parsing. Driving a REAL CLI is **UNTESTED HERE** (no CLIs). Clearly labeled.

## TESTABILITY HONESTY MATRIX (what "done" can mean in THIS environment)
| Capability | Verifiable here? |
|---|---|
| A2A types / serde | ✅ fully |
| A2A server receive + Task lifecycle + SSE | ✅ loopback (real HTTP) |
| A2A client send | ✅ loopback |
| Full send→gate→A2A→receive→adapter→reply→back | ✅ loopback, with a FAKE adapter |
| Multicast fan-out + correlation over A2A | ✅ loopback |
| Durable file audit | ✅ temp files |
| Runner process plumbing | ✅ via stand-in command |
| Real scp/rsync transfer between 2 hosts | ❌ UNTESTED HERE (no 2nd host / scp) |
| Driving a REAL claude/codex CLI (the last inch) | ❌ UNTESTED HERE (no CLIs) |
| True cross-HOST over SSH/WAN | ❌ UNTESTED HERE (loopback different-ports is the stand-in; same code path) |
| Real human elicitation approval | ❌ (driven by a scripted client, not a human) |

RULE: a block is only "done/green" for the rows marked ✅ that its tests actually exercise. Anything in a
❌ row is reported as **UNTESTED HERE** with exactly what real environment is needed to verify it. No block
claims end-to-end "it works" on the strength of fakes alone — the fakes prove the wiring, not the last inch.

## Methodology (unchanged, enforced)
Per block: I write the spec + scaffold; a Sonnet TEST-agent writes ONLY tests/ (black-box, must fail
test-first, no ignore/todo/tautology/skip); I gate the tests for cheats and confirm they fail correctly; a
Sonnet CODE-agent writes ONLY src/; then I INDEPENDENTLY re-run tests + coverage + clippy + fmt and check
the diff touched only src/ before committing. IO shells are verified by compile + loopback/smoke, and their
un-verifiable edges are labeled UNTESTED HERE.
