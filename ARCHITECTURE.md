# Wasl architecture

The target design is §5 (and the rules in §1) of the Wasit repo's `ARCHITECTURE.md`; this file
only records how Wasl implements it and where it deviates.

## Shape

```
agent CLI ──stdio MCP──▶ wasl mcp ──control socket──▶ host daemon
                                                     ├─ RuntimeService (wasit-runtime: sessions)
                                                     └─ NodeService    (wasl-node: wasl.*)
host daemon A ◀──bridge──▶ hub (RoutingService) ◀──bridge──▶ host daemon B
```

- **Addressing**: a peer is a session, `AgentId = "<host>/<session>"`.
- **Topology** (`wasl.topology.set`, per workstream, versioned, idempotent) is pushed by the
  supervisor and journaled by the node. Changes apply live. A session with no outgoing edge has
  no peers (`list_peers` is empty with a note; `delegate` says "no connections configured").
- **Tasks**: `delegate` creates one task per target and returns at once. Every state change is
  an upsert record (with a node-wide revision) in `<data>/wasl/journal.ndjson`, submitted under
  the state lock and awaited after it; the journal is compacted on open and its sequence numbers
  are the cursors of the `wasl.tasks` / `wasl.edges` / `wasl.spawns` topics. `task_wait` waits on per-task
  `watch` channels, never on a lock.
- **Handoff gates**: an `ask` edge raises `GateRequest::Handoff` on the sending session through
  Wasit's external-gate facility (`SessionManager::raise_gate`, journaled as session notes, so
  it shows in `pending_gates` and survives restarts). The answer is an `answer_gate` command,
  which the node sees as an `input` event when replaying the session's events from the cursor
  recorded before the raise. Allow once / always allow (`permission_updates` set; rewrites the
  edge, emits a `wasl.edges` event) / deny. Without a supervisor connection (no peer with role
  `hub`/`gui`/`supervisor`), the task waits for `wasl.task.decide` from the MCP front-end,
  which elicits or denies.
- **Routing**: same host = direct call. Other hosts = `wasl.node.*` requests addressed to the
  host and sent on the `Uplink` (the hub's connection into this daemon, or the hub's own links
  when the daemon is the hub). The outbox pump sends what the journal says is owed (deliveries,
  withdrawals, state reports), each as its own task, retrying retryable failures on backoff and
  at once when the uplink changes. `PeerTransport` allows other transports.
- **Delivery**: the receiving node queues tasks FIFO per session, waits for the session to be
  idle (continuing it if its process ended), records the session cursor and usage, sends the
  prompt (provenance header with a `[wasl task <id>]` marker + instruction) and follows the
  session's events: tool calls → progress, turn completed → result (final reply + usage delta),
  exit/loss → failed. A restart resumes from the recorded cursor (and resubmits only if the
  prompt never reached the journal). Cancel/deadline interrupt the running turn or dequeue.
- **Safety**: causation chain + hop on every task (re-entry ⇒ `cycle`, depth ⇒ `max_depth`),
  per-task deadline at both ends (⇒ `expired`), optional per-workstream budget.

- **Spawning** (`wasl.spawn`, MCP `spawn_agent` / `dismiss_agent`): Wasl never launches a
  process. The node asks its `SpawnAuthority` — the supervisor — in short calls:
  `wasl.supervisor.spawn_plan` (policy, limits, defaults resolved; whether a human must approve),
  then, after the human, `wasl.supervisor.spawn_launch` (launch, record in the workstream with a
  connection from the caller, return the new topology), which must be idempotent by the
  request id. In between the node raises `GateRequest::Spawn` on the calling session (choices:
  allow once / always allow spawning in this workstream / always allow spawning and handoffs to
  spawned agents / deny with an optional reason) and watches it like a handoff gate. Each
  request is journaled per request id (`asking → launching → launched → done | failed`): the MCP
  front-end reuses the id when it re-sends after a daemon restart, recovery resumes watching or
  launching, so nothing is started twice. With an instruction, the first task is delegated over
  the new connection (gated by its policy like any other). A daemon that is not the hub forwards
  `wasl.supervisor.*` over the hub's connection (`UplinkSpawnAuthority`, host-less requests the
  hub serves itself); the hub sets its own authority. No authority, or no supervisor reachable
  within `spawn_connect_wait`: `spawn_unavailable`. `list_peers` carries `spawnable` (hosts,
  drivers, models, remaining) from the authority.
- **How a request ended** (`wasl-proto/src/end.rs`, the one vocabulary for spawn requests and
  handoff gates): an `End` (`EndCode` machine word, plus the supervisor's error code as `cause`
  for `refused` / `launch_failed`). A spawn's final state (`Failed{code, message, end}`, or
  `Done` = `launched`) is journaled with it; its `message` is the sentence the agent's tool
  result carries and the human-facing detail. A final refusal before any gate (the supervisor's
  plan refusal, a deadline already past) is a journaled record too (no plan). The same code and
  sentence go to the session as the reason of the gate's resolution (`resolve_gate(.., reason)`,
  Wasit `GateReason`), and the node publishes every ended request once on the `wasl.spawns`
  topic (`SpawnEnded`; cursors are the journal's, replay yields every ended request). Not
  recorded: a supervisor that cannot be reached (nobody to tell; `RequestFailed` in the
  diagnostics, and the agent may ask again).

## Diagnostics

Wasl logs status only (Wasit's `ARCHITECTURE.md` §9): ids, sizes, counts, timings, states and
error codes, never what an agent or a person wrote (prompts, titles, results, progress text,
handoff reasons, agent and session names and descriptions, spawn text) and never anything read
back from the journal or the audit trail.

- Every log record is a `WaslEvent` (`wasl-proto/src/diag.rs`, declared with `diag_events!`):
  there is no `tracing` call in Wasl. Ids are `Ident`s (`host` and `session` are two fields: an
  agent address has a `/`); workstream ids are not written (a workstream is named by the
  supervisor).
- Errors reach a record by their source: `ErrorInfo::io` for an `io::Error`; `ours` for the
  node's own journal and audit-trail errors (an operation and the OS's reason; the audit
  reader's errors never quote a line); `foreign` (code, length, fingerprint) for everything else
  (`RpcError`, `CommandError`, transport and stream messages, a `serde` error read from the
  journal). `crates/wasl/tests/diag_sites.rs` pins every `ErrorInfo::ours(` and `DiagText::new(`
  site.
- Failures that used to be swallowed or only debug-logged (a failed decision after the human
  clicked, journal and audit write failures, resubscribes, peer-status errors, withdrawals) are
  recorded events; control flow and retries are unchanged.
- `wasl serve` writes `<data>/diag/daemon.ndjson`; `wasl bridge` appends to the shared
  `<data>/diag/aux.ndjson`; `wasl mcp` has no sink (it knows a socket and a session, not a data
  directory; the daemon's `wasl.request` / `wasl.request_failed` records show what it asked) and
  its panic prints and records nothing. `WASL_LOG=<filter>` is the developer override: plain
  `tracing` text on stderr and the usual panic message. `tracing` output of libraries is only
  counted.
- Not built yet: the task / gate / spawn trail (a projection of the journal) and trace ids.

## Wasit additions made for Wasl

- `wasit-runtime`: external gates (`raise_gate` / `resolve_gate`), `CreateSession::id`.
- `wasit-control`: `Uplink`, `ControlServer::connection_changes`.
- `wasit-core`: `GateRequest::Handoff`, `GateRequest::Spawn`, `McpServerConfig::trusted` (rendered per driver so the
  CLI does not prompt for Wasl's tools).

## Deviations

- The budget is accounted per sending node (the cost of results it received), not globally.
- `message` is a task of mode `message` (gated like any delegation; its result is ignored).
- The multi-daemon tests run on Linux, macOS and Windows (local endpoints are Unix sockets or named pipes).
- `transfer` is not implemented (a new task mode / node methods later).
- A spawn gate stays up if the asking turn is interrupted (as a handoff gate does); answering it
  later still starts the worker.
