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
  are the cursors of the `wasl.tasks` / `wasl.edges` topics. `task_wait` waits on per-task
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

## Wasit additions made for Wasl

- `wasit-runtime`: external gates (`raise_gate` / `resolve_gate`), `CreateSession::id`.
- `wasit-control`: `Uplink`, `ControlServer::connection_changes`.
- `wasit-core`: `GateRequest::Handoff`, `McpServerConfig::trusted` (rendered per driver so the
  CLI does not prompt for Wasl's tools).

## Deviations

- The budget is accounted per sending node (the cost of results it received), not globally.
- `message` is a task of mode `message` (gated like any delegation; its result is ignored).
- The multi-daemon tests use Unix sockets and run on Linux/macOS; Windows CI runs unit tests.
- `spawn_agent` and `transfer` are not implemented (new task modes / node methods later).
