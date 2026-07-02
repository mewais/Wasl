# Operator API — v1 wire contract (authoritative)

The **Operator API** is an optional, versioned HTTP+SSE surface exposed by a Wasl node. It lets an
external **operator** (a human-facing console, an automated **authority**, or a passive **observer**)
watch everything the node does and decide Tier-3 (external-authority) permission gates. Wasl is
consumer-agnostic: this document is the ONLY contract between a node and any consumer, and it never
names one. The serving crate is `wasl-operator` (see `docs/specs/operator.md` for the Rust side).

Two roles, one surface:
- **Observer** — consumes `GET /v1/events` (and `GET /v1/gates` snapshots). Read-only.
- **Authority** — additionally answers gates via `POST /v1/gates/{id}/decision`.

## Conventions (normative)

- All bodies are JSON, UTF-8. Requests with bodies MUST send `Content-Type: application/json`.
- Timestamps are RFC 3339 UTC with millisecond precision: `"2026-07-02T12:00:00.000Z"`.
- Durations on the wire are integer milliseconds (`*_ms`, non-negative).
- **Unknown-field tolerance:** consumers MUST ignore unknown JSON fields and unknown SSE event
  types. Producers MAY add fields/event types within v1 (additive-only; see §Versioning).
- `gate_id` is the node's own identifier for one surfaced gate, unique for the node process
  lifetime (shape is opaque to consumers; the reference implementation mints `"gate-1"`,
  `"gate-2"`, …). It is DISTINCT from `request.id`, which is the initiating tool call's id and may
  repeat. Consumers MUST correlate by `gate_id` only.
- Embedded objects reuse the Wasl wire forms verbatim (pinned by the fixtures, §Fixtures):
  - `GateRequest`: `{"id", "kind": "consult"|"command"|"message"|"transfer", "from", "summary",
    "preview"}` — `from` is a plain string agent id; `summary`/`preview` are human-readable and
    secret-free by construction.
  - `AuditEntry`: `{"seq", "kind": "consult"|"command"|"message"|"transfer"|"decision", "actor",
    "targets": [..], "summary", "decision": null | {"decision":"approve"} |
    {"decision":"deny","reason":"..."}}`.
- **Security (deployment, non-normative):** v1 defines no authentication. The decision endpoint
  carries send-approval authority — nodes MUST default to a loopback/trusted-interface bind and
  deployments are responsible for access control. Auth is a candidate additive v1.x concern
  (e.g. a bearer header), never a silent semantic change.

## Versioning

- The version is the URL path prefix (`/v1/...`). Everything below is **v1, frozen**.
- Within v1, changes are **additive only**: new optional response fields, new SSE event types, new
  error `code` values. Nothing existing is removed, renamed, retyped, or given new semantics.
- Any breaking change ships as `/v2/...` alongside a still-working `/v1`.
- The `hello` frame carries `"api_version": 1` so stream-only consumers can verify what they speak.

---

## `GET /v1/info` — node identity

`200 OK`, `application/json`. Exactly:

```json
{ "agent_id": "wasl-a", "host_id": "host-1", "version": "0.1.0" }
```

`agent_id`/`host_id` are the node's configured identity; `version` is the node's crate version.

## `GET /v1/events` — the event stream (SSE)

`200 OK`, `Content-Type: text/event-stream`. Standard Server-Sent Events:

- `event:` — the frame type: `hello`, `gate_requested`, `gate_decided`, or `audit`.
- `data:` — one line of compact JSON, the frame payload (schemas below). The payload does NOT
  repeat the frame type; the `event:` line is authoritative.
- `id:` — the node-global, monotonically increasing frame sequence number (`u64`, starts at 1),
  present on every frame EXCEPT `hello`. Sequence numbers are assigned at publish time and shared
  by all subscribers: two connected observers see the same frame with the same `id`. A **gap** in
  `id` on one connection means that connection missed frames (it lagged; see reconnect below) —
  the durable record is never affected (audit entries are persisted before broadcast).
- Comment lines (`: ka`) are sent periodically (~15 s) as keep-alives; clients MUST ignore them
  (per the SSE spec).

Frame order on one connection is ascending `id`; `gate_requested` always precedes its
`gate_decided`. No other cross-frame ordering is guaranteed (in particular, do not assume where a
gate's audit `decision` entry falls relative to its `gate_decided` frame).

### Frame: `hello` (first frame on every connection; connection-local, no `id:`)

```json
{
  "api_version": 1,
  "agent_id": "wasl-a",
  "host_id": "host-1",
  "version": "0.1.0",
  "last_seq": 42,
  "resume": "snapshot"
}
```

- `last_seq` — the sequence number of the last frame published before this subscription (0 if
  none). The first broadcast frame this connection receives will have `id > last_seq`.
- `resume` — how to recover state after (re)connecting. In v1 this is always `"snapshot"`:
  re-fetch `GET /v1/gates` (see reconnect semantics below).

### Frame: `gate_requested`

Published when a gate is surfaced through the operator hub (Tier-3 external mode, decidable) or
mirrored into the stream for observation (e.g. a Tier-1 elicitation gate; observe-only).

```json
{
  "gate_id": "gate-1",
  "mode": "external",
  "decidable": true,
  "request": {
    "id": "call-9",
    "kind": "message",
    "from": "wasl-a",
    "summary": "message to 1 peer(s)",
    "preview": "hi bob"
  },
  "requested_at": "2026-07-02T12:00:00.000Z",
  "timeout_ms": 120000,
  "deadline": "2026-07-02T12:02:00.000Z"
}
```

- `mode` — the gate mode it was surfaced in: `"elicitation"` | `"external"` (`"prepare_commit"`
  reserved; Tier-2 gates do not flow through this surface in v1).
- `decidable` — `true` iff this gate accepts `POST /v1/gates/{gate_id}/decision`. Observe-only
  gates (`decidable: false`, e.g. mirrored elicitations) never appear in `/v1/gates` and POSTing
  to them yields `404`.
- `deadline` = `requested_at + timeout_ms`; after it passes an undecided decidable gate resolves
  itself as `dismissed` / `timeout` (a `gate_decided` frame follows).

### Frame: `gate_decided`

Published exactly once per surfaced gate, when it resolves.

```json
{
  "gate_id": "gate-1",
  "outcome": "approved",
  "decided_by": "operator",
  "reason": null,
  "decided_at": "2026-07-02T12:00:03.500Z"
}
```

- `outcome` — `"approved"` | `"denied"` | `"dismissed"`.
- `decided_by` — who/how:
  - `"operator"` — resolved via `POST /v1/gates/{id}/decision` (any of the three outcomes).
  - `"elicitation"` — resolved by the node's own MCP client answering the elicitation (the gate
    was observe-only on this surface).
  - `"timeout"` — the decision window elapsed; `outcome` is always `"dismissed"`.
- `reason` — a string iff `outcome == "denied"` (the authority's reason, or the node's default
  `"denied by operator"` when omitted in the POST); otherwise `null`.

### Frame: `audit`

The observation feed: published for EVERY entry durably appended to the node's audit log —
including auto-approved / non-gated sends and gate decision records. The payload wraps the audit
entry (leaving room for additive frame-level metadata):

```json
{
  "entry": {
    "seq": 3,
    "kind": "decision",
    "actor": "wasl-a",
    "targets": ["wasl-b"],
    "summary": "gate Message",
    "decision": { "decision": "approve" }
  }
}
```

`entry.seq` is the audit log's own sequence (independent of the SSE `id:`). Durability ordering is
guaranteed: an `audit` frame is broadcast only AFTER the entry was durably appended; a frame lost
to a lagging connection is still in the durable log.

### Reconnect semantics (decision: explicit snapshot, not Last-Event-ID replay)

The server accepts and **ignores** the `Last-Event-ID` request header; v1 offers **no replay**. On
connect and after every reconnect a consumer MUST: (1) open `/v1/events`, (2) read `hello`, (3)
fetch `GET /v1/gates` and reconcile by `gate_id` (frames already received for a gate absent from
the snapshot mean it resolved; a snapshot gate with no frames yet means the frame was missed).

Why snapshot over replay: pending gates are live rendezvous state with deadlines, not a journal —
replaying stale `gate_requested` frames after their window invites deciding dead gates (the POST
answers `404`/`409` anyway, but the UX is wrong); the node's durable history is the audit sink
file, and duplicating a replay buffer in the hub would create a second, weaker source of truth;
the snapshot is O(pending) and race-free (`last_seq` in `hello` + gap-free `id`s tell the consumer
exactly whether it missed anything). The same rule covers mid-stream lag: on an `id` gap, re-fetch
`/v1/gates` (the stream itself stays open — see the lag policy in `operator.md`).

## `GET /v1/gates` — pending-gate snapshot

`200 OK`, `application/json`. All currently pending **decidable** gates, ascending `requested_at`
(ties by `gate_id`):

```json
{
  "last_seq": 42,
  "gates": [
    {
      "gate_id": "gate-1",
      "request": {
        "id": "call-9",
        "kind": "message",
        "from": "wasl-a",
        "summary": "message to 1 peer(s)",
        "preview": "hi bob"
      },
      "requested_at": "2026-07-02T12:00:00.000Z",
      "age_ms": 1500,
      "deadline": "2026-07-02T12:02:00.000Z"
    }
  ]
}
```

`age_ms` = server-clock now − `requested_at`. `last_seq` = the frame sequence at snapshot time
(lets a consumer order the snapshot against its stream position).

## `POST /v1/gates/{gate_id}/decision` — decide a gate

Request body:

```json
{ "outcome": "approved" }
{ "outcome": "denied", "reason": "not during business hours" }
{ "outcome": "dismissed" }
```

`outcome` ∈ `"approved" | "denied" | "dismissed"` (required); `reason` optional, only meaningful
with `"denied"` (ignored otherwise; when omitted on a denial the node records
`"denied by operator"`).

Responses:
- **`204 No Content`** — decision accepted: the waiting gate resolves with this outcome, a
  `gate_decided` frame (`decided_by: "operator"`) is broadcast, and the gate leaves `/v1/gates`.
  204 means the decision was delivered to the gate — not that any send subsequently succeeded
  (watch the `audit` feed for that).
- **`409 Conflict`** — the gate was already decided (by an earlier POST, elicitation, or timeout).
  Distinguishable because the node remembers resolved gate ids for its process lifetime.
- **`404 Not Found`** — `gate_id` was never a decidable pending gate on this node (unknown id, or
  an observe-only gate).
- **`400 Bad Request`** — malformed body: not JSON, missing/unknown `outcome`, wrong types.

Concurrent POSTs race safely: exactly one wins (`204`); the rest get `409`.

Error bodies (all 4xx above except where a framework-level 400 has no JSON body to parse) are:

```json
{ "error": "unknown_gate",    "detail": "gate-9" }
{ "error": "already_decided", "detail": "gate-1" }
{ "error": "malformed",       "detail": "<parser message, free-form>" }
```

`error` is a closed (but v1-extensible, additive) code set; `detail` is free-form and NOT part of
the contract.

---

## Golden fixtures — `crates/wasl-operator/fixtures/operator-v1/`

The canonical JSON bodies for everything above live as checked-in files. They are the
machine-checkable half of this contract: **this repo's** conformance tests assert that the live
types serialize to exactly these values (JSON value equality — key order irrelevant), and **any
consumer** vendors a verbatim copy of the directory and asserts its parsers/renderers accept every
file. SSE fixtures contain the `data:` payload only; the SSE framing (`event:`/`id:` lines) is
specified prose-only above.

| file | is |
|---|---|
| `info.json` | `GET /v1/info` body |
| `hello.json` | `hello` frame payload |
| `gate_requested_external.json` | decidable Tier-3 `gate_requested` |
| `gate_requested_elicitation.json` | observe-only mirrored `gate_requested` |
| `gate_decided_approved_operator.json` | `gate_decided`, approved by operator |
| `gate_decided_denied_operator.json` | `gate_decided`, denied with reason |
| `gate_decided_dismissed_timeout.json` | `gate_decided`, timeout → dismissed |
| `gate_decided_approved_elicitation.json` | `gate_decided`, decided by elicitation |
| `audit_decision.json` | `audit` frame: a gate decision entry |
| `audit_message.json` | `audit` frame: an auto-approved send (decision `null`) |
| `gates_snapshot.json` | `GET /v1/gates` body (one pending gate) |
| `decision_request_approved.json` | POST body, approve |
| `decision_request_denied.json` | POST body, deny + reason |
| `error_unknown_gate.json` | 404 body |
| `error_already_decided.json` | 409 body |
| `error_malformed.json` | 400 body (a representative `detail`) |

**Fixture policy:** the directory is **append-only for the life of v1**. Editing or deleting an
existing fixture IS a breaking change and requires `/v2` (with a new `fixtures/operator-v2/`).
New additive fields/frames get NEW fixture files. A consumer whose contract tests pass against its
vendored copy is compatible with every node that passes against the same copy — that shared
directory, not any shared code, is the compatibility boundary.
