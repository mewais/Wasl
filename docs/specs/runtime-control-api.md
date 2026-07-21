# Runtime Control API — v1 wire contract (authoritative)

The **Runtime Control API** is the versioned **local, inter-process** control plane between `wasl-session`
(R3, `docs/specs/session.md`) and the runtime owner (the `wasl-mcpd` process that owns the R1 actor + child
managed session). It exists because a `wasl-session` process **cannot** reach an actor + child owned by a
*different* `wasl-mcpd` process by linking the same crate (impl-plan Phase 3.5) — it must speak a wire.

It is modeled directly on `docs/specs/operator-api.md`: a snapshot + a sequenced event stream (SSE-like) with
`Lagged`/`Truncated` semantics, versioned under a `/v1` prefix, local-authorization posture, golden fixtures.
The transport is a **Unix-domain socket on unix** (loopback-HTTP elsewhere); the HTTP/SSE shapes are the same
in both.

**Boundary invariant (LOCKS §6-open-question "ProtocolEvent stays internal"):** the internal R1
`ProtocolEvent` (driver-normalized wire) is **NEVER** exposed on this API. Only the PUBLIC vocabulary crosses
the wire — `TurnEvent`, `TargetStatus`, `TurnReceipt`, and the small request bodies below. This is the same
"internal vs public" split as `wasl-runtime` §5.

The serving side is a small crate wrapping the R1 provider (analogous to `wasl-operator` wrapping the gate
hub); the consuming side is the R3 client. This document is the ONLY contract between them.

## Conventions (normative — mirror operator-api.md)

- All bodies are JSON, UTF-8. Requests with bodies MUST send `Content-Type: application/json`.
- Timestamps are RFC 3339 UTC millisecond precision: `"2026-07-02T12:00:00.000Z"`.
- Durations on the wire are integer milliseconds (`*_ms`, non-negative).
- **Enum tagging / casing:** enums are internally-tagged `{"type": "...", ...}` with `snake_case` type tags,
  matching `TurnEvent`'s serde form (R1 §5). Booleans/ints as-is.
- **Unknown-field tolerance:** consumers MUST ignore unknown JSON fields and unknown SSE event/`type` values.
  Producers MAY add fields / event types / `TurnEvent` variants within v1 (additive-only; §Versioning).
- `target_id` is the R1 `TargetId` string; `turn_id` is the R1 `TurnId` string; `seq` is the **per-target**
  `EventSeq` (`u64`, from 1) — NOT node-global (this differs from operator-api's node-global frame seq, because
  R1's journal is per-target; §Event stream).
- **Security (local authorization, deployment):** the default bind is a **Unix-domain socket with
  owner-only permissions** (loopback-HTTP + trusted-interface bind elsewhere). This API carries
  send/interrupt/stop/gate authority over a live session — nodes MUST default to a local/trusted bind;
  deployments own access control. Auth (e.g. a socket-peer-credential check or bearer header) is a candidate
  additive v1.x concern, never a silent semantic change. Mirrors operator-api.md §Security.

## Versioning (mirror operator-api.md)

- The version is the URL path prefix (`/v1/...`). Everything below is **v1, frozen**.
- Within v1, changes are **additive only**: new optional fields, new SSE event types, new `TurnEvent`
  variants, new `error` codes. Nothing existing is removed, renamed, retyped, or given new semantics.
- Any breaking change ships as `/v2/...` alongside a still-working `/v1`.
- The `hello` frame carries `"api_version": 1`.

---

## `GET /v1/info` — runtime identity

`200 OK`, `application/json`:

```json
{ "agent_id": "wasl-a", "host_id": "host-1", "version": "0.1.0", "runtime": "managed" }
```

`runtime` is the node's active runtime kind (`"ephemeral" | "managed"`), so a client knows whether live
targets are even possible on this node.

## `GET /v1/targets` — target registry snapshot

`200 OK`, `application/json`. All currently-known targets (backs R3 `list`):

```json
{
  "targets": [
    {
      "target_id": "tgt-1",
      "key": "ctx-42",
      "capabilities": {
        "persistent_process": true, "persistent_conversation": true, "structured_turns": true,
        "streaming_output": false, "attachable": true, "interruptible": false,
        "structured_provenance": true, "resumable": true
      },
      "status": { "type": "idle", "since": "2026-07-02T12:00:00.000Z" },
      "last_seq": 12,
      "queued": 0,
      "native_session_id": "…"
    }
  ]
}
```

`capabilities` is the exact R1 `RuntimeCapabilities` (8 booleans); `streaming_output`/`interruptible` here are
`false` on claude/serve targets per the ⛔ UNKNOWN facts carried from R1/R2. `last_seq` lets a client order a
subsequent event subscription.

## `POST /v1/targets` — get-or-create a managed target (R3 `start`)

Request body:

```json
{ "key": "ctx-42", "protocol": "opencode-acp" }
```

`protocol` optional (defaults to the node's configured managed protocol: `claude` | `opencode-acp` |
`opencode-serve`). Responses:

- **`200 OK`** — created or reused (idempotent per key, R1 §10): the target object (as in `/v1/targets`).
- **`429 Too Many Requests`** — admission control refused (target explosion / per-sender cap / rate limit;
  R1 §10) → body `{"error":"admission_refused","detail":"…"}`.
- **`400 Bad Request`** — malformed body / unknown `protocol`.

## `GET /v1/targets/{target_id}` — one target's snapshot + `last_seq`

`200 OK` the target object (as above) — the R1 `snapshot() -> (status, last_seq)` rendered for one target;
lets a client capture `(status, last_seq)` atomically before subscribing (the operator-api snapshot pattern,
per-target). `404` for an unknown id.

## `GET /v1/targets/{target_id}/events` — the per-target event stream (SSE)

`200 OK`, `Content-Type: text/event-stream`. Standard SSE:

- `event:` — the frame type: `hello`, `turn_event`, or `truncated`.
- `data:` — one line of compact JSON (schemas below). The payload does NOT repeat the frame type.
- `id:` — the **per-target** `EventSeq` (`u64`, from 1), present on every `turn_event` frame (NOT on `hello`
  or `truncated`). This is R1's journal seq; two `attach`ers to the same target see the same event with the
  same `id` (multi-observer, independent cursors).
- Comment lines (`: ka`) ~15 s keep-alives; clients MUST ignore them.
- Query: `?after_seq=N` (default 0) requests the replay-then-live stream from `EventSeq(N+1)` (R1 §4.3).

Frame order on one connection is ascending `id`, gap-free, exactly-once, for `after_seq >= floor`; per turn
the ordering guarantees of R1 §5 hold (`TurnAccepted` before deltas before exactly one terminal event;
`TurnCompleted` always carries `turn_id`).

### Frame: `hello` (first frame; connection-local, no `id:`)

```json
{ "api_version": 1, "agent_id": "wasl-a", "host_id": "host-1", "version": "0.1.0",
  "target_id": "tgt-1", "last_seq": 12, "floor": 3, "resume": "snapshot" }
```

- `last_seq` — highest journalled seq at subscription time (the first live frame has `id > last_seq`).
- `floor` — the lowest retained seq minus one (below-floor `after_seq` ⇒ a `truncated` frame follows).
- `resume` — always `"snapshot"` in v1: on a `truncated` frame or an `id` gap, re-fetch
  `GET /v1/targets/{id}` and re-subscribe from the new `last_seq`.

### Frame: `turn_event`

The payload is exactly a serialized R1 `TurnEvent` (internally tagged, snake_case). `ProtocolEvent` is NEVER
serialized here.

```json
{ "type": "assistant_delta", "turn_id": "turn-7", "text": "PON" }
```

```json
{ "type": "turn_completed", "turn_id": "turn-7", "reply": "PONG", "certainty": "completed" }
```

Every `TurnEvent` variant of R1 §5 may appear (`session_ready`, `turn_accepted`, `turn_queued`,
`assistant_delta`, `tool_started`, `tool_finished`, `native_gate_requested`, `native_gate_resolved`,
`turn_completed`, `turn_failed`, `turn_cancelled`, `turn_expired`, `status_changed`, `process_exited`). New
variants are additive within v1 (consumers ignore unknown `type`s).

### Frame: `truncated` (Lagged/Truncated semantics)

Published when `after_seq` (at subscribe) or the client's cursor (mid-stream lag) falls below `floor`:

```json
{ "earliest": 8, "latest": 20 }
```

The client MUST re-fetch `GET /v1/targets/{target_id}` (snapshot) and re-subscribe from `latest` — it never
silently replays stale events (R1 §4.3; the operator-api reconnect discipline). This is the SSE surfacing of
R1's `StreamItem::Truncated`. **Lag policy (normative):** the server's per-target broadcast holds a bounded
number of frames; a slow subscriber that falls behind gets a `truncated` frame and the stream continues (no
close, no error frame) — the gap in `id:` is the client's signal to re-snapshot (mirrors `wasl-operator`'s
skip-and-continue lag policy, `docs/specs/operator.md`). A `Closed` recv (owner dropped) ends the stream.

## `POST /v1/targets/{target_id}/turns` — submit a turn (R3 `send`; also A2A managed executor)

Request body:

```json
{
  "delivery": { "context_id": "ctx-42", "from": "human", "kind": "message",
                "text": "remember token VIOLET-7F29", "data": [] },
  "source": { "type": "local_human" },
  "idempotency_key": "opt-key-1",
  "busy_policy": { "type": "queue", "max_wait_ms": 30000 },
  "causation": { "root_request": "req-1", "parent_request": null, "path": [], "hop": 0 }
}
```

`source` is the R1 `TurnSource` (`{"type":"agent","from":"…"}` | `{"type":"local_human"}`) — R3 `send`
ALWAYS sends `local_human` (the single-writer invariant: human input funnels through `submit_turn` exactly
like an agent turn). `causation.root_request`/`parent_request` are the R1 `Causation`'s `wasl_proto::RequestId`
fields — a `#[serde(transparent)]` newtype over `String`, so on the wire they are **plain JSON strings**
(`"req-1"` / `null`), and `causation.path` is a JSON array of `wasl_proto::AgentId` strings. `busy_policy`
optional (defaults per `kind`, R1 §4.2). `idempotency_key` optional.
Response `200 OK` = the R1 `TurnReceipt` (internally tagged):

```json
{ "type": "started", "turn_id": "turn-7", "first_seq": 13 }
```

Variants: `started` / `queued{position}` / `rejected_busy{active_turn}` / `rejected_unavailable{status}` /
`duplicate{original}`. `submit_turn` returns immediately with the receipt; the reply arrives on the event
stream (subscribe at `first_seq - 1` for atomic subscribe-then-admit). `400` for a malformed body;
`404` for an unknown target.

## `POST /v1/targets/{target_id}/cancel` — cancel a turn (R3 `interrupt`; also A2A cancel)

```json
{ "turn_id": "turn-7", "reason": "operator_request" }
```

Responses: `204 No Content` (accepted — reflected in the stream/status per R1 §7.2); `409 Conflict`
`{"error":"unsupported","detail":"target not interruptible"}` when `!interruptible` (the ⛔ UNKNOWN reality on
real substrates today — an honest refusal, not a silent no-op); `404` unknown target/turn where R1 §7.2 maps
it so (cancelling a finished turn is `204` no-op per R1). `reason` ∈ the R1 `CancelReason` set.

## `POST /v1/targets/{target_id}/gate` — resolve a native `AwaitingGate` (R3 `gate`)

```json
{ "turn_id": "turn-7", "decision": { "decision": "approve" } }
{ "turn_id": "turn-7", "decision": { "decision": "deny", "reason": "not allowed" } }
```

`decision` is the `wasl_proto::GateDecision` wire form (same as audit/operator). Routes to the actor's
`resolve_native_gate` **through the single-writer actor** (R1 §9 policy (i)); never a raw side-channel.
Responses: `204` accepted (a `native_gate_resolved` `turn_event` follows); `409`
`{"error":"gate_unresolvable","detail":"…"}` if the target is not in `AwaitingGate`; `404` unknown target.
This is the receive-side native gate — DISTINCT from Wasl's send-side gate (operator-api.md).

## `POST /v1/targets/{target_id}/shutdown` — stop a target (R3 `stop`)

```json
{ "mode": "drain" }   // or {"mode":"immediate"}
```

`204` accepted (the target moves to `Draining`/`Stopped` per R1 §7.3; a `process_exited` + `status_changed`
`turn_event` follow); `404` unknown target. `Immediate` on an injected-but-unfinished `command` results in an
`OutcomeUnknown` certainty (R1 §8), reflected in the terminal `turn_event`.

## Error bodies

All 4xx (except a framework-level 400 with no JSON body) are:

```json
{ "error": "unknown_target",    "detail": "tgt-9" }
{ "error": "admission_refused", "detail": "per-sender cap reached" }
{ "error": "unsupported",       "detail": "target not interruptible" }
{ "error": "gate_unresolvable", "detail": "target not awaiting a gate" }
{ "error": "malformed",         "detail": "<parser message, free-form>" }
```

`error` is a closed-but-v1-extensible (additive) code set drawn from the R1 `RuntimeError` taxonomy (§13);
`detail` is free-form and NOT part of the contract.

---

## Golden fixtures — `crates/<serving-crate>/fixtures/runtime-control-v1/`

As with operator-api.md, the canonical JSON bodies live as checked-in files and are the machine-checkable half
of this contract: this repo's conformance tests assert the live types serialize to exactly these values
(JSON value equality — key order irrelevant), and any consumer vendors a verbatim copy and asserts its
parsers accept every file. SSE fixtures contain the `data:` payload only; the SSE framing (`event:`/`id:`
lines) is prose-only above.

| file | is |
|---|---|
| `info.json` | `GET /v1/info` body |
| `targets_snapshot.json` | `GET /v1/targets` body (one target) |
| `target.json` | `GET /v1/targets/{id}` body |
| `create_request.json` | `POST /v1/targets` body |
| `hello.json` | `hello` frame payload |
| `turn_event_session_ready.json` | `turn_event`, session_ready |
| `turn_event_turn_accepted.json` | `turn_event`, turn_accepted |
| `turn_event_assistant_delta.json` | `turn_event`, assistant_delta |
| `turn_event_tool_started.json` | `turn_event`, tool_started |
| `turn_event_native_gate_requested.json` | `turn_event`, native_gate_requested |
| `turn_event_turn_completed.json` | `turn_event`, turn_completed (carries certainty) |
| `turn_event_turn_failed.json` | `turn_event`, turn_failed |
| `turn_event_process_exited.json` | `turn_event`, process_exited |
| `truncated.json` | `truncated` frame payload |
| `submit_request.json` | `POST …/turns` body (source local_human) |
| `receipt_started.json` | `TurnReceipt` started |
| `receipt_queued.json` | `TurnReceipt` queued |
| `receipt_rejected_busy.json` | `TurnReceipt` rejected_busy |
| `receipt_duplicate.json` | `TurnReceipt` duplicate (wraps original) |
| `cancel_request.json` | `POST …/cancel` body |
| `gate_request_approve.json` | `POST …/gate` body, approve |
| `gate_request_deny.json` | `POST …/gate` body, deny + reason |
| `shutdown_request_drain.json` | `POST …/shutdown` body |
| `error_unknown_target.json` | 404 body |
| `error_admission_refused.json` | 429 body |
| `error_unsupported.json` | 409 body (cancel when !interruptible) |
| `error_malformed.json` | 400 body |

**Fixture policy:** the directory is **append-only for the life of v1**. Editing/deleting an existing fixture
IS a breaking change requiring `/v2` (new `fixtures/runtime-control-v2/`). New additive fields/frames/variants
get NEW fixture files. A consumer whose contract tests pass against its vendored copy is compatible with every
runtime owner that passes against the same copy — that shared directory, not shared code, is the compatibility
boundary. **`ProtocolEvent` never appears in any fixture** (the boundary invariant).

---

## Testability honesty + coverage

The wire types (request/response bodies, `TurnEvent`/`TargetStatus`/`TurnReceipt` serialization, error
mapping, `hello`/`truncated` frames) + the fixture conformance are unit-tested to **100% line+region** with a
fake clock, counter ids, and a scriptable fake R1 provider — the `wasl-operator` regime. The HTTP/UDS router +
SSE writer are the IO shell, verified by **loopback integration tests** (UDS on unix, `127.0.0.1:0` HTTP
elsewhere, real client), NOT held to 100%; only the bind loop may be otherwise uncovered (carrying a
verified-unreachable note). Lag/truncation is exercised by a slow-subscriber loopback test (the `wasl-operator`
L6 pattern). No `#[ignore]`/`todo!`/tautology; loopback tests hermetic (ephemeral sockets/ports, spawned
servers, short timeouts). The `ProtocolEvent`-never-on-the-wire invariant is asserted by a test that scans the
serialized surface for any `ProtocolEvent`-only shape and fails if present.
