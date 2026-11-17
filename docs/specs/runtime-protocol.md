# R2 — `wasl-runtime-protocol` — concrete interactive drivers — spec (authoritative)

**Tier:** L2 (IO shell over the tested R1 core; real CLIs behind `--features live-cli`). **Deps:**
`wasl-runtime` (R1: `InteractiveProtocol`/`InteractiveTransport`/`ProtocolEvent`/`RuntimeCapabilities`/
`NativeSessionId`/`SpawnOptions`/`ComposedPrompt`/`RuntimeError`), `serde`, `serde_json`, `async-trait`,
`tokio` (process, io, sync, time), `uuid` (claude's client-supplied session-id, mirroring
`wasl-adapter-claude`). Dev: `tokio` (macros, rt). No `unsafe`.

This crate is the **one place** CLI-specific / ACP-specific / opencode-specific wire knowledge is allowed. It
provides the concrete `InteractiveProtocol` drivers that map each substrate's native wire shape onto R1's
generic `ProtocolEvent`. **It depends on R1 and never the reverse** (the plan's cycle-fix: the traits + the
internal `ProtocolEvent` enum live in R1; only concrete drivers live here). The generic scheduler/actor in R1
consumes `ProtocolEvent` and knows none of the below.

Four drivers:

1. **`FakeLineProtocol`** — the long-lived in-repo fake structured agent + its line-oriented JSON protocol.
   The deterministic substrate the R1 core's process/framing plumbing (impl-plan Phase 2B) is proven against.
2. **`ClaudeStreamProtocol`** — maps claude `stream-json` duplex → `ProtocolEvent` (impl-plan Phase 4).
3. **`OpenCodeAcpProtocol`** (primary) / `OpenCodeServeProtocol` (secondary) — maps opencode ACP JSON-RPC (and
   the `serve` HTTP path) → `ProtocolEvent` (impl-plan Phase 5).
4. **`CodexAppServerProtocol`** — maps codex `app-server --stdio` JSON-RPC → `ProtocolEvent` (Phase-0
   follow-up capture + Phase-6 impl). This is the Phase-0 follow-up the original Phase-0′ spike explicitly
   deferred (§"codex `app-server`" below, historical) — `codex exec`/`exec resume` is genuinely per-turn (that
   finding stands unchanged), but `app-server --stdio` is a SEPARATE, persistent JSON-RPC-over-stdio substrate
   Phase-0′ never investigated. It was captured and VERIFIED in the follow-up capture (CONTRACT STATUS below)
   and is a real R2 managed driver, not out of scope.

---

## ⚠️ CONTRACT STATUS — wire shapes VERIFIED against the real CLIs (Phase-0′/Phase-0 spike, GATE PASSED)

Ground truth is the **re-run** Phase-0′ spike (`spikes/live-substrate/out/`, real/redacted/secret-free
artifacts), which captured verbatim wire shapes from the real installed binaries (claude stream-json;
opencode `serve` HTTP + `acp` JSON-RPC). Every mapping row below cites its artifact and is VERIFIED **only**
for the exact shape the probe actually exercised. Tiers: **VERIFIED** = captured from a real CLI (cite the
artifact); **STATIC** = from `--help`/docs; **ASSUMED** = a design choice OR a wire shape the probes did NOT
exercise (documented form only — needs a follow-up capture); **UNKNOWN** = a fact a driver needs that the
spike did NOT settle and that MUST be probed by a **Phase-1-follow-up capture** before that driver commits an
API around it.

**What the probes actually exercised:** token-only prompts ("remember VIOLET-7F29" / "recall …"), two turns
each, over never-closed stdin/persistent server. That exercised session-ready, input framing, assistant
output, and turn boundaries — but **invoked NO tool and tripped NO native permission gate**. Consequently
every `tool_use`/`tool_result` (claude) and `ToolStarted`/`ToolFinished`/`NativeGateRequested`
(opencode acp/serve) row is **ASSUMED, not VERIFIED**, and carries a required follow-up-capture note.

Artifacts referenced: `out/capability-matrix.md`, `out/claude/{events.jsonl,argv.txt,verdict.json}`,
`out/opencode/serve/{turn_*_message.txt,discover_*.txt,verdict-serve.json}`, `out/opencode/acp/{messages.jsonl,
verdict-acp.json}`, `out/codex/{summary.txt,help.txt}` (Phase-0′, `exec`), `out/codex_appserver/{verdict.json,
events.jsonl,codex_toolcall_capture.jsonl}` + `out/codex_appserver_resume{,_B}/` (the Phase-0-follow-up
capture, `app-server --stdio`; scripts: `spikes/live-substrate/codex_appserver_probe.py` +
`codex_appserver_resume_probe.py`).

### claude `stream-json` duplex

| Claim | Status |
|---|---|
| Invocation: `claude -p --input-format stream-json --output-format stream-json --verbose --session-id <uuid>` | ✅ VERIFIED — `claude_probe.build_argv`; `out/claude/argv.txt` |
| **Client-SUPPLIED** `--session-id` UUID; **byte-stable across turns** (Wasl mints it, like `wasl-adapter-claude`'s UUIDv5) | ✅ VERIFIED — `claude_probe` supplies `str(uuid.uuid4())`; `out/claude/verdict.json` `native_session_id` |
| Persistent stdin, **never EOF between turns**; SAME pid serves turn 2 | ✅ VERIFIED — `out/claude/verdict.json` `pass=true`, `pid_turn1==pid_turn2` |
| Input line shape: `{"type":"user","message":{"role":"user","content":[{"type":"text","text":"…"}]}}` NDJSON, one per line | ✅ VERIFIED — `claude_probe.user_message_line` |
| `type=="system"`, `subtype=="init"` = session-ready + per-turn start; carries top-level `session_id` | ✅ VERIFIED — `claude_probe.classify_event` "ready"; `out/claude/events.jsonl` |
| `type=="assistant"` carries WHOLE content blocks (the CAPTURED blocks were `thinking` + `text`) — **NO token deltas observed** | ✅ VERIFIED (granularity only) — `out/claude/events.jsonl` lines 89-90,101-102 (whole `thinking`/`text` blocks, no partial-messages); the `tool_use` block variant was NOT captured (see the ASSUMED tool row below) |
| `type=="result"` = turn boundary; carries final `result` text, `stop_reason`, `usage`, `total_cost_usd`, `request_id`, `terminal_reason` | ✅ VERIFIED — `claude_probe.classify_event` "turn_end", `extract_assistant_text` reads `result` |
| `tool_use` appears in an `assistant` event; `tool_result` in a `user` event (→ `ToolStarted`/`ToolFinished`) | 🅰 **ASSUMED** — **NOT captured**: the token-only probe prompt invoked no tool, so `out/claude/events.jsonl` contains ZERO `tool_use`/`tool_result` blocks (`permission_denials: []`). The block SHAPE above is the documented Anthropic stream-json form, not a spike-captured fact — **needs a follow-up capture that exercises a tool** before `ClaudeStreamProtocol` commits the mapping |
| **Interruptibility** of an in-flight claude turn | ⛔ **UNKNOWN** — UNTESTED in Phase-0′; `ClaudeStreamProtocol` reports `interruptible: false` until a Phase-1-follow-up interrupt capture |
| **Streaming granularity** (partial deltas vs whole blocks; effect of a partial-messages flag) | ⛔ **UNKNOWN** — only whole `assistant` blocks seen; `streaming_output: false` until a Phase-1-follow-up streaming capture |

### opencode `serve` (HTTP) — SECONDARY

| Claim | Status |
|---|---|
| Launch: `opencode serve --hostname 127.0.0.1 --port 0 --print-logs`; port scraped from stdout `http://127.0.0.1:PORT` | ✅ VERIFIED — `opencode_probe.probe_serve` url/port regex; `out/opencode/serve/server.log` |
| `POST /session` `{}` → session id in field **`sessionID`** (`ses_` prefix) | ✅ VERIFIED — the `ses_…` id under `"sessionID"` recurs on every `POST /message` response `info`, `out/opencode/serve/turn_*_message.txt` (`"sessionID":"ses_079568c98…"`); `GET /session` listing captured in `out/opencode/serve/discover_session.txt` |
| `POST /session/{id}/message` body `{"parts":[{"type":"text","text":"…"}],"model":{"providerID":"…","modelID":"…"}}` is **SYNCHRONOUS**, returns the full assistant message | ✅ VERIFIED — `out/opencode/serve/turn_1_message.txt`, `turn_2_message.txt` (full `info`+`parts` message body) |
| Completion = `info.finish=="stop"` + trailing `{"type":"step-finish","reason":"stop"}` part; **NOT SSE** (no deltas on this path) | ✅ VERIFIED — `out/opencode/serve/turn_2_message.txt` (`info.finish=="stop"`, trailing `step-finish` `reason:"stop"`) |
| Requires isolated config env: `OPENCODE_DISABLE_PROJECT_CONFIG=1`, `OPENCODE_DISABLE_AUTOCOMPACT=1`, own `XDG_*`; key via env | ✅ VERIFIED — `out/opencode/opencode.json`; spike README §Prerequisites |
| tool-activity / permission / native-gate wire shapes on `serve` (→ `ToolStarted`/`ToolFinished`/`NativeGateRequested`) | 🅰 **ASSUMED** — **NOT captured**: the token-only prompt invoked no tool and tripped no gate; `turn_*_message.txt` parts are `step-start`/`text`/`step-finish` only. Needs a follow-up capture that exercises a tool/gate |
| Interruptibility / streaming deltas on `serve` | ⛔ **UNKNOWN** — synchronous path, no deltas; `false` until a follow-up capture |

### opencode `acp` (JSON-RPC / stdio) — PRIMARY (recommended)

| Claim | Status |
|---|---|
| Launch: `opencode acp` (persistent stdin/stdout); `initialize` → `agentCapabilities` incl. `sessionCapabilities {resume,fork,list,close}` | ✅ VERIFIED — `out/opencode/acp/messages.jsonl` lines 1-35 (`initialize` result, `sessionCapabilities {close,fork,list,resume}`) |
| `session/new` → session id in field **`sessionId`** (camelCase — DIFFERENT casing from serve's `sessionID`) | ✅ VERIFIED — `out/opencode/acp/messages.jsonl` line 40 (`"sessionId":"ses_079566635…"`); the normalizer MUST handle both casings |
| `session/prompt` with `{"prompt":[{"type":"text","text":"…"}]}` | ✅ VERIFIED — `out/opencode/acp/messages.jsonl` (prompt drives the id-3/id-4 `result`s) |
| `session/update` notifications incl. `agent_message_chunk` = **REAL streaming deltas** + `usage_update` | ✅ VERIFIED — `out/opencode/acp/messages.jsonl` lines 117-131,162-176 (`agent_message_chunk`), 132-147,177-192 (`usage_update`) |
| JSON-RPC `result` to the prompt id (with `stopReason`) = turn boundary | ✅ VERIFIED — `out/opencode/acp/messages.jsonl` lines 148-161,193-205 (`result` `stopReason:"end_turn"`) |
| Same pid across 2 turns over never-closed stdin | ✅ VERIFIED — `out/opencode/acp/verdict-acp.json` `pass=true`, `pid=338770` |
| ACP tool-activity + permission/native-gate `session/update` shapes (→ `ToolStarted`/`ToolFinished`/`NativeGateRequested`) | 🅰 **ASSUMED** — **NOT captured**: `out/opencode/acp/messages.jsonl` carries only `available_commands_update`/`agent_message_chunk`/`usage_update` `session/update`s — no tool call, no `request_permission`/gate notification. Needs a follow-up capture that exercises a tool/gate |
| Interruptibility (ACP `session/cancel` or equivalent) | ⛔ **UNKNOWN** — not exercised in Phase-0′; `interruptible: false` until a follow-up capture |

### codex `app-server` (JSON-RPC / stdio) — the Phase-0-follow-up substrate (now a real R2 driver)

`codex exec`/`exec resume` (the interface Phase-0′ actually probed) genuinely IS per-turn — that original
finding is unchanged and still correctly describes `exec`. It is a DIFFERENT subcommand from the one below;
`EphemeralRuntimeProvider` still uses `exec` for the headless/adapter path (`wasl-adapter-codex`) and that is
unaffected by anything here. `codex app-server --stdio` is the follow-up capture Phase-0′ explicitly deferred
(the row this table used to carry read "⛔ UNKNOWN — out of scope (Phase-0 follow-up, not 0′)") — it was
subsequently probed, and IS a persistent, multi-turn, JSON-RPC-over-stdio substrate, structurally the same
family as opencode's `acp` (request/response envelope + async notifications), not claude's raw stream-json
(which has no envelope at all). `codex exec-server` (a remote/websocket cloud-registration mode) and
`codex mcp-server` (codex acting as an MCP tool PROVIDER, the wrong direction for a managed driver) were also
checked and are correctly NOT used — `app-server --stdio` is the only one of the three that fits.

| Claim | Status |
|---|---|
| `codex exec`/`exec resume` is **per-turn** (`persistent_process` unsupported via the documented `exec` interface); `persistent_conversation` true | ✅ VERIFIED (Phase-0′, unchanged) — `out/capability-matrix.md` codex row = `unsupported-by-documented-interface`; `out/codex/summary.txt`; `EphemeralRuntimeProvider`/`wasl-adapter-codex` still use `exec`, untouched by the driver below |
| `codex app-server --stdio` IS a persistent, same-process, multi-turn JSON-RPC-over-stdio substrate | ✅ VERIFIED — `spikes/live-substrate/codex_appserver_probe.py`; `out/codex_appserver/verdict.json` (`P_persistent_process`/`C_context_retained`/`B_turn_boundary` all `verified-true`; same pid served 2 turns, turn 2 recalled turn 1's planted token) |
| Handshake: `initialize` (id=1, `capabilities.experimentalApi:true`) → `thread/start {cwd}` (id=2) → `result.thread.id` (a UUIDv7); a `thread/started` NOTIFICATION can arrive BEFORE its matching response (same ordering hazard opencode acp has) | ✅ VERIFIED (live capture) + ✅ VERIFIED (authoritative schema, `codex app-server generate-json-schema`, `ThreadStartParams`/`ThreadStartResponse`/`Thread.id`) |
| `thread/resume {threadId, cwd}` → same response shape as `thread/start` (`result.thread.id`); a thread whose ORIGINAL process has fully exited can be resumed by a genuinely FRESH, separate process and still recalls pre-exit context | ✅ VERIFIED — schema (`ThreadResumeParams`/`ThreadResumeResponse`, identical `required: [... "thread" ...]` shape to `ThreadStartResponse`) AND live cross-process capture (`spikes/live-substrate/codex_appserver_resume_probe.py`: process A plants a token and is SIGKILL'd/reaped, a SEPARATE process B resumes the same thread id and recalls the token — `R1_resume_succeeds=true`, `R2_context_retained=true`) AND a permanent regression test (`crates/wasl-runtime-protocol/tests/codex_appserver_resume_live.rs`, passing against the real CLI) |
| `turn/start {threadId, input:[{type:"text",text}]}` acks `status:"inProgress"` immediately; streaming via `item/agentMessage/delta`; terminal via ONE `turn/completed` notification whose `params.turn.status` carries `completed\|interrupted\|failed\|inProgress` — there is NO separate `turn/failed` METHOD (all terminal outcomes ride the same notification) | ✅ VERIFIED (live capture, `completed` AND `interrupted` statuses both observed — the interrupt probe/test's `turn/completed` carried `status:"interrupted"`) + ✅ VERIFIED-BY-SCHEMA that `completed`/`failed`/`interrupted` are all embedded `status` values under the single `turn/completed` method, not sibling methods. 🅰 **ASSUMED**: only `failed` was never actually exercised live (schema-derived only) |
| Tool calls: `item/started`/`item/completed` with `item.type=="commandExecution"` (id+command on start, no command re-echoed on completion — needs an id→command tracker) | ✅ VERIFIED — `out/codex_appserver/codex_toolcall_capture.jsonl` (a real `ls -a` shell tool call, 156 wire lines, `tool_seen=true`, `turn_completed=true`) |
| Server-initiated approval-flow REQUESTS (`execCommandApproval`, `applyPatchApproval`, `item/commandExecution/requestApproval`, `item/fileChange/requestApproval`, `item/permissions/requestApproval`, `item/tool/requestUserInput`, `mcpServer/elicitation/request`) — methods the SERVER can send carrying an `id`, requiring a response before codex proceeds | ⛔ **UNKNOWN / DORMANT, not proven unreachable** — confirmed to exist in the authoritative schema (`ServerRequest.json`) but NEVER observed on the wire in any capture (including the real shell-tool-call capture above) under the default `approvalPolicy` this crate has actually exercised (neither `thread/start` nor `thread/resume` sets `approvalPolicy`/`sandbox`). `CodexAppServerProtocol` does NOT support ANSWERING these (no `ProtocolEvent`/gate exists for it) — but it no longer HANGS on one either: `supervise` detects any genuine JSON-RPC REQUEST from the server (a structural `method`+`id` discriminator, not a curated method-name list, so it also catches any future method) and immediately fails the turn with a clear diagnostic, then tears the now-permanently-stuck process down — the same policy-free "refuse loudly, don't hang silently" floor as the existing `TooLong` handling. The RICHER response (auto-approve/auto-deny/a new gated `ProtocolEvent` mirroring `NativeGateRequested`) remains a real product/security decision, deliberately not picked unilaterally by this driver — **the hang is fixed; answering is still flagged, not fixed** |
| `turn/interrupt {threadId, turnId}` | ✅ **VERIFIED LIVE** — a turn running a real, genuinely slow shell tool call (`sleep 30`) was interrupted ~4.3s in (right after the tool call started) and `turn/completed` reported `status:"interrupted"` only ~30ms after `turn/interrupt` was sent, instead of the natural ~30s completion (`spikes/live-substrate/codex_appserver_interrupt_probe.py`, `out/codex_appserver_interrupt/interrupt_verdict.json`, `I1_interrupt_accepted=true`, `I2_turn_actually_stopped_early=true`). Also proven at the real driver level by a permanent regression test (`tests/codex_appserver_interrupt_live.rs`, passing against the real CLI, ~4s). `interruptible: true` — the FIRST of the three real-CLI drivers (claude/opencode/codex) to have this genuinely verified live, not rounded up (`ClaudeStreamProtocol`/`OpenCodeAcpProtocol` remain `false`, untested) |
| `codex exec-server` (remote/websocket cloud-registration mode) | checked and correctly NOT USED — a different substrate entirely (remote host registration, not a local stdio driver) |
| `codex mcp-server` (codex as an MCP tool PROVIDER) | checked and correctly NOT USED — the wrong direction for a managed driver (we need codex as the AGENT, not as a tool codex exposes to some other caller) |

**GATE PASSED** is real: `out/capability-matrix.md` records claude=`verified-true`, opencode=`verified-true`
(serve + acp), codex `exec`=`unsupported-by-documented-interface` (superseded for managed-driver purposes by
`app-server`'s own `verified-true` above), — ≥1 CLI proven same-process multi-turn,
so the go/no-go the whole core rests on is satisfied (and is now satisfied twice over, independently, by
codex `app-server` too).

**No `candidate` cells.** The two UNKNOWN facts (interruptibility, streaming granularity) AND the ASSUMED
tool/permission/native-gate wire shapes (claude `tool_use`/`tool_result`; opencode acp/serve `Tool*` +
permission/gate) are carried forward from R1 and MUST be resolved by a follow-up capture before a driver
commits `interruptible`/`streaming_output` true, or commits a `Tool*`/`NativeGateRequested` mapping around a
specific captured shape, for the affected driver. Codex's own remaining residual — the `failed` turn status
(schema-derived, never live-captured) and the approval-request methods (schema-real, never observed under
this crate's exercised
config) — are the SAME class of gap, tracked in the codex CONTRACT STATUS table above rather than repeated
here.

---

## `--features live-cli` discipline (repo convention)

Real-CLI tests are gated with `#![cfg(feature = "live-cli")]` — a cargo feature, **NOT** `#[ignore]` — so the
orchestrator asserts they RAN in explicit verification passes and default CI never spends tokens (mirrors
`adapter-opencode.md` §`tests/live.rs`). Pinned, tested CLI versions; the cheapest large-context model;
isolated config env for opencode (`OPENCODE_DISABLE_AUTOCOMPACT=1`, `OPENCODE_DISABLE_PROJECT_CONFIG=1`, own
`XDG_*`, key via env), on a host giving opencode ≥~4 GB. The deterministic proof of the driver *mapping* is
byte-exact fixtures (below); the live tests prove only the real wire.

---

## 1. `FakeLineProtocol` — the long-lived in-repo fake structured agent (impl-plan Phase 2B)

A **dev-only** long-lived test binary compiled INTO the repo (like the stand-in commands the existing
`process_invoker` tests use, but *persistent*), plus a trivial `FakeLineProtocol: InteractiveProtocol` driver
over a **line-oriented JSON** protocol (one request per line in, events one per line out).

### 1.1 The fake agent binary (dev-bin)

- **Remains running**, serving many turns over a persistent stdin (never EOF between turns).
- Maintains an **in-memory fact map** across turns (`remember K=V` / `recall K`) — proving context retention
  in the SAME live process (the property no headless spawn can have).
- **`hold`**: block a turn deterministically until released (test queueing without timing flakiness).
- **`crash`**: exit mid-turn (test recovery + `OutcomeUnknown`).
- **native-gate**: emit a native-gate request and block until resolved (test `AwaitingGate` + §9 resolution).
- **2C mid-turn callback / bidirectional extension:** a `tool` command that emits a `ToolStarted`-shaped event
  and BLOCKS the turn until the actor sends a value back over the same stdin — the sharper genericity probe
  (impl-plan Phase 2C). If R1's `ProtocolEvent` + single-writer actor carry this with no new variant and no
  second writer, `ProtocolEvent` is proven generic for bidirectional turns; if not, the driver documents that
  `ProtocolEvent` is proven generic **only for unidirectional turn loops** and scopes the claim (no silent
  second writer).

### 1.2 The line protocol (normative; the mapping FakeLineProtocol implements)

Requests (one JSON object per line into the fake's stdin) and events (one JSON object per line out) map onto
`ProtocolEvent` as:

| Fake event line | → `ProtocolEvent` |
|---|---|
| `{"ev":"ready","native":"<id>"}` | `SessionReady{native: Some(NativeSessionId)}` |
| `{"ev":"accepted"}` | `TurnAccepted{native_turn: None}` |
| `{"ev":"delta","text":"…"}` | `AssistantDelta{text}` |
| `{"ev":"tool_start","name":"…"}` | `ToolStarted{name}` |
| `{"ev":"tool_end","name":"…","ok":true}` | `ToolFinished{name, ok}` |
| `{"ev":"gate","summary":"…","preview":"…"}` | `NativeGateRequested{detail}` |
| `{"ev":"done","reply":"…"}` | `TurnCompleted{reply}` |
| `{"ev":"failed","error":"…"}` | `TurnFailed{error}` |
| process exit | `ProcessExited{code}` |

### 1.3 Hostile-input scripts (impl-plan Phase 2B — the tester WILL use these)

The fake is scriptable to emit adversarial wire the driver's line-framing MUST survive, each producing a
DEFINED `ProtocolEvent` sequence (or an ignored/logged non-event) per R1 §7.4 — never a panic, never a
re-emitted terminal event:

- split JSON across reads (a single object arriving in multiple stdin reads);
- multiple JSON objects in one read;
- malformed JSON between valid events (ignored + logged, framing continues);
- missing `accepted` (turn proceeds to `done` without an explicit accept);
- duplicate `done` (second ignored — no second `TurnCompleted`);
- `delta` AFTER `done` (ignored — R1 §7.4 "event referencing a completed turn");
- EOF without a `ProcessExited`-worthy code (mapped to `ProcessExited{code: None}`);
- exit-before-stdout-drained (drain remaining buffered events first, THEN `ProcessExited`);
- ignored interrupt (interrupt sent, no ack — the actor's `Interrupting` timeout applies);
- gate-then-exit (`gate` then process exit → `NativeGateRequested` then `ProcessExited`, per R1 §7.4/§9).

### 1.4 Process supervision (impl-plan Phase 2B — belongs to the R2 IO shell)

The driver/provider IO shell reaps normal/abnormal exit; kills descendants via a process group
(`start_new_session`, as the spike probes do); escalates interrupt→SIGINT→TERM→KILL each bounded; runs a
**hang watchdog** (opencode's known non-terminating overflow loop — `adapter-opencode.md` §Non-termination
guard); drains stderr; and cleans up on half-constructed actor / node shutdown / actor panic (no orphan
children). Capabilities the fake reports are set by the test to exercise each R1 capability-gated path.

---

## 2. `ClaudeStreamProtocol` — claude stream-json → `ProtocolEvent` (impl-plan Phase 4)

```rust
pub struct ClaudeStreamProtocol { /* private: bin, model, uuid namespace (context_id → session UUID) */ }
impl wasl_runtime::InteractiveProtocol for ClaudeStreamProtocol { /* spawn/resume/capabilities */ }
```

- **capabilities:** `persistent_process: true`, `persistent_conversation: true`, `structured_turns: true`,
  `attachable: true`, `structured_provenance: true`, `resumable: true`, **`streaming_output: false`** (⛔
  UNKNOWN — whole blocks only), **`interruptible: false`** (⛔ UNKNOWN — untested). These are the locked
  per-driver report; a broken driver setting all-false is a FAIL (impl-plan Phase-1).
- **spawn:** `claude -p --input-format stream-json --output-format stream-json --verbose --session-id <uuid>`,
  persistent stdin, never EOF between turns (VERIFIED shape). The `<uuid>` is CLIENT-SUPPLIED and byte-stable —
  minted from the `TargetKey`/`context_id` (UUIDv5, exactly as `wasl-adapter-claude::session_uuid`), so it is
  the `NativeSessionId` and the same key resumes the same session.
- **resume:** re-attach with the same `--session-id <uuid>` (within one node-process lifetime).
- **`send_turn`:** write one NDJSON line
  `{"type":"user","message":{"role":"user","content":[{"type":"text","text":"<ComposedPrompt>"}]}}` and flush;
  do NOT close stdin.

### 2.1 Mapping (byte-exact fixtures in `tests/`; `parse_reply`-style discipline)

| claude stream-json event | → `ProtocolEvent` |
|---|---|
| `{"type":"system","subtype":"init", "session_id":…}` | `SessionReady{native: Some(session_id)}` (also per-turn start) |
| `{"type":"assistant","message":{"content":[{"type":"text","text":…}]}}` | `AssistantDelta{text}` (WHOLE block — one delta; ⛔ no token deltas) |
| `{"type":"assistant", …"content":[{"type":"tool_use","name":…}]}` | `ToolStarted{name}` — 🅰 ASSUMED shape (NOT captured; token-only probe invoked no tool — see CONTRACT STATUS; needs a follow-up tool capture before this mapping is committed) |
| `{"type":"user", …"content":[{"type":"tool_result", …}]}` | `ToolFinished{name, ok}` (name/ok from the tool_use it answers) — 🅰 ASSUMED shape (NOT captured; needs a follow-up tool capture) |
| `{"type":"result","result":<text>,"stop_reason":…,"usage":…,"total_cost_usd":…,"request_id":…,"terminal_reason":…}` | `TurnCompleted{reply: result}` (turn boundary) |
| `{"type":"result", … error/terminal_reason indicating failure}` | `TurnFailed{error}` |
| process exit | `ProcessExited{code}` |

`native_turn` for `TurnAccepted` is left `None` (claude has no separate accept event — the `system/init`
per-turn start suffices; the actor emits its public `TurnAccepted` at admission). Fixtures are byte-for-byte
from `out/claude/events.jsonl`; the mapping is a pure function tested to 100% without the CLI. `thinking`
content blocks are ignored for reply text (audit provenance only), matching the adapter convention.

---

## 3. opencode drivers — `acp` (PRIMARY) and `serve` (secondary) (impl-plan Phase 5)

**Recommendation: `OpenCodeAcpProtocol` is the PRIMARY managed driver.** Rationale (VERIFIED): ACP is the only
opencode substrate that yields **real streaming deltas** (`session/update` `agent_message_chunk`), it is a
persistent stdio JSON-RPC connection (structurally closer to claude's persistent-stdin model, so it stresses
the abstraction the same way the spike gate proved), and it advertises `sessionCapabilities {resume, fork,
list, close}`. `serve` is HTTP and **synchronous with no deltas** — a valid managed substrate but strictly
weaker for the "live streaming" property, so it is the secondary driver and its `streaming_output` is `false`.
Phase 5's genericity assertion (conformance runs UNCHANGED across ephemeral+claude+opencode; any per-protocol
branch = leak = FAIL) is proven on the ACP driver.

```rust
pub struct OpenCodeAcpProtocol   { /* private: bin, model, isolated env */ }
pub struct OpenCodeServeProtocol { /* private: bin, model, isolated env, base_url discovery */ }
```

### 3.1 `OpenCodeAcpProtocol` mapping (session/delta/boundary shapes VERIFIED; tool/gate rows ASSUMED)

- **capabilities:** `persistent_process: true`, `persistent_conversation: true`, `structured_turns: true`,
  `attachable: true`, `structured_provenance: true`, `resumable: true` (per `sessionCapabilities`),
  **`streaming_output: true`** (VERIFIED — real deltas), **`interruptible: false`** (⛔ UNKNOWN until a
  follow-up capture).
- **spawn:** `opencode acp` under the isolated env; JSON-RPC handshake `initialize` (protocolVersion 1) then
  `session/new {"cwd":…,"mcpServers":[]}`.
- **native session id:** from `session/new` result field **`sessionId`** (camelCase). **The normalizer MUST
  handle BOTH `sessionId` (acp) and `sessionID` (serve)** — the casing DIVERGES between substrates (VERIFIED).
- **`send_turn`:** `session/prompt {"sessionId":…,"prompt":[{"type":"text","text":"<ComposedPrompt>"}]}`.

| ACP wire | → `ProtocolEvent` |
|---|---|
| `session/new` result `{"sessionId":"…"}` | `SessionReady{native: Some(sessionId)}` |
| `session/update` notif, `agent_message_chunk` (delta text) | `AssistantDelta{text}` (REAL delta) |
| `session/update` notif, tool activity | `ToolStarted{name}` / `ToolFinished{name, ok}` — 🅰 ASSUMED shape (NOT captured; token-only probe invoked no tool — needs a follow-up tool capture) |
| `session/update` notif, `usage_update` | (ignored for text; provenance only) |
| `session/update` notif, a permission/gate request | `NativeGateRequested{detail}` — 🅰 ASSUMED shape (NOT captured; no `request_permission`/gate notification appeared — needs a follow-up gate capture) |
| JSON-RPC `result` to the `session/prompt` id, with `stopReason` | `TurnCompleted{reply}` (turn boundary) |
| JSON-RPC `error` to the prompt id | `TurnFailed{error}` |
| process exit | `ProcessExited{code}` |

### 3.2 `OpenCodeServeProtocol` mapping (session/turn shapes VERIFIED; tool/gate/error rows ASSUMED; secondary)

- **capabilities:** as ACP but **`streaming_output: false`** (synchronous, no deltas) and
  **`interruptible: false`** (⛔ UNKNOWN).
- **spawn:** `opencode serve --hostname 127.0.0.1 --port 0 --print-logs` under the isolated env; scrape
  `http://127.0.0.1:PORT` from stdout; `POST /session {}` → **`sessionID`** (`ses_` prefix; casing differs
  from acp — same normalizer).
- **`send_turn`:** `POST /session/{id}/message` body
  `{"parts":[{"type":"text","text":"<ComposedPrompt>"}],"model":{"providerID":…,"modelID":…}}` (synchronous).

| serve wire | → `ProtocolEvent` |
|---|---|
| `POST /session` → `{"sessionID":"ses_…"}` | `SessionReady{native: Some(sessionID)}` |
| synchronous `POST /message` response body (full assistant message) | one `AssistantDelta{text}` then `TurnCompleted{reply}` |
| response `info.finish=="stop"` + trailing `{"type":"step-finish","reason":"stop"}` part | turn boundary (drives `TurnCompleted`) |
| response `{"type":"error", …}` (opencode error shape) | `TurnFailed{error}` — 🅰 ASSUMED shape (no error response captured in the spike; documented form only) |
| server process exit | `ProcessExited{code}` |

---

## 4. `CodexAppServerProtocol` — codex `app-server --stdio` JSON-RPC → `ProtocolEvent` (Phase-0-follow-up + Phase-6)

```rust
pub struct CodexAppServerProtocol { /* private: bin, timeouts */ }
impl wasl_runtime::InteractiveProtocol for CodexAppServerProtocol { /* spawn/resume/capabilities */ }
```

- **capabilities:** `persistent_process: true`, `persistent_conversation: true`, `structured_turns: true`,
  `streaming_output: true` (VERIFIED — real `item/agentMessage/delta` deltas), `attachable: true`,
  `structured_provenance: true`, `resumable: true` (VERIFIED, see below — AHEAD of `OpenCodeAcpProtocol`'s own
  resume gap, which remains ASSUMED), **`interruptible: true`** (VERIFIED LIVE — see the CONTRACT STATUS
  table above; a real, genuinely slow tool-running turn was interrupted well before its natural completion,
  both via a standalone probe and a permanent driver-level regression test. AHEAD of `ClaudeStreamProtocol`/
  `OpenCodeAcpProtocol`, both still `false` and untested on this axis).
- **spawn:** `codex app-server --stdio`, persistent stdin, never EOF between turns (VERIFIED). Handshake:
  `initialize` (id=1) → `thread/start {cwd}` (id=2) → native id from `result.thread.id` (a UUIDv7). A
  `thread/started` NOTIFICATION can arrive before its matching response — handled by the same
  Notification/ExpectedResponse/Anomaly handshake-line classifier opencode's driver uses (the two share one
  implementation, `json_rpc_handshake::classify_handshake_line`, since the logic is byte-for-byte identical).
- **resume:** `thread/resume {threadId: <native>, cwd}` (id=2), same response shape as `thread/start`. VERIFIED
  end-to-end cross-process: a thread whose original process has fully exited (SIGKILL'd, reaped) is resumed by
  a genuinely FRESH, separate process and recalls pre-exit conversational context — proven by a standalone
  probe AND a permanent regression test (`tests/codex_appserver_resume_live.rs`, passing against the real CLI).
- **`send_turn`:** `turn/start {threadId, input:[{type:"text",text:"<ComposedPrompt>"}]}`, one request per
  turn, incrementing request ids starting after the handshake's id=1/id=2.

### 4.1 Mapping (byte-exact fixtures in `tests/`; live shapes cross-checked against the authoritative schema)

| codex app-server wire | → `ProtocolEvent` |
|---|---|
| `thread/started` notification OR the `thread/start`/`thread/resume` RESPONSE, `result.thread.id` | `SessionReady{native: Some(thread.id)}` |
| `item/agentMessage/delta` notification, `params.delta` | `AssistantDelta{text}` (REAL delta) |
| `item/started`/`item/completed`, `item.type=="commandExecution"` | `ToolStarted{name: command}` / `ToolFinished{name, ok: exitCode==0}` — id-correlated across the pair (`CodexToolTracker`), cleared on the `turn/completed` turn boundary so an interrupted tool call cannot leak its id past the turn (the same fix already required for `ClaudeToolTracker`) |
| `turn/completed` notification, `params.turn.status=="completed"` | `TurnCompleted{reply}` (reply = the final `agentMessage` item's text; turn boundary) |
| `turn/completed` notification, `params.turn.status=="failed"` | `TurnFailed{error}` (from `turn.error.message`) — 🅰 ASSUMED shape (schema-derived; only `completed` was captured live, not `failed`) |
| `turn/completed` notification, `params.turn.status=="interrupted"` | `TurnFailed{error: "codex turn interrupted"}` — ✅ VERIFIED LIVE (both the standalone probe and `tests/codex_appserver_interrupt_live.rs` observed this exact status arrive after a genuine mid-flight `turn/interrupt`) |
| any `execCommandApproval`/`applyPatchApproval`/`item/*/requestApproval`/`item/tool/requestUserInput`/`mcpServer/elicitation/request` REQUEST (has both `method` and `id`) | ⛔ **UNHANDLED** — no `ProtocolEvent` mapping exists; silently dropped (see the CONTRACT STATUS row above and the doc comment above `supervise` in `codex_appserver.rs`) |
| process exit | `ProcessExited{code}` |

`native_turn` for `TurnAccepted` is left `None` (codex has no separate accept event distinct from
`turn/start`'s immediate `status:"inProgress"` ack; the actor emits its own public `TurnAccepted` at
admission), mirroring `ClaudeStreamProtocol`'s convention.

### 4.2 Shared plumbing (extracted once byte-for-byte duplication was independently verified across all four drivers)

`BoundedLineRead`/`read_bounded_line`/`discard_one_bounded_line`/`discard_until_newline` and the resource-bound
constants (`bounded_io.rs`), `next_wait`/`best_effort_signal`/`escalate`/`wait_for_exit` (`process_supervision.rs`),
and the JSON-RPC handshake classifier (`json_rpc_handshake.rs`, shared by `OpenCodeAcpProtocol` and
`CodexAppServerProtocol`) are single-implementation, crate-shared modules — not per-driver copies. This is an
implementation detail, not a wire-shape claim; it does not change any `Claim`/`Status` row above.

---

## 5. Common driver requirements

- **spawn/resume:** every driver implements both; `resume` is meaningful only where `resumable`; resume scope
  is within one node-process lifetime (R1 §State durability) for claude/opencode, but is VERIFIED
  cross-process for codex (a fresh, separate OS process can resume a thread whose original process has fully
  exited — the only driver this has been proven for so far).
- **turn-boundary detection:** the driver, not the actor, knows the native boundary (claude `type=="result"`;
  acp JSON-RPC `result` w/ `stopReason`; serve `info.finish=="stop"`/`step-finish`; codex `turn/completed`
  notification w/ `status`; fake `{"ev":"done"}`).
- **native-session-id handling:** each driver reports its `NativeSessionId`; the normalizer accepts
  `sessionId` and `sessionID` casings (VERIFIED divergence, opencode acp vs serve) plus claude's
  client-supplied UUID plus codex's server-assigned `thread.id` (a UUIDv7) — a THIRD distinct shape/casing.
- **honest UNKNOWN cells:** `ClaudeStreamProtocol`/`OpenCodeAcpProtocol`/`OpenCodeServeProtocol`'s
  `interruptible` remain `false` and (claude/serve) `streaming_output` is `false`, each carrying the UNKNOWN
  annotation; a change requires a cited follow-up capture. `CodexAppServerProtocol`'s `interruptible` is the
  one exception — `true`, VERIFIED LIVE (see its own section above) — a genuine per-driver difference, not a
  rounding-up of the others.
- **honest ASSUMED tool/gate shapes:** the `tool_use`/`tool_result` (claude), tool-activity + permission/
  native-gate `session/update` shapes (opencode acp/serve), and codex's `failed` turn status + approval-request
  methods that drive `ToolStarted`/`ToolFinished`/`NativeGateRequested`/`TurnFailed` were **NOT exercised** by
  the respective token-only/tool-only spike prompts actually run (no gate tripped for any driver; codex's own
  tool row AND its `interrupted` turn status ARE captured live, but its approval-flow and `failed` turn status
  are not). Those mapping rows are ASSUMED (documented form only) and a driver MUST NOT freeze a byte-exact
  fixture around them until a **follow-up capture** confirms the real shape — the `AwaitingGate` receive-side
  story (R1 §7.4/§9) depends on gate shapes none of the three real-CLI drivers have captured yet.

---

## 6. Testability honesty + coverage bar

**Deterministic (no CLI, no LLM):** the fake-agent + `FakeLineProtocol` line framing (incl. ALL §1.3 hostile
scripts) and each real driver's **mapping** (native wire → `ProtocolEvent`) via byte-exact fixtures — for the
shapes the spike actually captured (session-ready, input framing, assistant output, turn boundary) — proven
to 100% line without spawning any real CLI. **The `Tool*`/`NativeGateRequested` mapping rows have NO captured
fixture** (the token-only spike ran no tool and tripped no gate); their byte-exact fixture is deferred to the
follow-up tool/gate capture and until then the driver maps only the shapes it can prove. Process supervision
(§1.4) is exercised via stand-in commands (the `process_invoker` idiom) — real child processes, deterministic.

**UNTESTED HERE (needs `--features live-cli` and/or a follow-up capture):** real spawn/resume, real turn
boundaries, real native-session-id stability, **real tool-call + native-gate appearance (ASSUMED shapes
above)**, and the two UNKNOWN facts (interruptibility on all substrates; streaming granularity on claude +
opencode-`serve`). Each live driver has ONE minimal live test
(≤2–3 model calls, cheapest model, fresh context via `WASL_LIVE_NONCE`) asserting the SAME pid/native-session
served both a human `send` and an A2A `consult` (not a fresh spawn) plus a busy/queueing no-interleave test —
mirroring `adapter-opencode.md` §`tests/live.rs`.

`cargo llvm-cov -p wasl-runtime-protocol --fail-under-lines 100` MUST exit 0 for the mapping/framing/
supervision core; REGION residuals only on genuinely-unreachable IO `?` arms in the process shell, enumerated
in `AGENTS.md` with verified-unreachable notes. No `#[ignore]`/`todo!`/tautology. The `--features live-cli`
tests are asserted to have RUN in explicit verification passes, never in default CI.
