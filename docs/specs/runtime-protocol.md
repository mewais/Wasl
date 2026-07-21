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

Three drivers:

1. **`FakeLineProtocol`** — the long-lived in-repo fake structured agent + its line-oriented JSON protocol.
   The deterministic substrate the R1 core's process/framing plumbing (impl-plan Phase 2B) is proven against.
2. **`ClaudeStreamProtocol`** — maps claude `stream-json` duplex → `ProtocolEvent` (impl-plan Phase 4).
3. **`OpenCodeAcpProtocol`** (primary) / `OpenCodeServeProtocol` (secondary) — maps opencode ACP JSON-RPC (and
   the `serve` HTTP path) → `ProtocolEvent` (impl-plan Phase 5).

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
verdict-acp.json}`, `out/codex/{summary.txt,help.txt}`.

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

### codex / antigravity — out of scope for R2 managed drivers

| Claim | Status |
|---|---|
| codex `exec`/`exec resume` is **per-turn** (`persistent_process` unsupported via the documented interface); `persistent_conversation` true | ✅ VERIFIED — `out/capability-matrix.md` codex row = `unsupported-by-documented-interface`; `out/codex/summary.txt` ("exec is PER-TURN … persistent_process via the contracted exec interface: FALSE; persistent_conversation (resume-based): TRUE"); stays under `EphemeralRuntimeProvider` |
| codex experimental `app-server`/`exec-server`/`mcp-server` modes | ⛔ UNKNOWN — out of scope (Phase-0 follow-up, not 0′; `out/codex/summary.txt`); no R2 driver |
| antigravity (`agy`) | 🅰 ASSUMED per-turn like codex; **`not-installed`** on the spike host (`out/capability-matrix.md` antigravity row); no R2 driver |

**GATE PASSED** is real: `out/capability-matrix.md` records claude=`verified-true`, opencode=`verified-true`
(serve + acp), codex=`unsupported-by-documented-interface`, antigravity=`not-installed` — ≥1 CLI proven
same-process multi-turn, so the go/no-go the whole core rests on is satisfied.

**No `candidate` cells.** The two UNKNOWN facts (interruptibility, streaming granularity) AND the ASSUMED
tool/permission/native-gate wire shapes (claude `tool_use`/`tool_result`; opencode acp/serve `Tool*` +
permission/gate) are carried forward from R1 and MUST be resolved by a Phase-1-follow-up capture before Phase
4/5 commits `interruptible`/`streaming_output` true, or commits a `Tool*`/`NativeGateRequested` mapping around
a specific captured shape, for the affected driver.

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

## 4. Common driver requirements

- **spawn/resume:** every driver implements both; `resume` is meaningful only where `resumable`; resume scope
  is within one node-process lifetime (R1 §State durability).
- **turn-boundary detection:** the driver, not the actor, knows the native boundary (claude `type=="result"`;
  acp JSON-RPC `result` w/ `stopReason`; serve `info.finish=="stop"`/`step-finish`; fake `{"ev":"done"}`).
- **native-session-id handling:** each driver reports its `NativeSessionId`; the normalizer accepts both
  `sessionId` and `sessionID` casings (VERIFIED divergence) plus claude's client-supplied UUID.
- **honest UNKNOWN cells:** every driver's `interruptible` and (claude/serve) `streaming_output` are `false`
  and carry the UNKNOWN annotation; a change requires a cited Phase-1-follow-up capture.
- **honest ASSUMED tool/gate shapes:** the `tool_use`/`tool_result` (claude) and tool-activity + permission/
  native-gate `session/update` shapes (opencode acp/serve) that drive `ToolStarted`/`ToolFinished`/
  `NativeGateRequested` were **NOT exercised** by the token-only spike prompts (no tool ran, no gate tripped).
  Those mapping rows are ASSUMED (documented form only) and a driver MUST NOT freeze a byte-exact fixture
  around them until a **follow-up capture that exercises a tool/gate** confirms the real shape — the
  `AwaitingGate` receive-side story (R1 §7.4/§9) depends on that gate shape the spike never captured.

---

## 5. Testability honesty + coverage bar

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
