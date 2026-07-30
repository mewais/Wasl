# R2 — `wasl-runtime-protocol` — working contract

**Purpose:** the concrete interactive drivers (`FakeLineProtocol`, `ClaudeStreamProtocol`,
`OpenCodeAcpProtocol`/`OpenCodeServeProtocol`) that map each substrate's native wire onto R1's generic
`ProtocolEvent`. The one place CLI-/ACP-/opencode-specific wire knowledge is allowed. **Depends on R1 and
never the reverse** (the driver-seam traits + the internal `ProtocolEvent` live in R1; only concrete drivers
live here).
**Tier:** L2 (IO shell over the tested R1 core; real CLIs behind `--features live-cli`).
**Full spec (read first):** `../../docs/specs/runtime-protocol.md`.
**Architecture:** `../../ARCHITECTURE.md`

## What you may edit
- **code-agent:** write ONLY `crates/wasl-runtime-protocol/src/**` and `crates/wasl-runtime-protocol/Cargo.toml`.
  Read-only: `crates/wasl-runtime-protocol/tests/**`, the spec above, and the public API of `../wasl-runtime`.
- **test-agent:** write ONLY `crates/wasl-runtime-protocol/tests/**`. Read-only: the spec above.

## Phase-1 scope (THIS phase) — SKELETON ONLY
- In Phase-1 this crate has NO type surface of its own: it merely EXISTS and DEPENDS ON R1, proving the
  R2 → R1 dependency edge and that R1 owns the driver-seam traits + `ProtocolEvent`. The crate root re-exports
  the R1-owned generic contract to make the edge explicit.
- The concrete drivers are LATER phases (2B / 4 / 5), NOT Phase-1.

## Definition of done (Phase-1)
- `cargo test -p wasl-runtime-protocol` green (skeleton_smoke). `cargo build -p wasl-runtime-protocol` compiles.
- No `unsafe`. `cargo fmt` clean; `cargo clippy -p wasl-runtime-protocol --all-targets -- -D warnings` clean.

## Rules
- Tests are written BEFORE code and are read-only to the code-agent. Do not edit tests to pass.
- Implement the spec EXACTLY; invent no type. If a test references something not in the spec, STOP and report.
- No test may be `#[ignore]`d/`#[cfg]`-disabled/stubbed/a tautology.
- Deps: `wasl-runtime` (+ `serde`/`serde_json`/`async-trait`/`tokio`); later `uuid` for the claude driver.

## Coverage honesty (Phase-1 — skeleton)
Phase-1 is a re-export/dep-edge skeleton with no driver logic; it is exercised only by `skeleton_smoke.rs`.
The mapping/framing/supervision LINE+REGION-100% bar (spec §5) applies from the driver phases (2B/4/5) when
the concrete drivers land and are proven against byte-exact fixtures and stand-in commands. Report honestly;
do not game a type-only re-export with fake tests.

## Phase-2B scope (LANDED) — the fake structured agent + `FakeLineProtocol`

Phase-2B landed the first concrete driver and its long-lived in-repo test substrate, in `src/**` +
`Cargo.toml` ONLY (plus this file). NO `tests/**` were touched; NO other crate was touched (the
`SpawnOptions.target_key` visibility fix flagged in `hold_queueing.rs` turned out NOT to be required — the
driver owns its bin via its own constructor and `spawn` ignores `SpawnOptions`, so the generic provider's
`spawn(&SpawnOptions::default())` path drives the real driver unchanged; `wasl-runtime` was left untouched and
its suite is unaffected).

- **`src/bin/wasl-fake-agent.rs`** — the `[[bin]] name = "wasl-fake-agent"` dev-bin (§1.1). A persistent
  line-oriented-JSON agent: `ready` on start; `remember`/`recall` in-memory fact map (`recall pid` → own OS
  pid, the same-process proof); `hold`/`release` (deterministic block-until-released, no sleeps); `crash`
  (mid-turn `exit(code)`); `gate`/`gate_then_exit`; `spin` (non-terminating, the watchdog target — blocks on
  `thread::park`, zero CPU); `ignore_interrupt` (ignores SIGINT+SIGTERM via tokio's `signal` seam so the
  escalation ladder must reach SIGKILL); `stop_reading_stdin` (the M2 EPIPE setup); and the §1.3 hostile-wire
  emitters (`emit_split_done`, `emit_batched`, `emit_malformed_between`, `emit_done_no_accept`,
  `emit_duplicate_done`, `emit_delta_after_done`, `eof_without_code` [dies by SIGABRT → `code:None`],
  `emit_then_exit`, `flood_stderr`). Cargo defines `CARGO_BIN_EXE_wasl-fake-agent` for this crate's own tests
  (same mechanism as `wasl-mcp/tests/config_bin.rs`), so `tests/support/fake_agent_bin()` resolves.
- **`src/fake_line.rs`** — `FakeLineProtocol: InteractiveProtocol` (`new(bin)` / `with_timeouts(bin, t)`),
  `SupervisionTimeouts { interrupt_ack, term_after, kill_after, hang_watchdog }`, and the transport. It spawns
  the fake as a REAL child, maps the §1.2 wire → `ProtocolEvent` (`map_event_line`, a pure fn), and owns §1.4
  supervision: a concurrent stderr-drain task, a per-child supervisor task that reads/maps stdout + reaps, a
  hang watchdog, the bounded interrupt→SIGTERM→SIGKILL escalation ladder (each stage RACES the child's real
  exit against the injected timeout, via `kill(1)` — no `unsafe`/`libc`), the M2 `BrokenPipe`/`WriteZero`-is-
  benign stdin fix (mirrors `wasl-adapter-{codex,claude}::ProcessCliInvoker` exactly, per `epipe.rs`), and
  reaping-on-drop (kill signal + `kill_on_drop(true)` kernel backstop + detached supervisor that `wait()`s —
  no zombie/orphan). Post-terminal duplicate/late events (duplicate `done`, `delta`-after-`done`) are dropped
  at the transport (§1.3 / R1 §7.4). `next_event` returns `None` on a quiescent turn only after a bounded
  `POLL_IDLE` (kept > the injected 200ms watchdog and > the driver tests' 50ms held-window slice) so a held
  turn never spuriously ends AND the generic R1 actor can still park-and-service its mailbox (the genericity
  proof) — a genuinely-hung turn is surfaced as a watchdog `ProcessExited`, never a spurious `None`.

### Test results (Phase-2B) — all green, per file
`cargo test -p wasl-runtime-protocol`: **51 tests, 0 failed.** `src/lib.rs` unit 10 (pure `map_event_line`
table + accessors) · `src/bin` unit 3 (fact-map `recall` micro-tests) · `epipe.rs` 2 · `fake_agent_wire.rs` 6
· `fake_line_protocol.rs` 9 · `hold_queueing.rs` 3 (incl. the generic-actor queueing proof) · `hostile_inputs.rs`
10 (all §1.3 scripts) · `skeleton_smoke.rs` 2 · `supervision.rs` 6 (reap/escalation/stderr-drain/hang-watchdog).
`cargo clippy --all-targets -- -D warnings` clean; `cargo fmt --check` clean; `cargo build --workspace` ok.
`wasl-runtime` untouched → its suite is unaffected (not re-run necessary; no change).

### Coverage honesty (Phase-2B) — measured numbers + residuals
`cargo llvm-cov -p wasl-runtime-protocol`: TOTAL **79.73% lines** / 66.51% regions (671 lines). Split:
- **`fake_line.rs` (the driver: mapping + framing + supervision — the spec §5 core): 93.65% line / 91.85%
  region** (425 lines, 27 missed). This is the LINE+REGION bar's real target and its residual set is entirely
  genuinely-unreachable IO / defensive branches, enumerated below (the same honest-residual discipline as
  Phase-2A `wasl-runtime` [95.97%] and `wasl-adapter-*`'s `ProcessCliInvoker` IO arms — NOT `#[ignore]`d,
  stubbed, or gamed):
  1. **`spawn_child` IO-error arms** (`SpawnFailed` for `spawn()` failure + each of the 3 `.take()` pipe
     absences) — a valid bin spawns and piped stdio is ALWAYS present, so these are unreachable IO `?` arms
     (the `ProcessCliInvoker` precedent).
  2. **`resume`** — spec-required trait body with NO frozen integration test (resume is meaningful only where
     a driver re-attaches; the suite drives fresh `spawn`s). It cannot be covered from a `src` unit test
     either: `CARGO_BIN_EXE_wasl-fake-agent` is defined only for integration-test/bench compilation, not the
     lib's own unit tests. Kept live (it correctly re-spawns a fresh persistent fake).
  3. **`fire_watchdog` / `pull_one` defensive channel arms** (kill_tx-`None`; `recv()`→`None` /
     `try_recv`→`Disconnected` without a final `ProcessExited`) — the supervisor ALWAYS holds the sender and
     ALWAYS emits `ProcessExited` before closing, so these degrade-safely-to-clean-end arms are unreachable by
     the current suite. Kept live (a future supervisor bug would surface as a clean end, not a hang).
  4. **`supervise` receiver-gone-mid-stream break** (`ev_tx.send(...).is_err()`) — the transport outlives the
     supervisor on every tested path; this is the correct place to stop pushing when the transport was dropped
     mid-turn.
  5. **`escalate` stage-2 (SIGTERM) return + last-resort `wait` + `wait_for_exit` `Ok(Err)` arm** — the tested
     `ignore_interrupt` child ignores SIGINT+SIGTERM and dies at the SIGKILL stage, exercising stages 1→3 (and
     the SIGKILL return) but NOT the "dies specifically on SIGTERM" stage-2 return, the last-resort blocking
     reap, nor the `child.wait()` IO-error arm. These are spec-§1.4 defense-in-depth in the escalation ladder.
- **`src/bin/wasl-fake-agent.rs` (the TEST FIXTURE): 55.69% line.** This is NOT the mapping/framing/supervision
  core — it is the in-repo persistent analog of the `cat`/`sh -c` stand-in commands the adapter crates drive
  (which, being external binaries, are not coverage-counted AT ALL; ours is in-repo so llvm-cov sees it). Every
  command branch a frozen test exercises IS covered; the uncovered ~44% are (a) missing-field `.unwrap_or`
  fallbacks the tester's well-formed `req_*` builders never trigger, (b) commands/sub-paths no frozen test
  drives (bare `gate`, stray `release`, the fact-set-while-held nested path, unknown-command), and (c) the
  async stdin/stdout IO write arms. Its pure fact-map (`recall_reply`) has the `#[cfg(test)]` micro-tests the
  support module asks the coder to write; the fixture's OBSERVABLE wire is pinned black-box by the tester-owned
  `fake_agent_wire.rs`. Forcing 100% on a fixture by extracting artificial pure layers would be gaming-adjacent
  and is deliberately NOT done (per the task's honest-residual instruction).

`cargo llvm-cov -p wasl-runtime-protocol --fail-under-lines 100` therefore exits 1 (79.73% < 100%), for the
same reason Phase-2A `wasl-runtime`'s does: the residuals are genuinely-unreachable-by-the-public-API IO /
defensive branches (driver) + an intentionally-un-exhaustively-driven test fixture (bin). The
mapping/framing/supervision CORE is at 93.65% line with every miss enumerated and verified live above.

## Phase-2C scope (LANDED) — two new fake-agent wire commands for the bidirectional gate

Phase-2C added, to the fake-agent BINARY ONLY (`src/bin/wasl-fake-agent.rs`; the `FakeLineProtocol` driver in
`fake_line.rs` was NOT touched — both commands reuse EXISTING `ProtocolEvent` mappings, per the tester's
finding that no new event variant is needed), the two wire commands `bidirectional_genericity.rs` specifies:

- **`{"cmd":"ask_wasl_gate","key":"<K>"}`** (probe A, the WORKS case): emit `accepted`, then
  `{"ev":"gate","summary":"ask_wasl","preview":"<K>"}` (the existing `gate`→`NativeGateRequested` mapping),
  BLOCK reading exactly ONE more stdin line (the actor's `resolve_native_gate` answer, over the SAME pipe),
  then `{"ev":"done","reply":"answered:<K>"}`.
- **`{"cmd":"ask_wasl_tool","key":"<K>"}`** (probe B, the LEAK/sharper case): emit `accepted`, then
  `{"ev":"tool_start","name":"ask_wasl"}` (the existing `tool_start`→`ToolStarted` mapping), BLOCK reading
  exactly ONE more stdin line, then — ONLY after that line arrives — `{"ev":"tool_end","name":"ask_wasl",
  "ok":true}` and `{"ev":"done","reply":"answered:<K>"}`. Through the frozen API there is no way for the
  single-writer actor to send that line as an answer TO THIS TOOL CALL, so the turn stays blocked (the
  demonstrated bounded hang); the Phase-2C `wasl-runtime::InteractiveTransport::answer_tool_call` fix (in the
  R1 crate) is what would send it — the commented recommended-target test targets exactly that.

Two small event emitters (`tool_start`, `tool_end`) were added alongside the existing `gate`/`done`/`accepted`
emitters. All 3 live probes in `bidirectional_genericity.rs` are green; ALL Phase-2B tests (51) remain green
with ZERO regression (the fake's observable wire is still pinned black-box by `fake_agent_wire.rs`, unaffected).

### Coverage honesty (Phase-2C) — measured
`cargo llvm-cov -p wasl-runtime-protocol --summary-only`: `fake_line.rs` **93.65% line** (UNCHANGED — the
driver core was not touched); `bin/wasl-fake-agent.rs` ~51.6% line (the fixture, down slightly from 55.69%
because the two new command branches add well-formed-only-driven arms + the `tool_start`/`tool_end` emitters,
whose missing-field fallbacks the tester's well-formed `req_ask_wasl_*` builders never trigger — the same
intentionally-un-exhaustively-driven-fixture posture as Phase-2B, NOT gamed). TOTAL ~76.9% line.
`--fail-under-lines 100` exits 1 for the same documented reasons as Phase-2B.

## Phase-4 scope (LANDED) — `ClaudeStreamProtocol` (real claude `stream-json` driver)

Phase-4 added `src/claude_stream.rs` and re-export wiring in `src/lib.rs` (`mod claude_stream; pub use
claude_stream::ClaudeStreamProtocol;`). The driver launches the real claude CLI as a persistent child with the
VERIFIED argv shape (`claude -p --input-format stream-json --output-format stream-json --verbose --session-id
<uuid>`), maps stream-json lines to R1 `ProtocolEvent`, and owns the same IO-shell supervision discipline as
`FakeLineProtocol` (stderr drain, watchdog, bounded interrupt→TERM→KILL escalation, EPIPE/WriteZero-benign stdin
write policy, reap-on-drop).

Key behavioral pins that landed:
- **Native id minting:** `spawn` requires `SpawnOptions::target_key()` and derives `--session-id` via UUIDv5 with
  the SAME namespace literal as `wasl-adapter-claude` (`2441e851-1801-440d-a8e9-214d1df08e6e`), declared
  independently in this crate to keep dependency direction clean.
- **`resume`:** re-attaches using the supplied `NativeSessionId` bytes verbatim (`--session-id <native>`), not
  re-derived from `SpawnOptions`.
- **`send_turn`:** emits one JSON-escaped NDJSON user message line via `serde_json` + newline + flush.
- **Event mapping:** `system/init -> SessionReady`, `assistant.text -> AssistantDelta` (one per text block),
  `result{is_error:false,result} -> TurnCompleted`, `result{is_error:true,error?} -> TurnFailed` with the
  fixed missing-error fallback string. Malformed/unknown lines are ignored (framing continues).
- **First-turn-only `SessionReady` normalization:** claude emits repeated `system/init`; transport-level
  `session_ready_seen` suppresses all repeats after the first, so R1's public `TurnEvent::SessionReady` remains
  first-turn-only.

Coder-owned `#[cfg(test)]` unit tests in `src/claude_stream.rs` pin byte-exact mapping fixtures for captured
wire rows and document the intentional, spec-cited residual gaps: `assistant.tool_use` and `user.tool_result`
remain unmapped (`vec![]`) until a follow-up real tool/gate capture exists (`runtime-protocol.md` §2.1 ASSUMED
rows).

## Post-Phase-5 fix — the in-flight `pull_one` event-loss bug (found by the first real end-to-end run)

The very first real, end-to-end execution of `ClaudeStreamProtocol` against the actual `claude` CLI (by a
human, outside the sandboxed dev session — every earlier attempt crashed on an unrelated Bun memory issue
before reaching this code path) found a genuine, always-reproducing bug: `pull_one`'s in-flight wait was
capped at `POLL_IDLE` (500ms) regardless of the real hang-watchdog budget. `wasl-runtime`'s generic actor
(`actor.rs::run`, `PumpNext::Pump`) calls `transport.next_event().await` as a STANDALONE await; a spurious
`None` while a turn is active makes the actor park purely on its mailbox, re-woken only by a one-shot `Poke`
sent at subscribe. claude's real `system/init` lands 3-7s in (MCP-server connection time) — well past
500ms — so it was lost FOREVER every single time, hanging the turn until the watchdog eventually killed the
child. Fixed in both `claude_stream.rs` and `opencode_acp.rs`: `pull_one` now extracts its wait-duration
calculation into a pure `next_wait(...)` helper that, while a turn is in flight, waits the FULL remaining
hang-watchdog budget (not a short breather) — proven with `#[cfg(test)]` table-driven unit tests, fail-before/
pass-after verified live by an independent audit (reverting `next_wait` to the old `poll_idle.min(...)` body
reproducibies the bug's signature: `left: 500ms, right: 59.99s`).

**Investigated and correctly NOT applied to `fake_line.rs`:** the identical buggy pattern exists there too,
but applying the same fix regresses `tests/bidirectional_genericity.rs` (4 tests) — that suite's
`resolve_native_gate`/`answer_tool_call` mailbox commands, sent while a turn is active, depend on the actor
periodically returning from `next_event()` (via the short `POLL_IDLE` breather) to service its mailbox;
blocking for the full 60s watchdog starves them (their probe timeouts are ~3s, far shorter). `fake_line.rs`
was left unchanged; this was independently re-derived and confirmed by audit, not just asserted.

**Why the claude/opencode fix is safe today despite this:** neither `map_claude_line` nor `map_acp_line` ever
produces `ProtocolEvent::NativeGateRequested` or `ProtocolEvent::ToolStarted` (both drivers deliberately leave
every tool/gate wire shape ASSUMED-unmapped, per their own pure-mapping test suites) — so neither driver can
put the actor into `AwaitingGate`/`RunningTool`, and the `fake_line.rs` starvation scenario is structurally
impossible for them today. The one REAL, still-live residual: `cancel_turn`/`shutdown` also route through the
same mailbox and can now be delayed (bounded by the watchdog, never infinite) while a turn is in flight —
documented explicitly in both files' `pull_one` doc comments, not hidden.

**Required follow-up (gating, not optional) before either driver's mapping is EVER extended to cover a real
gate/tool wire shape (a Phase-0'-follow-up capture is already a documented prerequisite for that, per
`runtime-protocol.md` §2.1/§3.1):** the generic actor's `actor.rs::run` `PumpNext::Pump` arm must race
`next_event()` against `mailbox.recv()` (e.g. via `tokio::select!`) instead of a standalone await, so a
mailbox command preempts a blocking `next_event()` without needing it to return first — while preserving the
existing "never cancelled mid-event" invariant (a `select!` must only accept the mailbox branch when
`next_event()` genuinely hasn't produced anything yet, not cancel an in-flight event). Once that lands,
`fake_line.rs`'s `pull_one` should also be lifted to the same full-watchdog-budget policy for consistency
across all three drivers.

## Phase-5 scope (LANDED) — `OpenCodeAcpProtocol` (real opencode `acp` JSON-RPC-over-stdio driver)

Phase-5 added `src/opencode_acp.rs` plus crate-root export wiring in `src/lib.rs`
(`mod opencode_acp; pub use opencode_acp::OpenCodeAcpProtocol;`).

The landed driver intentionally mirrors the same supervision architecture used by
`FakeLineProtocol`/`ClaudeStreamProtocol` (stderr drain task, event mpsc channel, kill-request channel,
bounded interrupt→TERM→KILL escalation with `wait_for_exit`/`best_effort_signal`, hang watchdog, benign
`BrokenPipe`/`WriteZero` turn-write handling, reap-on-drop), while applying ACP-specific wire semantics.

What landed:
- **Capabilities locked to §3.1:**
  `persistent_process=true`, `persistent_conversation=true`, `structured_turns=true`,
  `streaming_output=true`, `attachable=true`, `interruptible=false`,
  `structured_provenance=true`, `resumable=true`.
- **Synchronous handshake inside `spawn`/`resume`:** writes `initialize` (id=1), reads+validates response;
  then writes `session/new` (spawn) or `session/load` (resume) (id=2), reads+validates response, and extracts
  native `sessionId` before creating the supervisor/transport split.
- **First observable event:** supervisor emits `SessionReady{native: Some(session_id)}` as the first event,
  then enters the normal read/map loop on the remaining lines.
- **Turn request ids:** handshake consumes ids 1 and 2; turn requests start at id 3 and increment per turn.
- **Accumulation design split (intentional):**
  `map_acp_line` stays pure/stateless (`agent_message_chunk -> AssistantDelta`, `result.stopReason ->
  TurnCompleted{reply:""}`), while `OpenCodeAcpTransport::next_event` performs stateful accumulation
  (`AssistantDelta` appends to `accumulated_reply`; `TurnCompleted` replaces empty reply with
  `mem::take(&mut accumulated_reply)`).
- **ASSUMED tool/gate rows remain intentionally unmapped:** non-`agent_message_chunk` updates (including
  tool/gate-shaped updates) are ignored until real captures pin concrete shapes.

Honest residual gap:
- **`resume` RPC shape is ASSUMED, not spike-captured.** Spike 0′ captured resume capability advertisement but
  did not exercise a resume round-trip. This landing uses `session/load {sessionId,cwd,mcpServers}` as the
  documented counterpart to `session/new`, with fallback to the requested native id when response omits
  `result.sessionId`. Follow-up live capture is required before calling this fully verified.

Verification summary for this landing:
- `cargo test -p wasl-runtime-protocol`: green.
- `cargo test -p wasl-runtime-protocol --features live-cli --no-run`: compile-clean (no live execution).
- `cargo build --workspace`: clean.
- `cargo clippy --workspace --all-targets -- -D warnings`: clean.
- `cargo fmt --check`: clean.

Coverage honesty (`cargo llvm-cov -p wasl-runtime-protocol --summary-only`):
- `opencode_acp.rs`: **27.54% lines / 26.38% regions**.
- This low default-run coverage is expected and honest: the uncovered area is predominantly spawn/handshake/
  supervise/escalation plumbing, which is only meaningfully exercised by deferred live-cli runs; mapping logic
  is covered by byte-exact unit fixtures in `#[cfg(test)]`.

Genericity assertion still holds:
- **No new `ProtocolEvent` variants and no new `TargetStatus` variants were required.**
- `crates/wasl-runtime/src/lib.rs` remained untouched in Phase-5.

## Phase-6 scope (LANDED) — `CodexAppServerProtocol` (real codex `app-server --stdio` JSON-RPC-over-stdio driver)

Phase-6 added `src/codex_appserver.rs` plus crate-root export wiring in `src/lib.rs`. Diagnosed from first
principles (no prior spec pin existed): `codex`'s CLI surface was explored fresh, `app-server --stdio`
confirmed as the persistent multi-turn substrate (`exec` is per-turn/respawn; `exec-server` is a remote/
websocket cloud registration, not a local stdio substrate; `mcp-server` is codex acting as a tool PROVIDER,
the wrong direction), and the wire shape captured live and cross-checked against the AUTHORITATIVE schema
(`codex app-server generate-json-schema` — not just live capture guesswork).

What landed:
- **Capabilities:** `persistent_process=true`, `persistent_conversation=true`, `structured_turns=true`,
  `streaming_output=true`, `attachable=true`, `interruptible=false` (consistent with `ClaudeStreamProtocol`/
  `OpenCodeAcpProtocol`, both also `false` — `turn/interrupt` is schema-real but was never exercised live, so
  the capability is not rounded up), `structured_provenance=true`, `resumable=true` (now VERIFIED, see below).
- **Wire shape:** NDJSON JSON-RPC; requests `{jsonrpc,id,method,params}`, responses `{id,result|error}`,
  notifications carry NO `id`/`jsonrpc` (`{method,params,emittedAtMs}`) — same family as opencode's ACP, NOT
  claude's raw stream-json (which has no request/response envelope at all). Handshake: `initialize` (id=1) →
  `thread/start` (spawn) or `thread/resume` (resume) (id=2); a `thread/started` notification can arrive BEFORE
  its matching response (the same handshake-ordering hazard opencode had, handled by the shared
  `json_rpc_handshake` classifier). Turns: `turn/start` acks `status:"inProgress"` immediately; streaming via
  `item/agentMessage/delta`; terminal via the single `turn/completed` notification, whose `params.turn.status`
  carries `completed|interrupted|failed|inProgress` — there is NO separate `turn/failed` method (confirmed
  against both the live capture and re-derivable from the schema's terminal-status enum living inside
  `turn/completed`, not as sibling methods).
- **Tool-call correlation:** `CodexToolTracker` correlates `item/started`/`item/completed` (where
  `item.type == "commandExecution"`) by `item.id`, mirroring `ClaudeToolTracker`'s id-correlation pattern. It
  clears its pending-id map on the `turn/completed` METHOD (not conditioned on the embedded `status`) — a
  bug found by dev-manager audit before this landed (an interrupted/failed tool call would otherwise leak one
  entry per turn for the life of the session, the same unbounded-growth class already fixed for
  `ClaudeToolTracker`) and fixed with a regression test proven to fail without the fix.
- **`resume` — VERIFIED, not ASSUMED:** implemented against `thread/resume`
  (`{threadId,cwd} -> {..., thread:{id,...}}`), whose request/response shape was confirmed against the
  authoritative schema (not just a live guess). The full cross-process round-trip — a thread started by one OS
  process, resumed by a genuinely FRESH, separate OS process after the original has fully exited, still
  recalling pre-exit conversational context — was proven live TWICE: a standalone probe
  (`spikes/live-substrate/codex_appserver_resume_probe.py`, R1_resume_succeeds=true,
  R2_context_retained=true) and a permanent regression-guarded test
  (`tests/codex_appserver_resume_live.rs::resume_in_a_fresh_process_recalls_pre_exit_context`, written from
  spec against the `InteractiveProtocol`/`InteractiveTransport` seam, passing against the real CLI in 8.68s).
  This is AHEAD of `OpenCodeAcpProtocol`'s own resume gap, which remains ASSUMED (Phase-5, above) — the two are
  independent and were not required to close together.

Shared-plumbing extraction (done alongside this landing, once the byte-for-byte duplication across ALL FOUR
driver files was independently verified): `src/bounded_io.rs` (`BoundedLineRead`/`read_bounded_line`/
`discard_one_bounded_line`/`discard_until_newline`/the two resource-bound constants), `src/process_supervision.rs`
(`next_wait`/`best_effort_signal`/`escalate`/`wait_for_exit`, plus consolidated `#[cfg(test)]` fixtures
previously duplicated 2-3x), `src/json_rpc_handshake.rs` (the canonical `HandshakeLineClass`/
`classify_handshake_line`, shared by `opencode_acp.rs` and `codex_appserver.rs`, with codex's pre-existing
external names `CodexHandshakeLineClass`/`classify_codex_handshake_line` kept as `lib.rs` re-export ALIASES of
the same shared items so the frozen `tests/codex_appserver_mapping_and_handshake.rs` needed zero changes).
`fake_line.rs` and `claude_stream.rs` also migrated to the shared `bounded_io`/`process_supervision` modules
(they have no JSON-RPC handshake to share). No behavior change; verified byte-for-byte equivalent per item
before extraction, all 27 test binaries green after, zero coverage silently dropped.

**Honest residual gap — NOT fixed, deliberately (a product/security decision, not a driver bug to silently
paper over):** the real codex schema (`ServerRequest.json`) defines several methods the SERVER can send as a
genuine JSON-RPC REQUEST requiring a response written back to stdin before codex proceeds —
`execCommandApproval`, `applyPatchApproval`, `item/commandExecution/requestApproval`,
`item/fileChange/requestApproval`, `item/permissions/requestApproval`, `item/tool/requestUserInput`,
`mcpServer/elicitation/request` — for interactive approval flows under a stricter `approvalPolicy`. This
driver never sets `approvalPolicy`/`sandbox` in `thread/start`/`thread/resume` (both fields are optional and
omitted, so codex falls back to its own/config.toml default), and empirically, under that default, a real
live-captured shell-command tool call completed with ZERO approval-request-shaped lines ever appearing on the
wire — the gap is currently DORMANT, not proven unreachable. If codex ever emits one of the methods above, it
is silently dropped by `map_codex_value`'s method-only match (no `id`-based request/notification distinction
exists in this driver) and codex is left blocked waiting for an answer that never comes — a SILENT hang,
bounded only by the hang watchdog eventually killing the process. Documented in detail directly above
`supervise` in `src/codex_appserver.rs`. Not fixed here because the correct response (auto-approve? auto-deny?
a new `ProtocolEvent` gate the caller must answer, mirroring `NativeGateRequested`?) is a real security
decision that should not be picked unilaterally inside a driver.

Verification summary for this landing:
- `cargo test -p wasl-runtime-protocol`: green (41 lib unit + all integration files, including the 29 frozen
  codex mapping/handshake tests and the 3+1 live-cli codex tests run separately against the real CLI). One
  pre-existing, environmental sandbox-contention flake (`hostile_inputs::eof_without_code_maps_to_process_exited_none`)
  reproduces identically with this landing's changes fully reverted — confirmed unrelated.
- `cargo test -p wasl-runtime-protocol --features live-cli --no-run`: compile-clean.
- `cargo build --workspace`: clean. `cargo clippy -p wasl-runtime-protocol --all-targets -- -D warnings`: clean.
  `cargo fmt --check`: clean.
- Independent dev-manager audit: SHIP, zero blocking findings (one non-blocking finding — a hypothetical
  distinct `turn/failed` method — checked against the real captured wire and confirmed not to exist; documented
  as an explicit assumption in the tracker's turn-boundary-clear comment).

Genericity assertion still holds:
- **No new `ProtocolEvent` variants and no new `TargetStatus` variants were required.**
- `crates/wasl-runtime/src/lib.rs` remained untouched in Phase-6.
