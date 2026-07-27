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
