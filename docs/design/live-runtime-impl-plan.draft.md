# Live / Managed-Runtime Delivery — IMPLEMENTATION PLAN (draft for review)

> Draft turning `docs/design/live-runtime-plan.md` (the accepted design contract) into a concrete,
> sequenced, test-first execution plan for the Wasl repo. Under review by the council. Not yet accepted.

## 1. Executive summary

An *alternative* delivery runtime that injects a turn into an **already-running, persistent, multi-turn
agent session**, sitting *above* — never modifying — the existing `AgentAdapter::deliver` seam. Three new
blocks (design §7):

- **R1 `wasl-runtime` (L1):** generic CLI-agnostic core — `RuntimeCapabilities`, `DeliveryTarget`,
  `RuntimeProvider`, the per-target single-writer actor/state-machine (FIFO queue, event journal,
  idempotency map, transcript projection, causation/delivery-certainty), and the two providers
  (`EphemeralRuntimeProvider` wrapping today's adapters unchanged; `WaslManagedRuntimeProvider`). 100%
  against an in-repo fake structured agent.
- **R2 `wasl-runtime-protocol` (L2):** concrete drivers (`FakeLineProtocol`, `ClaudeStreamProtocol`,
  `OpenCodeAcpProtocol`) implementing the R1 driver traits. IO shell; real CLIs behind `--features live-cli`.
- **R3 `wasl-session` (bin):** local control surface (`start|list|attach|send|interrupt|stop`), human input
  through the same actor.

**Core bet (3 legs):** (1) capability model over a `live:bool`; (2) providers layered above, adapters
wrapped not amended; (3) single-writer actor per managed target.

**Biggest risk:** Phase-0 feasibility is unproven and unverifiable in CI/sandbox — the feature is worthless
if no CLI can be driven same-process multi-turn with detectable turn boundaries. Everything downstream is
designed to remain fully proven against the fake even if 0/4 real CLIs support persistence (degrading to "a
correct generic scheduler with an honest empty set of live targets" — still shippable and honest).

## 2. Phases (test-first: tester writes tests/** from spec; coder writes src/** only; dev-manager audits)

### Phase 0 — Feasibility spikes (linchpin) — M — riskiest: everything
- Deliverable: throwaway spikes + ONE durable artifact `docs/design/live-runtime-capability-matrix.md`
  (design §2 table filled with VERIFIED values; zero `candidate` cells; each `true` backed by a captured
  transcript). Plus a written go/downgrade/kill decision for Phase 4 (claude) & 5 (opencode).
- Not test-first (exploratory); dev-manager audits matrix honesty.
- NOT verifiable in this sandbox — needs the user's box (CLIs installed, isolated/big-context configs;
  opencode OOMs <2GB & MCP-catalog bloats context).

### Phase 1 — Contracts + spec — M — riskiest: over/under-specifying the event journal
- Deliverables: `docs/specs/runtime.md`, `runtime-protocol.md`, `session.md` (adapter-spec template);
  frozen rustdoc'd trait/type surface (all §3 traits + §4 types), object-safe, type-only (no `todo!()`);
  crates registered in workspace Cargo.toml.
- Tests: object-safety compile checks + serde round-trip for wire types. ALL §4 decisions locked here.
- DoD: specs accepted; every §4 decision written into runtime.md; builds with type-only stubs.

### Phase 2 — Deterministic managed-runtime manager (100%-tested core) — XL — riskiest: actor concurrency/deadlock
- Deliverables (R1 src): single-writer actor + FIFO queue + monotonic event journal + idempotency map +
  transcript projection + `WaslManagedRuntimeProvider<P>` + causation/hop cycle-rejection + delivery-certainty.
  Fake structured agent lives as an R2 dev-bin (`FakeLineProtocol` + long-lived `fake-structured-agent`),
  mirroring process_invoker stand-ins. R1 driven via FakeLineProtocol only.
- Sequencing subtlety: put `InteractiveProtocol`/`InteractiveTransport` traits + `ProtocolEvent` in R1
  (generic contract); only concrete drivers in R2 → keeps `wasl-runtime ← wasl-runtime-protocol` acyclic.
- Tester writes tests from runtime.md w/o reading src: FIFO; reject-vs-queue; no-interleave; event replay
  exactly-once/no-gap; attach/detach; cancel; crash→OutcomeUnknown; dup idempotency-key→no double-inject;
  AwaitingGate blocks admission; causation cycle rejected; max-hop rejected — via a scripted fake transport
  (hold/crash/remember/recall), no sleeps.
- DoD: `cargo test -p wasl-runtime` green; llvm-cov --fail-under-lines 100; remember/recall proves
  same-process context retention; clippy/fmt clean; AGENTS.md testability-honesty matrix.
- Riskiest: actor deadlock/backpressure; adversarial concurrent-submit Barrier stress test (operator
  gate_requested_ordering precedent).

### Phase 3 — Wrap headless adapters as EphemeralTarget: DeliveryTarget — M — riskiest: proving zero behavior change
- Deliverables (R1 src + wasl-mcp/config.rs): `EphemeralRuntimeProvider<A: AgentAdapter>` + `EphemeralTarget`
  (submit_turn → one deliver; synthetic TurnAccepted→AssistantDelta(final)→TurnCompleted); config toggle
  `runtime = "ephemeral"|"managed"` (RuntimeKind, exact-match validated, default ephemeral).
- Tester: capability-gated conformance suite (§5.2) — EphemeralTarget passes persistent_conversation (resume)
  and is NOT asserted for persistent_process; differential equivalence vs calling deliver directly;
  ephemeral submit_turn only ever Started. The conformance suite is authored HERE and reused in 4/5.
- DoD: conformance green; differential-equivalence green; runtime="ephemeral" default reproduces today
  exactly (config_codex loopback-receive proof); 100% on new R1 code.

### Phase 4 — First managed protocol: ClaudeStreamProtocol — L — riskiest: Phase-0 substrate reality
- Deliverables (R2 src): ClaudeStreamProtocol + transport mapping stream-json → ProtocolEvent; spawn/resume;
  behind --features live-cli: tests/live.rs + scripts/demo-live-consult.sh.
- Tester: pure mapping fixtures (captured stream-json → exact ProtocolEvent seq, byte-exact); conformance
  suite with claude's claimed caps ASSERTED; live e2e (§5.3: two loopback nodes; node B holds live claude;
  human `wasl session send` establishes VIOLET-7F29; A2A consult from A retrieves it; assert SAME
  PID/native-session-id across human turn & consult, not a fresh spawn) + busy/queueing live test.
- DoD: mapping core --fail-under-lines 100; conformance green (all claimed caps asserted); live-cli passes
  vs pinned claude; demo proves same-process continuity. If Phase-0 downgraded claude → ship as
  persistent_process:false resume-based managed (honest) or defer — written decision.

### Phase 5 — Second protocol: OpenCodeAcpProtocol (ACP/JSON-RPC) — L — riskiest: proving abstraction didn't just get renamed
- Deliverables (R2 src): OpenCodeAcpProtocol over opencode `acp`/`serve`+`--attach`, ACP notifications →
  SAME ProtocolEvent. Live tests + demo extension.
- Tester: same structure; CRITICAL extra assertion — conformance suite runs UNCHANGED across
  ephemeral+claude+opencode; any per-protocol special-casing = abstraction leak = FAIL.
- DoD: mapping core 100%; conformance green with NO protocol-specific branches; ProtocolEvent/TargetStatus
  needed NO new variants (or written justification they're genuinely generic); live e2e vs pinned opencode
  with OOM/context mitigations (OPENCODE_DISABLE_AUTOCOMPACT=1, ≥~4GB, isolated config).

### Phase 6 — External-seam sketch — S — doc only
- Follow-up design doc sketching `ExternalManagedRuntimeProvider` (same DeliveryTarget contract, generic
  HTTP/JSON-RPC wire, Surfacer/Sink precedent); checklist confirming R1 leaked no consumer/CLI name. Only
  after 0–5 green.

## 3. Phase-0 spike detail (per CLI: P=same PID? R=ready signal? B=turn boundary? N=native id? I=interruptible? A=attachable?)
- claude (primary P4): feed `claude -p --output-format stream-json --input-format stream-json` on PERSISTENT
  stdin; write turn 2 without closing stdin; watch for 2nd result. Kill signal: if closing/blocking stdin is
  required to get a result → persistent_process:false, "strongest candidate" claim is false.
- opencode (P5, maybe P4): A `opencode serve` (HTTP, PID stays up); B `opencode acp` (ACP JSON-RPC over
  stdio: session/new, session/prompt, notifications for boundaries + permission→NativeGateRequested); C
  `serve` + `--attach <url>` (second observer = attachable). `run` is NOT the substrate (self-terminates).
- codex: any long-lived mode beyond exec/exec resume? Expected honest: persistent_conversation-only.
- Artifact: capability-matrix.md, no `candidate` remaining, each true → linked transcript; R/B/N/I/A signals
  become the ProtocolEvent mapping-spec input for 4/5.

## 4. Key decisions to LOCK in Phase 1 (recommendations)
1. Event-journal: BOUNDED ring + explicit `Truncated{earliest_seq}` floor; events(after_seq) exactly-once
   only for after_seq≥floor; below → Truncated marker (operator SSE Lagged precedent). Most important
   decision.
2. Idempotency key: `(TargetKey, key)`, deduped for actor lifetime, resend returns ORIGINAL receipt.
3. AwaitingGate vs Wasl's 3-tier gate: DIFFERENT sides, do NOT merge. wasl-gate stays SEND-side (unchanged);
   native CLI gate is RECEIVE-side, surfaced as AwaitingGate (busy) for observability/admission only — Wasl
   does NOT auto-approve or route it through wasl-gate. Managed turn still passes wasl-gate on send exactly
   like ephemeral. (W9 "receive side deliberately inert w.r.t. Wasl gating" precedent.)
4. Transcript projection: BOUNDED summary (last-N final replies + current deltas), not full transcript.
5. Actor mailbox: BOUNDED tokio mpsc; submit_turn sends cmd + oneshot for receipt, awaits ONLY the receipt;
   full mailbox → RejectedUnavailable (backpressure = unavailability, never blocks). Anti-deadlock rule.
6. TargetKey = context_id (Delivery.context_id); target_for = get-or-create per context (mirrors adapters'
   context_id→session_id ledger).
7. Crash/OutcomeUnknown persistence: IN-MEMORY ONLY (adapter-ledger + W9 precedent). Restart loses managed
   targets & OutcomeUnknown turns; only wasl-audit provenance survives. Durable journal = follow-up block.
   STATE THIS EXPLICITLY.
8. wasl-mcp wiring: receive path builds NodeAdapter via build_adapter → add `runtime` toggle;
   ephemeral→today's AdapterExecutor (unchanged); managed→RuntimeProvider-backed TaskExecutor calling
   target_for(context_id).submit_turn + await completion. DeliveryTarget consumed where AdapterExecutor sits
   (wasl-adapter/src/lib.rs:135). Exhaustive match on RuntimeKind (AdapterKind forcing-function precedent).

## 5. Risk register (top 8)
1. No CLI supports same-process multi-turn (Med/Critical) → Phase-0 hard gate + kill/downgrade decision;
   R1/R2 proven against fake even with 0 live targets.
2. Abstraction leakage claude-vs-ACP (Med-High/High) → Phase 5 is the probe; conformance suite runs
   unchanged; new variant needs justification.
3. Actor concurrency/deadlock (Med/High) → bounded mailbox; submit_turn awaits only receipt; Barrier
   concurrent-submit stress; status() advisory-only.
4. Event-journal replay incorrect (Med/High) → lock #4.1; bounded ring + Truncated floor; deterministic
   scripted-transport tests, no sleeps.
5. Testing liveness deterministically (Med/Med-High) → deterministic fake (hold/crash/remember/recall); live
   behind --features live-cli, pinned versions, orchestrator asserts RAN.
6. Scope/size blowout (High/Med) → only Phase 2 is XL; durability/external/PTY all non-goals; ephemeral-only
   (0–3) is a coherent shippable milestone if 4–5 slip.
7. Gate/audit interaction (Med/High) → lock #4.3; Phase-4 test asserts managed consult still passes wasl-gate
   (no bypass).
8. Human+agent input races (Med/High) → single-writer invariant; `wasl session send` = TurnRequest{LocalHuman}
   through same submit_turn; no raw shared stdin; Phase-2 no-interleave test.

## 6. Open questions (answer by/at Phase 1)
1. BusyPolicy max_wait/expiry defaults & units per verb; who overrides (delivery/node-config/both)?
2. InteractiveProtocol trait ownership R1 vs R2 (recommend R1 — else DAG cycle).
3. Fake agent binary location (recommend R2 dev-bin).
4. RuntimeError taxonomy (undefined in design; needs distinct-testable-error treatment like ConfigError).
5. EventSeq numbering scope (per-target monotonic — state it).
6. attach multiple concurrent observers? (events(after_seq) implies yes; confirm broadcast fan-out).
7. RuntimeKind config default + WASL_RUNTIME env override.
8. How Tier-2 commit (prepare→commit, minutes later) interacts with a managed target that may now be
   busy/gone: does commit re-submit_turn (→ RejectedBusy)? interacts with command+OutcomeUnknown no-retry.

## 7. Pushback on the design doc
1. R1/R2 split right, but trait ownership under-specified & risks a cycle → put InteractiveProtocol/
   InteractiveTransport traits + ProtocolEvent in R1; only concrete drivers in R2. Make explicit in Phase 1.
2. Phase 3 (wrap ephemeral) should land the conformance suite EARLY — build it + EphemeralTarget right after
   contracts freeze, in PARALLEL with Phase 2's fake-driven scheduler. De-risks abstraction earlier; gives a
   shippable ephemeral-through-new-seam milestone if 4–5 slip. (Strict 2→3 is defensible but delays real-code
   validation.)
3. Phase 5 ACP is the right generality probe; no cheaper one is meaningful (two self-designed fakes prove
   nothing). But gate it on Phase-0 opencode verdict; fallback probe = codex as resume-based managed
   (persistent_process:false) — a second structurally-different PROVIDER even if not a second live protocol.
Smaller: §4 in-memory-only must be stated plainly; RuntimeError + BusyPolicy defaults underspecified;
transcript projection must stay a bounded summary (guard against over-engineering).
