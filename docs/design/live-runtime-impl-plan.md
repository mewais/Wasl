# Live / Managed-Runtime Delivery — IMPLEMENTATION PLAN (v2, council-reviewed)

> Turns `docs/design/live-runtime-plan.md` (the accepted design contract) into a concrete, sequenced,
> test-first execution plan. **v2** incorporates an independent three-reviewer council pass
> (correctness/concurrency, breadth/operability, systems/framing). It is **not yet ready to start Phase 1**:
> two blocking questions (§0) require a human decision first, and the design contract itself needs two
> amendments the council surfaced.

---

## 0. BLOCKING — resolve before any code (council found these; they gate everything)

### 0.1 Premise decision: "own-the-session" vs "terminal-attach" (MUST be answered by a human)

The design's motivation (§1) says: deliver into "the session a human is **actively driving in their
terminal**." The design's locked safety core (§4) says: "**Nothing else may write to the underlying
process's stdin/socket directly. No shared raw handles, ever.**" **These are mutually exclusive.**

- If a human drives the CLI's native TUI, the TUI owns stdin → the human is a second writer → the
  single-writer invariant is already broken. This is the "human-keystroke race" the design itself rejects
  (§3 (b), §8 hard rule).
- If Wasl owns the only input path (`wasl session send`), the session is **Wasl-owned**, not "the terminal
  session the human is driving." The human is redefined as a `TurnSource::LocalHuman` who can **observe**
  (`attach`) and **inject via Wasl** (`send`) — but is not typing into a native REPL.

The plan as drafted **silently builds the second model** while the motivation advertises the first. They are
different products, and Phase-4's e2e proves the second. **Two honest options:**

- **Model A — Wasl-owned live session (what the architecture actually delivers, RECOMMENDED).** Redefine
  §1: "human-visible" = observe (`attach`) + Wasl-mediated input (`send`). Wasl spawns/owns the process; no
  native TUI is in play. The single-writer invariant holds cleanly. **Action:** amend design §1 to drop
  "the session a human is driving in their terminal," replace with "a Wasl-owned live session a human can
  observe and inject into."
- **Model B — true terminal-attach.** Attach to a process the human already runs in a real terminal.
  **This requires a shared input path (PTY / multi-client protocol) and is FORECLOSED by the locked
  single-writer invariant.** No Phase-0 spike can make it safe; it contradicts the design's own safety rule.
  If this is what's actually wanted, **this plan does not and cannot deliver it** — the design must be
  re-opened, not just re-planned.

**→ DECISION NEEDED:** Model A (proceed) or Model B (re-open the design)? Everything below assumes **A**.

### 0.2 Sequencing decision: spike ONE real CLI before building the generic core (RECOMMENDED)

The v1 draft built the XL generic `wasl-runtime` core (Phase 2) *before* confirming any CLI can actually be
driven same-process/multi-turn (Phase 0, unverifiable in sandbox). The draft's own contingency — "ship a
correct scheduler with an honest empty set of live targets" — is a **sunk cost relabeled as a milestone**: a
scheduler with zero live targets delivers nothing the current ephemeral path doesn't. The council (all three)
flags this as a sequencing inversion.

**→ RECOMMENDED:** insert **Phase 0′** — a throwaway ~200-line hardcoded end-to-end spike (no traits, no
actor, no `ProtocolEvent`): real claude on a persistent stdin, write turn 1, write turn 2 **without touching
stdin's EOF state**, assert **same PID** and **turn 2 sees turn 1's context**. This buys the one fact the
whole feature rests on, for days of effort instead of an XL crate. **Gate:** if no CLI passes, STOP and ship
only the capability-matrix finding — do not build the generic core speculatively.

**→ DECISION NEEDED:** spike-first (recommended) or build-core-first (accept the sunk-cost risk)?

### 0.3 Two design-contract amendments the council requires (mechanical, but must land in §1/§4 of the design)

1. **State plainly that all runtime state is in-memory only** (managed targets, idempotency map,
   OutcomeUnknown turns die on restart; only `wasl-audit` survives). The design's `RuntimeProvider`
   "constructs **or recovers**" language and the `resumable` capability *imply* durability that isn't there.
   Either add minimal durability (see D7) or explicitly scope recovery to within one node-process lifetime.
   **This is Wasl's first subsystem whose source of truth is NOT the append-only audit log** — say so.
2. **Give the native gate (`AwaitingGate`) a resolution path** (see D3/D9). As drafted, a session can enter
   `AwaitingGate` but nothing can resolve it → permanent wedge. This is a functional hole, not a nicety.

---

## 1. Executive summary

An *alternative* delivery runtime that injects a turn into a **Wasl-owned, persistent, multi-turn agent
session** (Model A, §0.1), sitting *above* — never modifying — the existing `AgentAdapter::deliver` seam.
Three new blocks (design §7): **R1 `wasl-runtime` (L1)** generic core; **R2 `wasl-runtime-protocol` (L2)**
concrete drivers; **R3 `wasl-session` (bin)** local control surface.

**Core bet (3 legs):** (1) capability model over a `live:bool`; (2) providers layered above, adapters
wrapped-not-amended; (3) single-writer actor per managed target.

**Council verdict on shape:** the layered decomposition is **right and native to Wasl** (mirrors
`AgentAdapter` / `CliInvoker` / `Surfacer`+`Sink`); the test-first discipline is right; the DAG/tiers are
correct (with the R1-owns-the-traits fix). **The problems are (a) the two §0 blockers, and (b) a substantial
set of Phase-1 contract gaps the v1 draft under-specified.** With §0 resolved and Phase 1 expanded, it is
executable.

**Biggest risk:** the substrate may not exist (Phase 0/0′) — mitigated by spiking first (§0.2).

---

## 2. Phases (test-first: tester writes tests/** from spec; coder writes src/** only; dev-manager audits)

### Phase 0′ — One-CLI substrate spike (NEW, from council) — S — the go/no-go
Throwaway hardcoded claude (then opencode) same-process multi-turn spike. Assert same PID across 2 turns +
context retention without Wasl EOF-ing stdin. **Gate:** ≥1 CLI passes → proceed; else STOP. Output: evidence
feeding the capability matrix. Not verifiable in this sandbox — needs the user's box.

### Phase 0 — Full feasibility spikes + capability matrix — M — riskiest: everything
Per CLI (claude/codex/opencode): P=same PID? R=ready signal? B=turn boundary? N=native id?
I=interruptible? A=attachable? **Also spike the control-plane substrate** (opencode `serve` HTTP vs `acp`
stdio vs `serve`+`--attach` are DISTINCT substrates — evaluate separately, pick one primary; council). Also
spike **native-gate behavior** (does a gate appear in headless/served mode; can it be resolved out-of-band?).
Durable artifact: `docs/design/live-runtime-capability-matrix.md` with explicit states
{verified-true, verified-false, unsupported-by-documented-interface, not-tested/env-unavailable, unknown} —
**"not installed" is NOT a capability verdict** (council). Zero `candidate` cells; each true → linked
transcript.

### Phase 1 — Contracts + spec (EXPANDED per council) — L (was M) — riskiest: locking the full contract
Deliverables: `docs/specs/runtime.md`, `runtime-protocol.md`, `session.md`, `runtime-control-api.md` (the
inter-process control plane, see D8b). Frozen, object-safe, type-only trait/type surface. **Every item below
must be locked in the spec before Phase 2 code** (this is the bulk of the expansion the council demanded):
- The **public `TurnEvent` vocabulary** (distinct from the internal `ProtocolEvent`) + its ordering, replay,
  and truncation guarantees. (gpt #1.5 — currently missing entirely.)
- A **full state×command transition matrix**: every `TargetStatus` × every `DeliveryTarget` method + every
  `ProtocolEvent`, defining resulting status, emitted events, certainty change, queue disposition. Covers
  shutdown(Drain/Immediate) in every phase, cancel of wrong/completed/queued turn, cancel when
  `!interruptible`, ProcessExited before/after injection/idle, TurnFailed vs ProcessExited vs interrupt.
- **All 8 capabilities** get one normative conformance test each (incl. streaming_output ordering,
  attachable multi-observer, structured_provenance id-correlation, resumable). Plus a **fixed
  expected-capability report per concrete driver** (capability-gating alone lets a broken driver set all
  flags false and pass — council).
- **`RuntimeError` taxonomy** (distinct testable variants, like `ConfigError`/`AdapterError`).
- **Delivery-certainty state machine**: exact transition points, which events/receipts carry it, how it
  enters `wasl-audit`, and — for `command` — a **write-ahead durable audit entry before injection** so a
  crash can't erase the evidence needed to prevent unsafe retry (o3 #1.5, gpt #1.6).
- **Causation + idempotency wired across A2A**: add fields to `wasl-proto`/dispatch metadata +
  `delivery_from_message`; not just R1-internal (gpt #1.7). Cycle rejection must cover **queued→queued**
  paths, not only synchronous (o3 #3.2).
- **Native-gate resolution policy** (D3/D9): pick one — (i) generic native-gate response API + human control
  via R3; (ii) managed profiles disable native gates, any request = terminal config fault; (iii) proven
  out-of-band native resolution. "Observe but never resolve" is banned.
- The **8 locked decisions** in §4 below.
- Injected **monotonic clock/timer seam** (tokio paused time) for all deadlines (gpt #2.6).
- **Wire versioning**: a `/v1` control contract with golden fixtures, mirroring `wasl-operator` (enum
  tagging, casing, unknown-field policy, additive compat).

### Phase 2 — Deterministic core (DECOMPOSED per council; was one XL) — split into 2A/2B/2C
- **2A — pure actor** (R1 src) against an **in-repo scripted `InteractiveTransport`** (lives in R1's own
  `tests/` support, NOT the R2 fake — avoids the reverse dev-dep cycle, gpt #2.3): single-writer actor, FIFO
  queue, event journal (bounded ring + **`snapshot()` API + atomic replay-to-live handoff**, o3 #1.3/gpt
  #6.4), idempotency (dedupe **outside** the mailbox via a shared per-key map so concurrent same-key submits
  can't both enqueue, o3 #1.4), transcript projection (bounded), causation/hop cycle-rejection, certainty.
  Tests: FIFO; reject-vs-queue; no-interleave; replay exactly-once/no-gap incl. truncation; attach/detach +
  **multi-observer** independent cursors; cancel; crash→OutcomeUnknown; dup-key→no double-inject; AwaitingGate
  blocks admission; cycle (sync AND queued) rejected; max-hop rejected. **Concurrency hardening (o3/gpt):**
  a `Barrier`-driven adversarial concurrent-submit stress test + **loom or proptest interleaving** exploration
  before 2A is done — line coverage alone won't catch ordering bugs.
- **2B — process/framing plumbing** (R2): the long-lived **fake structured agent** dev-bin + `FakeLineProtocol`,
  with **hostile scripts** (split JSON across reads, multiple msgs per read, malformed JSON between valid
  events, missing TurnAccepted, duplicate completion, delta-after-completion, EOF-without-ProcessExited,
  exit-before-stdout-drained, ignored interrupt, gate-then-exit) — gpt #2.2. **Process supervision** (gpt
  #3.1): reap normal/abnormal exit, kill descendants (process group), signal escalation
  interrupt→SIGINT→TERM→KILL each bounded, hang watchdog (opencode's known non-terminating loop!), stderr
  drain, cleanup on half-constructed actor / node shutdown / actor panic (no orphan children).
- **2C — the bidirectional genericity gate** (council #1, the sharper probe): extend the fake to a **mid-turn
  callback** (a `ToolStarted` that blocks the turn until the actor sends a value back). If `ProtocolEvent` +
  the single-writer actor carry it without new variants or a second writer, genericity is real; if not,
  document that `ProtocolEvent` is proven generic **only for unidirectional turn loops** and scope the claim.
  This finds control-flow leakage in a deterministic test now, not in year-2 when a bidirectional CLI appears.
- DoD (each sub-phase): `cargo test` green; `llvm-cov --fail-under-lines 100` with documented line/function/
  **region** residuals (match the repo's discipline, not just lines — gpt #2.8); clippy/fmt; AGENTS.md matrix.

### Phase 3 — EphemeralTarget + conformance suite + node wiring (reordered per council) — M
Land the **capability-gated conformance suite** here (Phases 4/5 depend on it) as **staged** suites (baseline
/ managed / per-capability / protocol-process / live-native — not one monolith a capability-gate can bypass,
gpt #4.3). `EphemeralRuntimeProvider<A>` wraps today's adapters UNCHANGED; capability report is **per concrete
adapter, injected** (not mechanically derived from the generic `AgentAdapter`, which doesn't promise resume —
gpt #1.2). Config toggle `runtime="ephemeral"|"managed"` (default ephemeral; **preserve today's
`AdapterExecutor` path literally** on the default path to avoid concurrency/behavior drift — gpt #6.2). The
runtime-backed **A2A `TaskExecutor`** is specified here with an atomic subscribe-then-admit (via
`Started{first_seq}`) so the executor can't miss the first event; map queue-wait/expiry/OutcomeUnknown to A2A
task states; map A2A cancel → `cancel_turn`; define requester-disconnect ownership (gpt #6.3).

### Phase 3.5 — R3 `wasl-session` bin + control plane (NEW — was missing entirely, gpt) — M
The v1 draft never implemented R3 yet Phase 4's demo calls `wasl session send`. Build it: `start|list|attach|
send|interrupt|stop [--drain]` **plus its inter-process control protocol** — a `wasl-session` process cannot
reach an actor+child owned by a *different* `wasl-mcpd` process by linking the same crate. Use a
Unix-domain-socket (loopback-HTTP elsewhere) **versioned control API** mirroring the operator's snapshot+SSE+
sequence+`Lagged` pattern, with local authorization. Without this the Phase-4 demo is false.

### Phase 4 — First managed protocol: ClaudeStreamProtocol — L — riskiest: substrate reality (gated on 0′)
Map stream-json → `ProtocolEvent`; spawn/resume; pure mapping fixtures (byte-exact, `parse_reply`
discipline); conformance suite with claude's claimed caps asserted; `--features live-cli` e2e (§5.3): assert
**same PID/native-session-id** across a human `send` and an A2A `consult` (not a fresh spawn) + busy/queueing
live test. If 0′/0 downgraded claude to non-persistent, it stays under **EphemeralRuntimeProvider** (do NOT
mislabel a per-turn-process as `persistent_process:false managed` — council #4.7) — written decision.

### Phase 5 — Second protocol: OpenCodeAcpProtocol (or chosen substrate) — L — riskiest: genericity
Same structure. **Critical assertion:** conformance runs UNCHANGED across ephemeral+claude+opencode; any
per-protocol branch = leak = FAIL; `ProtocolEvent`/`TargetStatus` needed no new variants (or written
justification they're genuinely generic). Live e2e with OOM/context mitigations (`OPENCODE_DISABLE_AUTOCOMPACT
=1`, ≥~4GB, isolated config — W9 note). Note: 2C already stress-tested control-flow genericity; Phase 5 tests
wire-format genericity.

### Phase 6 — External-seam sketch — S — doc only; plus a **compile-only `ExternalManagedRuntimeProvider`
stub in Phase 1** so we learn in Phase 1 (not Phase 6) if the trait surface can host it without leaking a
CLI-ism (council #3).

---

## 3. Phase-0 spike detail
(Per CLI P/R/B/N/I/A as v1.) Additions from council: evaluate opencode `serve` (HTTP) vs `acp` (stdio) vs
`serve`+`--attach` as **distinct** substrates and pick ONE primary; spike native-gate appearance/resolution;
spike control-plane feasibility (can a separate process observe+inject). Env reality: needs the user's box;
opencode OOMs <2GB + MCP-catalog bloats context (isolated config).

## 4. Decisions to LOCK in Phase 1 (updated with council fixes)
1. **Event journal:** bounded ring + `snapshot()` returning `(state, last_seq)` + **atomic replay-to-live
   handoff**; `events(after_seq)` exactly-once/no-gap for `after_seq ≥ floor`, else `Truncated{earliest,
   latest}` + client re-snapshots. `TurnCompleted` always carries `TurnId` so clients correlate by id, not
   seq (o3 #3.1).
2. **Idempotency:** `(TargetKey, key)` deduped **outside** the mailbox (shared map / per-key lock) so
   concurrent same-key submits can't both enqueue; resend returns the ORIGINAL receipt. Persistence: see #7.
3. **AwaitingGate vs wasl-gate:** different sides, don't merge; wasl-gate stays send-side unchanged; native
   gate is receive-side. **BUT it must be resolvable** (§0.3.2 / D9) — pick the Phase-1 policy.
4. **Transcript:** bounded summary (last-N replies + current deltas), byte-bounded not just count-bounded.
5. **Actor mailbox:** bounded mpsc; `submit_turn` sends cmd+oneshot, awaits ONLY the receipt. **Mailbox-full
   must not spuriously reject an idle target** (o3 #1.1) — either await space with timeout or reflect
   queue-pressure in status; RejectedUnavailable only for genuine unavailability.
6. **TargetKey = context_id**, get-or-create per context — **plus admission control** (max targets global/
   per-sender, rate limit, idle eviction) so untrusted `context_id` can't DoS via target explosion (gpt #3.5).
7. **Persistence:** in-memory by default, BUT a **command's certainty + idempotency record is write-ahead
   durable in `wasl-audit` before injection** (the one durability exception, for retry-safety); everything
   else in-memory (restart abandons managed targets — stated in the contract, §0.3.1).
8. **wasl-mcp wiring:** `runtime` toggle; ephemeral → today's `AdapterExecutor` **literally unchanged**;
   managed → runtime-backed `TaskExecutor`. Exhaustive match on `RuntimeKind`.
   **8b. Control plane:** versioned local API (UDS/loopback-HTTP) for R3 ↔ the runtime owner, operator-pattern
   (snapshot+SSE+seq+Lagged+auth). Its own spec `runtime-control-api.md`.
9. **Native-gate resolution:** the chosen policy from §0.3.2, with types/ops if (i).
10. **Liveness policy:** startup/acceptance/no-progress/gate/idle/shutdown-grace timeouts, phase-aware (a long
    tool run ≠ dead); heartbeat where the protocol supports it.
11. **Bounds on ALL retained state** (idempotency map TTL/cap, receipt cache, target registry, queue len+bytes,
    delta size, causation path len) — days-long sessions need byte bounds (gpt #3.4).
12. **Observability into the existing Sink/operator broadcast** — managed turn-lifecycle + certainty emit into
    the audit `Sink`, not a private parallel system (council #3); operator-surface or a versioned runtime API
    exposes status/queue-depth/awaiting-gate/last-event/restart-count/journal-floor (gpt #3.7).

## 5. Risk register (top, updated)
1. No CLI supports same-process multi-turn (Med/**Critical**) → **Phase 0′ spike-first hard gate** (§0.2); do
   NOT build the core speculatively.
2. **Premise contradiction** own-vs-attach (High if unresolved/**Critical**) → §0.1 human decision before code.
3. Native-gate wedge, no resolution (High/High) → D9 policy in Phase 1.
4. Concurrency/deadlock in actor (Med/High) → bounded mailbox + receipt-only await + Barrier/loom/proptest
   in 2A; mailbox-full ≠ reject-idle.
5. Event-journal replay/truncation loses events (Med/High) → snapshot API + atomic handoff (D1).
6. Crash-after-injection double-injects a command on restart (Med/**High, safety**) → write-ahead durable
   certainty for commands (D7); never blind-retry OutcomeUnknown.
7. Abstraction leakage (Med-High/High) → 2C bidirectional fake (control-flow) + Phase 5 ACP (wire-format);
   conformance runs unchanged.
8. R3/control-plane missing (was invisible)/High → Phase 3.5 + versioned control API (D8b).
9. Process leaks/hangs/zombies (Med/High) → Phase 2B supervision + hang watchdog.
10. Scope blowout (High/Med) → Phase 2 decomposed 2A/2B/2C; ephemeral-only (0–3) is the coherent smaller
    shippable subset **only if §0.1=Model A and it's framed honestly**.
11. Tier-2 commit vs busy/gone managed session (Med/High) → commit-time BusyPolicy=Queue{commit_timeout};
    gate layer owns retry while certainty=NotInjected; forbid re-submit after OutcomeUnknown (o3 #2.1).
12. Multi-peer fan-out amplifies target explosion + partial OutcomeUnknown (Med/Med) → admission control +
    a multi-peer E2E scenario (gpt #6.5).

## 6. Open questions still to answer at Phase 1
BusyPolicy defaults+units+override precedence; EventSeq scope (per-target monotonic); WASL_RUNTIME env
override; whether different source agents may SHARE one target (security — gpt #3.9 threat model); actor-
per-target scale envelope (many targets + slow observers stress test); whether `ProtocolEvent` stays internal
(recommended) while only `TurnEvent` is public/versioned.

## 7. Council verdict (synthesized)
- **o3 (correctness):** CONDITIONALLY NOT READY — must-fix: snapshot/replay contract, mailbox back-pressure,
  concurrent-idempotency, OutcomeUnknown durability, Tier-2-commit-vs-busy. Layering/test-first commendable.
- **gpt (breadth):** NOT READY as written — must-fix: R3 has no phase + no control protocol; native-gate has
  no resolution; full TurnEvent model + state matrix + all-8-caps tests missing; causation/certainty/audit
  must cross A2A; process supervision absent; in-memory-only contradicts the recovery contract; bound all
  state. Strengths real; do not start Phase 2 yet.
- **systems reviewer:** SHAPE IS RIGHT, sequencing inverted + a premise contradiction. Must resolve: own-vs-
  attach (§0.1, human), spike-before-build (§0.2), genericity probe is on the easy axis (add bidirectional
  fake), observability reinvented (wire into Sink). First subsystem outside "audit = durable truth" — say so.

**Net:** the architecture's *shape* is endorsed by all three. It is **not ready to start Phase 1** until
§0.1 (human premise decision) and §0.2 (spike-first) are settled and the design gets the two amendments in
§0.3. With those + the expanded Phase 1 contract, it is executable and well-grounded in Wasl's conventions.
