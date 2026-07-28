# Live-runtime hardening plan (post-live-verification review)

**Status:** council-reviewed (3 independent consultants, `APPROVE-WITH-CHANGES` / `APPROVE-WITH-CHANGES` /
`MAJOR-CONCERNS`, all three converging on the same core changes below) and revised accordingly. **Nothing in
this document is built yet.**

## 0.1 Changes made after council review (read this before implementing §2/§3/§4 below)

All three reviewers independently confirmed `tokio::select!` is cancellation-safe for the CURRENT three
drivers (verified: `mpsc::Receiver::recv()` is cancellation-safe, and none of `pull_one`'s three
implementations hold partial state across an `.await`) — so §2's core approach stands. But they converged on
several concrete refinements and one new bug:

1. **NEW bug found (medium-high, add to §3): `cancel_turn` never clears `self.running_tool`.** After a
   cancellation, the actor can still believe a tool is running from the cancelled turn, so a subsequent
   `answer_tool_call` could wrongly route an answer into the NEW turn's transport call. Fix alongside §3.3's
   "Cancelling" sub-state (clear `running_tool` when the cancellation is confirmed, same place `active` is
   cleared).
2. **§4.3 (stdin write timeout) had a real flaw, now fixed here:** `AsyncWriteExt::write_all` is not atomic —
   a timeout can fire after a PARTIAL write, leaving the child's stdin desynced for every subsequent write on
   that same pipe. The fix is corrected below to require: a write timeout is a FATAL fault for that transport
   (kill the child, mark the transport ended, never write to that pipe again) — not a per-turn failure that
   lets the next queued turn reuse the same, now-corrupted stream. The delivery-certainty classification for
   this path must also be `OutcomeUnknown`, not `FailedBeforeInjection` (a partial write is neither "definitely
   not sent" nor "definitely sent").
3. **§2 (Issue B) gains explicit engineering requirements, not just a sketch:** (a) document
   `InteractiveTransport::next_event()`'s cancellation-safety contract explicitly in `lib.rs` (dropping its
   future at an await point must never lose a normalized event or corrupt driver-internal framing state) and
   audit all three drivers against it as a concrete, written checklist, not just an implementer's private
   reasoning; (b) `fire_watchdog()` sends a kill request then awaits `ProcessExited` — if the mailbox branch of
   the `select!` wins WHILE that watchdog kill is in flight, the actor must not process an ordinary
   cancel/gate-answer command against a transport that is already mid-termination — add an explicit
   "terminating" guard so a command arriving during watchdog escalation is handled safely (e.g. queued/rejected
   with a clear reason), not silently raced against process teardown; (c) add adversarial tests: a mailbox
   command arriving exactly when an event is also ready (both branches ready simultaneously — must not lose
   either), a sustained stream of mailbox commands while a turn is active (must not indefinitely starve event
   delivery), and the watchdog-firing-concurrently-with-a-mailbox-command case from (b). One reviewer noted
   `POLL_IDLE` becomes unnecessary once the mailbox is properly raced (the actor no longer needs a
   short-breather signal to get a chance to check its mailbox) — evaluate removing it entirely once §2 lands,
   across all three drivers, as a follow-up simplification (not required for §2 itself to be correct).
4. **§4.6 (opencode handshake) refined:** a JSON-RPC message during the handshake read-loop can legally be (a)
   a notification (has `method`, no `id`) — ignore and keep reading; (b) the expected response (matching `id`)
   — done; (c) a response with a DIFFERENT id, or a malformed message — treat as a protocol anomaly (log/
   surface it, do not silently treat identically to a benign notification) rather than blindly ignoring. Bound
   the WHOLE handshake (both round-trips, including the writes) by one overall deadline, not a fresh timeout
   restarted on every skipped line.
5. **§3.5 (per-call-id tool tracking) is bigger than originally scoped — re-scoped, still deferred with Issue
   A, but now explicit about it:** `ProtocolEvent::ToolStarted`/`ToolFinished`, the public `TurnEvent`
   equivalents, and `answer_tool_call`'s signature ALL currently carry only a tool NAME, no call id anywhere in
   the normalized vocabulary. Doing this properly is a small, coupled vocabulary change (add a call-id field
   across that chain), not merely an internal `HashMap` swap inside the actor — scope it as part of the SAME
   design pass as Issue A's wire-shape capture (a real capture will also tell us whether the native wire even
   exposes a call id to key by), not a trivial mechanical follow-up.
6. **§4.9 (process-group signaling) is elevated from Hardening-6 to run alongside the transport-termination
   work (former Hardening-3)**, not deferred to the end — two reviewers independently flagged it as more urgent
   than originally scheduled: it's directly coupled to making "shutdown"/escalation actually terminate a real
   session (§3.2/§4.1/§4.2 don't fully deliver on "the process is gone" if orphaned subprocesses survive), and
   we have direct, real evidence (the Phase-0′ spike capture) that claude spawns its own subprocesses.
7. **Sequencing changed (see revised §6):** Issue B and §4.3 are now the FIRST phase, ahead of the other
   actor-core fixes, not the second — two of three reviewers independently argued Issue B is operationally more
   severe than even §3.1/§3.2 (it blocks ALL management commands — cancel, shutdown, future gate-answers — for
   up to 60 seconds on every single real turn, not just an edge case), and §4.3 shares the identical root
   pattern (an unraced, unbounded await that can freeze the whole actor) so fixing them together is more
   coherent than fixing one and discovering the other still breaks the same guarantee.
8. **Issue A's field-capture work (not the code-landing) should start in parallel now, not strictly after every
   hardening phase** — one reviewer correctly noted it's a real functional gap (not just future-proofing) and
   capture work (running a real turn that exercises a tool/gate and recording the redacted wire shape) doesn't
   block on any of the actor/driver code fixes above. The MAPPING code landing still comes last, since §3.4's
   gate-answer transport seam and §3.5's call-id plumbing decisions are informed by what the capture reveals.

## 0. Why this document exists

Phases 0′–6 of the live-runtime feature shipped, and the two real drivers (`ClaudeStreamProtocol`,
`OpenCodeAcpProtocol`) were, for the first time, actually run end-to-end against the real `claude`/`opencode`
CLIs by a human outside the sandboxed dev session. That surfaced one real bug (fixed: `pull_one`'s event-loss
race) and left two more identified-but-not-yet-fixed issues:

- **Issue A** — `claude_stream.rs`'s `map_claude_line` and `opencode_acp.rs`'s `map_acp_line` never recognize a
  real tool-call or native-permission-gate request from the underlying CLI. A turn that makes the real CLI
  invoke a tool or ask permission is silently dropped today; depending on the CLI's own behavior when nobody
  answers a permission prompt over a non-interactive pipe, the turn may hang or misbehave. **Not started** —
  needs a real capture of the wire shape (a Phase-0′-follow-up), then mapping it onto the existing
  `ProtocolEvent::ToolStarted`/`NativeGateRequested` variants.
- **Issue B** — `actor.rs::run`'s `PumpNext::Pump` arm calls `self.transport.next_event().await` as a
  STANDALONE await, never raced against `self.mailbox`. Since both real drivers now correctly wait up to the
  full hang-watchdog budget (~60s) for a real event, any mailbox command sent during that wait — `cancel_turn`,
  `shutdown`, or a future gate/tool answer-back — cannot be serviced until `next_event()` returns. Confirmed
  empirically: applying the same fix to the third (test-only) driver broke 4 tests that answer a gate/tool
  request mid-turn. **Not started** — needs `next_event()` raced against `mailbox.recv()` (e.g. `tokio::select!`)
  while preserving the existing "never abandon an event mid-read" invariant.

Before fixing A and B, we ran a 3-reviewer pass (independent LLM consultants, told about A and B explicitly and
asked to find OTHER major issues) against `actor.rs`, `claude_stream.rs`, and `opencode_acp.rs`. This document
synthesizes those findings — deduplicated, severity-recalibrated, and cross-checked against the actual source
by the orchestrator before writing this plan (one reviewer claim, that `ClaudeStreamProtocol` should report
`streaming_output: true`, was checked and is WRONG — claude's own wire only ever sends whole assistant blocks,
never token deltas, VERIFIED false per the Phase-0′ spike capture — and is excluded below) — into a single
prioritized fix plan.

## 1. Issue A — recognize tool-call / native-gate wire shapes (unchanged from prior framing)

Out of scope for a quick fix: this needs a **real capture** (a live run of `claude`/`opencode` that actually
exercises a tool call or hits a permission gate, redacted the same way the original Phase-0′ spike was), THEN
mapping that captured shape onto the existing `ProtocolEvent::ToolStarted`/`ToolFinished`/`NativeGateRequested`
variants in `map_claude_line`/`map_acp_line`. Scope for this plan: land the capture + mapping for **at least
claude** (primary), with opencode following the same pattern once the ACP tool/gate `session/update` shape is
similarly captured. This item is the prerequisite for Group 3's tool-callback correlation fix (§4) to matter in
practice — no point hardening `running_tool` tracking against concurrent real tool calls until real tool calls
can happen at all.

## 2. Issue B — race `next_event()` against the mailbox

The core fix: `actor.rs::run`'s `PumpNext::Pump` arm must let a mailbox command preempt a blocking
`next_event()` without needing it to return first, e.g.:

```rust
tokio::select! {
    ev = self.transport.next_event() => { /* existing handling, unchanged */ }
    cmd = self.mailbox.recv(), if <no event yet consumed this iteration> => { self.on_command(cmd).await }
}
```

This must preserve the existing "a transport event, once started being read, is never cancelled mid-event"
invariant (`actor.rs:199-200`'s comment) — `tokio::select!` cancels the LOSING branch's future, so if
`next_event()`'s own internal state machine would be corrupted by being dropped mid-poll (e.g. `pull_one`'s
`self.events.recv()` inside a `tokio::time::timeout`), the fix must ensure the select only ever cancels
`next_event()` at a safe boundary — realistically this means: `next_event()` itself should remain a single,
atomic "get the next fully-formed event or decide there isn't one yet" operation exactly as it is today
(it doesn't process partial state across awaits — check this holds for both real drivers), so cancelling ONE
POLL attempt (not a partially-consumed event) is safe. **This needs explicit verification against both real
drivers' `next_event`/`pull_one` implementations, not just theoretical reasoning, before landing** — the
implementer must confirm cancellation safety concretely, one driver at a time.

Once this lands, `fake_line.rs`'s `pull_one` should be lifted to the SAME full-watchdog-budget policy as
`claude_stream.rs`/`opencode_acp.rs` (currently it still uses the old `POLL_IDLE.min(...)` cap, which was only
correct because the actor-level race didn't exist yet) — for consistency across all three drivers, and because
leaving it stale would mean `fake_line.rs` no longer accurately represents the actor's real behavior for future
driver authors to copy.

## 3. Actor-core crash / resource-safety bugs (apply to every driver via `actor.rs`)

### 3.1 `cap_delta` panics on a non-UTF8-boundary truncate — CONFIRMED, HIGH PRIORITY
`actor.rs:1108-1114`: `text.truncate(self.config.delta_max_bytes)` operates on a byte length but
`String::truncate` panics if that index isn't a char boundary. Any assistant output containing a multi-byte
UTF-8 character (accented text, CJK, emoji — not exotic, completely ordinary model output) that happens to
straddle the configured byte cap **crashes the actor task**, killing the whole target. Verified directly
against the source. **Fix:** truncate at the last valid char boundary at or before `delta_max_bytes` (e.g. via
`floor_char_boundary` once stable, or a manual `while !s.is_char_boundary(i) { i -= 1 }` walk-back), never a
raw byte-length truncate on a `String`.

### 3.2 `shutdown` never actually stops the underlying process — CONFIRMED, HIGH PRIORITY
`actor.rs:850-907` (`shutdown`/`stop`): `stop()` sets `self.terminal = true`, closes the journal's broadcast,
and emits a `Stopped` status — but never touches `self.transport` at all. Verified directly: no reference to
`self.transport` anywhere in `stop()`. Since `pump_mode()` returns `PumpNext::Wait` once terminal, the actor
task then blocks on `self.mailbox.recv().await` **forever**, still holding `self.transport` (and therefore the
live child process) alive, until the LAST `ManagedTarget` handle elsewhere in the system happens to be dropped
— which may never happen promptly. **"Shutting down" a target today does not stop its real CLI process.**
**Fix:** make the actor's `transport` field droppable independent of the actor's own lifetime (e.g.
`Option<Box<dyn InteractiveTransport>>`, `.take()`'d and dropped during `stop()`, which both real drivers'
`Drop` impls already handle correctly — killing the child and reaping it), and decide explicitly whether the
actor task itself should exit promptly after an `Immediate` shutdown rather than parking forever in
`PumpNext::Wait` (parking is fine for `Drain`, which legitimately still needs to finish the active turn, but
`Immediate` shutting down should plausibly end the task, not just the target's turn-acceptance).

### 3.3 `cancel_turn` advances the queue without confirming interruption — CONFIRMED PATTERN, HIGH (raised post-council)
`actor.rs` (cancel path): calls `transport.interrupt()` and immediately clears the active turn / activates the
next queued turn, without waiting for the interrupted turn's REAL terminal event. A late terminal event from
the just-cancelled turn (which the transport may still emit, since `interrupt()` is explicitly documented as
best-effort, not synchronous) can then be misattributed to the NEWLY-activated turn. **Fix:** introduce an
explicit "cancelling" sub-state that only transitions to activating the next turn once the cancelled turn's
own terminal event is observed. **On a bounded timeout instead of a confirmed terminal, do NOT simply advance
to the next turn anyway** (council finding: without a native turn id anywhere in `ProtocolEvent`, a late event
from the old, still-alive transport remains indistinguishable from the new turn's events) — on timeout, the
transport/session itself must be terminated (or the target faulted) before any new turn is activated, never
silently reused. **Also clear `self.running_tool = None` at this same transition** (a newly-found bug: the
cancel path today never clears it, so a lingering "tool running" belief from the cancelled turn could route a
later `answer_tool_call` into the wrong turn's transport call). Mirror the existing pattern already used
elsewhere in this actor for other confirmed-transition state (e.g. `AwaitingGate`).

### 3.4 `resolve_gate` swallows/misreports transport delivery failure — CONFIRMED PATTERN, MEDIUM
The gate-resolution path calls `transport.send_turn(&answer)` (reusing the generic prompt-injection path) and
reports the gate as resolved regardless of whether that write actually succeeded. **Fix:** propagate the
transport error instead of unconditionally emitting `NativeGateResolved`; leave the gate `AwaitingGate` (or
transition to a fault state) if delivery genuinely failed, mirroring the certainty-tracking discipline already
used for `command` turns elsewhere in this actor. **Note:** the SEPARATE concern that reusing `send_turn`'s
generic envelope may not match a real gate-answer's actual required wire shape is real but is properly scoped
under Issue A (§1) — a driver that doesn't understand gates yet has no gate-answer wire shape to get wrong; do
not invent one preemptively.

### 3.5 (lower priority, same root cause as 3.2 in spirit) `running_tool` is a single slot, not per-call-ID — CONFIRMED, SCOPE-GATED BY ISSUE A
`actor.rs:134` (`running_tool: Option<String>`): a second concurrent (or same-named, sequential) tool call
during one turn silently overwrites/loses track of the first. Verified directly against the source (single
`Option<String>` field, set/cleared by name only, no call-ID). **This does not matter until Issue A lands**
(neither real driver can emit `ToolStarted` today), but it DOES matter for `FakeLineProtocol`'s own existing
bidirectional-tool-call tests today, and will matter immediately once Issue A ships. **Fix (scope for THIS
plan, done alongside or shortly after Issue A, not blocking it):** track pending tool calls by an actual
call-ID (a `HashMap<ToolCallId, ToolCallState>` or similar) instead of one global `Option<String>`, plumbed
through `ProtocolEvent::ToolStarted`/`ToolFinished`/`answer_tool_call`.

## 4. Driver process-supervision hangs/leaks (`claude_stream.rs` + `opencode_acp.rs`, check `fake_line.rs` for parity)

### 4.1 Unbounded `child.wait()` on stdout EOF — CONFIRMED PATTERN, HIGH PRIORITY
Both real drivers' `supervise()` loop: on stdout `Ok(None)`/`Err(_)` (EOF or read error), the code does
`child.wait().await` with NO bound and NO route through the escalation ladder, ignoring `kill_rx` entirely from
that point on. A child that closes stdout but does not actually exit (a real, observed category of misbehavior
— half-dead processes, a hung cleanup handler, a wedged child) hangs supervision forever with no way to force
a kill. **Fix:** treat unexpected stdout closure as a supervision failure and route through the SAME bounded
`escalate()` ladder already used for the kill-request path, rather than an unbounded `child.wait()`.

### 4.2 `escalate()`'s own final fallback wait is unbounded — CONFIRMED, LOWER PRIORITY (same root pattern as 4.1)
After SIGKILL, if `start_kill()` itself errors or the kernel is slow to report, the "last resort" `child.wait()`
is unbounded. Extremely unlikely to matter in practice (SIGKILL essentially always works), but for consistency
with the "every wait is bounded, every unbounded wait is a documented, justified exception" discipline this
codebase otherwise holds itself to, give this a final bounded deadline too, with an explicit
fault/error-surfacing path if the OS still hasn't reported exit by then (do not silently loop forever).

### 4.3 `stdin.write_all`/`flush` has no timeout — CONFIRMED PATTERN, HIGH PRIORITY (fix together with Issue B, same root pattern)
`write_stdin_line` (both real drivers, and the opencode handshake's own separate write path — include it too):
the actual write to the child's stdin pipe has no timeout. If the child's stdin buffer is full and it has
stopped reading (a genuinely stuck child, not just "hasn't replied yet"), this call blocks forever. Combined
with Issue B's standalone-await architecture, a stuck write here would freeze the ENTIRE actor task with no
watchdog able to intervene (the hang-watchdog only bounds the next-EVENT wait, not the write itself).

**Fix, corrected post-council (the original "just add a timeout" sketch was unsafe):** `AsyncWriteExt::write_all`
is NOT atomic — a timeout can fire after a PARTIAL write already reached the pipe, permanently desyncing the
child's own line-based parsing for every subsequent write on that same connection. A write timeout must
therefore be treated as a **fatal fault for that transport**, not an ordinary per-turn failure: kill the child,
mark the transport `ended` so it is never written to again, and do NOT let a subsequently-queued turn reuse the
same (possibly corrupted) pipe. The delivery-certainty this feeds into the actor's audit trail must be
`OutcomeUnknown` for this path (a partial write is neither "definitely not sent" — the current
`FailedBeforeInjection` classification — nor "definitely sent"), not the blanket `FailedBeforeInjection` every
`send_turn` error currently gets mapped to.

### 4.4 Unbounded event channel + unbounded line-read buffer — CONFIRMED, MEDIUM (real but lower-likelihood threat model)
Both real drivers' driver→actor event channel is `mpsc::unbounded_channel()`, and stdout is read via
line-buffered `AsyncBufReadExt::lines()` with no cap on line length. A sufficiently verbose or malfunctioning
child (not necessarily malicious — a real bug in claude/opencode itself, or a runaway tool loop echoing huge
output) can grow memory without bound, independent of the actor's own `delta_max_bytes`/`queue_max_len` caps
(which only apply AFTER the fact, at the actor/journal layer). **Fix:** bound the channel (sized generously for
a real burst, e.g. low thousands of events) with an explicit backpressure or fail-the-turn policy on overflow,
and cap the per-line read buffer size (fail the turn / kill the child on a pathologically oversized single
line rather than buffering unboundedly).

### 4.5 `opencode_acp.rs`'s `accumulated_reply` grows unbounded before any cap applies — CONFIRMED, MEDIUM
The transport-level reply accumulator (needed because opencode's own `session/prompt` response carries no
reply text — see the Phase-5 accumulation-architecture design) has no size cap of its own; `actor.rs::cap_delta`
only truncates the PUBLIC event after the fact, so the accumulator itself can balloon first. **Fix:** apply the
same byte-cap discipline at the transport level (a bounded accumulator that stops growing/truncates early)
rather than relying solely on the actor's downstream cap.

### 4.6 `opencode_acp.rs`'s handshake breaks on any interleaved JSON-RPC notification — CONFIRMED PATTERN, MEDIUM-HIGH
`perform_handshake` assumes the very next stdout line after each handshake request IS that request's response
— it has no tolerance for a legally-interleaved JSON-RPC notification arriving first (real ACP servers can and
do send notifications at any time). **Fix, refined post-council:** loop reading lines, bounded by ONE overall
handshake deadline (covering both round-trips and their writes — not a fresh timeout restarted per skipped
line), classifying each line as it arrives: (a) a notification (`method` present, no `id`) → ignore and keep
reading; (b) the expected response (matching `id`, has `result`/`error`) → done; (c) anything else — a response
carrying a DIFFERENT id, or a malformed line — treat as a protocol anomaly to surface/log, not silently treat
the same as a benign notification (a wrong-id response is a real signal something is off, not routine noise).
(§4.8's request-id correlation gap below should land together with this fix — same root cause, same file,
same review pass.)

### 4.7 `opencode_acp.rs`'s stderr isn't drained until after the handshake — CONFIRMED, LOWER-MEDIUM
The stderr-drain task is spawned AFTER both handshake round-trips complete, not immediately after the pipes are
taken. If the child writes enough to stderr during its own startup to fill the OS pipe buffer before the drain
task exists, the child can block on its own stderr write, indirectly stalling the handshake and producing a
misleading "handshake timeout" rather than the real cause. **Fix:** spawn the stderr-drain task immediately
after taking the child's stdio handles, before performing any handshake write/read.

### 4.8 Response/turn correlation by JSON-RPC id is implicit, not verified — CONFIRMED, MEDIUM (raised post-council; land together with §4.6)
`map_acp_line`'s response handling doesn't check the JSON-RPC `id` against the currently-outstanding request's
id — it relies on the invariant that at most one `session/prompt` is ever outstanding at a time (true today,
by construction of the single-writer actor). This is correct TODAY but silently fragile: a future change that
violates that invariant would misroute a response with no warning. **Fix (defense-in-depth, not urgent):**
thread the expected request id through and explicitly ignore/fault on a mismatched id, rather than relying on
an unstated invariant holding forever.

### 4.9 Signals target only the process leader, not its process group — CONFIRMED PLAUSIBLE, HIGH (raised post-council, moved earlier)
`best_effort_signal` signals only the single leader PID. We have DIRECT evidence (the Phase-0′ spike's own
captured `system/init`) that claude spawns its own subprocess MCP servers (`hares`, `playwright`, `agent-hub`)
— killing only the leader on escalation could orphan those. **Fix:** spawn the child in its own process group/
session (Unix: `std::os::unix::process::CommandExt::process_group(0)` or equivalent) and signal the whole group
(negative PID) on escalation, while still reaping only the direct child via `wait()`.

### 4.10 `best_effort_signal` shells out to `kill` resolved via `$PATH` — CONFIRMED, LOWER PRIORITY (real hardening, not urgent)
Spawns `Command::new("kill")`, resolved via PATH rather than a direct signal syscall. One reviewer framed this
as a PATH-hijack security issue; recalibrated here — exploiting it requires an attacker who can already write
somewhere earlier in this process's PATH than the real `kill` binary, which implies a level of existing
compromise where this is a marginal escalation, not the primary risk. Still: calling the signal syscall directly
(via the `nix` crate, or `libc::kill`) is strictly more robust (no extra process spawn per signal, no PATH
dependency at all) and easy to do. **Fix, non-urgent:** replace the `Command::new("kill")` shell-out with a
direct `kill(2)` call.

## 5. Explicitly excluded (checked and found incorrect, or genuinely a nit)

- **"`ClaudeStreamProtocol` should report `streaming_output: true`"** — checked directly against
  `claude_stream.rs:93` and the Phase-0′ spike's own VERIFIED finding ("NO token deltas observed" — claude only
  ever sends whole assistant blocks). The current `false` is correct and intentional. Not a bug.
- **Queue-byte-admission O(n) rescan per enqueue, repeated prompt clones** — real but genuinely a performance
  nit at today's scale (per-target queues are bounded to a small `queue_max_len` already); not worth plan time
  now. Revisit only if a real workload shows it matters.
- **"Add stress tests for memory-pressure scenarios"** — folded into the test-first implementation of §4.4/4.5
  above rather than tracked as a separate item; the fix and its test are the same unit of work.

## 6. Proposed sequencing for implementation (revised post-council)

Given the volume, sequence as sub-phases, each test-first (tester writes/extends tests proving the bug today
and the fix afterward, coder implements, dev-manager audits), landing as independent, reviewable commits — NOT
one giant change. **Resequenced from the original draft**: Issue B moves to first (two of three reviewers
independently argued it's operationally more severe than the crash/leak bugs, since it blocks ALL management
commands on every single real turn, not an edge case), bundled with §4.3 since both are the identical
unraced/unbounded-await root pattern; §4.9 moves up into the transport-termination phase; §3.1's trivial crash
fix stays first-in-practice since it's cheap and totally independent of everything else.

1. **Hardening-0 (trivial, independent, do immediately):** §3.1 (UTF-8 panic).
2. **Hardening-1 (Issue B — the actor/mailbox race — AND §4.3 stdin-write-timeout together, same root pattern):**
   §2 in full, including the explicit `next_event()` cancellation-safety contract + the watchdog-concurrent-
   with-mailbox-command guard + the three adversarial tests called out in §0.1(3), THEN §4.3's corrected
   fatal-fault-on-write-timeout fix (both real drivers + the opencode handshake write path), THEN lift
   `fake_line.rs`'s `pull_one` to match and evaluate removing `POLL_IDLE` entirely per §0.1(3)'s note. This is
   now the highest-priority phase: after it lands, cancel/shutdown/future-gate-answers are actually responsive,
   which several LATER phases' own tests depend on being able to assert cleanly.
3. **Hardening-2 (transport termination + process-group correctness, bundled since they define "the process is
   really gone" together):** §3.2 (shutdown doesn't stop the process), §4.1 (unbounded stdout-EOF wait), §4.2
   (escalate's final unbounded wait), §4.9 (process-group signaling — moved up from its original last-phase
   slot), §4.10 (direct-syscall kill instead of shelling out, bundled here since it touches the same signaling
   code as §4.9).
4. **Hardening-3 (cancellation correctness, depends on Hardening-1/2 existing first):** §3.3 (cancel
   confirmation + the newly-found `running_tool`-not-cleared bug), §3.4 (gate-resolve error propagation).
5. **Hardening-4 (resource bounds):** §4.4, §4.5 (unbounded channels/buffers/accumulator) — include the
   stderr-drain path in the bounded-reads fix, not just stdout.
6. **Hardening-5 (opencode handshake + correlation robustness, land together — same file, same review pass):**
   §4.6 (refined message classification + one absolute deadline), §4.7 (stderr drain timing), §4.8 (request-id
   correlation).
7. **Issue A (tool/gate wire-shape capture + mapping) + §3.5 (per-call-id tool tracking, re-scoped per
   §0.1(5)):** the largest, most design-heavy item. **Field-capture work may start in PARALLEL with phases 1-6
   above** (it doesn't depend on any of the code fixes), but the actual mapping/vocabulary-change code landing
   stays sequenced last, since §3.4's gate-answer transport seam and §3.5's call-id plumbing are properly
   informed by what the capture reveals rather than guessed ahead of it.

This ordering now front-loads BOTH the cheapest crash fix AND the highest-operational-impact responsiveness
fix (Issue B), then transport-termination correctness, then the more speculative/lower-frequency correctness
and resource-bound items, with Issue A's code landing last but its research track de-risked by starting now.
