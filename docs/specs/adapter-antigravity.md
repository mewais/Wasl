# W8′ — `wasl-adapter-antigravity` — concrete Antigravity-CLI (`agy`) `AgentAdapter` — spec (authoritative)

**Tier:** L2 (IO shell over a tested pure core). **Deps:** `wasl-adapter` (the `AgentAdapter` trait +
`Delivery`/`DeliveryKind`/`AdapterError`), `serde`, `serde_json`, `async-trait`, `tokio` (process). Dev:
`tokio` (macros, rt), `serde_json`. No `unsafe`. No `uuid` dep — like codex, **agy mints its own
conversation ids**; the adapter never fabricates one. This is ONE concrete adapter: it injects an incoming
`Delivery` into the local **Antigravity CLI** (`agy -p`, print mode) and returns its reply. It mirrors the
W7 pattern (`wasl-adapter-codex`, itself mirroring E3 `wasl-adapter-claude`): pure core + `CliInvoker`
seam + `ProcessCliInvoker` + per-context session ledger.

## ⚠️ CONTRACT STATUS — PARTIALLY LIVE-VERIFIED (agy 1.0.15, amended 2026-07-03)

Ground truth was originally the antigravity research doc (`Marsad/docs/specs/antigravity-research.md`),
produced against the real installed binary (`~/.local/bin/agy`, 1.0.15) under a hard **zero-live-call
constraint** (free-tier account): every fact came from `--help` output, `agy models`/`agy changelog`,
binary string tables, and real on-disk state — **not one `agy -p` invocation was ever run** at authoring
time. On **2026-07-03** the orchestrator ran `agy -p` live for the first time (multiple invocations) and
found three of the STATIC/ASSUMED rows below flatly WRONG. This section (the "W8′-fix correction") amends
those rows in place rather than pretending the original research held up; it is a spec-sanctioned reality
correction per the "Live-findings feedback rule" below, not a silent code fix. Status legend: **VERIFIED**
= observed locally at zero cost (command output or real on-disk file, or a live orchestrator run this
round); **STATIC** = real evidence from binary strings / changelog / `--help` prose, but never exercised at
runtime; **ASSUMED** = inference this spec depends on that only a live run can resolve.

### W8′-fix correction (orchestrator live verification, 2026-07-03)

Three findings, all VERIFIED live (twice, independently, on this machine):

1. **Hidden JSON flag.** Plain `-p <prompt>` (no other output flag) emits plain TEXT on stdout, not JSON.
   The JSON contract this adapter's `parse_reply` depends on requires the HIDDEN flag `--output-format
   json` — absent from `--help`'s documented flag list, but present in the binary's flag-string tables and
   now confirmed live: without it, stdout is prose, not a `{"conversation_id":…}` line.
2. **`-p`'s prompt is a FLAG VALUE, not a free-floating trailing positional.** `-p`/`--print` consumes the
   SINGLE argv token immediately following it as the prompt (ordinary Go `flag.String` behavior) — it is
   NOT "wherever the last bare positional happens to land". Placing `-p` before other flags is a live-
   confirmed bug: `agy -p --model X --sandbox --dangerously-skip-permissions "<prompt>"` handed the agent
   the literal string `"--model X --sandbox --dangerously-skip-permissions <prompt>"` as its prompt
   (verified: the reply discussed model flags, not the intended prompt). The corrected invocation puts
   `-p "<prompt>"` as the LAST TWO argv elements, strictly after every other flag:
   `agy --output-format json [--conversation <cid>] --model <m> --sandbox
   --dangerously-skip-permissions -p "<prompt>"`.
3. **The success mapping was wrong.** A `status:"SUCCESS"` reply was observed for the first time (below),
   confirming that value exists — but a GOOD reply (non-empty, on-topic `response`) was ALSO observed
   carrying `status:"ERROR"` plus a populated `error` string: the tool-denial-decoration shape first seen in
   research is not a failure at the response level, it is the model narrating a denied tool call inside an
   otherwise-useful reply. So `status`/`error` CANNOT gate success/failure — only `conversation_id` and
   `response` presence can. **Corrected mapping: success iff `conversation_id` is a non-empty string AND
   `response` is a non-empty string; otherwise `Err` carrying the raw JSON line** (status/error remain
   inside that raw line for the caller to read — they decorate the error message, they do not decide the
   branch).

   Verified SUCCESS sample (orchestrator, live, 2026-07-03):
   ```
   {"conversation_id":"3d206228-0e90-48b4-9c05-4a6d14ff3d8b","status":"SUCCESS","response":"bt-7731\n","error":null,"num_turns":1}
   ```
   Verified tool-denial-decoration sample (status `"ERROR"` + non-empty `error`, but `response` is ALSO
   non-empty — under the corrected mapping this is now `Ok`, not `Err`):
   ```
   {"conversation_id":"97197d46-2091-40e2-b36f-0dcedd34bcc4","status":"ERROR","response":"It looks like your request was just the letter \"x\". ...","error":"Permission denied for read_file(/home/mewais/.gemini/antigravity-cli). Matches hardcoded system protection boundary rule.","duration_seconds":2.70532965,"num_turns":1,"usage":{}}
   ```
   Both fixtures MUST appear verbatim in `tests/pure.rs` (see the updated Test plan below).

| Claim | Status |
|---|---|
| Flag surface (documented): `-p`/`--print`/`--prompt`, `--conversation <id>`, `--continue`/`-c`, `--model`, `--sandbox`, `--dangerously-skip-permissions`, `--print-timeout` (default `5m0s`) | ✅ VERIFIED (`agy --help`, complete enumeration of the DOCUMENTED surface) |
| Flag surface (HIDDEN): `--output-format json` — absent from `--help`, present in binary flag-string tables, REQUIRED for JSON print-mode output (plain `-p <prompt>` alone emits plain TEXT, not JSON) | ✅ VERIFIED live 2026-07-03, orchestrator (two independent runs) |
| `-p --output-format json` prints exactly ONE line of JSON at the end — no streaming/event output; plain `-p` WITHOUT `--output-format json` prints plain TEXT | ✅ VERIFIED live 2026-07-03, orchestrator (supersedes the prior STATIC-only claim) |
| Print-mode JSON field names: `conversation_id`, `status`, `response`, `error`, `duration_seconds`, `num_turns`, `usage` | ✅ VERIFIED live 2026-07-03 (both the SUCCESS sample and the tool-denial-decoration sample above; `duration_seconds`/`usage` are not always present — the verified SUCCESS sample omits both) |
| `status` enum values and the success/failure mapping | ✅ VERIFIED live 2026-07-03 — `"SUCCESS"` observed (see sample above); `"ERROR"` observed carrying a non-empty, on-topic `response` (tool-denial decoration, NOT a response-level failure). `status`/`error` are DEMONSTRATED to be unreliable success/failure signals (a good response can carry `status:"ERROR"`), so the mapping keys off `conversation_id`+`response` presence only — this is no longer a defensive guess, it is the observed correct contract |
| `-p --conversation <id>` prints ONLY the new turn's JSON, not a history replay | 📄 STATIC (changelog 1.0.9 fixed the replay bug) |
| Conversation ids are UUIDs; agy mints them (no way to supply our own) | ✅ VERIFIED on disk (`conversations/<uuid>.db`, `brain/<uuid>/`, `cache/last_conversations.json`) + `--help` (no id-supplying flag exists) + the live SUCCESS/decoration samples above |
| `-p` takes the prompt as ITS OWN FLAG VALUE (the single argv token immediately following `-p`), NOT a free-floating trailing positional independent of flag order | ✅ VERIFIED live 2026-07-03, orchestrator — supersedes the prior (WRONG) "trailing positional anywhere after flags" claim: `agy -p --model X ...` was observed handing the agent `"--model X ..."` as its prompt. The corrected form places `-p "<prompt>"` strictly LAST |
| `--model` accepts the display strings printed by `agy models` (e.g. `"Gemini 3.5 Flash (Low)"`) verbatim | ❌ ASSUMED — `agy models` output is VERIFIED, but no successful `--model`-bearing turn has been folded back into this file yet; still a live-suite target |
| `--sandbox` is honored in `-p` mode | 📄 STATIC (changelog 1.0.6 fix) |
| `--sandbox` / `--dangerously-skip-permissions` accepted ALONGSIDE `--conversation` (resume) | ❌ ASSUMED — codex verifiably REJECTS `--sandbox` on resume; agy has no such evidence either way. The live 2-turn test detects a rejection (see test plan) |
| `--dangerously-skip-permissions` auto-approves ALL tool permissions, overriding even saved "always deny" rules | 📄 STATIC (`--help` + binary log strings: `"--dangerously-skip-permissions: auto-approving all tool permissions"`) |
| Permission-prompt behavior in `-p` mode WITHOUT the skip flag | ✅ VERIFIED live 2026-07-03 (corrected probe, post-W8′-fix argv): **trichotomy (a) — headless `-p` AUTO-ALLOWS tool execution** (a shell command RAN with no skip flag). The earlier "auto-deny" reading came from the pre-fix mangled-prompt probe and is VOID. Consequence: `--dangerously-skip-permissions` is moot-but-harmless headless; **`--sandbox` is the load-bearing containment** on the receive side (present in every argv this adapter builds). OWNER-VISIBLE TRADEOFF: a Wasl-delivered consult to an agy agent executes inside agy's sandbox with no per-tool HITL — the HITL for the *delivery itself* is Wasl's send-side gate. |
| `usage` sub-key set in the print-mode summary object | ❌ ASSUMED (token-field names exist in the binary; the live SUCCESS sample omits `usage` entirely, the decoration sample has `"usage":{}` — shape is still unpinned); the adapter deliberately does not parse `usage` |
| Auth: OAuth-personal, already authenticated on this machine; `-p` attempts silent auth first, escalates to interactive OAuth on failure | ✅ VERIFIED on disk / 📄 STATIC (binary strings). On an unauthenticated box a `-p` spawn may hang on an OAuth prompt — deployment note, not adapter logic |

**Live-findings feedback rule:** live runs resolve the ❌ rows over time. Observed outcomes MUST be recorded
(crate `AGENTS.md` contract table + a status flip in this file); where they contradict an ASSUMED choice
below (e.g. resume rejects the sandbox flag; `--model` needs a different string form), that is a spec
amendment, not a silent code fix — exactly as this W8′-fix correction itself was.

### Target CLI contract (the implementation target, as best evidenced)
- **Session identity:** agy mints a UUID `conversation_id` per conversation and reports it in-band in the
  print-mode JSON result. There is NO way to supply our own id, so the claude trick (deterministic UUIDv5
  from `context_id`) is impossible. The adapter keeps a **context_id → conversation_id ledger** learned
  from each turn's output (design below) — same shape as W7's thread ledger.
- **First turn for a context (CORRECTED 2026-07-03, see the W8′-fix correction above):**
  `agy --output-format json --model "<model>" --sandbox --dangerously-skip-permissions -p "<prompt>"`.
- **Subsequent turns:**
  `agy --output-format json --conversation <conversation_id> --model "<model>" --sandbox
  --dangerously-skip-permissions -p "<prompt>"`.
- **`--output-format json` is REQUIRED on every invocation** (hidden flag; plain `-p` alone emits plain
  text — VERIFIED live, see the W8′-fix correction). **`-p "<prompt>"` is the LAST TWO argv elements**, in
  that order: `-p` takes the prompt as its own flag value (the single argv token immediately following it),
  not as a free trailing positional, so it must be placed AFTER every other flag or it swallows the next
  flag token instead (VERIFIED live: `agy -p --model X ...` handed the agent `"--model X ..."` as its
  prompt). `--continue`/`-c` is NOT used: it resolves "most recent conversation for this workspace" via
  `cache/last_conversations.json` — workspace-global state that races across concurrent contexts;
  `--conversation <id>` is the only per-context-correct form.
- **Reply parsing:** the single JSON result line on stdout → (`conversation_id`, `response`) or an error
  (mapping below — corrected 2026-07-03: success keys off `conversation_id`+`response` presence, NOT
  `status`/`error`). No mid-turn events exist: a `-p` turn is **call-and-wait**, with zero visibility into
  tool calls/thinking while running (the research's `transcript_full.jsonl` tailing idea is
  observe-side Marsad territory, explicitly out of W8′ scope).
- **`--print-timeout` left at its `5m0s` default** in adapter argv — it bounds any residual hang; the
  permission probe passes a short explicit value (test plan).

### Prompt in argv — the one deliberate deviation from the claude/codex seam
claude (`-p` reading stdin) and codex (positional `-`) both take the prompt on **stdin**, which is what the
`CliInvoker::invoke(argv, stdin)` seam was shaped around. For agy, no stdin-prompt form is evidenced
anywhere: `--help` documents none. So this adapter passes `compose_prompt(d)` **inside argv, as `-p`'s flag
value (the strictly-last two elements: `"-p"`, then the prompt)** and always invokes with `stdin = ""` —
the seam signature is kept identical for pattern parity (and the invoker still closes the child's stdin, so
an agy that unexpectedly reads stdin sees immediate EOF, not a hang). Flagged consequences, all accepted and
documented:
- **Prompt is visible in `ps`/process listings** (claude/codex prompts are not). Owner-visible tradeoff;
  revisit if a live probe ever discovers a working stdin form.
- **`-p` must come LAST, immediately before the prompt** (CORRECTED 2026-07-03 — this row was WRONG in the
  original spec, which described the prompt as a free trailing positional and placed `-p` first). VERIFIED
  live: `-p` consumes the SINGLE argv token immediately following it as its value (ordinary Go
  `flag.String` semantics); `agy -p --model "…" "say hi"` — the ORIGINAL spec's own example recipe — does
  NOT work as intended, because `-p` swallows `"--model"` itself as the prompt. `build_argv` therefore
  emits `["-p", prompt]` as the strictly-last two elements, after `--output-format`, `--conversation`
  (if any), `--model`, `--sandbox`, and `--dangerously-skip-permissions`; pure tests pin both that ordering
  and the final two-element shape.
- `compose_prompt` never returns an empty string (the verb header is always present), so the
  `"Print mode: empty prompt, exiting"` path is unreachable from `deliver`.

## Session identity: the context_id → conversation_id LEDGER

**Decision: in-memory only** — `conversations: Mutex<HashMap<String, String>>` (context_id →
conversation_id), learned from the reply JSON's `conversation_id` field on a successful turn, consulted on
every `deliver`. Identical shape and rationale to W7.

**Documented consequence:** an adapter restart empties the ledger, so the next `deliver` for a known
context starts a **fresh agy conversation** — prior conversational context is lost for that context
(delivery still succeeds), and the old conversation is orphaned on disk under
`~/.gemini/antigravity-cli/{conversations,brain}/` (harmless; agy keeps them indefinitely anyway).

**Why not re-derive from agy's disk state:** rejected for correctness — the same counterfeit-risk argument
as W7, and it is stronger here. Nothing agy writes carries our `context_id`: `cache/last_conversations.json`
is only a workspace-path → most-recent-conversation map (workspace-global, not per-context — wrong key
entirely); the canonical `conversations/<id>.db` store is raw protobuf inside SQLite (not parseable without
`cortex_go_proto`); and the readable `brain/<id>/.../transcript_full.jsonl` projection embeds the user
prompt VERBATIM inside `<USER_REQUEST>` blocks — so the only matchable signal would be our
`compose_prompt` header, which any peer can counterfeit inside a message body, letting an attacker bind
another context's conversation to their own. A heuristic steerable by message content is not a
session-identity mechanism. (A wasl-owned ledger file remains a possible later block if restart-survivable
continuity is ever required — same deferral as W7.)

**Ledger update rule:** written ONLY on a fully-successful `deliver` (invoker Ok AND `parse_reply` Ok). If
the invoker succeeds but the reply maps to an error (per the corrected mapping: missing/empty
`conversation_id` or missing/empty `response` — NOT `status`/`error`, which no longer gate this decision;
see the W8′-fix correction), the ledger is untouched: a conversation whose last turn produced no usable
`response` must not be resumed blind; the next turn for that context starts fresh instead. Concurrent
first-`deliver`s for one fresh context may race and create two agy conversations; last-write-wins in the
ledger (same benign race as E3/W7).

## Pure core (genuine 100% — unit-tested)
```rust
/// The pinned default model — the cheapest entry in `agy models` (free-tier rule; see "Model selection").
pub const DEFAULT_MODEL: &str = "Gemini 3.5 Flash (Low)";

/// Compose the prompt injected into agy from a Delivery (verb header + provenance + body + any data).
/// IDENTICAL composition to wasl-adapter-claude/-codex so a message reads the same regardless of
/// receiver CLI: Consult -> "[Consult from {from}]\n\n{text}"; Command/Message analogous. Non-empty
/// `d.data` appends "\n\nAttached data:" then one line per JSON value (`Value::to_string`).
pub fn compose_prompt(d: &wasl_adapter::Delivery) -> String;

/// How this turn binds to the agy conversation.
pub enum Turn<'a> { First, Continuation(&'a str) }   // Continuation carries the learned conversation_id

/// Build the agy argv for a turn. `bin` is the program (default "agy"); `model` is ALWAYS passed
/// (never optional — see "Model selection"); `prompt` is the composed prompt, emitted as `-p`'s flag
/// value in the LAST TWO argv slots (CORRECTED 2026-07-03 — see the W8′-fix correction: `-p` takes the
/// single argv token immediately following it as the prompt, so it cannot be placed before other flags).
pub fn build_argv(bin: &str, model: &str, turn: Turn<'_>, prompt: &str) -> Vec<String>;
//   First             -> [bin, "--output-format", "json", "--model", model, "--sandbox",
//                         "--dangerously-skip-permissions", "-p", prompt]
//   Continuation(cid) -> [bin, "--output-format", "json", "--conversation", cid, "--model", model,
//                         "--sandbox", "--dangerously-skip-permissions", "-p", prompt]
//   "--output-format json" is the HIDDEN flag required for JSON output (VERIFIED live 2026-07-03;
//   absent from --help). "-p" and prompt are strictly the last TWO elements, in that order (VERIFIED
//   live: -p consumes the next argv token as its value — placing it first swallowed "--model ..." as
//   the prompt in a live run). Whether resume accepts --sandbox/--dangerously-skip-permissions is
//   ASSUMED (contract table); the live 2-turn test detects a rejection. No --print-timeout (default
//   5m0s stands); no --continue/-c (workspace-global, racy).

/// The parsed outcome of one agy print-mode turn.
pub struct AgyReply { pub conversation_id: String, pub text: String }

/// Parse `agy --output-format json -p <prompt>` stdout (one JSON result line) into
/// (conversation_id, response) or an AdapterError.
pub fn parse_reply(stdout: &str) -> Result<AgyReply, wasl_adapter::AdapterError>;
//   Line selection: take the LAST non-empty line that parses as a JSON object (tolerates any stray
//   non-JSON banner lines, ASSUMED absent). No such line -> Err("agy output had no JSON result line").
//   Success/error mapping — CORRECTED 2026-07-03 (see the W8′-fix correction above; this REPLACES the
//   original status/error-keyed defensive mapping, which live evidence proved wrong: a good, on-topic
//   `response` was observed alongside `status:"ERROR"` + a populated `error`):
//     - success iff `conversation_id` is present AND is a non-empty string, AND `response` is present
//       AND is a non-empty string -> Ok(AgyReply { conversation_id, text: response }).
//     - otherwise -> Err("agy turn failed: {raw line}") for EVERY other shape: missing/empty
//       `conversation_id`, missing/non-string/empty `response`, or both. The RAW LINE is the honest
//       detail — it already contains `status`/`error` verbatim, so those fields DECORATE the message
//       (a caller can inspect them) without GATING the branch. There is no longer a distinct "no
//       conversation_id" / "no response" message: one failure shape, one message form.
//     - `status` and `error` are NEVER consulted to decide success/failure (VERIFIED WRONG to do so:
//       see the tool-denial-decoration sample below, which carries `status:"ERROR"` + non-empty `error`
//       but is a SUCCESS under this mapping because `response` is non-empty and on-topic).
//     - `usage`, `num_turns`, `duration_seconds` are IGNORED (present, absent, any shape) — their
//       exact surface is unverified and nothing in Wasl needs them.
//
//   VERIFIED fixtures (orchestrator, live, 2026-07-03) — both MUST appear verbatim in tests/pure.rs:
//
//   SUCCESS sample (the first-ever observed `status:"SUCCESS"` reply):
//     {"conversation_id":"3d206228-0e90-48b4-9c05-4a6d14ff3d8b","status":"SUCCESS",
//      "response":"bt-7731\n","error":null,"num_turns":1}
//   -> Ok(AgyReply { conversation_id: "3d206228-0e90-48b4-9c05-4a6d14ff3d8b", text: "bt-7731\n" })
//
//   Tool-denial-decoration sample (status "ERROR" + non-empty error, but response is ALSO non-empty —
//   the model narrating a denied tool call inside an otherwise-useful reply):
//     {"conversation_id":"97197d46-2091-40e2-b36f-0dcedd34bcc4","status":"ERROR",
//      "response":"It looks like your request was just the letter \"x\". ...",
//      "error":"Permission denied for read_file(/home/mewais/.gemini/antigravity-cli). Matches
//               hardcoded system protection boundary rule.",
//      "duration_seconds":2.70532965,"num_turns":1,"usage":{}}
//   -> Ok(AgyReply { conversation_id: "97197d46-2091-40e2-b36f-0dcedd34bcc4",
//                    text: "It looks like your request was just the letter \"x\". ..." })
//      (NOT Err — this is the corrected mapping's central behavior change from the original spec,
//      which treated any non-empty `error`/failure-looking `status` as a hard failure. That original
//      mapping was RESEARCH-STAGE speculation with no live sample to test it against; this live sample
//      disproves it.)
//   VERIFIED facts these two pin: status "SUCCESS" and "ERROR" (uppercase) both occur; conversation_id
//   IS present on both; response AND error can BOTH be non-empty simultaneously on a reply that a
//   caller should still treat as a usable, deliverable answer; exit code is 0 in both cases; a denied
//   tool call (read_file blocked by a protection rule) did NOT hang the turn — directly relevant to
//   THE PERMISSION QUESTION, whose live PROBE remains required for the explicit-permission-prompt path.
```

## The invoker seam (IO behind a trait, like Runner/Sink)
```rust
#[async_trait::async_trait]
pub trait CliInvoker: Send + Sync {
    /// Run `argv` (argv[0]=program), feeding `stdin`, returning captured stdout (or an error).
    async fn invoke(&self, argv: &[String], stdin: &str) -> Result<String, wasl_adapter::AdapterError>;
}

/// The REAL invoker: spawns the process via tokio, pipes stdin, captures stdout, maps exit status.
#[derive(Debug, Default, Clone)]
pub struct ProcessCliInvoker;
```
`ProcessCliInvoker::invoke` is BEHAVIORALLY IDENTICAL to E3/W7's: `split_first` (empty argv → Err "empty
argv"); spawn with piped stdin/stdout/stderr (spawn error → Err "spawn {prog}: {e}"); write all of `stdin`
to the child then drop the handle (for agy `stdin` is always `""` — the drop still delivers EOF, so an agy
that unexpectedly reads stdin cannot hang on it); `wait_with_output`; success → stdout as lossy UTF-8;
non-success → `Err("{prog} {status}: {stderr}")`. Use the single named error helper `fn ad(msg) ->
AdapterError` at every error site (E3's llvm-cov technique). This trait + invoker are deliberately
DUPLICATED from the claude/codex crates (~40 lines) for the same reason W7 gave: cross-adapter coupling
for two tiny types is worse than duplication, and hoisting into `wasl-adapter` is a C1 spec change outside
W8′'s blast radius.

## The adapter
```rust
/// A concrete AgentAdapter that drives the Antigravity CLI via a CliInvoker.
pub struct AntigravityAdapter<I: CliInvoker> {
    /* invoker: I, bin: String, model: String, conversations: Mutex<HashMap<String, String>> */
}
impl<I: CliInvoker> AntigravityAdapter<I> {
    pub fn new(invoker: I) -> Self;                                  // bin = "agy", model = DEFAULT_MODEL
    pub fn with_bin(invoker: I, bin: impl Into<String>) -> Self;     // model = DEFAULT_MODEL
    pub fn with_model(invoker: I, bin: impl Into<String>, model: impl Into<String>) -> Self;
    /// The learned agy conversation_id for a context, if any (observability + live-test assertions).
    pub fn conversation_of(&self, context_id: &str) -> Option<String>;
}
#[async_trait::async_trait]
impl<I: CliInvoker> wasl_adapter::AgentAdapter for AntigravityAdapter<I> {
    // deliver:
    //   prompt = compose_prompt(d);
    //   turn   = match ledger.get(context_id) { Some(cid) => Continuation(cid), None => First };
    //   attempt invoker.invoke(build_argv(bin, model, turn, &prompt), "");
    //   fallback (ONE retry, ONE direction only — same blind design as W7):
    //     Continuation + ANY invoker error -> retry once as First (fresh conversation);
    //     First + invoker error            -> propagate (nothing to fall back to: agy mints ids, so an
    //                                         id collision cannot exist).
    //   on invoker success: reply = parse_reply(stdout)?; ledger.insert(context_id, reply.conversation_id);
    //   return Ok(reply.text). Parse errors (incl. error-status replies) propagate WITHOUT retry and
    //   WITHOUT touching the ledger.
    async fn deliver(&self, d: &Delivery) -> Result<String, AdapterError>;
}
```

**Error mapping, and why the retry is blind (same argument as W7, weaker evidence):** nothing about agy's
failure surface is verified — not the exit code of resuming a nonexistent conversation, not whether an
error-status reply exits non-zero, nothing. So, exactly as W7 concluded for codex's unverified rows, any
stderr substring match would be a guess dressed as a contract. A Continuation invoker error most plausibly
means the conversation is gone or the resume argv was rejected; the honest fail-open response is one
fresh-conversation retry — context lost, delivery succeeds, the same degradation the ledger already
accepts on restart. If the cause was environmental (binary missing, auth expired, network down), the First
retry fails identically and propagates after exactly 2 invocations. The retry never loops. **Documented
failure mode this creates:** if resume argv is systematically rejected (e.g. `--sandbox` illegal with
`--conversation` — ASSUMED-accepted, contract table), EVERY continuation would blind-retry into a fresh
conversation: continuity silently lost at double spawn cost. The live 2-turn test is designed to catch
precisely this (it asserts turn-2 recall AND a stable `conversation_of`), which is why it is mandatory
before this adapter is trusted for multi-turn contexts. One more admitted rough edge: if an error-status
reply also exits non-zero (unverified), a Continuation hitting it pays a blind fresh-conversation retry in
an already-failing path — acceptable, documented, removable once live-verified (same caveat as W7's
`turn.failed`).

## THE PERMISSION QUESTION (the RECEIVE-side safety story)

What agy does in `-p` mode when the model requests a tool permission — WITHOUT
`--dangerously-skip-permissions` — is **unverified and unknowable from static evidence**: the binary has
an explicit interactive `PromptUser` code path, implying a non-interactive fallback must exist for
TTY-less print mode, but whether that fallback hangs until `--print-timeout`, auto-denies, or consults
saved rules is not evidenced anywhere (research §3.4 + §6 #1 — flagged there as the single most important
unknown for driving agy headlessly, and a launch-blocker for this adapter).

**Interim decision — pass `--dangerously-skip-permissions` on every invocation, inside Wasl's gate
envelope.** Rationale, stated plainly:
- Wasl's OWN gate + audit wraps every delivery on the send side (the D-blocks): a human decision point and
  an audit record exist for every message BEFORE agy is ever spawned. The receive side is a single bounded
  turn executing an already-approved delivery — the same posture as W7, where codex's hardcoded
  `approval_policy="never"` made mid-turn HITL structurally unavailable and the send-side gate was
  accepted as THE control point.
- Without the flag, the worst plausible behavior (hang to `--print-timeout`, i.e. 5 minutes per delivery
  that touches any tool) is operationally unacceptable, and the best plausible behavior (auto-deny) is
  still an unverified guess.
- **Defense-in-depth:** `--sandbox` is ALSO passed on every invocation (honored in `-p` since 1.0.6,
  changelog-verified) so "skip permissions" happens inside agy's terminal-restricted sandbox, not with
  full authority. Its exact restriction scope ("terminal restrictions") is thinner and less specified
  than codex's `read-only` tier — noted, not resolvable statically. The sandbox tier is NOT
  config-exposed in W8′; widening is a future spec change, exactly as W7 ruled.

**NOTE — owner-visible tradeoff, to confirm:** this is the one decision in W8′ made under acknowledged
uncertainty. The FIRST live-cli test is therefore a **permission-behavior probe** (test plan below): one
cheap `-p` call WITHOUT the skip flag, short `--print-timeout`, a prompt that requests a shell command.
Its recorded outcome either confirms the interim decision (hang/deny ⇒ flag stays, rationale upgraded from
assumed to observed) or reopens it (e.g. saved-rules consultation works headlessly ⇒ a pre-seeded
allow-rules posture becomes a viable, tighter alternative — spec amendment). Until the probe has run, this
adapter must be presented to the owner as carrying this open tradeoff. Elicitation (MCP `elicitation/
create`) is a distinct interaction type that `--dangerously-skip-permissions` likely does NOT cover
(research §2) — out of W8′ scope: no MCP servers are configured on the target machine, and an elicitation
mid-`-p`-turn surfaces as a failed/hung turn bounded by `--print-timeout`, never as silent approval.

## Model selection (free-tier rule)

Unlike W7 (where `model: Option`, `None` → account default), agy's `--model` is **always passed
explicitly**, defaulting to `DEFAULT_MODEL = "Gemini 3.5 Flash (Low)"` — the cheapest entry in the
VERIFIED `agy models` list. Reasons: the account is free-tier (the research's zero-cost constraint exists
because of it); agy's implicit default model is unknown and the list includes expensive options (Claude
Opus 4.6, Gemini 3.1 Pro) that an unpinned default could silently select; and a fleet adapter should be
deterministic about spend. `with_model` overrides (tests/embedders). No `adapter.model` config key in W8′
— same "not a knob to smuggle in" stance as W7's sandbox; exposing it is a small future node-config spec
change. Caveat (contract table): that `--model` accepts the display string verbatim is ASSUMED; the live
suite's first successful turn is also the verification of this — if it fails on the model flag, the
correct string form is a live finding to fold back here.

## Node-config integration (W2 delta, in node-config.md terms)

- `AdapterKind` gains `Antigravity`; `adapter.kind = "antigravity"` (exact, case-sensitive, same
  second-pass validation; `"Antigravity"`/`"agy"` → `UnknownAdapterKind`).
- `adapter.bin` keeps its meaning: absent → `"agy"` default (`AntigravityAdapter::new(ProcessCliInvoker)`),
  present → `AntigravityAdapter::with_bin(ProcessCliInvoker, bin)`. No other keys in W8′ (no model, no
  sandbox, no skip-permissions toggle).
- `NodeAdapter` (the exhaustive-match delegating enum in `wasl-mcp`, introduced by W7) gains its third arm:
  ```rust
  pub enum NodeAdapter {
      Claude(wasl_adapter_claude::ClaudeAdapter<wasl_adapter_claude::ProcessCliInvoker>),
      Codex(wasl_adapter_codex::CodexAdapter<wasl_adapter_codex::ProcessCliInvoker>),
      Antigravity(wasl_adapter_antigravity::AntigravityAdapter<wasl_adapter_antigravity::ProcessCliInvoker>),
  }
  impl AgentAdapter for NodeAdapter { /* match self { ... } => inner.deliver(d).await */ }
  pub fn build_adapter(cfg: &AdapterConfig) -> NodeAdapter;   // exhaustive match on AdapterKind
  ```
  Still an enum, not `Box<dyn AgentAdapter>` — W2's "adding a kind forces a decision" compile-time forcing
  function is exactly what this third arm exercises.

## Test plan
### `tests/pure.rs` (genuine 100% of the pure core)
CORRECTED 2026-07-03 (W8′-fix): the canonical happy fixture is now a REAL VERIFIED live sample rather than
a synthetic schema-derived one — the orchestrator's live runs upgraded this file's evidence base. Two
fixtures MUST appear verbatim (byte-for-byte, per the W8′-fix correction above):
```
VERIFIED_SUCCESS_SAMPLE:
{"conversation_id":"3d206228-0e90-48b4-9c05-4a6d14ff3d8b","status":"SUCCESS","response":"bt-7731\n","error":null,"num_turns":1}

VERIFIED_TOOL_DENIAL_DECORATION_SAMPLE:
{"conversation_id":"97197d46-2091-40e2-b36f-0dcedd34bcc4","status":"ERROR","response":"It looks like your request was just the letter \"x\". ...","error":"Permission denied for read_file(/home/mewais/.gemini/antigravity-cli). Matches hardcoded system protection boundary rule.","duration_seconds":2.70532965,"num_turns":1,"usage":{}}
```
Both are real captured lines, not synthetic. Remaining fixtures used only for shape-coverage (structural
error cases, line-selection, ignored fields) may still be synthetic/hand-built — clearly commented as such.
- `compose_prompt`: one test per `DeliveryKind` (exact header+body); non-empty `data` → exact
  "Attached data:" block; empty `data` → no block. (Same assertions as E3/W7 — outputs must be identical.)
- `build_argv` exact vecs (CORRECTED 2026-07-03 — see the W8′-fix correction): `First` →
  `["agy","--output-format","json","--model","Gemini 3.5 Flash (Low)","--sandbox",
  "--dangerously-skip-permissions","-p","<prompt>"]` (NO `--conversation`; `"-p"` then the prompt are the
  strictly-last TWO elements — pinned by index, not just membership); `Continuation("97…c4")` → starts
  `["agy","--output-format","json","--conversation","97…c4","--model",…]`, `-p`+prompt last again; custom
  bin/model spliced correctly. Also assert: `--output-format`/`json` appear immediately after `bin` (before
  `--conversation`, when present); `"-p"` is always `argv[argv.len()-2]` and the prompt is always
  `argv[argv.len()-1]`.
- `parse_reply` happy path: `VERIFIED_SUCCESS_SAMPLE` →
  `Ok(AgyReply{conversation_id:"3d206228-0e90-48b4-9c05-4a6d14ff3d8b", text:"bt-7731\n"})`.
- `parse_reply` tool-denial-decoration path: `VERIFIED_TOOL_DENIAL_DECORATION_SAMPLE` →
  `Ok(AgyReply{conversation_id:"97197d46-2091-40e2-b36f-0dcedd34bcc4", text: <the full response string,
  verbatim, including the "It looks like..." tool-denial narration>})` — NOT an error, despite
  `status:"ERROR"` and a non-empty `error`. This is the pinned regression test for the corrected mapping:
  it MUST fail loudly if a future edit reintroduces status/error gating.
- status/error IRRELEVANT to success (the corrected-mapping contract, replacing the old "status tolerance"
  suite): any `status` value (`"SUCCESS"`, `"ERROR"`, absent, garbage) and any `error` value (present,
  absent, null, non-empty) → still `Ok` as long as `conversation_id` and `response` are both non-empty
  strings. At least one case combines a non-empty `error` with an unrelated/arbitrary `status` string to
  prove neither field is consulted.
- failure mapping (single unified shape, CORRECTED — no more separate "no conversation_id"/"no response"
  messages): missing/empty `conversation_id`; missing/non-string/empty `response`; both missing — ALL →
  `Err("agy turn failed: {raw line}")`, message containing the raw line. Include a case where `response`
  is a non-empty string but `conversation_id` is missing (still `Err`, even though a naive "response
  looks fine" heuristic might have called it Ok).
- line selection: fixture surrounded by blank lines and a stray non-JSON banner line → same Ok; two JSON
  object lines → LAST wins; empty stdout / pure garbage → `Err("agy output had no JSON result line")`.
- `usage`/`num_turns`/`duration_seconds` absent, or `usage` with unexpected shape → still Ok when
  `conversation_id`/`response` are present (ignored fields).

### `tests/adapter.rs` (deliver wiring via a FAKE CliInvoker — the ledger learn/reuse flow)
Fake `CliInvoker` records every `(argv, stdin)` and returns scriptable per-call results. Let `OK(cid)` mean
a happy fixture line whose `conversation_id` is `cid`. Mirrors W7's suite one-for-one:
- **learn:** first deliver for ctx → fake saw `build_argv("agy", DEFAULT_MODEL, Turn::First, &prompt)`
  with `prompt == compose_prompt(d)` as the last argv element AND `stdin == ""`; returns `OK("c1")` →
  reply text parsed; `conversation_of(ctx) == Some("c1")`.
- **reuse:** second deliver, SAME ctx → fake sees `Continuation("c1")` argv (the LEARNED id spliced in).
- **per-context:** different ctx → `First` again; each ctx tracks its own conversation.
- **fallback (blind retry):** ctx has `c1`; fake errs on the Continuation call, then `OK("c2")` for the
  First retry → Ok; exactly 2 invocations; `conversation_of(ctx) == Some("c2")`; third deliver resumes `c2`.
- **no double-retry:** Continuation errs, First retry errs → the RETRY's error propagates; exactly 2
  invocations; ledger still `c1`.
- **First error propagates:** fresh ctx, fake errs → propagates after exactly 1 invocation;
  `conversation_of(ctx) == None`; next deliver is `First` again.
- **parse failure ≠ retry:** fresh ctx, fake returns a line with a `conversation_id` but an EMPTY
  `response` (invoker Ok, exit 0 — CORRECTED 2026-07-03: under the new mapping a `status:"ERROR"` line
  with a non-empty `response` is now a SUCCESS, so this fixture must fail on `response` emptiness, not on
  `status`, to genuinely exercise the parse-failure path) → parse error propagates, exactly 1 invocation,
  ledger untouched — EVEN IF that line carried a `conversation_id` (assert `conversation_of(ctx) == None`:
  the ledger-on-full-success rule, pinned).
- **tool-denial-decoration reply is a SUCCESS through the whole adapter (new, 2026-07-03):** fresh ctx,
  fake returns `VERIFIED_TOOL_DENIAL_DECORATION_SAMPLE` verbatim (invoker Ok) → `deliver` returns
  `Ok(<the decoration response text>)`, exactly 1 invocation, `conversation_of(ctx) ==
  Some("97197d46-2091-40e2-b36f-0dcedd34bcc4")` — the ledger IS updated, because this reply is a genuine
  success under the corrected mapping despite its `status:"ERROR"` + populated `error`.
- **`with_bin("agy-next", …)`** → argv[0] == `"agy-next"`; **`with_model(…, "Gemini 3.5 Flash (High)")`**
  → that string follows `--model` on both First and Continuation.
- malformed stdout (invoker Ok, garbage) → parse `AdapterError` propagates.

### `tests/process_invoker.rs` (ProcessCliInvoker via stand-in commands — real child processes)
Same five as E3/W7 (plumbing proof, nothing about the real `agy` binary): `cat` echoes stdin;
`sh -c 'printf %s abc'` → `Ok("abc")`; `sh -c 'echo oops 1>&2; exit 4'` → Err containing `oops`;
nonexistent program → Err containing `spawn`; empty argv → `Err("empty argv")`.

### `tests/live.rs` (REAL `agy` — behind `--features live-cli`, excluded from default runs)
Gated with `#![cfg(feature = "live-cli")]` (a cargo feature, NOT `#[ignore]`). Free-tier rule: cheapest
model (`DEFAULT_MODEL` — this run doubles as the `--model`-string verification), single-word prompts,
**3 model calls total**. Fresh context per run (`context_id` includes env `WASL_LIVE_NONCE`). Tests are
independent (own contexts/workspaces) so default parallel execution is safe.

1. **`permission_probe` — THE FIRST-PRIORITY TEST (resolves THE PERMISSION QUESTION).** Drives
   `ProcessCliInvoker` directly (not the adapter): argv `["agy","--model",DEFAULT_MODEL,"--sandbox",
   "--print-timeout","45s","-p","run the shell command: echo agy_probe_hi"]` (CORRECTED 2026-07-03 — `-p`
   and its prompt moved to the strictly-last two positions per the W8′-fix correction; this test asserts on
   raw stdout substring content, not JSON, so `--output-format json` is intentionally omitted here) —
   deliberately WITHOUT `--dangerously-skip-permissions`, short explicit timeout, on a workspace with no
   pre-seeded permission rules. Asserts ONLY that the invocation terminates within a generous test-side
   bound (~90s) — it must
   not assert WHICH behavior occurs, since that is exactly the unknown. The test then prints/records the
   trichotomy: (a) tool ran (headless auto-allow — alarming; skip flag moot but sandbox load-bearing),
   (b) tool denied but turn completed (auto-deny — skip flag confirmed necessary for tool-touching
   deliveries), (c) timeout hit (hang — skip flag confirmed MANDATORY). Outcome goes into the crate
   `AGENTS.md` contract table and flips this spec's interim-decision NOTE from assumed to observed.
2. **`two_turn_continuity` (2 calls, the adapter proper):** construct
   `AntigravityAdapter::new(ProcessCliInvoker)`;
   (i) deliver a Consult `Reply with exactly the word: PING_OK` → assert reply contains `PING_OK`; assert
   `conversation_of(ctx)` is `Some(cid)` and `cid` parses as a UUID (ledger learned agy's minted id — and
   the turn succeeding at all live-verifies the `--model` display string, sandbox+skip-permissions argv
   acceptance, and the single-line JSON contract in one shot);
   (ii) deliver in the SAME context `What exact word did I ask for? Answer with just that word.` → assert
   reply contains `PING_OK` (proves `--conversation` continuity through the adapter) AND
   `conversation_of(ctx)` still equals `cid`. A byte-identical `cid` with real recall is also the designed
   detector for the resume-argv-rejection failure mode (blind retry would have minted a NEW id and lost
   the word).

Runs only in explicit verification passes (`cargo test -p wasl-adapter-antigravity --features live-cli`);
the orchestrator asserts it RAN there. Default CI never spends tokens. Deployment precondition: the
machine must already be OAuth-authenticated (`~/.gemini/oauth_creds.json`) — on an unauthenticated box
`-p` may escalate to an interactive OAuth prompt and the probe's timeout bound is what saves the run.

## Coverage bar
`cargo llvm-cov -p wasl-adapter-antigravity --fail-under-lines 100` MUST exit 0 (genuine LINE 100%).
REGION may carry documented residuals ONLY on genuinely-unreachable IO error `?` branches in
`ProcessCliInvoker` (child-stdin write failure, `wait` failure — same enumerated set as E3/W7). Enumerate
them in the crate `AGENTS.md`. Do NOT fake coverage with dead code/panics. No `#[ignore]`/`todo!`/
tautology. Tests compile only against the public API + `wasl_adapter` public types.
