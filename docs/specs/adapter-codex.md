# W7 — `wasl-adapter-codex` — concrete Codex-CLI `AgentAdapter` — spec (authoritative)

**Tier:** L2 (IO shell over a tested pure core). **Deps:** `wasl-adapter` (the `AgentAdapter` trait +
`Delivery`/`DeliveryKind`/`AdapterError`), `serde`, `serde_json`, `async-trait`, `tokio` (process). Dev:
`tokio` (macros, rt), `serde_json`. No `unsafe`. No `uuid` dep — unlike claude, **codex mints its own
thread ids**; the adapter never fabricates one. This is ONE concrete adapter: it injects an incoming
`Delivery` into the local **OpenAI Codex** CLI (`codex exec`) and returns its reply. It mirrors the E3
pattern (`wasl-adapter-claude`): pure core + `CliInvoker` seam + `ProcessCliInvoker` + per-context session
tracking.

## ⚠️ CONTRACT STATUS — VERIFIED AGAINST THE REAL CLI (codex-cli 0.142.5, 2026-07-02)

Ground truth is the R2 codex research doc (`Marsad/docs/specs/codex-research.md`), verified against the
real installed binary with two live model calls plus recursive `--help` trees, generated protocol schemas,
and direct reads of the on-disk session files. The facts this spec depends on, and their status:

| Claim | Status (verified 2026-07-02, codex-cli 0.142.5) |
|---|---|
| `codex exec --json` emits JSONL (one JSON object per line) on stdout | ✅ verified live |
| First event is `{"type":"thread.started","thread_id":"<uuid-v7>"}` — codex assigns the id | ✅ verified live |
| Reply text arrives as `{"type":"item.completed","item":{...,"type":"agent_message","text":"..."}}` | ✅ verified live |
| `codex exec resume <thread_id> --json ...` continues the SAME thread (context preserved; `thread_id` byte-identical; same rollout file appended) | ✅ verified live (continuity PROVEN: turn 2 recalled turn 1's word) |
| Prompt on stdin via positional `-` (`codex exec -` / `codex exec resume <id> -`) | ✅ documented `--help` convention; stdin auto-append observed live |
| Outside a trusted git repo, runs fail without `--skip-git-repo-check` | ✅ hit live (exit 1) |
| `codex exec resume` REJECTS `--sandbox` (`unexpected argument`) — sandbox is fixed by the original thread | ✅ hit live (arg-parse error) |
| `codex exec` has NO `-a/--ask-for-approval`; `turn_context.approval_policy` is hardcoded `"never"` | ✅ verified live |
| Sessions stored at `~/.codex/sessions/<Y>/<M>/<D>/rollout-<ts>-<thread_id>.jsonl` | ✅ verified live |
| `-m <model>` accepted by BOTH `exec` and `exec resume` | ✅ verified live (`gpt-5.4-mini` both turns) |
| Error string/exit code of `codex exec resume <nonexistent-id>` | ❌ UNVERIFIED — never exercised; this spec must not substring-match on it |
| `turn.failed` payload shape and whether it forces a non-zero exit | ❌ UNVERIFIED — event name from binary strings only |

The two ❌ rows shape the error-mapping design below: where claude's adapter could substring-match two
verified stderr messages, codex's cannot, so the fallback is structural (blind, single, one-direction)
rather than string-matched.

### Verified Codex CLI contract (the implementation target)
- **Session identity:** codex mints a UUIDv7-shaped `thread_id` per thread and reports it in the
  `thread.started` JSONL event. There is NO way to supply our own id (no `--session-id` equivalent on
  `exec`), so the claude trick — deterministic UUIDv5 from `context_id` — is impossible. The adapter keeps
  a **context_id → thread_id ledger** learned from each first turn's output (design below).
- **First turn for a context:**
  `codex exec --json --skip-git-repo-check --sandbox read-only [-m <model>] -`, prompt on stdin.
- **Subsequent turns:**
  `codex exec resume <thread_id> --json --skip-git-repo-check [-m <model>] -`, prompt on stdin.
  No `--sandbox` here (rejected by the CLI; the thread keeps its original sandbox).
- **Why explicit `-`:** with a positional prompt AND piped stdin, codex auto-appends stdin as a `<stdin>`
  block (observed live). Passing `-` makes stdin the *sole* prompt source, which matches the
  `CliInvoker::invoke(argv, stdin)` seam exactly (prompt travels only on stdin, as with claude's `-p`).
- **Why not `--output-last-message <FILE>`:** it needs a temp file (IO in what should be pure parsing) and
  gives strictly less than the JSONL stream, which we must parse anyway to learn `thread_id`.
- **Why not `--ephemeral`:** it skips session persistence and therefore forfeits `resume` — the whole point.
- **Reply parsing:** from the JSONL stream: `thread_id` from `thread.started`, reply text from the LAST
  `item.completed` with `item.type == "agent_message"`. Unknown event/item types are IGNORED
  (forward-compat: the full enum includes `turn.started`, `turn.completed`, `item.started`, `item.updated`,
  `reasoning`, `command_execution`, ... which carry nothing we need). A `turn.failed` line is an error.

## Session identity: the context_id → thread_id LEDGER

**Decision: in-memory only** — `threads: Mutex<HashMap<String, String>>` (context_id → thread_id), learned
from `thread.started` on a successful first turn, consulted on every `deliver`.

**Documented consequence:** an adapter restart empties the ledger, so the next `deliver` for a known
context starts a **fresh codex thread** — prior conversation context is lost for that context (delivery
still succeeds), and the old thread is orphaned on disk under `~/.codex/sessions/` (harmless; codex keeps
rollout files indefinitely anyway).

**Why not re-derive by scanning `~/.codex/sessions/`:** rejected, for correctness, not laziness. The
rollout files contain NO field for our `context_id` — codex records `session_meta`, `turn_context`,
`response_item`, `event_msg`, none of which carry caller-supplied correlation ids. The only recoverable
signal would be matching our `compose_prompt` header (`[Consult from {from}]`) inside recorded
`user_message` payloads — but that header is followed by `d.text` VERBATIM, so any peer can embed a
counterfeit header inside a message body and a scanner would happily bind another context's conversation
to the attacker's thread. A heuristic that can be steered by message content is not a session-identity
mechanism. (A wasl-owned ledger *file* would be sound but adds filesystem state, a config key, and
corruption/locking concerns for a failure mode — process restart — that degrades gracefully anyway; if
restart-survivable continuity is later required, that is a small follow-up block, not a reason to
complicate W7.)

**Ledger update rule:** the ledger is written ONLY on a fully-successful `deliver` (invoker Ok AND
`parse_reply` Ok). If the invoker succeeds but parsing fails (e.g. `turn.failed`, missing
`agent_message`), the ledger is untouched: the next turn for that context starts fresh rather than
resuming a thread whose last turn is in an unknown state. Concurrent first-`deliver`s for the same fresh
context may race and create two codex threads; last-write-wins in the ledger (same benign race the E3
`started` set has).

## Pure core (genuine 100% — unit-tested)
```rust
/// Compose the prompt injected into Codex from a Delivery (verb header + provenance + body + any data).
/// IDENTICAL composition to wasl-adapter-claude so a message reads the same regardless of receiver CLI:
///   Consult -> "[Consult from {from}]\n\n{text}"; Command/Message analogous. Non-empty `d.data` appends
///   "\n\nAttached data:" then one line per JSON value (`Value::to_string`).
pub fn compose_prompt(d: &wasl_adapter::Delivery) -> String;

/// How this turn binds to the codex thread.
pub enum Turn<'a> { First, Continuation(&'a str) }   // Continuation carries the learned thread_id

/// Build the Codex CLI argv for a turn. `bin` is the program (default "codex"); `model` is an optional
/// `-m` override (used by the live suite to force the cheapest model; None → account default).
pub fn build_argv(bin: &str, model: Option<&str>, turn: Turn<'_>) -> Vec<String>;
//   First             -> [bin, "exec", "--json", "--skip-git-repo-check", "--sandbox", "read-only",
//                         ("-m", model)?, "-"]
//   Continuation(tid) -> [bin, "exec", "resume", tid, "--json", "--skip-git-repo-check",
//                         ("-m", model)?, "-"]
//   NOTE: no "--sandbox" on Continuation — codex exec resume rejects it (verified arg-parse error);
//   the thread keeps the sandbox it was created with.

/// The parsed outcome of one codex exec turn.
pub struct CodexReply { pub thread_id: String, pub text: String }

/// Parse `codex exec --json` JSONL stdout into (thread_id, reply text) or an AdapterError.
pub fn parse_reply(stdout: &str) -> Result<CodexReply, wasl_adapter::AdapterError>;
//   Per line (skip empty/whitespace-only lines):
//     - must parse as a JSON object with a string "type" -> else Err("parse codex output line: {e}");
//     - "thread.started"  -> record `thread_id` (first occurrence wins);
//     - "item.completed" with item.type == "agent_message" -> record item.text (LAST occurrence wins);
//     - "turn.failed"     -> Err("codex turn failed: {raw line}") — payload shape is UNVERIFIED (binary
//                            strings only), so the raw line is the honest detail; do not invent fields;
//     - anything else (turn.started, turn.completed, item.started, item.updated, other item types,
//       unknown future types) -> ignored.
//   End: no thread.started -> Err("codex output had no thread.started");
//        no agent_message  -> Err("codex output had no agent_message").
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
`ProcessCliInvoker::invoke` is BEHAVIORALLY IDENTICAL to E3's: `split_first` (empty argv → Err "empty
argv"); spawn with piped stdin/stdout/stderr (spawn error → Err "spawn {prog}: {e}"); write all of `stdin`
to the child then drop the handle (EOF — codex needs EOF to stop reading the `-` prompt); `wait_with_output`;
success → stdout as lossy UTF-8; non-success → `Err("{prog} {status}: {stderr}")`. Codex's stderr chatter
(e.g. `Reading additional input from stdin...`) never pollutes stdout, so JSONL parsing is unaffected. Use
the single named error helper `fn ad(msg) -> AdapterError` at every error site (E3's llvm-cov technique).
This trait + invoker are deliberately DUPLICATED from `wasl-adapter-claude` (~40 lines): importing them
from the claude crate would couple codex builds to claude's crate for two tiny types, and hoisting them
into `wasl-adapter` is a C1 spec change outside W7's blast radius (a legitimate later refactor, not now).

## The adapter
```rust
/// A concrete AgentAdapter that drives the Codex CLI via a CliInvoker.
pub struct CodexAdapter<I: CliInvoker> {
    /* invoker: I, bin: String, model: Option<String>, threads: Mutex<HashMap<String, String>> */
}
impl<I: CliInvoker> CodexAdapter<I> {
    pub fn new(invoker: I) -> Self;                                     // bin = "codex", model = None
    pub fn with_bin(invoker: I, bin: impl Into<String>) -> Self;        // model = None
    pub fn with_model(invoker: I, bin: impl Into<String>, model: impl Into<String>) -> Self;
    /// The learned codex thread_id for a context, if any (observability + live-test assertions).
    pub fn thread_of(&self, context_id: &str) -> Option<String>;
}
#[async_trait::async_trait]
impl<I: CliInvoker> wasl_adapter::AgentAdapter for CodexAdapter<I> {
    // deliver:
    //   prompt = compose_prompt(d);
    //   turn   = match ledger.get(context_id) { Some(tid) => Continuation(tid), None => First };
    //   attempt invoker.invoke(build_argv(bin, model, turn), prompt);
    //   fallback (ONE retry, ONE direction only):
    //     Continuation + ANY invoker error -> retry once as First (fresh thread);
    //     First + invoker error            -> propagate (nothing to fall back to: codex mints ids, so
    //                                         an "already in use" collision cannot exist).
    //   on invoker success: reply = parse_reply(stdout)?; ledger.insert(context_id, reply.thread_id);
    //   return Ok(reply.text). Parse errors (incl. turn.failed) propagate WITHOUT retry and WITHOUT
    //   touching the ledger.
    async fn deliver(&self, d: &Delivery) -> Result<String, AdapterError>;
}
```

**Error mapping, and why the retry is blind (unlike E3):** claude's adapter matched two VERIFIED stderr
substrings. For codex, the error text of resuming a missing/deleted thread is UNVERIFIED (contract table),
so any substring match would be a guess dressed as a contract. Instead: a Continuation invoker error most
plausibly means the thread is gone (deleted sessions dir, `--ephemeral` interference, codex version bump),
and the honest fail-open response is one fresh-thread retry — conversation context is lost but the
delivery succeeds, which is the same degradation the ledger design already accepts on restart. If the real
cause was environmental (binary missing, network down), the First retry fails identically and that error
propagates after exactly 2 invocations — one wasted spawn, no wasted tokens. The retry never loops: a
First attempt (original or retry) that fails propagates. On retry success the ledger entry is overwritten;
the old thread stays orphaned on disk, which is codex's normal steady state. One admitted rough edge: if a
`turn.failed` turn ALSO exits non-zero (unverified), a Continuation hitting it gets the blind fresh-thread
retry and pays for a second model call in an already-failing path — acceptable, documented, and removable
once the `turn.failed` exit behavior is live-verified.

## Approvals & sandbox posture (the RECEIVE-side safety story)

`codex exec` hardcodes `approval_policy = "never"` and exposes NO `-a/--ask-for-approval` flag (verified:
the flag exists on interactive `codex`/`codex resume`/`codex fork` only, and the live session recorded
`"never"` unrequested). **Therefore this adapter can never surface an interactive approval prompt** — mid-
turn HITL gating is structurally unavailable in spawn-per-turn exec mode (it exists only in `codex
app-server`'s JSON-RPC protocol, which is Marsad's territory, not Wasl's). The consequences, stated
plainly:

- The receive side's ONLY risk control is the up-front sandbox tier, and this spec pins it to
  **`--sandbox read-only`** on every First turn: the injected model turn can read the filesystem but not
  write it and has restricted network (verified live: `sandbox_policy {type: "read-only"}`, restricted
  `permission_profile`). Wasl's own gate (send-side, D-blocks) remains the human decision point; the codex
  receive side is deliberately inert.
- The sandbox tier is NOT config-exposed in W7. Widening to `workspace-write`/`danger-full-access` is a
  spec change (a future `adapter.sandbox` key in node-config), not a knob to smuggle in — especially since
  `exec resume` cannot change tier afterwards (rejected flag), so a widened first turn pins a widened
  thread forever.
- What codex does when the sandboxed model *attempts* a forbidden action is unverified (research §f: the
  documented `"never"` semantics say failures return to the model, which then routes around or gives up) —
  either way it surfaces to Wasl as reply text or a failed turn, never as a hung prompt.

## Node-config integration (W2 delta, in node-config.md terms)

`docs/specs/node-config.md` planned for this: *"Exactly one variant in this block; W7 (codex) adds more."*
The delta W7 prescribes there:

- `AdapterKind` gains `Codex`; `adapter.kind = "codex"` (exact, case-sensitive, same second-pass
  validation; `"Codex"` → `UnknownAdapterKind`).
- `adapter.bin` keeps its meaning: absent → `"codex"` default (`CodexAdapter::new(ProcessCliInvoker)`),
  present → `CodexAdapter::with_bin(ProcessCliInvoker, bin)`. No other keys in W7 (no model, no sandbox —
  `with_model` exists for tests/embedders, not for the config file).
- `build_adapter`'s concrete return type (`ClaudeAdapter<ProcessCliInvoker>`) no longer works with two
  kinds. It becomes an exhaustive-match delegating enum (kept in `wasl-mcp`, where `build_adapter` lives):
  ```rust
  pub enum NodeAdapter {
      Claude(wasl_adapter_claude::ClaudeAdapter<wasl_adapter_claude::ProcessCliInvoker>),
      Codex(wasl_adapter_codex::CodexAdapter<wasl_adapter_codex::ProcessCliInvoker>),
  }
  impl AgentAdapter for NodeAdapter { /* match self { ... } => inner.deliver(d).await */ }
  pub fn build_adapter(cfg: &AdapterConfig) -> NodeAdapter;   // exhaustive match on AdapterKind
  ```
  An enum, not `Box<dyn AgentAdapter>`, because W2 explicitly wants "an exhaustive match on AdapterKind so
  adding a kind forces a decision" — a trait object would erase that compile-time forcing function.

## Test plan
### `tests/pure.rs` (genuine 100% of the pure core)
Fixtures are the research doc's CAPTURED JSONL — byte-for-byte, not invented:
```
{"type":"thread.started","thread_id":"019f237a-f03d-7710-ab8f-7af32b222df8"}
{"type":"turn.started"}
{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"OK"}}
{"type":"turn.completed","usage":{"input_tokens":9065,"cached_input_tokens":3456,"output_tokens":15,"reasoning_output_tokens":8}}
```
- `compose_prompt`: one test per `DeliveryKind` (exact header+body); non-empty `data` → exact
  "Attached data:" block; empty `data` → no block. (Same assertions as E3 — the outputs must be identical.)
- `build_argv`: exact vec for `First` (contains `exec`, `--json`, `--skip-git-repo-check`,
  `--sandbox read-only`, trailing `-`; NO `resume`) and `Continuation("019f…df8")` (contains
  `exec resume <tid>`; NO `--sandbox`); with `model: Some("gpt-5.4-mini")` → `-m gpt-5.4-mini` present in
  both; `model: None` → absent from both.
- `parse_reply` happy path: the 4-line fixture → `Ok(CodexReply{thread_id: "019f…df8", text: "OK"})`.
- tolerance: fixture plus blank lines and an unknown event (`{"type":"item.started"}`) and a non-message
  item (`{"type":"item.completed","item":{"type":"reasoning"}}`) → same Ok; two `agent_message` items →
  LAST text wins.
- errors: stream missing `thread.started` → `Err("… no thread.started")`; missing any `agent_message` →
  `Err("… no agent_message")`; a `{"type":"turn.failed"}` line → `Err("codex turn failed: …")` containing
  the raw line; a non-JSON line (`not json`) → `Err("parse codex output line: …")`; a JSON line with no
  string `type` → same parse error; empty stdout → the no-thread.started error.

### `tests/adapter.rs` (deliver wiring via a FAKE CliInvoker — the ledger learn/reuse flow)
Fake `CliInvoker` records every `(argv, stdin)` and returns scriptable per-call results. Let `OK(tid)` mean
a happy fixture stream whose thread.started carries `tid`.
- **learn:** first deliver for ctx → fake saw `build_argv("codex", None, Turn::First)` and
  `stdin == compose_prompt(d)`; returns `OK("t1")` → reply text parsed; `thread_of(ctx) == Some("t1")`.
- **reuse:** second deliver, SAME ctx → fake sees `Turn::Continuation("t1")` argv (asserts the LEARNED id
  is spliced in, not a derived one).
- **per-context:** deliver for a DIFFERENT ctx → `First` again; each ctx tracks its own thread.
- **fallback (blind retry):** ctx has `t1`; fake errs on the Continuation call (any message, e.g. `boom`),
  then returns `OK("t2")` for the First retry → Ok; exactly 2 invocations; `thread_of(ctx) == Some("t2")`;
  a third deliver resumes `t2`.
- **no double-retry:** Continuation errs, First retry also errs → the RETRY's error propagates; exactly 2
  invocations; ledger still `t1` (unchanged on failure).
- **First error propagates:** fresh ctx, fake errs → error propagates after exactly 1 invocation;
  `thread_of(ctx) == None`; next deliver is `First` again.
- **parse failure ≠ retry:** fresh ctx, fake returns a `turn.failed` stream (invoker Ok) → parse error
  propagates, exactly 1 invocation, ledger untouched.
- **`with_bin("codex-cli", …)`** → argv[0] == `"codex-cli"`; **`with_model(…, "gpt-5.4-mini")`** → argv
  contains `-m gpt-5.4-mini` on both First and Continuation calls.
- malformed stdout (invoker Ok, garbage) → parse `AdapterError` propagates.

### `tests/process_invoker.rs` (ProcessCliInvoker via stand-in commands — real child processes)
Same five as E3 (they prove plumbing, nothing about the real `codex` binary):
`cat` echoes stdin; `sh -c 'printf %s abc'` → `Ok("abc")`; `sh -c 'echo oops 1>&2; exit 4'` → Err
containing `oops`; nonexistent program → Err containing `spawn`; empty argv → `Err("empty argv")`.

### `tests/live.rs` (REAL `codex` — behind `--features live-cli`, excluded from default runs)
Gated with `#![cfg(feature = "live-cli")]` (a cargo feature, NOT `#[ignore]`). ONE test, exactly 2 model
calls, minimal tokens, and — per the free-tier rule (the live account is `plan_type: "free"`, confirmed in
the research runs) — the CHEAPEST model: construct
`CodexAdapter::with_model(ProcessCliInvoker, "codex", "gpt-5.4-mini")`. Fresh context per run
(`context_id` includes env `WASL_LIVE_NONCE`):
1. deliver a Consult with body `Reply with exactly the word: PING_OK` → assert reply contains `PING_OK`;
   assert `thread_of(ctx)` is `Some(tid)` and `tid` parses as a UUID (the ledger learned codex's id).
2. deliver in the SAME context: `What exact word did I ask for? Answer with just that word.` → assert reply
   contains `PING_OK` (proves `exec resume` continuity through the adapter) and `thread_of(ctx)` still
   equals `tid` (byte-stable across resume, as verified in research).
Runs only in explicit verification passes (`cargo test -p wasl-adapter-codex --features live-cli`); the
orchestrator asserts it RAN there. Default CI never spends tokens. Note: live runs require a git-trusted or
any directory — `--skip-git-repo-check` is baked into every argv precisely so the suite doesn't depend on
where CI checks out.

## Coverage bar
`cargo llvm-cov -p wasl-adapter-codex --fail-under-lines 100` MUST exit 0 (genuine LINE 100%). REGION may
carry documented residuals ONLY on genuinely-unreachable IO error `?` branches in `ProcessCliInvoker`
(child-stdin write failure, `wait` failure — same enumerated set as E3). Enumerate them in `AGENTS.md`. Do
NOT fake coverage with dead code/panics. No `#[ignore]`/`todo!`/tautology. Tests compile only against the
public API + `wasl_adapter` public types.
