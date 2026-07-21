# W9 — `wasl-adapter-opencode` — concrete opencode-CLI `AgentAdapter` — spec (authoritative)

**Tier:** L2 (IO shell over a tested pure core). **Deps:** `wasl-adapter` (the `AgentAdapter` trait +
`Delivery`/`DeliveryKind`/`AdapterError`), `serde`, `serde_json`, `async-trait`, `tokio` (process). Dev:
`tokio` (macros, rt), `serde_json`. No `unsafe`. No `uuid` dep — like codex/agy and unlike claude,
**opencode mints its own session ids**; the adapter never fabricates one. This is ONE concrete adapter: it
injects an incoming `Delivery` into the local **opencode** CLI (`opencode run`) and returns its reply. It
mirrors the E3/W7/W8′ pattern (`wasl-adapter-claude`/`-codex`/`-antigravity`): pure core + `CliInvoker`
seam + `ProcessCliInvoker` + per-context session ledger.

## ⚠️ CONTRACT STATUS — VERIFIED AGAINST THE REAL CLI (opencode 1.18.4, 2026-07-20)

Ground truth is direct capture of the real installed binary: three live model calls (first turn, a
successful resume, and a bogus-session resume) plus recursive `--help` trees and `session list --format
json`. Every fixture below is byte-for-byte from those captures, not invented. The facts this spec depends
on, and their status:

| Claim | Status (verified 2026-07-20, opencode 1.18.4) |
|---|---|
| Headless mode is `opencode run [message..]` | ✅ verified (`--help`) |
| Prompt is delivered as a **positional argv** (`message` array), NOT stdin | ✅ verified (`--help`; stdin unused) |
| `opencode run --format json` emits JSONL (one JSON object per line) on stdout | ✅ verified live |
| Every event has top-level shape `{"type":..,"timestamp":..,"sessionID":..,"part"\|"error":..}` | ✅ verified live |
| Session id is the top-level `"sessionID"` field on every event (`ses_` + 27 base62) | ✅ verified live |
| opencode MINTS the session id; NO flag supplies one for a NEW session (`-s` is resume-only) | ✅ verified live |
| Reply text: concat `.part.text` of `type=="text"` events, grouped by `.part.messageID`, ordered by `.part.time.start` | ✅ verified live |
| Turn completion: `type=="step_finish"` with `.part.reason=="stop"`, then the process exits 0 | ✅ verified live (self-terminates on success) |
| In-stream failure: `type=="error"` on **stdout** carrying `.error.name` / `.error.data.message` | ✅ verified live (`ContextOverflowError`) |
| `opencode run --format json -s <id> ... "<prompt>"` continues the SAME session (context preserved; `sessionID` byte-identical) | ✅ verified live (turn 2 recalled turn 1's word; id byte-stable) |
| `-m <provider/model>` accepted by BOTH first turn AND resume (no rejection on resume) | ✅ verified live (Claude-Haiku-4.5 both turns) |
| Resuming a missing/malformed session → plain `Error: Session not found` on **stderr**, exit 1 | ✅ verified live (identical for malformed `-s Session` and well-formed-nonexistent `-s ses_000…`) |
| A pathological error (e.g. context overflow with autocompact) makes `run` LOOP and NOT self-terminate | ✅ observed live (17-min `step_start→text→error→step_finish` loop until killed) |

Two contract properties shape the design below:
1. **Prompt delivery differs from codex/agy-stdin**: opencode takes the prompt as a positional argv
   element, so the `CliInvoker::invoke(argv, stdin)` seam is called with `stdin == ""` (the prompt rides in
   argv). This is the agy pattern, not the claude/codex stdin pattern.
2. **The resume-error string IS verified and stable** (`Session not found` on stderr, exit 1), so — unlike
   codex/agy's blind retry — opencode can use a **matched retry** (claude-style): a `Continuation` invoker
   error whose message contains `Session not found` triggers exactly one fresh-`First` retry; any other
   `Continuation` error propagates (it is environmental — bad binary, network — not a stale-session case).

### Verified opencode CLI contract (the implementation target)
- **Session identity:** opencode mints a `ses_`-prefixed id per session and reports it as the top-level
  `sessionID` on every JSONL event. There is NO way to supply our own id for a new session (`-s/--session`
  is documented "session id to continue" — resume-only), so the claude trick (deterministic UUIDv5 from
  `context_id`) is impossible. The adapter keeps a **context_id → session_id ledger** learned from each
  first turn's output (design below).
- **First turn for a context:**
  `opencode run --format json [-m <model>] "<prompt>"` — prompt is the trailing positional argv element;
  the invoker is fed `stdin == ""`.
- **Subsequent turns:**
  `opencode run --format json -s <session_id> [-m <model>] "<prompt>"` — `-s` resumes; `-m` is still
  accepted (verified). Prompt again positional; `stdin == ""`.
- **Why positional, not stdin:** opencode `run` has no stdin-prompt convention (no `-` positional like
  codex); the prompt is `run`'s `message..` positional. Putting it in argv matches the CLI exactly. The
  invoker seam stays `(argv, stdin)` for uniformity with the other three adapters; opencode simply passes
  `stdin == ""`.
- **Why not `--continue`:** `-c/--continue` resumes the *last* session globally, which cannot distinguish
  per-`context_id` conversations; `-s <id>` is the only per-context-correct resume.
- **Reply parsing:** from the JSONL stream: `session_id` from any event's top-level `sessionID`; reply text
  by concatenating the `.part.text` of all `type=="text"` events belonging to the LAST assistant message
  (grouped by `.part.messageID`), ordered within that message by `.part.time.start`. A `type=="error"`
  event is a failed turn. Unknown/other event types (`step_start`, `step_finish`, future types) carry
  nothing we need and are ignored for text purposes; `step_finish` with `reason=="stop"` is the semantic
  end-of-turn marker but is NOT required for parsing (the id + at least one text part suffice).

## Session identity: the context_id → session_id LEDGER

**Decision: in-memory only** — `sessions: Mutex<HashMap<String, String>>` (context_id → session_id),
learned from the top-level `sessionID` on a successful first turn, consulted on every `deliver`.

**Documented consequence:** an adapter restart empties the ledger, so the next `deliver` for a known
context starts a **fresh opencode session** — prior conversation context is lost for that context
(delivery still succeeds), and the old session is orphaned in opencode's on-disk session store (harmless;
opencode keeps sessions indefinitely).

**Why not re-derive by scanning opencode's session store:** rejected for correctness, not laziness (same
reasoning as codex/agy). opencode's persisted sessions carry no field for our `context_id`; the only
recoverable signal would be matching our `compose_prompt` header inside a stored user message, and that
header is followed by `d.text` VERBATIM, so any peer could embed a counterfeit header in a message body and
steer a scanner to bind another context's conversation to the attacker's session. A heuristic steerable by
message content is not a session-identity mechanism. (A wasl-owned ledger *file* would be sound but adds
filesystem state, a config key, and corruption/locking concerns for a failure mode — process restart —
that degrades gracefully anyway; a follow-up block if restart-survivable continuity is ever required.)

**Ledger update rule:** the ledger is written ONLY on a fully-successful `deliver` (invoker Ok AND
`parse_reply` Ok). If the invoker succeeds but parsing fails (a `type=="error"` stream, or no assistant
text), the ledger is untouched: the next turn for that context starts fresh rather than resuming a session
whose last turn is in an unknown state. Concurrent first-`deliver`s for the same fresh context may race and
create two opencode sessions; last-write-wins in the ledger (the same benign race the other adapters have).

## Pure core (genuine 100% — unit-tested)
```rust
/// Compose the prompt injected into opencode from a Delivery (verb header + provenance + body + any data).
/// IDENTICAL composition to wasl-adapter-claude/-codex/-antigravity so a message reads the same regardless
/// of receiver CLI:
///   Consult -> "[Consult from {from}]\n\n{text}"; Command/Message analogous. Non-empty `d.data` appends
///   "\n\nAttached data:" then one line per JSON value (`Value::to_string`).
pub fn compose_prompt(d: &wasl_adapter::Delivery) -> String;

/// How this turn binds to the opencode session.
pub enum Turn<'a> { First, Continuation(&'a str) }   // Continuation carries the learned session_id

/// Build the opencode CLI argv for a turn. `bin` is the program (default "opencode"); `model` is an
/// optional `-m` override (used by the live suite to force the cheapest model; None -> config default).
/// `prompt` is the composed prompt, placed as the trailing positional argv element.
pub fn build_argv(bin: &str, model: Option<&str>, turn: Turn<'_>, prompt: &str) -> Vec<String>;
//   First             -> [bin, "run", "--format", "json", ("-m", model)?, prompt]
//   Continuation(sid) -> [bin, "run", "--format", "json", "-s", sid, ("-m", model)?, prompt]
//   NOTE: prompt is ALWAYS the LAST element. `-m` is accepted on BOTH turns (verified), unlike codex which
//   drops --sandbox on resume.

/// The parsed outcome of one opencode run turn.
pub struct OpencodeReply { pub session_id: String, pub text: String }

/// Parse `opencode run --format json` JSONL stdout into (session_id, reply text) or an AdapterError.
pub fn parse_reply(stdout: &str) -> Result<OpencodeReply, wasl_adapter::AdapterError>;
//   Per line (skip empty/whitespace-only lines):
//     - must parse as a JSON object -> else Err("parse opencode output line: {e}");
//     - the top-level "sessionID" (string) is recorded (first occurrence wins);
//     - "type" == "error"  -> Err("opencode error: {name}: {message}") from .error.name/.error.data.message
//                             (fall back to the raw line if those fields are absent — do not invent shapes);
//     - "type" == "text"   -> collect (.part.messageID, .part.time.start, .part.text) for text assembly;
//     - anything else (step_start, step_finish, unknown future types) -> ignored.
//   Assemble reply text: take the text parts of the LAST messageID seen (the final assistant message),
//     order them by .part.time.start, concatenate .part.text.
//   End: no sessionID -> Err("opencode output had no sessionID");
//        no text parts -> Err("opencode output had no assistant text").
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
`ProcessCliInvoker::invoke` is BEHAVIORALLY IDENTICAL to the other three adapters' — with **the M2 EPIPE
fix applied uniformly across all four** (see below): `split_first` (empty argv → Err "empty argv"); spawn
with piped stdin/stdout/stderr (spawn error → Err "spawn {prog}: {e}"); write all of `stdin` to the child
then drop the handle (EOF) — **a `BrokenPipe`/`WriteZero` (EPIPE) on that write is IGNORED, not fatal**,
because a CLI that reads no stdin (opencode: prompt is in argv, so it never reads stdin) can close its stdin
end before we finish writing, and its valid stdout must still be captured; any OTHER write error is fatal;
`wait_with_output`; success → stdout as lossy UTF-8; non-success → `Err("{prog} {status}: {stderr}")`. Use
the single named error helper `fn ad(msg) -> AdapterError` at every fatal error site (the llvm-cov
technique). This trait + invoker are deliberately DUPLICATED from the sibling adapters (~40 lines):
importing them would couple opencode builds to a sibling crate for two tiny types, and hoisting them into
`wasl-adapter` is a C1 spec change outside W9's blast radius (a legitimate later refactor, not now).

### M2 — the EPIPE fix (applies to ALL FOUR adapters)
The pre-W9 `ProcessCliInvoker` in claude/codex/antigravity did
`si.write_all(stdin.as_bytes()).await.map_err(ad)?` — surfacing a child's `BrokenPipe` as a hard error and
discarding valid stdout when the child exits/ignores stdin before draining it (proven by
`wasl-mcp/tests/config_wiring.rs`'s `printf` stub failing 3/3). W9 corrects this everywhere: the stdin
write treats `ErrorKind::BrokenPipe` (and `WriteZero`) as a benign no-op (the child simply isn't reading),
proceeding to `wait_with_output`; all other write errors remain fatal. opencode makes this fix mandatory,
not incidental: it never reads stdin at all (prompt is positional), so on a fast reply it can close stdin
before the (empty) write completes — an unfixed invoker would intermittently EPIPE. The fix is behavior-
preserving for the other three (they feed a stdin-reading CLI, so a BrokenPipe there only ever happened on
an already-failing child whose non-zero exit + stderr now surface correctly instead of being masked by the
write error).

## The adapter
```rust
/// A concrete AgentAdapter that drives the opencode CLI via a CliInvoker.
pub struct OpencodeAdapter<I: CliInvoker> {
    /* invoker: I, bin: String, model: Option<String>, sessions: Mutex<HashMap<String, String>> */
}
impl<I: CliInvoker> OpencodeAdapter<I> {
    pub fn new(invoker: I) -> Self;                                     // bin = "opencode", model = None
    pub fn with_bin(invoker: I, bin: impl Into<String>) -> Self;        // model = None
    pub fn with_model(invoker: I, bin: impl Into<String>, model: impl Into<String>) -> Self;
    /// The learned opencode session_id for a context, if any (observability + live-test assertions).
    pub fn session_of(&self, context_id: &str) -> Option<String>;
}
#[async_trait::async_trait]
impl<I: CliInvoker> wasl_adapter::AgentAdapter for OpencodeAdapter<I> {
    // deliver:
    //   prompt = compose_prompt(d);
    //   turn   = match ledger.get(context_id) { Some(sid) => Continuation(sid), None => First };
    //   attempt invoker.invoke(build_argv(bin, model, turn, &prompt), "");   // stdin is EMPTY
    //   MATCHED retry (ONE retry, ONE direction only):
    //     Continuation + invoker error whose message contains "Session not found"
    //                                      -> retry once as First (fresh session);
    //     Continuation + any OTHER invoker error -> propagate (environmental, not a stale session);
    //     First + invoker error            -> propagate (nothing to fall back to: opencode mints ids).
    //   on invoker success: reply = parse_reply(stdout)?; ledger.insert(context_id, reply.session_id);
    //   return Ok(reply.text). Parse errors (incl. type=="error" streams) propagate WITHOUT retry and
    //   WITHOUT touching the ledger.
    async fn deliver(&self, d: &Delivery) -> Result<String, AdapterError>;
}
```

**Error mapping, and why the retry is MATCHED (like E3, unlike W7/W8′):** the resume-error contract is
VERIFIED — resuming a missing/malformed session prints `Error: Session not found` to stderr with exit 1
(byte-identical for a malformed id and a well-formed-nonexistent id). So a `Continuation` invoker error
carrying `Session not found` is a *known* stale-session condition and the honest response is one fresh-
`First` retry (conversation context is lost but the delivery succeeds — the same graceful degradation the
ledger already accepts on restart). A `Continuation` error WITHOUT that substring is environmental (binary
missing, gateway down) and is propagated unchanged — retrying it as a fresh session would waste a spawn and
a model call in an already-failing path. The retry never loops: a `First` attempt (original or retry) that
fails propagates. On retry success the ledger entry is overwritten; the old session stays orphaned, which
is opencode's normal steady state. The substring match is on the invoker's error *message*, which
`ProcessCliInvoker` builds as `"{prog} {status}: {stderr}"` — so the stderr `Error: Session not found` is
present in that message (ANSI colour codes in the stderr are tolerated: the match is a `contains`, and
`Session not found` is emitted uncolored between the coloured `Error:` prefix and the newline).

## Non-termination guard (the pathological-loop case)

A clean successful turn self-terminates (exit 0 after `step_finish reason=="stop"`), so `wait_with_output`
suffices for the happy path — no watchdog needed there. HOWEVER, a pathological error class (verified: a
context-overflow with autocompact) makes `opencode run` loop indefinitely, never exiting. In W9 this is
handled at the RECEIVE/deployment layer, NOT inside `ProcessCliInvoker`:
- The node's A2A receive path already bounds task execution; a hung child is a hung task, surfaced as a
  failed/timed-out task, not a silent hang of the adapter itself. Documenting this here keeps W9's blast
  radius to the adapter contract.
- The deployment recommendation (mirrored in the live-verify/live tests and node docs) is to run opencode
  with `OPENCODE_DISABLE_AUTOCOMPACT=1` and an adequately-large-context model, which turns the overflow
  loop into a single `type=="error"` event that `parse_reply` maps to a clean `AdapterError` and a normally-
  exiting process. Building an in-invoker JSONL-streaming watchdog (detect `step_finish reason=="stop"` or
  a repeated `error` and kill the child) is a real but SEPARATE enhancement (a future W-block); W9 does not
  add streaming parse to the invoker, keeping the `(argv, stdin) -> stdout` seam identical to the siblings.
  This is called out honestly in `AGENTS.md` under Testability honesty as an UNGUARDED edge.

## Approvals & sandbox posture (the RECEIVE-side safety story)

`opencode run` exposes `--auto` ("auto-approve permissions that are not explicitly denied (dangerous!)",
default false). W9's posture: the adapter does **not** pass `--auto`. In headless `run` mode the model turn
either completes without needing an interactive approval, or — if a tool requests one — opencode's
non-interactive behavior governs (its exact hang/deny behavior without `--auto` is UNVERIFIED in this
round; the overflow capture never reached a tool-approval point). The consequences, stated plainly:
- The receive side's risk control is opencode's own permission config (e.g. `permission`/`tools` keys in
  the opencode config, which can hard-deny `bash` etc.) plus the deployment's model/config isolation. Wasl's
  own gate (send-side, D-blocks) remains the human decision point; the opencode receive side is deliberately
  inert with respect to Wasl gating (mid-turn HITL is opencode's server/ACP territory, not the `run`-per-
  turn adapter's).
- W9 does NOT expose `--auto` or a sandbox tier via node-config. Adding an `adapter.auto`/permission knob is
  a future spec change, not a knob to smuggle in — especially since a hang-on-approval possibility is
  unverified and would be a deployment note, bounded by the receive path's task timeout.

## Node-config integration (W2 delta, in node-config.md terms)

The delta W9 prescribes (identical shape to W7/W8′):
- `AdapterKind` gains `Opencode`; `adapter.kind = "opencode"` (exact, case-sensitive, same second-pass
  validation; `"Opencode"` and the bin name → `UnknownAdapterKind`).
- `adapter.bin` keeps its meaning: absent → `"opencode"` default (`OpencodeAdapter::new(ProcessCliInvoker)`),
  present → `OpencodeAdapter::with_bin(ProcessCliInvoker, bin)`. No other keys in W9 (no model, no auto —
  `with_model` exists for tests/embedders, not for the config file).
- `NodeAdapter` gains a fourth exhaustive-match variant (kept in `wasl-mcp`, where `build_adapter` lives):
  ```rust
  pub enum NodeAdapter {
      Claude(..), Codex(..), Antigravity(..),
      Opencode(wasl_adapter_opencode::OpencodeAdapter<wasl_adapter_opencode::ProcessCliInvoker>),
  }
  impl AgentAdapter for NodeAdapter { /* match self { ... } => inner.deliver(d).await */ }
  pub fn build_adapter(cfg: &AdapterConfig) -> NodeAdapter;   // exhaustive match on AdapterKind
  ```
  An enum, not `Box<dyn AgentAdapter>`, because W2 wants "an exhaustive match on AdapterKind so adding a
  kind forces a decision" — a trait object would erase that compile-time forcing function.

## Test plan
### `tests/pure.rs` (genuine 100% of the pure core)
Fixtures are the CAPTURED JSONL — byte-for-byte from the 2026-07-20 opencode 1.18.4 runs, not invented.
Happy first turn (`turn1b.out`, session `ses_07ed5b65cffePjAU3JRLYTeADV`, reply `PONG`):
```
{"type":"step_start","timestamp":1784578462654,"sessionID":"ses_07ed5b65cffePjAU3JRLYTeADV","part":{"id":"prt_f812a5364001zQ8vo13Yuy1k6w","messageID":"msg_f812a4d44001pqw91yKuNKhijv","sessionID":"ses_07ed5b65cffePjAU3JRLYTeADV","type":"step-start"}}
{"type":"text","timestamp":1784578462655,"sessionID":"ses_07ed5b65cffePjAU3JRLYTeADV","part":{"id":"prt_f812a536e001tTpqTtarThtbN4","messageID":"msg_f812a4d44001pqw91yKuNKhijv","sessionID":"ses_07ed5b65cffePjAU3JRLYTeADV","type":"text","text":"PONG","time":{"start":1784578462574,"end":1784578462586}}}
{"type":"step_finish","timestamp":1784578462656,"sessionID":"ses_07ed5b65cffePjAU3JRLYTeADV","part":{"id":"prt_f812a538c001nDzxxVVSwioNQy","reason":"stop","messageID":"msg_f812a4d44001pqw91yKuNKhijv","sessionID":"ses_07ed5b65cffePjAU3JRLYTeADV","type":"step-finish","tokens":{"total":8820,"input":3,"output":6,"reasoning":0,"cache":{"write":8811,"read":0}},"cost":0}}
```
In-stream error fixture (from the overflow run, `ContextOverflowError`):
```
{"type":"error","timestamp":1784576908836,"sessionID":"ses_07eed7be0ffeyspV4SRn90vWfl","error":{"name":"ContextOverflowError","data":{"message":"This model's maximum context length is 32000 tokens..."}}}
```
- `compose_prompt`: one test per `DeliveryKind` (exact header+body); non-empty `data` → exact
  "Attached data:" block; empty `data` → no block. (Same assertions/output as the sibling adapters.)
- `build_argv`: exact vec for `First` (contains `run`, `--format json`, NO `-s`, prompt LAST) and
  `Continuation("ses_…ADV")` (contains `-s ses_…ADV`, prompt LAST); with `model: Some("amd-anthropic/Claude-Haiku-4.5")`
  → `-m amd-anthropic/Claude-Haiku-4.5` present in BOTH; `model: None` → absent from both. Assert the
  prompt is the final element in every case.
- `parse_reply` happy path: the 3-line fixture → `Ok(OpencodeReply{session_id: "ses_…ADV", text: "PONG"})`.
- tolerance: fixture plus blank lines and an unknown event (`{"type":"tool","sessionID":"ses_…ADV",...}`)
  → same Ok; multi-part assembly: two `text` parts sharing one messageID (out of `time.start` order in the
  stream) → concatenated in `time.start` order; two DIFFERENT messageIDs → the LAST message's text wins.
- errors: a `type=="error"` line → `Err` containing the error name/message (from the ContextOverflow
  fixture); stream with no `sessionID` → `Err("… no sessionID")`; stream with a sessionID but no `text`
  part → `Err("… no assistant text")`; a non-JSON line (`not json`) → `Err("parse opencode output line:
  …")`; empty stdout → the no-sessionID error.

### `tests/adapter.rs` (deliver wiring via a FAKE CliInvoker — the ledger learn/reuse flow)
Fake `CliInvoker` records every `(argv, stdin)` and returns scriptable per-call results. Let `OK(sid)` mean
a happy fixture stream whose events carry sessionID `sid`.
- **learn:** first deliver for ctx → fake saw `build_argv("opencode", None, Turn::First, compose_prompt(d))`
  and `stdin == ""` (opencode's prompt is in argv); returns `OK("s1")` → reply text parsed; `session_of(ctx)
  == Some("s1")`.
- **reuse:** second deliver, SAME ctx → fake sees `Turn::Continuation("s1")` argv (asserts the LEARNED id
  is spliced in after `-s`, not derived).
- **per-context:** deliver for a DIFFERENT ctx → `First` again; each ctx tracks its own session.
- **matched retry (Session not found):** ctx has `s1`; fake errs on the Continuation call with a message
  CONTAINING `Session not found`, then returns `OK("s2")` for the First retry → Ok; exactly 2 invocations;
  `session_of(ctx) == Some("s2")`; a third deliver resumes `s2`.
- **NON-matching Continuation error propagates (no retry):** ctx has `s1`; fake errs on the Continuation
  call with a message WITHOUT the substring (e.g. `spawn opencode: No such file`) → that error propagates
  after EXACTLY 1 invocation; ledger still `s1`.
- **no double-retry:** Continuation errs with `Session not found`, First retry also errs → the RETRY's
  error propagates; exactly 2 invocations; ledger still `s1`.
- **First error propagates:** fresh ctx, fake errs → error propagates after exactly 1 invocation;
  `session_of(ctx) == None`; next deliver is `First` again.
- **parse failure ≠ retry:** fresh ctx, fake returns a `type=="error"` stream (invoker Ok) → parse error
  propagates, exactly 1 invocation, ledger untouched.
- **`with_bin("oc", …)`** → argv[0] == `"oc"`; **`with_model(…, "amd-anthropic/Claude-Haiku-4.5")`** → argv
  contains `-m amd-anthropic/Claude-Haiku-4.5` on both First and Continuation calls.
- malformed stdout (invoker Ok, garbage) → parse `AdapterError` propagates.

### `tests/process_invoker.rs` (ProcessCliInvoker via stand-in commands — real child processes)
The five plumbing tests plus the M2 EPIPE regression:
`cat` echoes stdin (with a non-empty stdin, proving the pipe still works); `sh -c 'printf %s abc'` (IGNORES
stdin — proves the M2 fix: `Ok("abc")`, not a BrokenPipe error); `sh -c 'echo oops 1>&2; exit 4'` → Err
containing `oops`; nonexistent program → Err containing `spawn`; empty argv → `Err("empty argv")`. Because
opencode is fed `stdin == ""`, also assert an empty-stdin call to a stdin-ignoring `printf` stub returns Ok
(the exact opencode call shape).

### `tests/live.rs` (REAL `opencode` — behind `--features live-cli`, excluded from default runs)
Gated with `#![cfg(feature = "live-cli")]` (a cargo feature, NOT `#[ignore]`). ONE test, exactly 2 model
calls, minimal tokens, the CHEAPEST large-context model to avoid the 32k-limit overflow trap: construct
`OpencodeAdapter::with_model(ProcessCliInvoker, "opencode", "amd-anthropic/Claude-Haiku-4.5")`. Fresh
context per run (`context_id` includes env `WASL_LIVE_NONCE`). The orchestrator runs this with an isolated
opencode config (no MCP catalog, `OPENCODE_DISABLE_AUTOCOMPACT=1`, big-context model) per the deployment
note, on a host giving opencode ≥~4 GB RSS:
1. deliver a Consult with body `Reply with exactly the word: PING_OK` → assert reply contains `PING_OK`;
   assert `session_of(ctx)` is `Some(sid)` and `sid` starts with `ses_` (the ledger learned opencode's id).
2. deliver in the SAME context: `What exact word did I ask for? Answer with just that word.` → assert reply
   contains `PING_OK` (proves `-s` resume continuity through the adapter) and `session_of(ctx)` still equals
   `sid` (byte-stable across resume, as verified in capture).
Runs only in explicit verification passes (`cargo test -p wasl-adapter-opencode --features live-cli`); the
orchestrator asserts it RAN there. Default CI never spends tokens and never needs ≥4 GB.

## Coverage bar
`cargo llvm-cov -p wasl-adapter-opencode --fail-under-lines 100` MUST exit 0 (genuine LINE 100%). REGION
may carry documented residuals ONLY on genuinely-unreachable IO error `?` branches in `ProcessCliInvoker`
(the fatal non-EPIPE stdin-write branch, `wait` failure) — enumerate them in `AGENTS.md`. The EPIPE branch
itself IS exercised (the `printf` stand-in stub). Do NOT fake coverage with dead code/panics. No
`#[ignore]`/`todo!`/tautology. Tests compile only against the public API + `wasl_adapter` public types.
