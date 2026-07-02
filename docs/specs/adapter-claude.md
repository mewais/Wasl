# E3 — `wasl-adapter-claude` — concrete Claude-CLI `AgentAdapter` (the last inch) — spec (authoritative)

**Tier:** L2 (IO shell over a tested pure core). **Deps:** `wasl-adapter` (the `AgentAdapter` trait +
`Delivery`/`DeliveryKind`/`AdapterError`), `serde`, `serde_json`, `async-trait`, `tokio` (process). Dev:
`tokio` (macros, rt), `serde_json`. No `unsafe`. This is ONE concrete adapter: it injects an incoming
`Delivery` into the local **Claude Code** CLI and returns its reply.

## ⚠️ CONTRACT STATUS — VERIFIED AGAINST THE REAL CLI (claude 2.1.198, 2026-07-02)
The original assumed contract was tested on a machine with the real `claude` binary. Two assumptions
were WRONG and this spec is amended accordingly (v2):

| Original assumption | Real behavior (verified 2026-07-02) |
|---|---|
| `--session-id <context_id>` accepts any string | **REJECTED unless a valid UUID**: exit 1, stderr `Error: Invalid session ID. Must be a valid UUID.` |
| Re-running with the same `--session-id` continues the conversation | **FAILS**: exit 1, stderr `Error: Session ID <id> is already in use.` Continuation is **`--resume <uuid>`** (verified: context is preserved; returned `session_id` stays the same) |
| `-p` reads the prompt from stdin | ✅ CORRECT |
| stdout is one JSON object with `result` / `is_error` (+`error`) | ✅ CORRECT (`{"type":"result","subtype":"success","is_error":false,"result":"...","session_id":"...",...}`) |
| `--resume <uuid>` of a nonexistent session | exit 1, empty stdout, stderr `No conversation found with session ID: <uuid>` |

### Verified Claude CLI contract (v2 — the implementation target)
- **Session identity:** derive `session_uuid = Uuid::new_v5(&WASL_SESSION_NS, context_id.as_bytes())`
  where `pub const WASL_SESSION_NS: Uuid = uuid!("2441e851-1801-440d-a8e9-214d1df08e6e")` (fixed forever;
  deterministic ⇒ the same A2A `contextId` maps to the same claude session across adapter restarts).
- **First turn for a context:** `claude -p --output-format json --session-id <session_uuid>`, prompt on stdin.
- **Subsequent turns:** `claude -p --output-format json --resume <session_uuid>`, prompt on stdin.
- **Turn tracking:** the adapter keeps an in-memory set of context_ids it has started. Symmetric
  one-shot fallback (handles adapter restarts and externally deleted sessions):
  - not-in-set + invoker error containing `is already in use` → retry once with `--resume`, then mark started;
  - in-set + invoker error containing `No conversation found` → retry once with `--session-id`.
  Any other error propagates unchanged. On success, the context is marked started.
- **Reply parsing:** unchanged (`result`/`is_error`/`error`). CLI failures arrive as invoker errors
  (non-zero exit; stderr in the error detail), NOT as JSON.

## Pure core (genuine 100% — unit-tested)
```rust
/// Compose the prompt injected into Claude from a Delivery (verb header + provenance + body + any data).
pub fn compose_prompt(d: &wasl_adapter::Delivery) -> String;
//   Consult -> "[Consult from {from}]\n\n{text}"; Command/Message analogous. If `d.data` is non-empty,
//   append "\n\nAttached data:" then one line per JSON value (`Value::to_string`).

/// The fixed UUIDv5 namespace for deriving claude session ids from A2A context ids.
pub const WASL_SESSION_NS: uuid::Uuid; // = uuid!("2441e851-1801-440d-a8e9-214d1df08e6e")

/// Derive the deterministic claude session UUID for an A2A context id.
pub fn session_uuid(context_id: &str) -> uuid::Uuid; // Uuid::new_v5(&WASL_SESSION_NS, bytes)

/// How this turn binds to the claude session.
pub enum Turn { First, Continuation }

/// Build the Claude CLI argv for a session turn. `bin` is the program (default "claude").
pub fn build_argv(bin: &str, session_uuid: &uuid::Uuid, turn: Turn) -> Vec<String>;
//   First        -> [bin, "-p", "--output-format", "json", "--session-id", <uuid string>]
//   Continuation -> [bin, "-p", "--output-format", "json", "--resume",     <uuid string>]

/// Parse Claude's `--output-format json` stdout into the reply text (or an AdapterError).
pub fn parse_reply(stdout: &str) -> Result<String, wasl_adapter::AdapterError>;
//   parse (trimmed) as { result: Option<String>, is_error: bool(default false), error: Option<String> };
//   malformed JSON -> Err("parse claude output: {e}"); is_error==true -> Err(error|result|"claude reported
//   an error"); else result.ok_or(Err("claude output had no result field")).
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
`ProcessCliInvoker::invoke`: `split_first` (empty argv -> Err "empty argv"); spawn with piped
stdin/stdout/stderr (spawn error -> Err "spawn {prog}: {e}"); if `child.stdin.take()` is `Some`, write all
of `stdin` then drop the handle (EOF); `wait_with_output().await`; success -> `Ok(stdout as lossy UTF-8)`;
non-success -> `Err("{prog} {status}: {stderr}")`. Use a single named error helper `fn ad(msg) ->
AdapterError` at every `map_err` site (NOT per-site closures) so the unreachable IO-error `?` branches cost
regions, never lines/functions (same technique as `wasl-filesink`).

## The adapter
```rust
/// A concrete AgentAdapter that drives the Claude Code CLI via a CliInvoker.
pub struct ClaudeAdapter<I: CliInvoker> { /* invoker: I, bin: String, started: Mutex<HashSet<String>> */ }
impl<I: CliInvoker> ClaudeAdapter<I> {
    pub fn new(invoker: I) -> Self;                       // bin = "claude"
    pub fn with_bin(invoker: I, bin: impl Into<String>) -> Self;
}
#[async_trait::async_trait]
impl<I: CliInvoker> wasl_adapter::AgentAdapter for ClaudeAdapter<I> {
    // deliver:
    //   prompt = compose_prompt(d); su = session_uuid(d.context_id);
    //   turn   = if started.contains(context_id) { Continuation } else { First };
    //   attempt invoker.invoke(build_argv(bin, &su, turn), prompt);
    //   fallback (one retry, symmetric):
    //     First error containing "is already in use"     -> retry as Continuation;
    //     Continuation error containing "No conversation found" -> retry as First;
    //   on success: started.insert(context_id); parse_reply(stdout).
    //   Any other invoker/parse error propagates as AdapterError.
    async fn deliver(&self, d: &Delivery) -> Result<String, AdapterError>;
}
```

## Test plan
### `tests/pure.rs` (genuine 100% of the pure core)
- `compose_prompt`: one test per `DeliveryKind` asserting the exact header+body; one with non-empty `data`
  asserting the "Attached data:" block with each JSON value line; one with empty data asserting NO data block.
- `session_uuid`: deterministic (same context_id twice → same UUID); distinct context_ids → distinct UUIDs;
  the derived value for a pinned context_id matches a hard-coded expected UUID (guards the namespace constant).
- `build_argv`: asserts the exact vec for `Turn::First` (`--session-id`) and `Turn::Continuation`
  (`--resume`) for a given bin + uuid.
- `parse_reply`: success JSON -> `Ok("<text>")`; `is_error:true` with `error` -> `Err(error)`; `is_error:true`
  with only `result` -> `Err(result)`; `is_error:true` with neither -> `Err("claude reported an error")`;
  success-shaped JSON missing `result` -> `Err("... no result field")`; malformed (`not json`) -> `Err(parse)`.

### `tests/adapter.rs` (deliver wiring via a FAKE CliInvoker)
Define a fake `CliInvoker` in the test that records every `(argv, stdin)` it was called with and returns
scriptable per-call `Result<String, AdapterError>`s.
- happy first turn: fake returns success JSON -> `deliver` returns the parsed reply; assert the fake SAW
  `argv == build_argv("claude", &session_uuid(ctx), Turn::First)` and `stdin == compose_prompt(d)`.
- second `deliver` for the SAME context -> fake sees `Turn::Continuation` argv (`--resume`).
- `deliver` for a DIFFERENT context -> `Turn::First` again (per-context tracking, not global).
- fallback A: first call errors with detail containing `is already in use` -> adapter retries once with
  `--resume` argv; success JSON on retry -> Ok; context marked started (third call uses `--resume` directly).
- fallback B: context already started, call errors with `No conversation found` -> retries once with
  `--session-id` argv; success -> Ok.
- no double-retry: fallback attempt also errors -> that error propagates (exactly 2 invocations).
- unrelated invoker error (e.g. `spawn failed`) -> propagates immediately (exactly 1 invocation).
- `with_bin("claude-cli", …)` -> fake sees argv[0] == "claude-cli".
- invoker returns malformed stdout -> `deliver` returns a parse AdapterError.

### `tests/process_invoker.rs` (ProcessCliInvoker via stand-in commands — real child processes)
- `invoke(["cat"], "hello")` -> `Ok(s)` where `s == "hello"` (stdin piped through to stdout).
- `invoke(["sh","-c","printf %s abc"], "")` -> `Ok("abc")`.
- `invoke(["sh","-c","echo oops 1>&2; exit 4"], "")` -> `Err(detail)` containing `"oops"`.
- `invoke(["this-program-does-not-exist-wasl-xyz"], "")` -> `Err(detail)` containing `"spawn"`.
- `invoke([], "")` -> `Err("empty argv")`.
(These prove the process plumbing; they do NOT prove anything about the real `claude` binary.)

### `tests/live.rs` (REAL `claude` — behind `--features live-cli`, excluded from default runs)
Gated with `#![cfg(feature = "live-cli")]` (a cargo feature, NOT `#[ignore]`). One test, minimal tokens:
- via `ProcessCliInvoker` + `ClaudeAdapter`, deliver a Consult for a FRESH context (context_id includes a
  caller-supplied nonce from env `WASL_LIVE_NONCE` so reruns don't collide) with body
  `Reply with exactly the word: PING_OK` -> assert reply contains `PING_OK`;
- deliver a second message in the SAME context asking `What exact word did I ask for? Answer with just
  that word.` -> assert reply contains `PING_OK` (proves --resume continuity through the adapter).
This suite runs only in explicit verification passes (`cargo test -p wasl-adapter-claude --features
live-cli`); the orchestrator asserts it RAN (not skipped) there. Default CI never spends tokens.

## Coverage bar
`cargo llvm-cov -p wasl-adapter-claude --fail-under-lines 100` MUST exit 0 (genuine LINE 100%). REGION may
carry documented residuals ONLY on genuinely-unreachable IO error `?` branches in `ProcessCliInvoker` (e.g.
a write-to-child-stdin failure, a `wait` failure, the `if let Some(stdin)` no-op else that never occurs
because stdin is always piped). Enumerate them in `AGENTS.md`. Do NOT fake coverage with dead code/panics.
No `#[ignore]`/`todo!`/tautology. Tests compile only against the public API + `wasl_adapter` public types.
