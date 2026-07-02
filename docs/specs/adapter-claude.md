# E3 — `wasl-adapter-claude` — concrete Claude-CLI `AgentAdapter` (the last inch) — spec (authoritative)

**Tier:** L2 (IO shell over a tested pure core). **Deps:** `wasl-adapter` (the `AgentAdapter` trait +
`Delivery`/`DeliveryKind`/`AdapterError`), `serde`, `serde_json`, `async-trait`, `tokio` (process). Dev:
`tokio` (macros, rt), `serde_json`. No `unsafe`. This is ONE concrete adapter: it injects an incoming
`Delivery` into the local **Claude Code** CLI and returns its reply.

## ⚠️ TESTABILITY HONESTY — READ FIRST
There is NO `claude` binary in this environment, so the adapter's behavior against the REAL Claude CLI
CANNOT be verified here. What IS and ISN'T verified:

| Part | Verified here? |
|---|---|
| `compose_prompt` (Delivery → prompt text) | ✅ genuine 100% (pure) |
| `build_argv` (session id → CLI flags) | ✅ genuine 100% (pure) |
| `parse_reply` (CLI JSON stdout → reply / error) | ✅ genuine 100% (pure) |
| `ClaudeAdapter::deliver` wiring (compose→invoke→parse, error propagation) | ✅ via a FAKE `CliInvoker` |
| `ProcessCliInvoker` process plumbing (spawn, feed stdin, capture stdout, exit→Ok/Err) | ✅ via stand-in cmds (`cat`, `sh -c`) |
| **That the REAL `claude` CLI accepts these flags, reads the prompt on stdin, and emits this JSON** | ❌ **UNTESTED HERE (ASSUMPTION)** |
| **That `--session-id <ctx>` actually persists conversation context across messages** | ❌ **UNTESTED HERE (ASSUMPTION)** |

The Claude-CLI CONTRACT below is an ASSUMPTION encoded in ONE place (`build_argv` + `parse_reply` + the
stdin-prompt convention). It MUST be verified against a real `claude` binary before relying on it; if the
real flags/JSON differ, fix those two functions — the rest of the design is contract-agnostic.

### Assumed Claude CLI contract (to verify on a real host)
- Invocation: `claude -p --output-format json --session-id <context_id>` with the **prompt piped on stdin**
  (headless "print" mode; `-p` with piped stdin uses stdin as the prompt).
- `--session-id <context_id>` binds the turn to a stable session so the SAME `context_id` continues the SAME
  conversation (this is how Wasl's persistent-session requirement is met).
- stdout is a single JSON object: `{"type":"result","subtype":"success","is_error":false,"result":"<text>",
  "session_id":"...", ...}`. On failure `is_error` is `true` (and/or a non-success `subtype`); the human
  reply text is in `result`.

## Pure core (genuine 100% — unit-tested)
```rust
/// Compose the prompt injected into Claude from a Delivery (verb header + provenance + body + any data).
pub fn compose_prompt(d: &wasl_adapter::Delivery) -> String;
//   Consult -> "[Consult from {from}]\n\n{text}"; Command/Message analogous. If `d.data` is non-empty,
//   append "\n\nAttached data:" then one line per JSON value (`Value::to_string`).

/// Build the Claude CLI argv for a session. `bin` is the program (default "claude").
pub fn build_argv(bin: &str, session_id: &str) -> Vec<String>;
//   -> [bin, "-p", "--output-format", "json", "--session-id", session_id]

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
pub struct ClaudeAdapter<I: CliInvoker> { /* invoker: I, bin: String */ }
impl<I: CliInvoker> ClaudeAdapter<I> {
    pub fn new(invoker: I) -> Self;                       // bin = "claude"
    pub fn with_bin(invoker: I, bin: impl Into<String>) -> Self;
}
#[async_trait::async_trait]
impl<I: CliInvoker> wasl_adapter::AgentAdapter for ClaudeAdapter<I> {
    // deliver: compose_prompt(d) -> build_argv(bin, d.context_id) -> invoker.invoke(argv, prompt) ->
    //          parse_reply(stdout). Any invoker/parse error propagates as AdapterError.
    async fn deliver(&self, d: &Delivery) -> Result<String, AdapterError>;
}
```

## Test plan
### `tests/pure.rs` (genuine 100% of the pure core)
- `compose_prompt`: one test per `DeliveryKind` asserting the exact header+body; one with non-empty `data`
  asserting the "Attached data:" block with each JSON value line; one with empty data asserting NO data block.
- `build_argv`: asserts the exact vec for a given bin+session id; a second with the default via `ClaudeAdapter`
  is not needed here (bin default covered by the adapter tests).
- `parse_reply`: success JSON -> `Ok("<text>")`; `is_error:true` with `error` -> `Err(error)`; `is_error:true`
  with only `result` -> `Err(result)`; `is_error:true` with neither -> `Err("claude reported an error")`;
  success-shaped JSON missing `result` -> `Err("... no result field")`; malformed (`not json`) -> `Err(parse)`.

### `tests/adapter.rs` (deliver wiring via a FAKE CliInvoker)
Define a fake `CliInvoker` in the test that records the `(argv, stdin)` it was called with and returns a
scriptable `Result<String, AdapterError>`.
- happy: fake returns a success JSON -> `deliver` returns the parsed reply; assert the fake SAW
  `argv == build_argv("claude", ctx)` and `stdin == compose_prompt(d)` (proves the wiring + session binding).
- `with_bin("claude-cli", …)` -> fake sees argv[0] == "claude-cli".
- invoker error -> `deliver` returns that AdapterError (propagation).
- invoker returns malformed stdout -> `deliver` returns a parse AdapterError.

### `tests/process_invoker.rs` (ProcessCliInvoker via stand-in commands — real child processes)
- `invoke(["cat"], "hello")` -> `Ok(s)` where `s == "hello"` (stdin piped through to stdout).
- `invoke(["sh","-c","printf %s abc"], "")` -> `Ok("abc")`.
- `invoke(["sh","-c","echo oops 1>&2; exit 4"], "")` -> `Err(detail)` containing `"oops"`.
- `invoke(["this-program-does-not-exist-wasl-xyz"], "")` -> `Err(detail)` containing `"spawn"`.
- `invoke([], "")` -> `Err("empty argv")`.
(These prove the process plumbing; they do NOT prove anything about the real `claude` binary.)

## Coverage bar
`cargo llvm-cov -p wasl-adapter-claude --fail-under-lines 100` MUST exit 0 (genuine LINE 100%). REGION may
carry documented residuals ONLY on genuinely-unreachable IO error `?` branches in `ProcessCliInvoker` (e.g.
a write-to-child-stdin failure, a `wait` failure, the `if let Some(stdin)` no-op else that never occurs
because stdin is always piped). Enumerate them in `AGENTS.md`. Do NOT fake coverage with dead code/panics.
No `#[ignore]`/`todo!`/tautology. Tests compile only against the public API + `wasl_adapter` public types.
