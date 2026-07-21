//! wasl-adapter-antigravity — a concrete `wasl_adapter::AgentAdapter` (W8′):
//! the last inch that injects an incoming `Delivery` into the local
//! **Antigravity** CLI (`agy -p`, print mode) and returns its reply.
//!
//! Unlike `wasl-adapter-claude`, agy mints its own `conversation_id` (a UUID
//! reported in-band in the print-mode JSON result) — there is no way to
//! supply our own, so this adapter keeps an in-memory
//! `context_id -> conversation_id` ledger learned from each fully-successful
//! turn. See `docs/specs/adapter-antigravity.md` (authoritative) for the full
//! contract, including the CONTRACT STATUS table (agy 1.0.15,
//! RESEARCHED-NOT-LIVE-VERIFIED per row), THE PERMISSION QUESTION, and the
//! rationale for the blind single fresh-conversation retry on `Continuation`
//! invoker errors.

use std::collections::HashMap;
use std::fmt::Display;
use std::process::Stdio;
use std::sync::Mutex;

use wasl_adapter::{AdapterError, AgentAdapter, Delivery, DeliveryKind};

/// The pinned default model — the cheapest entry in `agy models` (free-tier
/// rule; see the spec's "Model selection").
pub const DEFAULT_MODEL: &str = "Gemini 3.5 Flash (Low)";

/// Single named error helper used at EVERY error site (a named `fn`, NOT
/// per-site closures) so llvm-cov attributes error handling to ONE covered
/// function: the genuinely-unreachable IO-error `?` branches (child-stdin
/// write, `wait`) then cost only a region, never a line/function. Passing
/// `ad` bare to `map_err` keeps those lines covered by the executed
/// method-call region (same technique as `wasl-adapter-codex`). Generic over
/// `Display` so it accepts both static messages and `io::Error` values.
fn ad<M: Display>(msg: M) -> AdapterError {
    AdapterError {
        message: msg.to_string(),
    }
}

/// Compose the prompt injected into agy from a [`Delivery`] (verb header +
/// provenance + body + any attached data). IDENTICAL composition to
/// `wasl-adapter-claude`/`wasl-adapter-codex` so a message reads the same
/// regardless of receiver CLI.
pub fn compose_prompt(d: &Delivery) -> String {
    let header = match d.kind {
        DeliveryKind::Consult => format!("[Consult from {}]", d.from),
        DeliveryKind::Command => format!("[Command from {}]", d.from),
        DeliveryKind::Message => format!("[Message from {}]", d.from),
    };

    let mut out = format!("{header}\n\n{}", d.text);

    if !d.data.is_empty() {
        out.push_str("\n\nAttached data:");
        for value in &d.data {
            out.push('\n');
            out.push_str(&value.to_string());
        }
    }

    out
}

/// How this turn binds to the agy conversation. `Continuation` carries the
/// learned `conversation_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Turn<'a> {
    /// Start a brand-new agy conversation.
    First,
    /// Resume a previously-started agy conversation by id.
    Continuation(&'a str),
}

/// Build the agy CLI argv for a turn. `bin` is the program (default `"agy"`);
/// `model` is ALWAYS passed (never optional — see the spec's "Model
/// selection"); `prompt` is the composed prompt, emitted as `-p`'s FLAG VALUE
/// in the strictly-LAST TWO argv slots.
///
/// CORRECTED 2026-07-03 (W8′-fix, orchestrator live verification — see
/// `docs/specs/adapter-antigravity.md`'s "W8′-fix correction" section): the
/// original spec had this wrong in two ways, both now fixed:
///   1. `--output-format json` is a HIDDEN flag (absent from `--help`, present
///      in binary strings) REQUIRED for JSON output — plain `-p <prompt>`
///      alone was VERIFIED live to emit plain TEXT, not JSON.
///   2. `-p`/`--print` takes the prompt as the SINGLE argv token immediately
///      following it (ordinary Go `flag.String` semantics), not as a free
///      trailing positional. Placing `-p` before other flags was VERIFIED
///      live to be a bug: `agy -p --model X ...` handed the agent the literal
///      string `"--model X ..."` as its prompt. `-p` and the prompt must
///      therefore be the strictly-last two argv elements, after every other
///      flag.
pub fn build_argv(bin: &str, model: &str, turn: Turn<'_>, prompt: &str) -> Vec<String> {
    let mut argv = vec![
        bin.to_string(),
        "--output-format".to_string(),
        "json".to_string(),
    ];

    if let Turn::Continuation(cid) = turn {
        argv.push("--conversation".to_string());
        argv.push(cid.to_string());
    }

    argv.push("--model".to_string());
    argv.push(model.to_string());
    argv.push("--sandbox".to_string());
    argv.push("--dangerously-skip-permissions".to_string());
    argv.push("-p".to_string());
    argv.push(prompt.to_string());

    argv
}

/// The parsed outcome of one agy print-mode turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgyReply {
    pub conversation_id: String,
    pub text: String,
}

/// Parse `agy --output-format json -p <prompt>` stdout (one JSON result
/// line) into an [`AgyReply`] (or an [`AdapterError`]).
///
/// Line selection: take the LAST non-empty line that parses as a JSON
/// object (tolerates any stray non-JSON banner lines). No such line ->
/// `Err("agy output had no JSON result line")`.
///
/// Success/error mapping — CORRECTED 2026-07-03 (W8′-fix, orchestrator live
/// verification — see `docs/specs/adapter-antigravity.md`'s "W8′-fix
/// correction" section). This REPLACES the original status/error-keyed
/// defensive mapping, which live evidence proved wrong: a reply carrying a
/// non-empty, on-topic `response` was observed alongside `status:"ERROR"`
/// plus a populated `error` (the model narrating a denied tool call inside an
/// otherwise-useful reply — "tool-denial decoration", not a response-level
/// failure). `status`/`error` are therefore NEVER consulted to decide
/// success/failure:
///
/// - success iff `conversation_id` is present and a non-empty string, AND
///   `response` is present and a non-empty string.
/// - otherwise -> `Err("agy turn failed: {raw line}")` for every other shape
///   (missing/empty `conversation_id`, missing/non-string/empty `response`,
///   or both). The raw line already contains `status`/`error` verbatim, so
///   they decorate the error message without gating the branch; there is no
///   longer a distinct "no conversation_id"/"no response" message.
/// - `usage`/`num_turns`/`duration_seconds` are ignored.
pub fn parse_reply(stdout: &str) -> Result<AgyReply, AdapterError> {
    let mut last: Option<(&str, serde_json::Map<String, serde_json::Value>)> = None;

    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if let Ok(serde_json::Value::Object(obj)) = serde_json::from_str::<serde_json::Value>(line)
        {
            last = Some((line, obj));
        }
    }

    let (raw, obj) = last.ok_or_else(|| ad("agy output had no JSON result line"))?;

    let conversation_id = obj
        .get("conversation_id")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty());

    let text = obj
        .get("response")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty());

    match (conversation_id, text) {
        (Some(conversation_id), Some(text)) => Ok(AgyReply {
            conversation_id: conversation_id.to_string(),
            text: text.to_string(),
        }),
        _ => Err(ad(format!("agy turn failed: {raw}"))),
    }
}

/// The invoker seam: run a CLI, feeding stdin and returning captured stdout.
#[async_trait::async_trait]
pub trait CliInvoker: Send + Sync {
    /// Run `argv` (argv[0]=program), feeding `stdin`, returning captured
    /// stdout (or an error).
    async fn invoke(&self, argv: &[String], stdin: &str) -> Result<String, AdapterError>;
}

/// The REAL invoker: spawns the process via tokio, pipes stdin, captures
/// stdout, maps exit status.
#[derive(Debug, Default, Clone)]
pub struct ProcessCliInvoker;

#[async_trait::async_trait]
impl CliInvoker for ProcessCliInvoker {
    async fn invoke(&self, argv: &[String], stdin: &str) -> Result<String, AdapterError> {
        let (prog, rest) = argv.split_first().ok_or_else(|| ad("empty argv"))?;

        let mut child = tokio::process::Command::new(prog)
            .args(rest)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| ad(format!("spawn {prog}: {e}")))?;

        if let Some(mut si) = child.stdin.take() {
            use tokio::io::AsyncWriteExt;
            match si.write_all(stdin.as_bytes()).await {
                Ok(()) => {}
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::WriteZero
                    ) => {}
                Err(e) => return Err(ad(e)),
            }
        }

        let output = child.wait_with_output().await.map_err(ad)?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        } else {
            Err(ad(format!(
                "{prog} {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            )))
        }
    }
}

/// A concrete [`AgentAdapter`] that drives the Antigravity CLI via a
/// [`CliInvoker`], tracking a `context_id -> conversation_id` ledger learned
/// from each fully-successful turn.
pub struct AntigravityAdapter<I: CliInvoker> {
    invoker: I,
    bin: String,
    model: String,
    conversations: Mutex<HashMap<String, String>>,
}

impl<I: CliInvoker> AntigravityAdapter<I> {
    /// Wrap `invoker`, using the default `agy` binary and [`DEFAULT_MODEL`].
    pub fn new(invoker: I) -> Self {
        Self {
            invoker,
            bin: "agy".to_string(),
            model: DEFAULT_MODEL.to_string(),
            conversations: Mutex::new(HashMap::new()),
        }
    }

    /// Wrap `invoker`, overriding the binary name ([`DEFAULT_MODEL`] stands).
    pub fn with_bin(invoker: I, bin: impl Into<String>) -> Self {
        Self {
            invoker,
            bin: bin.into(),
            model: DEFAULT_MODEL.to_string(),
            conversations: Mutex::new(HashMap::new()),
        }
    }

    /// Wrap `invoker`, overriding both the binary name and the `--model`
    /// value.
    pub fn with_model(invoker: I, bin: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            invoker,
            bin: bin.into(),
            model: model.into(),
            conversations: Mutex::new(HashMap::new()),
        }
    }

    /// The learned agy `conversation_id` for a context, if any (observability
    /// + live-test assertions).
    pub fn conversation_of(&self, context_id: &str) -> Option<String> {
        self.conversations.lock().unwrap().get(context_id).cloned()
    }

    fn learn(&self, context_id: &str, conversation_id: &str) {
        self.conversations
            .lock()
            .unwrap()
            .insert(context_id.to_string(), conversation_id.to_string());
    }
}

#[async_trait::async_trait]
impl<I: CliInvoker> AgentAdapter for AntigravityAdapter<I> {
    async fn deliver(&self, d: &Delivery) -> Result<String, AdapterError> {
        let prompt = compose_prompt(d);
        let learned = self.conversation_of(&d.context_id);

        let is_continuation = learned.is_some();
        let argv = match &learned {
            Some(cid) => build_argv(&self.bin, &self.model, Turn::Continuation(cid), &prompt),
            None => build_argv(&self.bin, &self.model, Turn::First, &prompt),
        };

        let result = self.invoker.invoke(&argv, "").await;

        let stdout = match result {
            Ok(stdout) => stdout,
            Err(e) => {
                if is_continuation {
                    // Blind single fresh-conversation retry: a Continuation
                    // invoker error most plausibly means the conversation is
                    // gone or the resume argv was rejected. Retry once as a
                    // fresh First; the retry is never retried again.
                    let retry_argv = build_argv(&self.bin, &self.model, Turn::First, &prompt);
                    self.invoker.invoke(&retry_argv, "").await?
                } else {
                    // First-turn errors have nothing to fall back to: agy
                    // mints ids, so an "already in use" collision cannot
                    // exist.
                    return Err(e);
                }
            }
        };

        let reply = parse_reply(&stdout)?;
        self.learn(&d.context_id, &reply.conversation_id);
        Ok(reply.text)
    }
}
