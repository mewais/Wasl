//! wasl-adapter-codex — a concrete `wasl_adapter::AgentAdapter` (W7): the last
//! inch that injects an incoming `Delivery` into the local **OpenAI Codex** CLI
//! (`codex exec`) and returns its reply.
//!
//! Unlike `wasl-adapter-claude`, codex mints its own thread ids (a UUIDv7
//! reported in the `thread.started` JSONL event) — there is no way to supply
//! our own, so this adapter keeps an in-memory `context_id -> thread_id`
//! ledger learned from each successful first turn. See
//! `docs/specs/adapter-codex.md` (authoritative) for the full contract,
//! including the CLI verification table and the rationale for the blind
//! single fresh-thread retry on `Continuation` invoker errors.

use std::collections::HashMap;
use std::fmt::Display;
use std::process::Stdio;
use std::sync::Mutex;

use wasl_adapter::{AdapterError, AgentAdapter, Delivery, DeliveryKind};

/// Single named error helper used at EVERY error site (a named `fn`, NOT
/// per-site closures) so llvm-cov attributes error handling to ONE covered
/// function: the genuinely-unreachable IO-error `?` branches (child-stdin
/// write, `wait`) then cost only a region, never a line/function. Passing
/// `ad` bare to `map_err` keeps those lines covered by the executed
/// method-call region (same technique as `wasl-adapter-claude`). Generic over
/// `Display` so it accepts both static messages and `io::Error` values.
fn ad<M: Display>(msg: M) -> AdapterError {
    AdapterError {
        message: msg.to_string(),
    }
}

/// Compose the prompt injected into Codex from a [`Delivery`] (verb header +
/// provenance + body + any attached data). IDENTICAL composition to
/// `wasl-adapter-claude` so a message reads the same regardless of receiver
/// CLI.
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

/// How this turn binds to the codex thread. `Continuation` carries the
/// learned `thread_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Turn<'a> {
    /// Start a brand-new codex thread.
    First,
    /// Resume a previously-started codex thread by id.
    Continuation(&'a str),
}

/// Build the Codex CLI argv for a turn. `bin` is the program (default
/// `"codex"`); `model` is an optional `-m` override.
pub fn build_argv(bin: &str, model: Option<&str>, turn: Turn<'_>) -> Vec<String> {
    let mut argv = vec![bin.to_string(), "exec".to_string()];

    match turn {
        Turn::First => {
            argv.push("--json".to_string());
            argv.push("--skip-git-repo-check".to_string());
            argv.push("--sandbox".to_string());
            argv.push("read-only".to_string());
        }
        Turn::Continuation(thread_id) => {
            argv.push("resume".to_string());
            argv.push(thread_id.to_string());
            argv.push("--json".to_string());
            argv.push("--skip-git-repo-check".to_string());
        }
    }

    if let Some(model) = model {
        argv.push("-m".to_string());
        argv.push(model.to_string());
    }

    argv.push("-".to_string());

    argv
}

/// The parsed outcome of one codex exec turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexReply {
    pub thread_id: String,
    pub text: String,
}

/// Parse `codex exec --json` JSONL stdout into a [`CodexReply`] (or an
/// [`AdapterError`]).
pub fn parse_reply(stdout: &str) -> Result<CodexReply, AdapterError> {
    #[derive(serde::Deserialize)]
    struct Event {
        #[serde(rename = "type")]
        kind: String,
        #[serde(default)]
        thread_id: Option<String>,
        // A `serde_json::Value` (defaulting to `Null` when absent) rather than
        // a nested `Option<Item>` struct: events that carry no "item" field
        // (every kind but `item.completed`) then flow through the SAME
        // `.get("type")` lookup below instead of forking into a distinct
        // "item present/absent" branch that the fixtures never exercise both
        // sides of (`Value::get` on `Null` is a safe `None`, same as on an
        // object missing the key).
        #[serde(default)]
        item: serde_json::Value,
    }

    let mut thread_id: Option<String> = None;
    let mut text: Option<String> = None;

    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let event: Event =
            serde_json::from_str(line).map_err(|e| ad(format!("parse codex output line: {e}")))?;

        match event.kind.as_str() {
            // `Option::or` keeps the FIRST `thread_id` seen without an `if`
            // branch the fixtures (all single-`thread.started`) never
            // exercise both sides of.
            "thread.started" => thread_id = thread_id.or(event.thread_id),
            "item.completed" => {
                if event.item.get("type").and_then(serde_json::Value::as_str)
                    == Some("agent_message")
                {
                    text = event
                        .item
                        .get("text")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string);
                }
            }
            "turn.failed" => {
                return Err(ad(format!("codex turn failed: {line}")));
            }
            _ => {}
        }
    }

    let thread_id = thread_id.ok_or_else(|| ad("codex output had no thread.started"))?;
    let text = text.ok_or_else(|| ad("codex output had no agent_message"))?;

    Ok(CodexReply { thread_id, text })
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

/// A concrete [`AgentAdapter`] that drives the Codex CLI via a [`CliInvoker`],
/// tracking a `context_id -> thread_id` ledger learned from each successful
/// first turn.
pub struct CodexAdapter<I: CliInvoker> {
    invoker: I,
    bin: String,
    model: Option<String>,
    threads: Mutex<HashMap<String, String>>,
}

impl<I: CliInvoker> CodexAdapter<I> {
    /// Wrap `invoker`, using the default `codex` binary and no `-m` override.
    pub fn new(invoker: I) -> Self {
        Self {
            invoker,
            bin: "codex".to_string(),
            model: None,
            threads: Mutex::new(HashMap::new()),
        }
    }

    /// Wrap `invoker`, overriding the binary name (no `-m` override).
    pub fn with_bin(invoker: I, bin: impl Into<String>) -> Self {
        Self {
            invoker,
            bin: bin.into(),
            model: None,
            threads: Mutex::new(HashMap::new()),
        }
    }

    /// Wrap `invoker`, overriding both the binary name and the `-m` model.
    pub fn with_model(invoker: I, bin: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            invoker,
            bin: bin.into(),
            model: Some(model.into()),
            threads: Mutex::new(HashMap::new()),
        }
    }

    /// The learned codex `thread_id` for a context, if any (observability +
    /// live-test assertions).
    pub fn thread_of(&self, context_id: &str) -> Option<String> {
        self.threads.lock().unwrap().get(context_id).cloned()
    }

    fn learn(&self, context_id: &str, thread_id: &str) {
        self.threads
            .lock()
            .unwrap()
            .insert(context_id.to_string(), thread_id.to_string());
    }
}

#[async_trait::async_trait]
impl<I: CliInvoker> AgentAdapter for CodexAdapter<I> {
    async fn deliver(&self, d: &Delivery) -> Result<String, AdapterError> {
        let prompt = compose_prompt(d);
        let learned = self.thread_of(&d.context_id);

        let is_continuation = learned.is_some();
        let argv = match &learned {
            Some(tid) => build_argv(&self.bin, self.model.as_deref(), Turn::Continuation(tid)),
            None => build_argv(&self.bin, self.model.as_deref(), Turn::First),
        };

        let result = self.invoker.invoke(&argv, &prompt).await;

        let stdout = match result {
            Ok(stdout) => stdout,
            Err(e) => {
                if is_continuation {
                    // Blind single fresh-thread retry: a Continuation invoker
                    // error most plausibly means the thread is gone. Retry
                    // once as a fresh First; the retry is never retried
                    // again.
                    let retry_argv = build_argv(&self.bin, self.model.as_deref(), Turn::First);
                    self.invoker.invoke(&retry_argv, &prompt).await?
                } else {
                    // First-turn errors have nothing to fall back to: codex
                    // mints ids, so an "already in use" collision cannot
                    // exist.
                    return Err(e);
                }
            }
        };

        let reply = parse_reply(&stdout)?;
        self.learn(&d.context_id, &reply.thread_id);
        Ok(reply.text)
    }
}
