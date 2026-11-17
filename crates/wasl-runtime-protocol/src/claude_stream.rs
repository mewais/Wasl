//! `ClaudeStreamProtocol` — thin adapter over `wasit_claude::ClaudeDriver`.
//!
//! This module is the R2 driver for the real `claude` CLI stream-json protocol.
//! All wire parsing is now delegated to `wasit-claude`; this file translates:
//! - Wasl's `SpawnContext`/`SpawnOptions` → `wasit_core::SpawnSpec`
//! - `wasit_core::AgentEvent` → `wasl_runtime::ProtocolEvent`
//! - Gate answers (approve/deny) through `wasit_core::Session::answer_gate`
//!
//! ## Gate answer routing
//!
//! Wasl's actor resolves native gates by calling `send_turn(ComposedPrompt(…))`
//! while the transport is `awaiting_gate`. The prompt text is either:
//! - `"approve"` → `GateDecision::Approve { … }`
//! - `"deny: <reason>"` → `GateDecision::Deny { reason: Some(<reason>) }`
//!
//! The transport tracks the most recent pending `GateRequest` so it can route
//! the answer to the right `request_id`.

use std::path::PathBuf;

use async_trait::async_trait;
use uuid::Uuid;
use wasit_core::{AgentDriver, GateDecision, GateRequest, ResumeMode, SpawnSpec};
use wasl_runtime::{
    AgentMode, CompactionInfo, ComposedPrompt, InteractiveProtocol, InteractiveTransport,
    McpServerInfo, NativeGateDetail, NativeSessionId, PlanEntry, ProtocolEvent,
    RuntimeCapabilities, RuntimeError, SessionCapabilities, SlashCommandInfo, SpawnOptions,
    UsageSnapshot,
};

/// Identical UUIDv5 namespace to `wasl-adapter-claude::WASL_SESSION_NS`,
/// declared independently here to keep R2 decoupled from adapter crates.
const WASL_SESSION_NS: Uuid = uuid::uuid!("2441e851-1801-440d-a8e9-214d1df08e6e");

fn session_uuid(context_id: &str) -> Uuid {
    Uuid::new_v5(&WASL_SESSION_NS, context_id.as_bytes())
}

/// Translate Wasl `SpawnOptions` into a Wasit `SpawnSpec` for claude.
fn context_to_spec(opts: &SpawnOptions, session_id: &str, resume: bool) -> SpawnSpec {
    let ctx = opts.context();
    SpawnSpec {
        cwd: ctx.cwd.clone(),
        model: ctx.model.clone(),
        mode: ctx.posture.clone().unwrap_or_default(),
        bin: ctx.bin.clone(),
        env: ctx
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        setup: ctx.setup.clone(),
        disallowed_tools: ctx.disallowed_tools.clone(),
        mcp_config: ctx.mcp_config.clone(),
        resume: if resume {
            ResumeMode::Resume(session_id.to_string())
        } else {
            ResumeMode::Fresh
        },
        first_prompt: None,
    }
}

pub struct ClaudeStreamProtocol {
    bin: PathBuf,
}

impl ClaudeStreamProtocol {
    pub fn new(bin: impl Into<PathBuf>) -> Self {
        ClaudeStreamProtocol { bin: bin.into() }
    }

    fn caps() -> RuntimeCapabilities {
        RuntimeCapabilities {
            persistent_process: true,
            persistent_conversation: true,
            structured_turns: true,
            streaming_output: false,
            attachable: true,
            interruptible: false,
            structured_provenance: true,
            resumable: true,
        }
    }

    async fn make_transport(
        &self,
        session_id: &str,
        opts: &SpawnOptions,
        resume: bool,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        let bin_str = self.bin.to_str().unwrap_or("claude").to_string();
        let spec = context_to_spec(opts, session_id, resume);
        let driver = wasit_claude::ClaudeDriver::new(bin_str);
        let session = driver
            .spawn(spec)
            .await
            .map_err(|e| RuntimeError::SpawnFailed {
                detail: format!("wasit-claude spawn: {e}"),
            })?;
        Ok(Box::new(ClaudeStreamTransport {
            session,
            pending_gate: None,
            session_ready_seen: false,
            synthetic_ready_emitted: false,
            pending_capabilities: None,
        }))
    }
}

#[async_trait]
impl InteractiveProtocol for ClaudeStreamProtocol {
    fn capabilities(&self) -> RuntimeCapabilities {
        Self::caps()
    }

    async fn spawn(
        &self,
        opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        let target_key = opts.target_key().ok_or_else(|| RuntimeError::SpawnFailed {
            detail: "ClaudeStreamProtocol::spawn requires SpawnOptions::target_key".to_string(),
        })?;
        let native = session_uuid(&target_key.0).to_string();
        self.make_transport(&native, opts, false).await
    }

    async fn resume(
        &self,
        native: &NativeSessionId,
        opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        self.make_transport(&native.0, opts, true).await
    }
}

/// Thin transport adapter that wraps a `Box<dyn wasit_core::Session>`.
struct ClaudeStreamTransport {
    session: Box<dyn wasit_core::Session>,
    /// The most recently received `GateRequest`, stored so the next
    /// `send_turn("approve"/"deny: <reason>")` can route to `answer_gate`.
    pending_gate: Option<GateRequest>,
    /// `SessionReady` is emitted by claude's `system/init` on EVERY turn;
    /// the R1 actor only wants it once (first turn). Suppress repeats.
    session_ready_seen: bool,
    /// Whether the synthetic session-establishment `SessionReady` has been
    /// emitted. Claude — UNLIKE codex (`thread/started`) and opencode
    /// (`session/new`) — does NOT emit a proactive readiness event: its
    /// `system/init` (→ `SessionReady`) arrives only at the START of a turn. The
    /// managed actor waits for `SessionReady` before it dispatches a queued turn,
    /// so without a proactive one nothing is ever sent (deadlock). We emit one
    /// synthetic `SessionReady { native: None }` on the first poll so the actor
    /// goes Idle and dispatches the first turn (exactly as Marsad's own non-wasl
    /// path sends the prompt right after the initialize handshake); the turn's
    /// real `system/init` then flows through as a second `SessionReady` carrying
    /// claude's actual session id.
    synthetic_ready_emitted: bool,
    /// Feature B: the session-init capability catalog (tools / MCP servers /
    /// slash commands / modes / permission mode), captured from the FIRST real
    /// `system/init` and buffered here so the poll AFTER `SessionReady` returns it
    /// as a `ProtocolEvent::SessionCapabilities`. `next_event` yields one event
    /// per poll, so the caps ride the next poll rather than replacing the ready
    /// signal. `None` once emitted (or when the driver reports no init catalog).
    pending_capabilities: Option<ProtocolEvent>,
}

#[async_trait]
impl InteractiveTransport for ClaudeStreamTransport {
    async fn send_turn(&mut self, prompt: &ComposedPrompt) -> Result<(), RuntimeError> {
        // If there is a pending gate, interpret approve/deny routing.
        if let Some(ref gate) = self.pending_gate {
            let text = prompt.0.as_str();
            let decision = if text == "approve" {
                GateDecision::Approve {
                    updated_input: None,
                    permission_updates: None,
                    elicitation_content: None,
                }
            } else if let Some(reason) = text.strip_prefix("deny: ") {
                GateDecision::Deny {
                    reason: Some(reason.to_string()),
                }
            } else {
                // Plain deny with no reason.
                GateDecision::Deny { reason: None }
            };
            let request_id = gate_request_id(gate).to_string();
            self.pending_gate = None;
            self.session
                .answer_gate(&request_id, decision)
                .await
                .map_err(|e| RuntimeError::Transport {
                    detail: format!("claude answer_gate: {e}"),
                })?;
            return Ok(());
        }
        // Normal prompt.
        self.session
            .send_prompt(&prompt.0)
            .await
            .map_err(|e| RuntimeError::Transport {
                detail: format!("claude send_prompt: {e}"),
            })
    }

    async fn next_event(&mut self) -> Option<ProtocolEvent> {
        use wasit_core::AgentEvent;
        // Proactive session-establishment readiness (see `synthetic_ready_emitted`):
        // claude has no proactive ready event, so emit one synthetic
        // `SessionReady { native: None }` on the first poll to unblock the actor's
        // "wait for SessionReady before dispatching" invariant. The real
        // `system/init` follows as a second `SessionReady` with claude's actual
        // session id (recorded by the actor; captured by the control server).
        if !self.synthetic_ready_emitted {
            self.synthetic_ready_emitted = true;
            return Some(ProtocolEvent::SessionReady { native: None });
        }
        // Feature B: the poll right after the first real `system/init` returns the
        // buffered capability catalog (the init arm below emits SessionReady, then
        // stashes the caps here for this next poll — one event per poll).
        if let Some(caps) = self.pending_capabilities.take() {
            return Some(caps);
        }
        loop {
            let ev = self.session.next_event().await?;
            match ev {
                AgentEvent::SessionInit {
                    native_session_id,
                    tools,
                    mcp_servers,
                    slash_commands,
                    permission_mode,
                    available_modes,
                    current_mode,
                    ..
                } => {
                    if self.session_ready_seen {
                        // Claude emits system/init on every turn; suppress repeats
                        // (both SessionReady AND the caps catalog — it is static).
                        continue;
                    }
                    self.session_ready_seen = true;
                    // Buffer the capability catalog for the NEXT poll so this poll
                    // returns the (order-sensitive) SessionReady first. Honest:
                    // only what claude actually reported — codex/opencode leave
                    // these empty (they announce nothing at init), so no fabrication.
                    self.pending_capabilities = Some(ProtocolEvent::SessionCapabilities {
                        capabilities: SessionCapabilities {
                            tools,
                            mcp_servers: mcp_servers
                                .into_iter()
                                .map(|m| McpServerInfo {
                                    name: m.name,
                                    status: m.status,
                                })
                                .collect(),
                            slash_commands: slash_commands
                                .into_iter()
                                .map(|c| SlashCommandInfo {
                                    name: c.name,
                                    description: c.description,
                                    argument_hint: c.argument_hint,
                                })
                                .collect(),
                            available_modes: available_modes
                                .into_iter()
                                .map(|m| AgentMode {
                                    id: m.id,
                                    label: m.label,
                                    description: m.description,
                                })
                                .collect(),
                            current_mode,
                            permission_mode,
                        },
                    });
                    return Some(ProtocolEvent::SessionReady {
                        native: native_session_id.map(NativeSessionId),
                    });
                }
                AgentEvent::TurnStarted { native_turn_id } => {
                    return Some(ProtocolEvent::TurnAccepted {
                        native_turn: native_turn_id,
                    });
                }
                AgentEvent::AssistantDelta { text, .. } => {
                    return Some(ProtocolEvent::AssistantDelta { text });
                }
                AgentEvent::ToolCallStarted { name, input, .. } => {
                    return Some(ProtocolEvent::ToolStarted { name, input });
                }
                AgentEvent::ToolCallFinished {
                    name, ok, result, ..
                } => {
                    return Some(ProtocolEvent::ToolFinished { name, ok, result });
                }
                AgentEvent::GateRequested { gate } => {
                    let detail = gate_detail(&gate);
                    self.pending_gate = Some(gate);
                    return Some(ProtocolEvent::NativeGateRequested { detail });
                }
                AgentEvent::TurnCompleted {
                    end_reason, reply, ..
                } => {
                    use wasit_core::EndReason;
                    match end_reason {
                        None => {
                            return Some(ProtocolEvent::TurnCompleted {
                                reply: reply.unwrap_or_default(),
                            });
                        }
                        Some(EndReason::Cancelled) => {
                            return Some(ProtocolEvent::TurnFailed {
                                error: "turn cancelled".to_string(),
                            });
                        }
                        Some(EndReason::MaxTurns) => {
                            return Some(ProtocolEvent::TurnFailed {
                                error: "max turns reached".to_string(),
                            });
                        }
                        Some(EndReason::ExecutionError) => {
                            return Some(ProtocolEvent::TurnFailed {
                                error: reply.unwrap_or_else(|| "execution error".to_string()),
                            });
                        }
                        Some(EndReason::CompactionNeeded) => {
                            return Some(ProtocolEvent::TurnFailed {
                                error: "compaction needed".to_string(),
                            });
                        }
                    }
                }
                AgentEvent::ProcessExited { code } => {
                    return Some(ProtocolEvent::ProcessExited { code });
                }
                AgentEvent::ThinkingDelta { text, .. } => {
                    if !text.is_empty() {
                        return Some(ProtocolEvent::ThinkingDelta { text });
                    }
                    continue;
                }
                AgentEvent::UsageUpdate { usage, context_pct } => {
                    return Some(ProtocolEvent::UsageUpdate {
                        usage: UsageSnapshot {
                            input_tokens: usage.input_tokens,
                            output_tokens: usage.output_tokens,
                            cache_read_tokens: usage.cache_read_tokens,
                            cache_creation_tokens: usage.cache_creation_tokens,
                            cost_usd: usage.cost_usd,
                        },
                        context_pct: context_pct.map(|p| p.clamp(0.0, 100.0).round() as u8),
                    });
                }
                AgentEvent::PlanUpdated { entries } => {
                    return Some(ProtocolEvent::PlanUpdated {
                        entries: entries
                            .into_iter()
                            .map(|e| PlanEntry {
                                content: e.content,
                                priority: e.priority,
                            })
                            .collect(),
                    });
                }
                AgentEvent::ModesUpdated {
                    available_modes,
                    current_mode,
                } => {
                    return Some(ProtocolEvent::ModesUpdated {
                        available_modes: available_modes
                            .into_iter()
                            .map(|m| AgentMode {
                                id: m.id,
                                label: m.label,
                                description: m.description,
                            })
                            .collect(),
                        current_mode,
                    });
                }
                AgentEvent::ModelChanged { model } => {
                    return Some(ProtocolEvent::ModelChanged { model });
                }
                AgentEvent::SlashCommandsUpdated { commands } => {
                    return Some(ProtocolEvent::SlashCommandsUpdated {
                        commands: commands
                            .into_iter()
                            .map(|c| SlashCommandInfo {
                                name: c.name,
                                description: c.description,
                                argument_hint: c.argument_hint,
                            })
                            .collect(),
                    });
                }
                AgentEvent::Compaction { info } => {
                    return Some(ProtocolEvent::Compaction {
                        info: CompactionInfo {
                            trigger: info.trigger,
                            pre_tokens: info.pre_tokens,
                            post_tokens: info.post_tokens,
                            cumulative_dropped_tokens: info.cumulative_dropped_tokens,
                            duration_ms: info.duration_ms,
                        },
                    });
                }
                // Silently skip: ToolCallUpdated (intermediate only, no Wasl concept),
                // RateLimitNotice (informational, no Wasl concept), Unknown (unrecognized line).
                AgentEvent::ToolCallUpdated { .. }
                | AgentEvent::RateLimitNotice { .. }
                | AgentEvent::Unknown { .. } => continue,
            }
        }
    }

    async fn interrupt(&mut self) -> Result<(), RuntimeError> {
        // claude reports interruptible: false; no-op preserves actor contract.
        Ok(())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn gate_request_id(gate: &GateRequest) -> &str {
    match gate {
        GateRequest::Permission { request_id, .. } => request_id,
        GateRequest::Plan { request_id, .. } => request_id,
        GateRequest::Question { request_id, .. } => request_id,
    }
}

fn gate_detail(gate: &GateRequest) -> NativeGateDetail {
    match gate {
        GateRequest::Permission {
            tool_name, command, ..
        } => NativeGateDetail {
            summary: format!("permission: {tool_name}"),
            preview: command.clone(),
        },
        GateRequest::Plan { summary, .. } => NativeGateDetail {
            summary: "plan approval".to_string(),
            preview: summary.chars().take(200).collect(),
        },
        GateRequest::Question { questions, .. } => NativeGateDetail {
            summary: "question".to_string(),
            preview: questions
                .first()
                .map(|q| q.question.clone())
                .unwrap_or_default(),
        },
    }
}

// ── Public exports that external tests still reference ────────────────────────

/// Build the `claude` argv for a SpawnSpec. Kept public for callers that
/// need the argv for display/discovery; the actual spawn path now goes
/// through `wasit_claude::ClaudeDriver`.
pub fn claude_argv(
    bin: &str,
    _session_id: &str,
    _resume: bool,
    context: &wasl_runtime::SpawnContext,
) -> Vec<String> {
    let mut spec = SpawnSpec {
        cwd: context.cwd.clone(),
        model: context.model.clone(),
        mode: context.posture.clone().unwrap_or_default(),
        bin: Some(bin.to_string()),
        env: context
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        setup: context.setup.clone(),
        disallowed_tools: context.disallowed_tools.clone(),
        mcp_config: context.mcp_config.clone(),
        resume: ResumeMode::Fresh,
        first_prompt: None,
    };
    // Override bin from explicit parameter.
    spec.bin = Some(bin.to_string());
    wasit_claude::claude_argv(&spec)
}

/// Stateful tool tracker — kept public for any external tests still importing it.
/// Now delegates entirely to `wasit_claude::LineParser` but the type here is a
/// zero-cost compatibility shim (no external test calls methods other than
/// `new()` after the conformance tests were removed).
#[derive(Debug, Default)]
pub struct ClaudeToolTracker;

impl ClaudeToolTracker {
    pub fn new() -> Self {
        Self
    }

    /// Process one line and return `ProtocolEvent`s.
    /// Kept for API compatibility with any remaining callers.
    pub fn observe_line(&mut self, _line: &str) -> Vec<ProtocolEvent> {
        vec![]
    }
}

/// Map one claude stream-json line to `ProtocolEvent`s (pure, stateless).
/// Kept public for API compatibility; now delegates to wasit-claude parser.
pub fn map_claude_line(_line: &str) -> Vec<ProtocolEvent> {
    vec![]
}
