//! `CodexAppServerProtocol` — thin adapter over `wasit_codex::CodexDriver`.
//!
//! All wire parsing/JSON-RPC is now delegated to `wasit-codex`. This module
//! translates Wasl's `SpawnOptions` → `wasit_core::SpawnSpec` and maps
//! `wasit_core::AgentEvent` → `wasl_runtime::ProtocolEvent`.
//!
//! ## Gate answer routing
//!
//! Like the claude adapter, gate answers arrive via `send_turn` while the
//! transport tracks a pending `GateRequest`. The text `"approve"` approves;
//! `"deny: <reason>"` denies with a reason; any other text is treated as a
//! plain denial.

use async_trait::async_trait;
use std::path::PathBuf;
use wasit_core::{AgentDriver, GateDecision, GateRequest, ResumeMode, SpawnSpec};
use wasl_runtime::{
    AgentMode, CompactionInfo, ComposedPrompt, InteractiveProtocol, InteractiveTransport,
    NativeGateDetail, NativeSessionId, PlanEntry, ProtocolEvent, RuntimeCapabilities,
    RuntimeError, SlashCommandInfo, SpawnOptions, UsageSnapshot,
};

/// Translate Wasl `SpawnOptions` into a Wasit `SpawnSpec` for codex.
fn context_to_spec(opts: &SpawnOptions, native_id: Option<&str>) -> SpawnSpec {
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
        resume: match native_id {
            Some(id) => ResumeMode::Resume(id.to_string()),
            None => ResumeMode::Fresh,
        },
        first_prompt: None,
    }
}

pub struct CodexAppServerProtocol {
    bin: PathBuf,
}

impl CodexAppServerProtocol {
    pub fn new(bin: impl Into<PathBuf>) -> Self {
        CodexAppServerProtocol { bin: bin.into() }
    }

    fn caps() -> RuntimeCapabilities {
        RuntimeCapabilities {
            persistent_process: true,
            persistent_conversation: true,
            structured_turns: true,
            streaming_output: true,
            attachable: true,
            interruptible: true,
            structured_provenance: true,
            resumable: true,
        }
    }
}

#[async_trait]
impl InteractiveProtocol for CodexAppServerProtocol {
    fn capabilities(&self) -> RuntimeCapabilities {
        Self::caps()
    }

    async fn spawn(
        &self,
        opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        let bin_str = self
            .bin
            .to_str()
            .unwrap_or("codex")
            .to_string();
        let spec = context_to_spec(opts, None);
        let driver = wasit_codex::CodexDriver::new(bin_str);
        let session = driver
            .spawn(spec)
            .await
            .map_err(|e| RuntimeError::SpawnFailed {
                detail: format!("wasit-codex spawn: {e}"),
            })?;
        Ok(Box::new(CodexAppServerTransport {
            session,
            pending_gate: None,
            session_ready_seen: false,
        }))
    }

    async fn resume(
        &self,
        native: &NativeSessionId,
        opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        let bin_str = self
            .bin
            .to_str()
            .unwrap_or("codex")
            .to_string();
        let spec = context_to_spec(opts, Some(&native.0));
        let driver = wasit_codex::CodexDriver::new(bin_str);
        let session = driver
            .spawn(spec)
            .await
            .map_err(|e| RuntimeError::SpawnFailed {
                detail: format!("wasit-codex resume: {e}"),
            })?;
        Ok(Box::new(CodexAppServerTransport {
            session,
            pending_gate: None,
            session_ready_seen: false,
        }))
    }
}

/// Thin transport adapter that wraps a `Box<dyn wasit_core::Session>`.
struct CodexAppServerTransport {
    session: Box<dyn wasit_core::Session>,
    /// The most recently received `GateRequest`, stored so the next
    /// `send_turn("approve"/"deny: <reason>")` can route to `answer_gate`.
    pending_gate: Option<GateRequest>,
    /// Suppress duplicate `SessionReady` events (codex emits `thread/started`
    /// once, but guard defensively in line with the claude precedent).
    session_ready_seen: bool,
}

#[async_trait]
impl InteractiveTransport for CodexAppServerTransport {
    async fn send_turn(&mut self, prompt: &ComposedPrompt) -> Result<(), RuntimeError> {
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
                GateDecision::Deny { reason: None }
            };
            let request_id = gate_request_id(gate).to_string();
            self.pending_gate = None;
            self.session
                .answer_gate(&request_id, decision)
                .await
                .map_err(|e| RuntimeError::Transport {
                    detail: format!("codex answer_gate: {e}"),
                })?;
            return Ok(());
        }
        self.session
            .send_prompt(&prompt.0)
            .await
            .map_err(|e| RuntimeError::Transport {
                detail: format!("codex send_prompt: {e}"),
            })
    }

    async fn next_event(&mut self) -> Option<ProtocolEvent> {
        use wasit_core::AgentEvent;
        loop {
            let ev = self.session.next_event().await?;
            match ev {
                AgentEvent::SessionInit { native_session_id, .. } => {
                    if self.session_ready_seen {
                        continue;
                    }
                    self.session_ready_seen = true;
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
                AgentEvent::ToolCallStarted { name, .. } => {
                    return Some(ProtocolEvent::ToolStarted { name });
                }
                AgentEvent::ToolCallFinished { name, ok, .. } => {
                    return Some(ProtocolEvent::ToolFinished { name, ok });
                }
                AgentEvent::GateRequested { gate } => {
                    let detail = gate_detail(&gate);
                    self.pending_gate = Some(gate);
                    return Some(ProtocolEvent::NativeGateRequested { detail });
                }
                AgentEvent::TurnCompleted { end_reason, reply, .. } => {
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
                        context_pct: context_pct
                            .map(|p| p.clamp(0.0, 100.0).round() as u8),
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
        self.session
            .interrupt()
            .await
            .map_err(|e| RuntimeError::Transport {
                detail: format!("codex interrupt: {e}"),
            })
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

// ── Public exports for API compatibility ─────────────────────────────────────

/// Pure per-line codex map — kept public for API compatibility; now a no-op
/// shim (parsing lives in wasit-codex).
pub fn map_codex_line(_line: &str) -> Vec<ProtocolEvent> {
    vec![]
}

/// Stateful commandExecution correlator — kept public for API compatibility.
/// Real tracking now lives in `wasit_codex::MessageParser`.
#[derive(Debug, Default)]
pub struct CodexToolTracker;

impl CodexToolTracker {
    pub fn new() -> Self {
        Self
    }

    pub fn observe_line(&mut self, _line: &str) -> Vec<ProtocolEvent> {
        vec![]
    }
}
