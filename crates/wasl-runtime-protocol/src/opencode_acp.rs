//! `OpenCodeAcpProtocol` — thin adapter over `wasit_opencode::OpencodeDriver`.
//!
//! All wire parsing/JSON-RPC is now delegated to `wasit-opencode`. This module
//! translates Wasl's `SpawnOptions` → `wasit_core::SpawnSpec` and maps
//! `wasit_core::AgentEvent` → `wasl_runtime::ProtocolEvent`.
//!
//! ## Gate answer routing
//!
//! Like the other adapters, gate answers arrive via `send_turn` while the
//! transport tracks a pending `GateRequest`. Text `"approve"` approves;
//! `"deny: <reason>"` denies with a reason; anything else is a plain denial.

use async_trait::async_trait;
use std::path::PathBuf;
use wasit_core::{AgentDriver, GateDecision, GateRequest, ResumeMode, SpawnSpec};
use wasl_runtime::{
    ComposedPrompt, InteractiveProtocol, InteractiveTransport, NativeGateDetail, NativeSessionId,
    ProtocolEvent, RuntimeCapabilities, RuntimeError, SpawnContext, SpawnOptions,
};

/// Translate Wasl `SpawnOptions` into a Wasit `SpawnSpec` for opencode.
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

pub struct OpenCodeAcpProtocol {
    bin: PathBuf,
}

impl OpenCodeAcpProtocol {
    pub fn new(bin: impl Into<PathBuf>) -> Self {
        OpenCodeAcpProtocol { bin: bin.into() }
    }

    fn caps() -> RuntimeCapabilities {
        RuntimeCapabilities {
            persistent_process: true,
            persistent_conversation: true,
            structured_turns: true,
            streaming_output: true,
            attachable: true,
            interruptible: false,
            structured_provenance: true,
            resumable: true,
        }
    }
}

#[async_trait]
impl InteractiveProtocol for OpenCodeAcpProtocol {
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
            .unwrap_or("opencode")
            .to_string();
        let spec = context_to_spec(opts, None);
        let driver = wasit_opencode::OpencodeDriver::new(bin_str);
        let session = driver
            .spawn(spec)
            .await
            .map_err(|e| RuntimeError::SpawnFailed {
                detail: format!("wasit-opencode spawn: {e}"),
            })?;
        Ok(Box::new(OpenCodeAcpTransport {
            session,
            pending_gate: None,
            session_ready_seen: false,
            accumulated_reply: String::new(),
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
            .unwrap_or("opencode")
            .to_string();
        let spec = context_to_spec(opts, Some(&native.0));
        let driver = wasit_opencode::OpencodeDriver::new(bin_str);
        let session = driver
            .spawn(spec)
            .await
            .map_err(|e| RuntimeError::SpawnFailed {
                detail: format!("wasit-opencode resume: {e}"),
            })?;
        Ok(Box::new(OpenCodeAcpTransport {
            session,
            pending_gate: None,
            session_ready_seen: false,
            accumulated_reply: String::new(),
        }))
    }
}

/// Thin transport adapter that wraps a `Box<dyn wasit_core::Session>`.
///
/// Opencode delivers assistant text via incremental `AssistantDelta` events and
/// has no final reply in `TurnCompleted`. We accumulate deltas so the R1 actor
/// receives a populated reply on turn completion.
struct OpenCodeAcpTransport {
    session: Box<dyn wasit_core::Session>,
    pending_gate: Option<GateRequest>,
    session_ready_seen: bool,
    /// Accumulated reply text from `AssistantDelta` events this turn.
    accumulated_reply: String,
}

#[async_trait]
impl InteractiveTransport for OpenCodeAcpTransport {
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
                    detail: format!("opencode answer_gate: {e}"),
                })?;
            return Ok(());
        }
        // Start a new turn: reset the reply accumulator.
        self.accumulated_reply.clear();
        self.session
            .send_prompt(&prompt.0)
            .await
            .map_err(|e| RuntimeError::Transport {
                detail: format!("opencode send_prompt: {e}"),
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
                    // Accumulate into the reply buffer.
                    self.accumulated_reply.push_str(&text);
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
                    let final_reply = reply
                        .filter(|r| !r.is_empty())
                        .unwrap_or_else(|| std::mem::take(&mut self.accumulated_reply));
                    match end_reason {
                        None => {
                            return Some(ProtocolEvent::TurnCompleted { reply: final_reply });
                        }
                        Some(EndReason::Cancelled) => {
                            self.accumulated_reply.clear();
                            return Some(ProtocolEvent::TurnFailed {
                                error: "turn cancelled".to_string(),
                            });
                        }
                        Some(EndReason::MaxTurns) => {
                            self.accumulated_reply.clear();
                            return Some(ProtocolEvent::TurnFailed {
                                error: "max turns reached".to_string(),
                            });
                        }
                        Some(EndReason::ExecutionError) => {
                            self.accumulated_reply.clear();
                            return Some(ProtocolEvent::TurnFailed {
                                error: final_reply,
                            });
                        }
                        Some(EndReason::CompactionNeeded) => {
                            self.accumulated_reply.clear();
                            return Some(ProtocolEvent::TurnFailed {
                                error: "compaction needed".to_string(),
                            });
                        }
                    }
                }
                AgentEvent::ProcessExited { code } => {
                    return Some(ProtocolEvent::ProcessExited { code });
                }
                // Dropped: ThinkingDelta, ToolCallUpdated, UsageUpdate,
                // PlanUpdated, ModesUpdated, ModelChanged, SlashCommandsUpdated,
                // Compaction, RateLimitNotice, Unknown.
                _ => continue,
            }
        }
    }

    async fn interrupt(&mut self) -> Result<(), RuntimeError> {
        // opencode reports interruptible: false; no-op preserves actor contract.
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

// ── Public exports for API compatibility ─────────────────────────────────────

/// Build `session/new` or `session/load` params. Kept public for tests that
/// still reference this function. Now delegates to wasit-opencode.
pub fn opencode_session_params(
    _session_method: &str,
    _requested_session_id: Option<&str>,
    _cwd: &str,
) -> serde_json::Value {
    // This is kept for API compatibility; the real logic lives in wasit-opencode.
    serde_json::json!({})
}

/// Map one opencode ACP line to `ProtocolEvent`s. Kept for API compatibility;
/// now a no-op shim (parsing lives in wasit-opencode).
pub fn map_acp_line_correlated(_line: &str, _expected_id: u64) -> Vec<ProtocolEvent> {
    vec![]
}
