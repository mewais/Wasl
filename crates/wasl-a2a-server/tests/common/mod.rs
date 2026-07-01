#![allow(dead_code)]
//! Shared black-box test fixtures for `wasl_a2a_server`.
//!
//! Nothing here touches `src/` — these helpers only build A2A wire fixtures
//! (from `wasl_a2a_types`) and a scriptable fake `TaskExecutor` implementing the
//! public seam under test. Both `core.rs` and `loopback.rs` `mod common;` this.

use std::sync::Mutex;

use wasl_a2a_types::{AgentCapabilities, AgentCard, AgentSkill, Message, MessageSendParams, Part};

use wasl_a2a_server::{ExecError, TaskExecutor};

/// A deterministic `AgentCard` used across the suite.
pub fn sample_card() -> AgentCard {
    AgentCard {
        name: "Test Agent".to_string(),
        description: "A2A server under test".to_string(),
        url: "http://127.0.0.1/".to_string(),
        version: "0.1.0".to_string(),
        capabilities: AgentCapabilities {
            streaming: true,
            push_notifications: false,
        },
        default_input_modes: vec!["text/plain".to_string()],
        default_output_modes: vec!["text/plain".to_string()],
        skills: vec![AgentSkill {
            id: "echo".to_string(),
            name: "Echo".to_string(),
            description: "echoes text".to_string(),
            tags: vec!["demo".to_string()],
            examples: vec![],
        }],
    }
}

/// Two text parts, the canonical successful executor reply.
pub fn two_parts() -> Vec<Part> {
    vec![
        Part::Text {
            text: "hello".to_string(),
        },
        Part::Text {
            text: "world".to_string(),
        },
    ]
}

/// `MessageSendParams` for a user message with no `context_id` set.
pub fn params_no_context(text: &str, message_id: &str) -> MessageSendParams {
    MessageSendParams {
        message: Message::user_text(text, message_id),
        metadata: None,
    }
}

/// `MessageSendParams` for a user message carrying an explicit `context_id`.
pub fn params_with_context(text: &str, message_id: &str, context_id: &str) -> MessageSendParams {
    let mut message = Message::user_text(text, message_id);
    message.context_id = Some(context_id.to_string());
    MessageSendParams {
        message,
        metadata: None,
    }
}

/// A scriptable [`TaskExecutor`]: it returns a fixed `Ok(parts)`/`Err(ExecError)`
/// and records every `(context_id, message)` it was invoked with.
pub struct FakeExecutor {
    result: Result<Vec<Part>, ExecError>,
    seen: Mutex<Vec<(String, Message)>>,
}

impl FakeExecutor {
    /// Executor that always succeeds with `parts`.
    pub fn ok(parts: Vec<Part>) -> Self {
        FakeExecutor {
            result: Ok(parts),
            seen: Mutex::new(Vec::new()),
        }
    }

    /// Executor that always fails with an `ExecError` carrying `message`.
    pub fn err(message: &str) -> Self {
        FakeExecutor {
            result: Err(ExecError {
                message: message.to_string(),
            }),
            seen: Mutex::new(Vec::new()),
        }
    }

    /// A snapshot of the `(context_id, message)` pairs the executor observed.
    pub fn seen(&self) -> Vec<(String, Message)> {
        self.seen.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl TaskExecutor for FakeExecutor {
    async fn execute(&self, context_id: String, message: Message) -> Result<Vec<Part>, ExecError> {
        self.seen.lock().unwrap().push((context_id, message));
        self.result.clone()
    }
}
