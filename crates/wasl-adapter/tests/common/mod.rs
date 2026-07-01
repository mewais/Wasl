//! Shared test helpers for the `wasl-adapter` black-box suite.
//!
//! A FAKE `AgentAdapter` that records the `Delivery` it received and is
//! scriptable to return either a reply string or an `AdapterError`, plus small
//! builders for A2A `Message` fixtures carrying `metadata.wasl`.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;

use wasl_a2a_types::{Message, Part};
use wasl_adapter::{AdapterError, AgentAdapter, Delivery};

/// What the fake should answer when `deliver` is called.
#[derive(Clone)]
enum Script {
    Reply(String),
    Fail(AdapterError),
}

/// A scriptable `AgentAdapter` that records every `Delivery` it is handed.
///
/// Cloneable: clone it *before* moving one copy into an `AdapterExecutor` so the
/// test keeps a handle onto the shared recording buffer.
#[derive(Clone)]
pub struct FakeAdapter {
    received: Arc<Mutex<Vec<Delivery>>>,
    script: Script,
}

impl FakeAdapter {
    /// A fake that answers `deliver` with `reply`.
    pub fn returning(reply: impl Into<String>) -> Self {
        FakeAdapter {
            received: Arc::new(Mutex::new(Vec::new())),
            script: Script::Reply(reply.into()),
        }
    }

    /// A fake that fails `deliver` with `err`.
    pub fn failing(err: AdapterError) -> Self {
        FakeAdapter {
            received: Arc::new(Mutex::new(Vec::new())),
            script: Script::Fail(err),
        }
    }

    /// Number of `deliver` calls observed.
    pub fn call_count(&self) -> usize {
        self.received.lock().unwrap().len()
    }

    /// The single `Delivery` the fake saw; panics unless exactly one call landed.
    pub fn only_delivery(&self) -> Delivery {
        let seen = self.received.lock().unwrap();
        assert_eq!(seen.len(), 1, "expected exactly one deliver() call");
        seen[0].clone()
    }
}

#[async_trait]
impl AgentAdapter for FakeAdapter {
    async fn deliver(&self, d: &Delivery) -> Result<String, AdapterError> {
        self.received.lock().unwrap().push(d.clone());
        match &self.script {
            Script::Reply(s) => Ok(s.clone()),
            Script::Fail(e) => Err(e.clone()),
        }
    }
}

/// Build a `Message` with the given `metadata` and content `parts`.
pub fn msg(metadata: Option<Value>, parts: Vec<Part>) -> Message {
    let mut m = Message::user_text("", "msg-fixture-1");
    m.parts = parts;
    m.metadata = metadata;
    m
}

/// `metadata.wasl = {"kind": <kind>, "from": <from>}`.
pub fn wasl_meta(kind: &str, from: &str) -> Value {
    serde_json::json!({ "wasl": { "kind": kind, "from": from } })
}
