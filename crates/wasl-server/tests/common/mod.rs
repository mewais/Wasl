//! Shared test fakes for the `wasl_server` black-box suite.
//!
// Rust compiles this shared helper into each integration-test binary separately,
// so a fake used by only one binary reads as dead in the other — expected for a
// common test module. Allow it rather than fragmenting the fakes per-file.
#![allow(dead_code)]
//!
//! A scriptable fake `Surfacer` (captures the `GateMode` it was called with and
//! returns a preloaded `GateOutcome`, or asserts it was NOT called) and a fake
//! `Sink` (records appended entries and can be scripted to fail on `append`).
//!
//! The `Gatekeeper` takes the surfacer/sink by value, so each fake exposes a
//! cloneable, shared observation handle (`Rc<RefCell<_>>`) that the test keeps in
//! order to inspect what the fake saw after the `Gatekeeper` has run.

use std::cell::RefCell;
use std::rc::Rc;

use wasl_audit::{AuditEntry, AuditError, Sink};
use wasl_gate::{GateMode, GateOutcome};
use wasl_proto::GateRequest;
use wasl_server::Surfacer;

/// What a [`FakeSurfacer`] observed across its calls.
#[derive(Debug, Default, Clone)]
pub struct SurfacerLog {
    /// Number of times `surface` was invoked.
    pub calls: usize,
    /// The mode passed on the last `surface` call (`None` until first call).
    pub last_mode: Option<GateMode>,
    /// The request passed on the last `surface` call (`None` until first call).
    pub last_req: Option<GateRequest>,
}

/// A scriptable fake `Surfacer`.
///
/// It returns a preloaded [`GateOutcome`] and records — into a shared
/// [`SurfacerLog`] handle — how many times it was called and the
/// [`GateMode`]/[`GateRequest`] it last saw.
pub struct FakeSurfacer {
    outcome: GateOutcome,
    log: Rc<RefCell<SurfacerLog>>,
}

impl FakeSurfacer {
    /// Build a fake that will return `outcome` when surfaced.
    pub fn new(outcome: GateOutcome) -> Self {
        Self {
            outcome,
            log: Rc::new(RefCell::new(SurfacerLog::default())),
        }
    }

    /// A cloneable handle onto what this surfacer observed. Keep it in the test
    /// before moving the surfacer into the `Gatekeeper`.
    pub fn log(&self) -> Rc<RefCell<SurfacerLog>> {
        Rc::clone(&self.log)
    }
}

impl Surfacer for FakeSurfacer {
    fn surface(&mut self, mode: GateMode, req: &GateRequest) -> GateOutcome {
        let mut log = self.log.borrow_mut();
        log.calls += 1;
        log.last_mode = Some(mode);
        log.last_req = Some(req.clone());
        self.outcome.clone()
    }
}

/// What a [`FakeSink`] recorded.
#[derive(Debug, Default, Clone)]
pub struct SinkLog {
    /// Every entry successfully appended, in order.
    pub appended: Vec<AuditEntry>,
}

/// A fake `Sink` that records appended entries and can be scripted to fail.
pub struct FakeSink {
    /// When `true`, `append` returns `Err(AuditError::Sink(_))` and records nothing.
    fail: bool,
    log: Rc<RefCell<SinkLog>>,
}

impl FakeSink {
    /// A sink that accepts every append.
    pub fn ok() -> Self {
        Self {
            fail: false,
            log: Rc::new(RefCell::new(SinkLog::default())),
        }
    }

    /// A sink that fails every append.
    pub fn failing() -> Self {
        Self {
            fail: true,
            log: Rc::new(RefCell::new(SinkLog::default())),
        }
    }

    /// A cloneable handle onto what this sink recorded.
    pub fn log(&self) -> Rc<RefCell<SinkLog>> {
        Rc::clone(&self.log)
    }
}

impl Sink for FakeSink {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
        if self.fail {
            return Err(AuditError::Sink("scripted failure".to_string()));
        }
        self.log.borrow_mut().appended.push(entry.clone());
        Ok(())
    }
}
