//! Phase 6 — the compile-only `ExternalManagedRuntimeProvider` stub
//! (`docs/design/live-runtime-impl-plan.md` Phase 6: "plus a compile-only
//! `ExternalManagedRuntimeProvider` stub in Phase 1 so we learn in Phase 1
//! (not Phase 6) if the trait surface can host it without leaking a CLI-ism
//! (council #3)").
//!
//! **Process note (honest):** this check was supposed to land in Phase 1 and
//! did not — it is closed out here, in Phase 6, once phases 0'-5 have
//! actually shipped and the real drivers (`ClaudeStreamProtocol`,
//! `OpenCodeAcpProtocol`) exist to compare against. Late, but closed for
//! real: this file is compiled by CI (`cargo build -p wasl-runtime --example
//! external_seam_stub`, exercised by `cargo build --workspace --all-targets`)
//! wired ONLY against `wasl-runtime`'s PUBLIC API — no `wasl-runtime-protocol`
//! dependency, no `tokio::process`, no `PathBuf`, no CLI type of any kind.
//!
//! This is a SKETCH, not a shipped feature (`docs/design/live-runtime-plan.md`
//! §8: "Explicitly out of scope... a separate follow-on document"; the fuller
//! design sketch lives in `docs/design/external-runtime-seam.md`). Its ONLY
//! job is to answer one question definitively: **can `RuntimeProvider` /
//! `DeliveryTarget` / `NativeGateResolver` / `ToolCallAnswerer` be implemented
//! by something that owns NO process/PTY/OS handle at all — a pure generic
//! wire client — without inventing a new method, a new generic parameter, or
//! leaking any CLI-specific concept into the trait signatures?** If this file
//! compiles, the answer is yes: every method below is implementable using
//! only `wasl-runtime`'s existing public vocabulary (`TargetId`,
//! `RuntimeCapabilities`, `TurnRequest`/`TurnReceipt`, `EventSeq`,
//! `TurnEventStream`, `CancelReason`, `ShutdownMode`, `TurnId`,
//! `wasl_proto::GateDecision`, `RuntimeError`) plus a generic transport this
//! sketch never has to name (an `Arc<dyn ExternalWireClient>` seam below,
//! standing in for "HTTP/JSON-RPC to an external supervisor" — deliberately
//! never a concrete HTTP client, since picking one is real Phase-6-follow-on
//! work, not this compile check's job).

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;

use wasl_runtime::{
    BusyPolicy, CancelReason, DeliveryTarget, EventSeq, NativeGateResolver, RuntimeCapabilities,
    RuntimeError, RuntimeProvider, ShutdownMode, StreamItem, TargetId, TargetKey, TargetStatus,
    ToolCallAnswerer, TurnEventStream, TurnId, TurnReceipt, TurnRequest,
};

/// The seam a real implementation would flesh out: SOME generic way to talk
/// to an external supervisor about ONE target, using only generic terms
/// (`target_id`, capability flags, turn requests/receipts/events) — never a
/// CLI argv, a `PathBuf`, a `Command`, or a raw process/PTY handle (the hard
/// rule `live-runtime-plan.md` §8 locks even for this future seam). This
/// trait is intentionally never implemented for a real transport in this
/// compile-only stub — picking HTTP vs. gRPC vs. something else is real
/// follow-on design work, out of scope here.
trait ExternalWireClient: std::fmt::Debug + Send + Sync {
    // Left deliberately empty: the compile check is about whether
    // `DeliveryTarget` et al. can be hosted by *something* backed by a
    // generic wire client, not about designing that wire client's own API.
}

/// A `DeliveryTarget` fronting a target owned and run entirely by an
/// external supervisor. Notice: this struct holds NOTHING CLI-specific —
/// only the generic seam above plus the two identifiers every target has
/// regardless of substrate.
#[derive(Debug)]
struct ExternalTarget {
    target_id: TargetId,
    caps: RuntimeCapabilities,
    #[allow(dead_code)]
    wire: Arc<dyn ExternalWireClient>,
}

#[async_trait]
impl DeliveryTarget for ExternalTarget {
    fn target_id(&self) -> &TargetId {
        &self.target_id
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        self.caps
    }

    async fn status(&self) -> TargetStatus {
        // A real implementation would poll/subscribe the external supervisor
        // for status; the compile check only needs a value of the right type.
        TargetStatus::Offline
    }

    async fn submit_turn(&self, _req: TurnRequest, _busy_policy: BusyPolicy) -> TurnReceipt {
        TurnReceipt::RejectedUnavailable {
            status: TargetStatus::Offline,
        }
    }

    async fn events(&self, _after_seq: EventSeq) -> TurnEventStream {
        let empty: Pin<Box<dyn futures::Stream<Item = StreamItem> + Send>> =
            Box::pin(stream::empty());
        empty
    }

    async fn cancel_turn(
        &self,
        _turn_id: TurnId,
        _reason: CancelReason,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported {
            operation: "cancel_turn (external-seam compile-only stub)".to_string(),
        })
    }

    async fn shutdown(&self, _mode: ShutdownMode) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported {
            operation: "shutdown (external-seam compile-only stub)".to_string(),
        })
    }
}

#[async_trait]
impl NativeGateResolver for ExternalTarget {
    async fn resolve_native_gate(
        &self,
        _turn_id: TurnId,
        _decision: wasl_proto::GateDecision,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported {
            operation: "resolve_native_gate (external-seam compile-only stub)".to_string(),
        })
    }
}

#[async_trait]
impl ToolCallAnswerer for ExternalTarget {
    async fn answer_tool_call(&self, _turn_id: TurnId, _value: String) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported {
            operation: "answer_tool_call (external-seam compile-only stub)".to_string(),
        })
    }
}

/// FUTURE / NOT PART OF ANY SHIPPED PLAN — mirrors
/// `docs/design/live-runtime-plan.md` §8's sketch verbatim in spirit: "a
/// provider that fronts a target OWNED and RUN by an external supervisor,
/// spoken to over a GENERIC transport ... using only GENERIC terms." Holds
/// only generic endpoint config (here, nothing at all — a real one would
/// carry e.g. a base URL) — never a CLI bin path, never a spawned child.
struct ExternalManagedRuntimeProvider {
    #[allow(dead_code)]
    wire: Arc<dyn ExternalWireClient>,
}

#[async_trait]
impl RuntimeProvider for ExternalManagedRuntimeProvider {
    async fn target_for(&self, key: &TargetKey) -> Result<Arc<dyn DeliveryTarget>, RuntimeError> {
        let target = ExternalTarget {
            target_id: TargetId(key.0.clone()),
            caps: RuntimeCapabilities {
                persistent_process: true,
                persistent_conversation: true,
                structured_turns: true,
                streaming_output: false,
                attachable: true,
                interruptible: false,
                structured_provenance: true,
                resumable: true,
            },
            wire: self.wire.clone(),
        };
        Ok(Arc::new(target) as Arc<dyn DeliveryTarget>)
    }

    async fn list_targets(&self) -> Vec<TargetId> {
        Vec::new()
    }
}

/// A trivial implementor, purely so `main` can construct a real
/// `ExternalManagedRuntimeProvider` (proving the whole assembly compiles end
/// to end, not just that the impl blocks parse in isolation) without pulling
/// in an async runtime or picking a real wire protocol.
#[derive(Debug)]
struct DummyWireClient;
impl ExternalWireClient for DummyWireClient {}

fn main() {
    // Compile-only stub: proving the trait surface is object-safe and
    // implementable by a generic-wire-backed provider is the entire point.
    // Construction (not invocation — that needs an async runtime this
    // example deliberately doesn't pull in) is enough: it proves
    // `Arc<dyn DeliveryTarget>` really can be produced by something that
    // never touches a CLI type.
    let provider = ExternalManagedRuntimeProvider {
        wire: Arc::new(DummyWireClient),
    };
    let _: &dyn RuntimeProvider = &provider;
}
