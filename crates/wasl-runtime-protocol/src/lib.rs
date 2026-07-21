//! wasl-runtime-protocol — R2 — concrete interactive drivers (Phase-1 skeleton).
//!
//! This crate is the **one place** CLI-specific / ACP-specific / opencode-specific
//! wire knowledge is allowed. It provides the concrete [`InteractiveProtocol`]
//! drivers that map each substrate's native wire shape onto R1's generic
//! [`ProtocolEvent`]. **It depends on R1 and never the reverse** (the plan's
//! cycle-fix: the driver-seam traits + the internal [`ProtocolEvent`] enum live
//! in R1; only concrete drivers live here).
//!
//! ## Phase-1 scope
//!
//! In Phase-1 this crate has **NO type surface of its own** — it merely EXISTS
//! and DEPENDS ON R1, proving the R2 → R1 dependency edge and that R1 owns the
//! driver-seam traits. The concrete drivers (`FakeLineProtocol` (Phase 2B),
//! `ClaudeStreamProtocol` (Phase 4), `OpenCodeAcpProtocol` / `OpenCodeServeProtocol`
//! (Phase 5)) land in later phases.
//!
//! The re-export below documents the R1-owned generic contract these drivers will
//! implement, and makes the dependency edge explicit at the crate root.
//!
//! [`InteractiveProtocol`]: wasl_runtime::InteractiveProtocol
//! [`ProtocolEvent`]: wasl_runtime::ProtocolEvent

#![forbid(unsafe_code)]

// The R1-owned generic driver contract every R2 driver implements/maps onto.
// Phase-1 only re-surfaces it here to make the R2 → R1 edge explicit; the
// concrete driver structs arrive in Phase 2B/4/5.
pub use wasl_runtime::{
    ComposedPrompt, InteractiveProtocol, InteractiveTransport, NativeSessionId, ProtocolEvent,
    RuntimeCapabilities, RuntimeError, SpawnOptions,
};
