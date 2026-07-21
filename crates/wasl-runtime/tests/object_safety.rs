//! Phase-1 OBJECT-SAFETY tests for `wasl-runtime` (R1).
//!
//! The whole point of Phase-1 is a FROZEN, object-safe type/trait surface with
//! NO scheduler/actor/driver logic. These tests are compile-time assertions:
//! every trait the spec says is addressed as a trait object MUST be object-safe.
//! If a trait is NOT object-safe as written (e.g. a non-`async_trait` generic
//! method, a `Self`-by-value method, an associated const), the corresponding
//! `dyn Trait` reference below fails to compile — catching the defect in Phase-1
//! rather than Phase-2.
//!
//! Spec citations (docs/specs/runtime.md):
//!   * `DeliveryTarget` — §3: "Object-safe."; used as `Arc<dyn DeliveryTarget>`
//!     (§10 `RuntimeProvider::target_for -> Arc<dyn DeliveryTarget>`, line 736).
//!   * `RuntimeProvider` — §10 (line 733): a `#[async_trait]` trait; the two
//!     initial providers are addressed generically, and the crate exposes it as
//!     the get-or-create seam. Asserted object-safe here.
//!   * `InteractiveProtocol` — §6 (line 505): `spawn`/`resume` return
//!     `Box<dyn InteractiveTransport>`; the provider holds a driver behind this
//!     seam → used as `dyn`.
//!   * `InteractiveTransport` — §6 (line 517): returned as
//!     `Box<dyn InteractiveTransport>` — MUST be object-safe.
//!
//! TEST-FIRST / EXPECTED RED: this file will NOT compile until the coder creates
//! the `wasl-runtime` crate and emits the specified trait surface. That is the
//! correct red state for Phase-1.

use std::sync::Arc;

use wasl_runtime::{DeliveryTarget, InteractiveProtocol, InteractiveTransport, RuntimeProvider};

// ---------------------------------------------------------------------
// DeliveryTarget — the ONLY seam generic callers address (§3). Used as
// `Arc<dyn DeliveryTarget>` per §10. Must be object-safe both by-ref and
// behind Arc.
// ---------------------------------------------------------------------

fn _assert_delivery_target_object_safe_by_ref(_t: &dyn DeliveryTarget) {}

fn _assert_delivery_target_object_safe_arc(_t: Arc<dyn DeliveryTarget>) {}

#[test]
fn delivery_target_is_object_safe() {
    // A `None` typed as `Option<Arc<dyn DeliveryTarget>>` only compiles if the
    // trait is object-safe (matches §10's `Arc<dyn DeliveryTarget>` return).
    let _: Option<Arc<dyn DeliveryTarget>> = None;
    // And a boxed form, to be thorough.
    let _: Option<Box<dyn DeliveryTarget>> = None;
}

// ---------------------------------------------------------------------
// RuntimeProvider — the get-or-create seam (§10). Node-config wiring (D8,
// deferred to Phase-3) selects between the two providers behind this trait;
// it must be object-safe to be chosen at runtime.
// ---------------------------------------------------------------------

fn _assert_runtime_provider_object_safe_by_ref(_p: &dyn RuntimeProvider) {}

#[test]
fn runtime_provider_is_object_safe() {
    let _: Option<Arc<dyn RuntimeProvider>> = None;
    let _: Option<Box<dyn RuntimeProvider>> = None;
}

// ---------------------------------------------------------------------
// InteractiveProtocol — the driver seam (§6). `spawn`/`resume` return
// `Box<dyn InteractiveTransport>`; the managed provider holds a driver behind
// this trait, so it is addressed as `Box<dyn InteractiveProtocol>`.
// ---------------------------------------------------------------------

fn _assert_interactive_protocol_object_safe_by_ref(_p: &dyn InteractiveProtocol) {}

#[test]
fn interactive_protocol_is_object_safe() {
    let _: Option<Box<dyn InteractiveProtocol>> = None;
    let _: Option<Arc<dyn InteractiveProtocol>> = None;
}

// ---------------------------------------------------------------------
// InteractiveTransport — the live connection (§6). Returned as
// `Box<dyn InteractiveTransport>` from spawn/resume; the actor is the sole
// holder. MUST be object-safe.
// ---------------------------------------------------------------------

fn _assert_interactive_transport_object_safe_by_ref(_t: &mut dyn InteractiveTransport) {}

#[test]
fn interactive_transport_is_object_safe() {
    // The spec's own signature: `spawn` returns `Box<dyn InteractiveTransport>`.
    let _: Option<Box<dyn InteractiveTransport>> = None;
}

// A function whose signature MIRRORS the spec's `InteractiveProtocol::spawn`
// return type (`Result<Box<dyn InteractiveTransport>, RuntimeError>`). It only
// compiles if `InteractiveTransport` is object-safe AND `RuntimeError` exists —
// a second, structural object-safety guard tying the two seams together.
fn _spawn_return_shape_compiles(
) -> Result<Box<dyn InteractiveTransport>, wasl_runtime::RuntimeError> {
    Err(wasl_runtime::RuntimeError::Shutdown)
}
