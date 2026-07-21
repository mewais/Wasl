//! Phase-1 SKELETON smoke test for R2 `wasl-runtime-protocol`.
//!
//! Per runtime-protocol.md, R2 in Phase-1 has NO type surface of its own — the
//! driver-seam traits (`InteractiveProtocol`/`InteractiveTransport`) and the
//! internal `ProtocolEvent` enum LIVE IN R1 (the plan's cycle-fix: "It depends
//! on R1 and never the reverse … the traits + the internal `ProtocolEvent` enum
//! live in R1; only concrete drivers live here", lines 9-13). The concrete
//! drivers (`FakeLineProtocol`, `ClaudeStreamProtocol`, `OpenCodeAcpProtocol`,
//! `OpenCodeServeProtocol`) are LATER phases (2B / 4 / 5), not Phase-1.
//!
//! So Phase-1 for R2 needs only: the crate EXISTS and DEPENDS ON R1, and R1 owns
//! the driver-seam traits. This test proves exactly that — it `use`s R1's traits
//! and types THROUGH the dependency edge, and asserts the dep direction is
//! R2 → R1 (R2 can name R1's public surface). No behavior, no driver.
//!
//! TEST-FIRST / EXPECTED RED: will not compile until the coder creates both the
//! `wasl-runtime` crate (owning the traits) AND the `wasl-runtime-protocol`
//! crate skeleton that depends on it.

// The dep edge + "R1 owns the traits": these `use`s only resolve if
// `wasl-runtime-protocol`'s Cargo.toml depends on `wasl-runtime` and R1 exports
// exactly these items (runtime-protocol.md Deps line 4-6).
use wasl_runtime::{
    ComposedPrompt, InteractiveProtocol, InteractiveTransport, NativeSessionId, ProtocolEvent,
    RuntimeCapabilities, RuntimeError, TurnId,
};

// =====================================================================
// The driver-seam traits are R1-owned and object-safe; R2 will implement them
// on concrete drivers LATER. Proving R2 can NAME them as trait objects here is
// the Phase-1 skeleton obligation (dep edge proven).
// =====================================================================

fn _r2_can_reference_r1_protocol_trait_object(_p: &dyn InteractiveProtocol) {}
fn _r2_can_reference_r1_transport_trait_object(_t: &mut dyn InteractiveTransport) {}

#[test]
fn r2_depends_on_r1_and_r1_owns_the_driver_seam_traits() {
    // R2 can construct R1's generic driver contract types through the dep edge.
    let _: Option<Box<dyn InteractiveProtocol>> = None;
    let _: Option<Box<dyn InteractiveTransport>> = None;

    // R1 owns ProtocolEvent (the internal normalized vocabulary R2 drivers will
    // map their native wire ONTO). R2 can name + construct it.
    let ev = ProtocolEvent::SessionReady {
        native: Some(NativeSessionId("ses_1".to_string())),
    };
    assert_eq!(ev.clone(), ev);

    // R1 owns RuntimeCapabilities (each R2 driver reports a fixed one, §2/§3).
    let caps = RuntimeCapabilities {
        persistent_process: true,
        persistent_conversation: true,
        structured_turns: true,
        streaming_output: false,
        attachable: true,
        interruptible: false,
        structured_provenance: true,
        resumable: true,
    };
    assert!(caps.persistent_process);
    assert!(!caps.interruptible);

    // R1 owns the seam value types R2's send_turn/next_event signatures use.
    let _prompt = ComposedPrompt("hello".to_string());
    let _turn = TurnId("turn-1".to_string());
    let _err = RuntimeError::Shutdown;
}

// =====================================================================
// The spec's per-driver capability reports (runtime-protocol.md §2 lines
// 189-192, §3.1 lines 239-242) are the honest fixed reports. Phase-1 pins only
// that these reports are EXPRESSIBLE via R1's RuntimeCapabilities (a driver
// setting all-false is a FAIL — impl-plan Phase-1); the concrete driver structs
// that HOLD them are later phases, so we assert the shape, not a driver.
// =====================================================================

#[test]
fn r1_capabilities_can_express_the_locked_per_driver_reports() {
    // ClaudeStreamProtocol locked report (runtime-protocol.md §2 lines 189-192):
    // streaming_output:false + interruptible:false (⛔ UNKNOWN), the rest true.
    let claude = RuntimeCapabilities {
        persistent_process: true,
        persistent_conversation: true,
        structured_turns: true,
        streaming_output: false,
        attachable: true,
        interruptible: false,
        structured_provenance: true,
        resumable: true,
    };
    // OpenCodeAcpProtocol locked report (§3.1 lines 239-242): streaming_output
    // TRUE (real deltas), interruptible false.
    let opencode_acp = RuntimeCapabilities {
        streaming_output: true,
        ..claude
    };
    assert!(!claude.streaming_output, "claude has no proven deltas");
    assert!(opencode_acp.streaming_output, "acp has real deltas");
    assert!(!opencode_acp.interruptible, "interruptibility ⛔ UNKNOWN");

    // A driver reporting all-false is a FAIL per impl-plan Phase-1 — assert the
    // honest reports are NOT the all-false report.
    let all_false = RuntimeCapabilities {
        persistent_process: false,
        persistent_conversation: false,
        structured_turns: false,
        streaming_output: false,
        attachable: false,
        interruptible: false,
        structured_provenance: false,
        resumable: false,
    };
    assert_ne!(claude, all_false);
    assert_ne!(opencode_acp, all_false);
}
