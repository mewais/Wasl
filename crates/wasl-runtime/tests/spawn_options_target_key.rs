//! Phase-4 — closes the FROZEN-SURFACE GAP `tests/support/mod.rs` flagged for
//! Phase-2A (`docs/specs/runtime.md` §6 line 547: `SpawnOptions` documents a
//! private `target_key` field, "kept opaque for Phase-1; R2 populates these
//! behind constructors it owns"). Phase 4's `ClaudeStreamProtocol` is the FIRST
//! real driver whose native identity genuinely depends on the target's
//! context — it mints a UUIDv5 session id FROM the context id — so it needs
//! `SpawnOptions` to actually carry the target key at `spawn` time. This test
//! proves the real end-to-end wiring: `WaslManagedRuntimeProvider::target_for`
//! populates `SpawnOptions` with the SAME `TargetKey` it was asked for, driven
//! through the REAL provider (not a hand-built `SpawnOptions` literal), plus
//! the pure default-has-no-key case.

mod support;

use std::sync::Arc;

use wasl_runtime::SpawnOptions;

use support::{
    key, provider, sink_arc, tight_config, ProtocolScript, RecordingSink, ScriptedProtocol,
    TestClock,
};

/// A `SpawnOptions` nobody populated (e.g. a driver-level unit test that
/// doesn't go through a provider) carries no target key — the honest default,
/// not a bogus placeholder.
#[test]
fn default_spawn_options_has_no_target_key() {
    let opts = SpawnOptions::default();
    assert!(
        opts.target_key().is_none(),
        "SpawnOptions::default() must carry no target key"
    );
}

/// The real end-to-end proof: `target_for(&TargetKey("ctx-claude-like"))`
/// results in the driver's `spawn` being called with
/// `opts.target_key() == Some(&TargetKey("ctx-claude-like"))` — the exact fact
/// `ClaudeStreamProtocol::spawn` needs to mint its byte-stable UUIDv5 native
/// session id (runtime-protocol.md §2).
#[tokio::test]
async fn target_for_populates_spawn_options_with_its_own_context_id() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-claude-like",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(wasl_runtime::ProtocolEvent::TurnAccepted { native_turn: None }),
    );
    let clock: Arc<TestClock> = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto.clone(), tight_config(), clock, sink);

    assert!(
        proto.last_spawn_target_key().is_none(),
        "no spawn has happened yet"
    );

    let _target = prov
        .target_for(&key("ctx-claude-like"))
        .await
        .expect("target_for must succeed");

    assert_eq!(
        proto.last_spawn_target_key(),
        Some(key("ctx-claude-like")),
        "the provider must pass the SAME TargetKey it was asked for through to \
         the driver's spawn(&SpawnOptions), so a real driver can derive a \
         context-scoped native identity from it"
    );
}

/// Two DIFFERENT target keys must each be observed distinctly by the driver
/// (not e.g. only the first key ever recorded) — proves the key genuinely
/// flows per-call, not just once at provider construction.
#[tokio::test]
async fn distinct_target_keys_are_each_passed_through_on_their_own_spawn() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-a",
        ProtocolScript::new()
            .ready(Some("ses-a"))
            .emit(wasl_runtime::ProtocolEvent::TurnAccepted { native_turn: None }),
    );
    proto.install(
        "ctx-b",
        ProtocolScript::new()
            .ready(Some("ses-b"))
            .emit(wasl_runtime::ProtocolEvent::TurnAccepted { native_turn: None }),
    );
    let clock: Arc<TestClock> = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto.clone(), tight_config(), clock, sink);

    let _a = prov
        .target_for(&key("ctx-a"))
        .await
        .expect("target_for(ctx-a) must succeed");
    assert_eq!(proto.last_spawn_target_key(), Some(key("ctx-a")));

    let _b = prov
        .target_for(&key("ctx-b"))
        .await
        .expect("target_for(ctx-b) must succeed");
    assert_eq!(proto.last_spawn_target_key(), Some(key("ctx-b")));
}
