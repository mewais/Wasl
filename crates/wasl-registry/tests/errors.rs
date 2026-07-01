//! Black-box error-path tests for `wasl_registry` per docs/specs/registry.md §Test plan (E1-E2).

use wasl_proto::{AgentCard, AgentId, HostId};
use wasl_registry::{PeerRegistry, RegistryError};

fn agent_id(id: &str) -> AgentId {
    AgentId(id.to_string())
}

fn host_id(id: &str) -> HostId {
    HostId(id.to_string())
}

fn card(id: &str, host: &str) -> AgentCard {
    AgentCard {
        id: agent_id(id),
        host: host_id(host),
        driver: None,
        workstream: None,
        endpoint: None,
    }
}

// ---------------------------------------------------------------------
// E1 — resolve unknown target -> Err(UnknownPeer(id)) with the right id
// ---------------------------------------------------------------------

#[test]
fn e1_resolve_unknown_target_errors_with_the_right_id() {
    let mut registry = PeerRegistry::new();
    registry.register(card("alice", "host-1"));

    let result = registry.resolve(&host_id("host-1"), &agent_id("ghost"));

    assert_eq!(result, Err(RegistryError::UnknownPeer(agent_id("ghost"))));
}

#[test]
fn e1_resolve_unknown_target_on_empty_registry_errors() {
    let registry = PeerRegistry::new();

    let result = registry.resolve(&host_id("host-1"), &agent_id("nobody"));

    assert_eq!(
        result,
        Err(RegistryError::UnknownPeer(agent_id("nobody")))
    );
}

// ---------------------------------------------------------------------
// E2 — RegistryError Display non-empty + mentions the peer id; std::error::Error implemented
// ---------------------------------------------------------------------

#[test]
fn e2_registry_error_display_non_empty_and_mentions_peer_id() {
    let err = RegistryError::UnknownPeer(agent_id("mystery-peer"));
    let text = err.to_string();

    assert!(!text.is_empty(), "Display must be non-empty");
    assert!(
        text.contains("mystery-peer"),
        "Display must mention the peer id: {text}"
    );
}

#[test]
fn e2_registry_error_implements_std_error() {
    fn assert_std_error<E: std::error::Error>(_: &E) {}
    let err = RegistryError::UnknownPeer(agent_id("some-peer"));
    assert_std_error(&err);

    // Exercise the trait object path too, confirming source()/Display are usable
    // through `dyn std::error::Error` as real call sites would use it.
    let boxed: Box<dyn std::error::Error> = Box::new(RegistryError::UnknownPeer(agent_id("boxed-peer")));
    let boxed_text = boxed.to_string();
    assert!(!boxed_text.is_empty());
    assert!(boxed_text.contains("boxed-peer"));
}
