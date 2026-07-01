//! Black-box happy-path tests for `wasl_registry` per docs/specs/registry.md §Test plan (R1-R8).

use wasl_proto::{AgentCard, AgentId, HostId};
use wasl_registry::{PeerRegistry, Resolved, Route};

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
// R1 — register + get + contains + len/is_empty; register same id overwrites
// ---------------------------------------------------------------------

#[test]
fn r1_register_get_contains_len_is_empty() {
    let mut registry = PeerRegistry::new();
    assert!(registry.is_empty());
    assert_eq!(registry.len(), 0);
    assert!(!registry.contains(&agent_id("alice")));
    assert_eq!(registry.get(&agent_id("alice")), None);

    let alice = card("alice", "host-1");
    registry.register(alice.clone());

    assert!(!registry.is_empty());
    assert_eq!(registry.len(), 1);
    assert!(registry.contains(&agent_id("alice")));
    assert_eq!(registry.get(&agent_id("alice")), Some(&alice));
}

#[test]
fn r1_register_default_is_empty() {
    let registry = PeerRegistry::default();
    assert!(registry.is_empty());
    assert_eq!(registry.len(), 0);
}

#[test]
fn r1_register_same_id_overwrites_len_unchanged_new_card_returned() {
    let mut registry = PeerRegistry::new();
    let v1 = AgentCard {
        id: agent_id("bob"),
        host: host_id("host-1"),
        driver: Some("driver-v1".to_string()),
        workstream: None,
        endpoint: None,
    };
    let v2 = AgentCard {
        id: agent_id("bob"),
        host: host_id("host-2"),
        driver: Some("driver-v2".to_string()),
        workstream: Some("ws-2".to_string()),
        endpoint: Some("http://host-2".to_string()),
    };

    registry.register(v1.clone());
    assert_eq!(registry.len(), 1);
    assert_eq!(registry.get(&agent_id("bob")), Some(&v1));

    registry.register(v2.clone());
    assert_eq!(registry.len(), 1, "overwrite must not add an entry");
    assert_eq!(
        registry.get(&agent_id("bob")),
        Some(&v2),
        "get must return the new card after overwrite"
    );
    assert_ne!(registry.get(&agent_id("bob")), Some(&v1));
}

// ---------------------------------------------------------------------
// R2 — unregister present -> true and gone; unregister absent -> false
// ---------------------------------------------------------------------

#[test]
fn r2_unregister_present_returns_true_and_removes() {
    let mut registry = PeerRegistry::new();
    registry.register(card("carol", "host-1"));
    assert!(registry.contains(&agent_id("carol")));

    let removed = registry.unregister(&agent_id("carol"));

    assert!(removed);
    assert!(!registry.contains(&agent_id("carol")));
    assert_eq!(registry.get(&agent_id("carol")), None);
    assert_eq!(registry.len(), 0);
    assert!(registry.is_empty());
}

#[test]
fn r2_unregister_absent_returns_false() {
    let mut registry = PeerRegistry::new();
    registry.register(card("dave", "host-1"));

    let removed = registry.unregister(&agent_id("nobody"));

    assert!(!removed);
    assert_eq!(registry.len(), 1, "unrelated entry must be untouched");
    assert!(registry.contains(&agent_id("dave")));
}

// ---------------------------------------------------------------------
// R3 — list sorted by id ascending regardless of insertion order
// ---------------------------------------------------------------------

#[test]
fn r3_list_sorted_by_id_ascending_regardless_of_insertion_order() {
    let mut registry = PeerRegistry::new();
    registry.register(card("zoe", "host-1"));
    registry.register(card("alice", "host-2"));
    registry.register(card("mike", "host-1"));

    let listed = registry.list();
    let ids: Vec<&str> = listed.iter().map(|c| c.id.as_str()).collect();

    assert_eq!(ids, vec!["alice", "mike", "zoe"]);
    assert_eq!(listed.len(), 3);
}

#[test]
fn r3_list_empty_registry_is_empty_vec() {
    let registry = PeerRegistry::new();
    assert_eq!(registry.list(), Vec::<&AgentCard>::new());
}

// ---------------------------------------------------------------------
// R4 — by_host returns only that host's cards, sorted; empty for an unknown host
// ---------------------------------------------------------------------

#[test]
fn r4_by_host_returns_only_matching_host_sorted() {
    let mut registry = PeerRegistry::new();
    registry.register(card("zoe", "host-a"));
    registry.register(card("alice", "host-a"));
    registry.register(card("bob", "host-b"));

    let on_a = registry.by_host(&host_id("host-a"));
    let ids: Vec<&str> = on_a.iter().map(|c| c.id.as_str()).collect();

    assert_eq!(ids, vec!["alice", "zoe"]);
    assert!(on_a.iter().all(|c| c.host == host_id("host-a")));
}

#[test]
fn r4_by_host_unknown_host_is_empty() {
    let mut registry = PeerRegistry::new();
    registry.register(card("alice", "host-a"));

    let result = registry.by_host(&host_id("host-does-not-exist"));

    assert_eq!(result, Vec::<&AgentCard>::new());
}

// ---------------------------------------------------------------------
// R5 — resolve local: target on self_host -> Resolved::Local{peer}
// ---------------------------------------------------------------------

#[test]
fn r5_resolve_local_target_on_self_host() {
    let mut registry = PeerRegistry::new();
    registry.register(card("alice", "host-1"));

    let resolved = registry
        .resolve(&host_id("host-1"), &agent_id("alice"))
        .expect("alice is registered");

    assert_eq!(
        resolved,
        Resolved::Local {
            peer: agent_id("alice")
        }
    );
}

// ---------------------------------------------------------------------
// R6 — resolve remote: target on another host -> Resolved::Remote{peer,host} with right host
// ---------------------------------------------------------------------

#[test]
fn r6_resolve_remote_target_on_another_host() {
    let mut registry = PeerRegistry::new();
    registry.register(card("bob", "host-2"));

    let resolved = registry
        .resolve(&host_id("host-1"), &agent_id("bob"))
        .expect("bob is registered");

    assert_eq!(
        resolved,
        Resolved::Remote {
            peer: agent_id("bob"),
            host: host_id("host-2"),
        }
    );
}

// ---------------------------------------------------------------------
// R7 — resolve_many: mix of local+remote+unknown -> resolved list + unknown list, order preserved
// ---------------------------------------------------------------------

#[test]
fn r7_resolve_many_mixed_preserves_order_in_both_lists() {
    let mut registry = PeerRegistry::new();
    registry.register(card("local-1", "host-1"));
    registry.register(card("remote-1", "host-2"));
    registry.register(card("local-2", "host-1"));
    registry.register(card("remote-2", "host-3"));

    let targets = vec![
        agent_id("remote-1"),
        agent_id("ghost-1"),
        agent_id("local-1"),
        agent_id("ghost-2"),
        agent_id("local-2"),
        agent_id("remote-2"),
    ];

    let (resolved, unknown) = registry.resolve_many(&host_id("host-1"), &targets);

    assert_eq!(
        resolved,
        vec![
            Resolved::Remote {
                peer: agent_id("remote-1"),
                host: host_id("host-2"),
            },
            Resolved::Local {
                peer: agent_id("local-1")
            },
            Resolved::Local {
                peer: agent_id("local-2")
            },
            Resolved::Remote {
                peer: agent_id("remote-2"),
                host: host_id("host-3"),
            },
        ],
        "resolved entries must preserve relative input order"
    );

    assert_eq!(
        unknown,
        vec![agent_id("ghost-1"), agent_id("ghost-2")],
        "unknown entries must preserve relative input order"
    );
}

#[test]
fn r7_resolve_many_all_unknown() {
    let registry = PeerRegistry::new();
    let targets = vec![agent_id("x"), agent_id("y")];

    let (resolved, unknown) = registry.resolve_many(&host_id("host-1"), &targets);

    assert_eq!(resolved, Vec::<Resolved>::new());
    assert_eq!(unknown, vec![agent_id("x"), agent_id("y")]);
}

#[test]
fn r7_resolve_many_empty_targets() {
    let mut registry = PeerRegistry::new();
    registry.register(card("alice", "host-1"));

    let (resolved, unknown) = registry.resolve_many(&host_id("host-1"), &[]);

    assert_eq!(resolved, Vec::<Resolved>::new());
    assert_eq!(unknown, Vec::<AgentId>::new());
}

// ---------------------------------------------------------------------
// R8 — serde: Resolved round-trips (Local/Remote tagged snake_case); Route round-trips
// ---------------------------------------------------------------------

#[test]
fn r8_resolved_local_serde_tagged_snake_case() {
    let resolved = Resolved::Local {
        peer: agent_id("alice"),
    };

    let value = serde_json::to_value(&resolved).expect("serialize Resolved::Local");
    assert_eq!(
        value,
        serde_json::json!({
            "route": "local",
            "peer": "alice",
        })
    );

    let back: Resolved = serde_json::from_value(value).expect("deserialize Resolved::Local");
    assert_eq!(back, resolved);
}

#[test]
fn r8_resolved_remote_serde_tagged_snake_case() {
    let resolved = Resolved::Remote {
        peer: agent_id("bob"),
        host: host_id("host-2"),
    };

    let value = serde_json::to_value(&resolved).expect("serialize Resolved::Remote");
    assert_eq!(
        value,
        serde_json::json!({
            "route": "remote",
            "peer": "bob",
            "host": "host-2",
        })
    );

    let back: Resolved = serde_json::from_value(value).expect("deserialize Resolved::Remote");
    assert_eq!(back, resolved);
}

#[test]
fn r8_route_serde_round_trip_both_variants() {
    let cases = [(Route::Local, "\"local\""), (Route::Remote, "\"remote\"")];
    for (route, expected_json) in cases {
        let json = serde_json::to_string(&route).expect("serialize Route");
        assert_eq!(json, expected_json, "serialized form for {route:?}");
        let back: Route = serde_json::from_str(&json).expect("deserialize Route");
        assert_eq!(back, route, "round-trip for {route:?}");
    }
}
