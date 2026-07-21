//! Black-box tests for `wasl_proto`'s `RequestId` identity newtype.
//!
//! `RequestId` is a request-correlation id threaded across A2A by the
//! live-runtime feature's `Causation`/`TurnRequest` types. It mirrors the
//! existing `AgentId`/`HostId` newtypes EXACTLY: a `#[serde(transparent)]`
//! tuple struct over `String` with the same derives and an `as_str`. These
//! cases follow the H1 identity-newtype conventions in `tests/happy.rs`.
//!
//! This file will NOT compile until the code-agent adds `RequestId` to
//! `crates/wasl-proto/src/**` and re-exports it at the crate root — that is
//! the expected red state.

use std::collections::{HashMap, HashSet};

use wasl_proto::RequestId;

// ---------------------------------------------------------------------
// R1 — Construction + as_str() returns the inner string.
// ---------------------------------------------------------------------

#[test]
fn r1_request_id_as_str_returns_inner() {
    let id = RequestId("req-1".to_string());
    assert_eq!(id.as_str(), "req-1");
}

#[test]
fn r1_request_id_public_field_is_the_inner_string() {
    let id = RequestId("req-abc".to_string());
    assert_eq!(id.0, "req-abc");
}

// ---------------------------------------------------------------------
// R2 — serde is TRANSPARENT: serializes to a bare JSON string (not an
// object) and round-trips back equal.
// ---------------------------------------------------------------------

#[test]
fn r2_request_id_serializes_as_bare_string() {
    let id = RequestId("req-1".to_string());
    let json = serde_json::to_string(&id).unwrap();
    assert_eq!(json, "\"req-1\"");
}

#[test]
fn r2_request_id_serializes_not_as_object() {
    let id = RequestId("req-1".to_string());
    let json = serde_json::to_string(&id).unwrap();
    // Transparent tuple newtype must not wrap the value in a JSON object.
    assert!(!json.starts_with('{'));
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(value.is_string());
    assert_eq!(value, serde_json::json!("req-1"));
}

#[test]
fn r2_request_id_round_trips() {
    let id = RequestId("req-42".to_string());
    let json = serde_json::to_string(&id).unwrap();
    let back: RequestId = serde_json::from_str(&json).unwrap();
    assert_eq!(back, id);
}

// ---------------------------------------------------------------------
// R3 — Deserialize from a bare JSON string works; from a JSON object or
// number fails (proving transparent).
// ---------------------------------------------------------------------

#[test]
fn r3_request_id_deserializes_from_bare_string() {
    let id: RequestId = serde_json::from_str("\"req-xyz\"").unwrap();
    assert_eq!(id, RequestId("req-xyz".to_string()));
}

#[test]
fn r3_request_id_deserialize_from_object_fails() {
    let result: Result<RequestId, _> = serde_json::from_str(r#"{"0":"req-1"}"#);
    assert!(
        result.is_err(),
        "a transparent String newtype must not deserialize from a JSON object"
    );
}

#[test]
fn r3_request_id_deserialize_from_number_fails() {
    let result: Result<RequestId, _> = serde_json::from_str("123");
    assert!(
        result.is_err(),
        "a transparent String newtype must not deserialize from a JSON number"
    );
}

// ---------------------------------------------------------------------
// R4 — Derives: Clone/PartialEq/Eq equality + inequality; Hash usable as a
// HashMap/HashSet key.
// ---------------------------------------------------------------------

#[test]
fn r4_request_id_clone_equals_original() {
    let id = RequestId("req-1".to_string());
    let cloned = id.clone();
    assert_eq!(cloned, id);
}

#[test]
fn r4_request_id_equality_and_inequality() {
    let a = RequestId("req-1".to_string());
    let a2 = RequestId("req-1".to_string());
    let b = RequestId("req-2".to_string());
    assert_eq!(a, a2);
    assert_ne!(a, b);
}

#[test]
fn r4_request_id_usable_as_hashset_key() {
    let mut set: HashSet<RequestId> = HashSet::new();
    set.insert(RequestId("req-1".to_string()));
    set.insert(RequestId("req-2".to_string()));
    assert_eq!(set.len(), 2);
    assert!(set.contains(&RequestId("req-1".to_string())));
    assert!(set.contains(&RequestId("req-2".to_string())));
    assert!(!set.contains(&RequestId("req-3".to_string())));
    // Inserting an equal key again does not grow the set.
    set.insert(RequestId("req-1".to_string()));
    assert_eq!(set.len(), 2);
}

#[test]
fn r4_request_id_usable_as_hashmap_key() {
    let mut map: HashMap<RequestId, i32> = HashMap::new();
    map.insert(RequestId("req-1".to_string()), 1);
    map.insert(RequestId("req-2".to_string()), 2);
    assert_eq!(map.get(&RequestId("req-1".to_string())), Some(&1));
    assert_eq!(map.get(&RequestId("req-2".to_string())), Some(&2));
    assert_eq!(map.get(&RequestId("req-3".to_string())), None);
}

// ---------------------------------------------------------------------
// R5 — Debug format is non-empty and contains the inner value.
// ---------------------------------------------------------------------

#[test]
fn r5_request_id_debug_contains_inner_value() {
    let id = RequestId("req-dbg".to_string());
    let rendered = format!("{id:?}");
    assert!(!rendered.is_empty());
    assert!(rendered.contains("req-dbg"));
    assert!(rendered.contains("RequestId"));
}
