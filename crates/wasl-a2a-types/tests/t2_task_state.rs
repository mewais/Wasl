//! T2 — `TaskState` serializes kebab-case (assert `"input-required"` exactly)
//! and round-trips all variants.

use serde_json::{from_value, json, to_value};
use wasl_a2a_types::TaskState;

#[test]
fn input_required_is_exactly_kebab_case() {
    assert_eq!(to_value(TaskState::InputRequired).unwrap(), json!("input-required"));
}

#[test]
fn all_states_serialize_kebab_and_roundtrip() {
    let cases = [
        (TaskState::Submitted, "submitted"),
        (TaskState::Working, "working"),
        (TaskState::InputRequired, "input-required"),
        (TaskState::Completed, "completed"),
        (TaskState::Canceled, "canceled"),
        (TaskState::Failed, "failed"),
    ];

    for (state, expected) in cases {
        let v = to_value(&state).unwrap();
        assert_eq!(v, json!(expected));

        let back: TaskState = from_value(v).unwrap();
        assert_eq!(state, back);
    }
}
