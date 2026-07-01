//! T7 — SSE events: `TaskStatusUpdateEvent` serializes `"kind":"status-update"`,
//! the `final` key (not `final_`), camelCase `taskId`/`contextId`;
//! `TaskArtifactUpdateEvent` `"kind":"artifact-update"`. Round-trip both.

use serde_json::{from_value, json, to_value};
use wasl_a2a_types::{TaskArtifactUpdateEvent, TaskState, TaskStatusUpdateEvent};

#[test]
fn status_update_event_kind_final_and_camelcase() {
    let canonical = json!({
        "kind": "status-update",
        "taskId": "t1",
        "contextId": "c1",
        "status": { "state": "working" },
        "final": true
    });

    let ev: TaskStatusUpdateEvent = from_value(canonical).unwrap();
    assert_eq!(ev.task_id, "t1");
    assert_eq!(ev.context_id, "c1");
    assert_eq!(ev.final_, true);
    assert_eq!(ev.status.state, TaskState::Working);

    let v = to_value(&ev).unwrap();
    let obj = v.as_object().unwrap();
    assert_eq!(v["kind"], "status-update");
    assert_eq!(v["taskId"], "t1");
    assert_eq!(v["contextId"], "c1");
    // The wire key must be spelled `final`, never `final_`.
    assert!(obj.contains_key("final"));
    assert!(!obj.contains_key("final_"));
    assert_eq!(v["final"], true);

    let back: TaskStatusUpdateEvent = from_value(v).unwrap();
    assert_eq!(ev, back);
}

#[test]
fn artifact_update_event_kind_and_camelcase() {
    let canonical = json!({
        "kind": "artifact-update",
        "taskId": "t2",
        "contextId": "c2",
        "artifact": {
            "artifactId": "a1",
            "parts": [ { "kind": "text", "text": "hi" } ]
        },
        "append": false,
        "lastChunk": true
    });

    let ev: TaskArtifactUpdateEvent = from_value(canonical).unwrap();
    assert_eq!(ev.task_id, "t2");
    assert_eq!(ev.context_id, "c2");
    assert_eq!(ev.artifact.artifact_id, "a1");
    assert_eq!(ev.append, false);
    assert_eq!(ev.last_chunk, true);

    let v = to_value(&ev).unwrap();
    assert_eq!(v["kind"], "artifact-update");
    assert_eq!(v["taskId"], "t2");
    assert_eq!(v["contextId"], "c2");
    assert_eq!(v["append"], false);
    assert_eq!(v["lastChunk"], true);
    assert_eq!(v["artifact"]["artifactId"], "a1");
    assert_eq!(v["artifact"]["parts"][0]["kind"], "text");

    let back: TaskArtifactUpdateEvent = from_value(v).unwrap();
    assert_eq!(ev, back);
}
