//! T4 — `Task` round-trips (with + without artifacts/history); JSON contains
//! `"kind":"task"`, camelCase `contextId`; empty artifacts/history omitted.
//! `Task::new` + `Artifact::text` constructors. Also covers `TaskStatus` and
//! `Artifact` optional fields.

use serde_json::{from_value, to_value};
use wasl_a2a_types::{part_text, Artifact, Message, Part, Task, TaskState, TaskStatus};

#[test]
fn task_new_is_empty_and_omits_collections() {
    let t = Task::new("id1", "ctx1", TaskState::Submitted);

    let v = to_value(&t).unwrap();
    let obj = v.as_object().unwrap();

    assert_eq!(v["kind"], "task");
    assert_eq!(v["id"], "id1");
    assert_eq!(v["contextId"], "ctx1");
    assert_eq!(v["status"]["state"], "submitted");
    // Empty artifacts/history must be absent.
    assert!(!obj.contains_key("artifacts"));
    assert!(!obj.contains_key("history"));
    assert!(!obj.contains_key("metadata"));

    let back: Task = from_value(v).unwrap();
    assert_eq!(t, back);
}

#[test]
fn artifact_text_ctor_and_task_with_content() {
    let art = Artifact::text("a1", "result");
    assert_eq!(art.artifact_id, "a1");
    assert_eq!(art.parts.len(), 1);
    assert_eq!(part_text(&art.parts[0]), Some("result"));

    let mut t = Task::new("id2", "ctx2", TaskState::Completed);
    t.artifacts.push(Artifact::text("a1", "result"));
    t.history.push(Message::user_text("q", "m1"));

    let v = to_value(&t).unwrap();
    let obj = v.as_object().unwrap();

    assert_eq!(v["kind"], "task");
    assert!(obj.contains_key("artifacts"));
    assert!(obj.contains_key("history"));
    assert_eq!(v["artifacts"][0]["artifactId"], "a1");
    assert_eq!(v["artifacts"][0]["parts"][0]["text"], "result");
    assert_eq!(v["history"][0]["kind"], "message");

    let back: Task = from_value(v).unwrap();
    assert_eq!(t, back);
}

#[test]
fn artifact_optional_fields_present_and_absent() {
    let a = Artifact {
        artifact_id: "a2".to_string(),
        name: Some("out".to_string()),
        description: Some("d".to_string()),
        parts: vec![Part::Text {
            text: "x".to_string(),
        }],
    };
    let v = to_value(&a).unwrap();
    assert_eq!(v["artifactId"], "a2");
    assert_eq!(v["name"], "out");
    assert_eq!(v["description"], "d");
    let back: Artifact = from_value(v).unwrap();
    assert_eq!(a, back);

    let a2 = Artifact {
        artifact_id: "a3".to_string(),
        name: None,
        description: None,
        parts: vec![],
    };
    let v2 = to_value(&a2).unwrap();
    let obj = v2.as_object().unwrap();
    assert_eq!(v2["artifactId"], "a3");
    assert!(!obj.contains_key("name"));
    assert!(!obj.contains_key("description"));
    let back2: Artifact = from_value(v2).unwrap();
    assert_eq!(a2, back2);
}

#[test]
fn task_status_optional_message_and_timestamp() {
    let s = TaskStatus {
        state: TaskState::Working,
        message: Some(Message::agent_text("w", "m")),
        timestamp: Some("2026-07-01T00:00:00Z".to_string()),
    };
    let v = to_value(&s).unwrap();
    assert_eq!(v["state"], "working");
    assert_eq!(v["timestamp"], "2026-07-01T00:00:00Z");
    assert_eq!(v["message"]["kind"], "message");
    let back: TaskStatus = from_value(v).unwrap();
    assert_eq!(s, back);

    let s2 = TaskStatus {
        state: TaskState::Failed,
        message: None,
        timestamp: None,
    };
    let v2 = to_value(&s2).unwrap();
    let obj = v2.as_object().unwrap();
    assert_eq!(v2["state"], "failed");
    assert!(!obj.contains_key("message"));
    assert!(!obj.contains_key("timestamp"));
    let back2: TaskStatus = from_value(v2).unwrap();
    assert_eq!(s2, back2);
}
