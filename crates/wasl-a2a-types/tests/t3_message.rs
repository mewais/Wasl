//! T3 — `Message` round-trips incl. optional taskId/contextId absent vs present;
//! JSON contains `"kind":"message"` and camelCase `messageId`/`contextId`.
//! `Message::user_text`/`agent_text` produce the right role + part.

use serde_json::{from_value, to_value};
use wasl_a2a_types::{part_text, Message, Role};

#[test]
fn user_text_ctor_produces_role_and_text_part() {
    let m = Message::user_text("hi", "m1");

    assert_eq!(m.role, Role::User);
    assert_eq!(m.message_id, "m1");
    assert_eq!(m.parts.len(), 1);
    assert_eq!(part_text(&m.parts[0]), Some("hi"));

    let v = to_value(&m).unwrap();
    let obj = v.as_object().unwrap();
    assert_eq!(v["kind"], "message");
    assert_eq!(v["role"], "user");
    assert_eq!(v["messageId"], "m1");
    assert_eq!(v["parts"][0]["kind"], "text");
    assert_eq!(v["parts"][0]["text"], "hi");
    // No task/context supplied -> keys absent.
    assert!(!obj.contains_key("taskId"));
    assert!(!obj.contains_key("contextId"));
    assert!(!obj.contains_key("metadata"));

    let back: Message = from_value(v).unwrap();
    assert_eq!(m, back);
}

#[test]
fn agent_text_ctor_produces_agent_role() {
    let m = Message::agent_text("done", "m3");

    assert_eq!(m.role, Role::Agent);
    assert_eq!(part_text(&m.parts[0]), Some("done"));

    let v = to_value(&m).unwrap();
    let obj = v.as_object().unwrap();
    assert_eq!(v["kind"], "message");
    assert_eq!(v["role"], "agent");
    assert_eq!(v["messageId"], "m3");
    assert!(!obj.contains_key("taskId"));

    let back: Message = from_value(v).unwrap();
    assert_eq!(m, back);
}

#[test]
fn message_with_optional_ids_present_uses_camelcase() {
    let mut m = Message::agent_text("yo", "m2");
    m.task_id = Some("t1".to_string());
    m.context_id = Some("c1".to_string());

    let v = to_value(&m).unwrap();
    assert_eq!(v["kind"], "message");
    assert_eq!(v["role"], "agent");
    assert_eq!(v["messageId"], "m2");
    assert_eq!(v["taskId"], "t1");
    assert_eq!(v["contextId"], "c1");

    let back: Message = from_value(v).unwrap();
    assert_eq!(m, back);
}
