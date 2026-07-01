//! T5 — `AgentCard` round-trips (capabilities camelCase `pushNotifications`;
//! skills with/without examples; camelCase `defaultInputModes`).

use serde_json::{from_value, to_value};
use wasl_a2a_types::{AgentCapabilities, AgentCard, AgentSkill};

#[test]
fn agent_card_roundtrips_with_camelcase_keys() {
    let card = AgentCard {
        name: "Agent".to_string(),
        description: "desc".to_string(),
        url: "https://a/".to_string(),
        version: "1.0".to_string(),
        capabilities: AgentCapabilities {
            streaming: true,
            push_notifications: true,
        },
        default_input_modes: vec!["text/plain".to_string()],
        default_output_modes: vec!["text/plain".to_string()],
        skills: vec![
            AgentSkill {
                id: "s1".to_string(),
                name: "S1".to_string(),
                description: "d1".to_string(),
                tags: vec!["t".to_string()],
                examples: vec!["e".to_string()],
            },
            AgentSkill {
                id: "s2".to_string(),
                name: "S2".to_string(),
                description: "d2".to_string(),
                tags: vec![],
                examples: vec![],
            },
        ],
    };

    let v = to_value(&card).unwrap();

    assert_eq!(v["name"], "Agent");
    assert_eq!(v["url"], "https://a/");
    assert_eq!(v["capabilities"]["streaming"], true);
    assert_eq!(v["capabilities"]["pushNotifications"], true);
    assert_eq!(v["defaultInputModes"][0], "text/plain");
    assert_eq!(v["defaultOutputModes"][0], "text/plain");

    // Skill with examples keeps them.
    assert_eq!(v["skills"][0]["id"], "s1");
    assert_eq!(v["skills"][0]["examples"][0], "e");
    // Skill without examples omits the key (skip_serializing_if empty).
    let skill1 = v["skills"][1].as_object().unwrap();
    assert!(!skill1.contains_key("examples"));

    let back: AgentCard = from_value(v).unwrap();
    assert_eq!(card, back);
}

#[test]
fn agent_capabilities_false_and_empty_skills_roundtrip() {
    let card = AgentCard {
        name: "n".to_string(),
        description: "d".to_string(),
        url: "u".to_string(),
        version: "v".to_string(),
        capabilities: AgentCapabilities {
            streaming: false,
            push_notifications: false,
        },
        default_input_modes: vec![],
        default_output_modes: vec![],
        skills: vec![],
    };

    let v = to_value(&card).unwrap();
    assert_eq!(v["capabilities"]["streaming"], false);
    assert_eq!(v["capabilities"]["pushNotifications"], false);

    let back: AgentCard = from_value(v).unwrap();
    assert_eq!(card, back);
}
