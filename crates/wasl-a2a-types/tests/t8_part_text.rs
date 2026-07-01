//! T8 — `part_text` returns the text for a Text part and None for File/Data.

use serde_json::json;
use wasl_a2a_types::{part_text, FileContent, Part};

#[test]
fn part_text_returns_some_for_text_part() {
    let p = Part::Text {
        text: "hello".to_string(),
    };
    assert_eq!(part_text(&p), Some("hello"));
}

#[test]
fn part_text_returns_none_for_file_and_data_parts() {
    let file = Part::File {
        file: FileContent {
            name: None,
            mime_type: None,
            bytes: Some("AA==".to_string()),
            uri: None,
        },
    };
    assert_eq!(part_text(&file), None);

    let data = Part::Data {
        data: json!({ "k": 1 }),
    };
    assert_eq!(part_text(&data), None);
}
