//! T1 — `Part` round-trips each variant AND the JSON carries the right `"kind"`
//! ("text"/"file"/"data"); `FileContent` omits `bytes`/`uri`/`mimeType` when
//! None (absent keys), keeps camelCase `mimeType`.

use serde_json::{from_value, json, to_value};
use wasl_a2a_types::{FileContent, Part};

#[test]
fn text_part_serializes_with_kind_text() {
    let p = Part::Text {
        text: "hello".to_string(),
    };
    let v = to_value(&p).unwrap();
    assert_eq!(v, json!({ "kind": "text", "text": "hello" }));
    assert_eq!(v["kind"], "text");

    let back: Part = from_value(v).unwrap();
    assert_eq!(p, back);
}

#[test]
fn file_part_serializes_with_kind_file_and_camelcase_mime() {
    let fc = FileContent {
        name: Some("a.txt".to_string()),
        mime_type: Some("text/plain".to_string()),
        bytes: None,
        uri: Some("https://example/y".to_string()),
    };
    let p = Part::File { file: fc };
    let v = to_value(&p).unwrap();

    assert_eq!(v["kind"], "file");
    assert_eq!(v["file"]["name"], "a.txt");
    assert_eq!(v["file"]["mimeType"], "text/plain");
    assert_eq!(v["file"]["uri"], "https://example/y");
    // bytes was None -> must be absent.
    assert!(v["file"].get("bytes").is_none());

    let back: Part = from_value(v).unwrap();
    assert_eq!(p, back);
}

#[test]
fn file_content_omits_none_optionals() {
    let fc = FileContent {
        name: Some("only".to_string()),
        mime_type: None,
        bytes: None,
        uri: None,
    };
    let v = to_value(&fc).unwrap();
    let obj = v.as_object().unwrap();

    assert!(obj.contains_key("name"));
    assert!(!obj.contains_key("mimeType"));
    assert!(!obj.contains_key("bytes"));
    assert!(!obj.contains_key("uri"));

    let back: FileContent = from_value(v).unwrap();
    assert_eq!(fc, back);
}

#[test]
fn file_content_bytes_variant_roundtrips() {
    let fc = FileContent {
        name: None,
        mime_type: Some("application/octet-stream".to_string()),
        bytes: Some("AA==".to_string()),
        uri: None,
    };
    let v = to_value(&fc).unwrap();
    let obj = v.as_object().unwrap();

    assert_eq!(v["mimeType"], "application/octet-stream");
    assert_eq!(v["bytes"], "AA==");
    assert!(!obj.contains_key("name"));
    assert!(!obj.contains_key("uri"));

    let back: FileContent = from_value(v).unwrap();
    assert_eq!(fc, back);
}

#[test]
fn data_part_serializes_with_kind_data() {
    let p = Part::Data {
        data: json!({ "n": 1, "s": "x" }),
    };
    let v = to_value(&p).unwrap();

    assert_eq!(v, json!({ "kind": "data", "data": { "n": 1, "s": "x" } }));
    assert_eq!(v["kind"], "data");

    let back: Part = from_value(v).unwrap();
    assert_eq!(p, back);
}
