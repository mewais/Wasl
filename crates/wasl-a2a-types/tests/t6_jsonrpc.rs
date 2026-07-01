//! T6 — JSON-RPC: `JsonRpcRequest::new` sets `jsonrpc:"2.0"`; round-trip a
//! `message/send` request (params = MessageSendParams). `JsonRpcResponse::ok`/
//! `err` serialize to the `result` XOR `error` shape and round-trip;
//! `JsonRpcId` accepts a number AND a string.

use serde_json::{from_value, json, to_value};
use wasl_a2a_types::{
    JsonRpcError, JsonRpcId, JsonRpcRequest, JsonRpcResponse, Message, MessageSendParams, Task,
    TaskIdParams, TaskState,
};

#[test]
fn request_new_sets_jsonrpc_and_roundtrips() {
    let params = MessageSendParams {
        message: Message::user_text("hi", "m1"),
        metadata: None,
    };
    let req = JsonRpcRequest::new(JsonRpcId::Num(7), "message/send", params);

    let v = to_value(&req).unwrap();
    assert_eq!(v["jsonrpc"], "2.0");
    assert_eq!(v["id"], 7);
    assert_eq!(v["method"], "message/send");
    assert_eq!(v["params"]["message"]["kind"], "message");
    assert_eq!(v["params"]["message"]["messageId"], "m1");
    // metadata None -> absent.
    assert!(!v["params"].as_object().unwrap().contains_key("metadata"));

    let back: JsonRpcRequest<MessageSendParams> = from_value(v).unwrap();
    assert_eq!(req, back);
}

#[test]
fn response_ok_has_result_and_no_error() {
    let task = Task::new("id", "ctx", TaskState::Completed);
    let resp = JsonRpcResponse::ok(JsonRpcId::Str("abc".to_string()), task);

    let v = to_value(&resp).unwrap();
    let obj = v.as_object().unwrap();

    assert_eq!(v["jsonrpc"], "2.0");
    assert_eq!(v["id"], "abc");
    assert!(obj.contains_key("result"));
    assert!(!obj.contains_key("error"));
    assert_eq!(v["result"]["kind"], "task");

    let back: JsonRpcResponse<Task> = from_value(v).unwrap();
    assert_eq!(resp, back);
}

#[test]
fn response_err_has_error_and_no_result() {
    let err = JsonRpcError {
        code: -32601,
        message: "Method not found".to_string(),
        data: None,
    };
    let resp: JsonRpcResponse<Task> = JsonRpcResponse::err(JsonRpcId::Num(1), err);

    let v = to_value(&resp).unwrap();
    let obj = v.as_object().unwrap();

    assert_eq!(v["jsonrpc"], "2.0");
    assert_eq!(v["id"], 1);
    assert!(obj.contains_key("error"));
    assert!(!obj.contains_key("result"));
    assert_eq!(v["error"]["code"], -32601);
    assert_eq!(v["error"]["message"], "Method not found");

    let back: JsonRpcResponse<Task> = from_value(v).unwrap();
    assert_eq!(resp, back);
}

#[test]
fn jsonrpc_id_accepts_number_and_string() {
    let n = JsonRpcId::Num(42);
    assert_eq!(to_value(&n).unwrap(), json!(42));
    let back_n: JsonRpcId = from_value(json!(42)).unwrap();
    assert_eq!(n, back_n);

    let s = JsonRpcId::Str("id-x".to_string());
    assert_eq!(to_value(&s).unwrap(), json!("id-x"));
    let back_s: JsonRpcId = from_value(json!("id-x")).unwrap();
    assert_eq!(s, back_s);
}

#[test]
fn jsonrpc_error_data_present_and_absent() {
    let err = JsonRpcError {
        code: 100,
        message: "x".to_string(),
        data: Some(json!({ "info": true })),
    };
    let v = to_value(&err).unwrap();
    assert_eq!(v["code"], 100);
    assert_eq!(v["message"], "x");
    assert_eq!(v["data"]["info"], true);
    let back: JsonRpcError = from_value(v).unwrap();
    assert_eq!(err, back);

    let err2 = JsonRpcError {
        code: 1,
        message: "y".to_string(),
        data: None,
    };
    let v2 = to_value(&err2).unwrap();
    assert!(!v2.as_object().unwrap().contains_key("data"));
    let back2: JsonRpcError = from_value(v2).unwrap();
    assert_eq!(err2, back2);
}

#[test]
fn task_id_params_roundtrips() {
    let p = TaskIdParams {
        id: "t9".to_string(),
    };
    let v = to_value(&p).unwrap();
    assert_eq!(v["id"], "t9");
    let back: TaskIdParams = from_value(v).unwrap();
    assert_eq!(p, back);
}
