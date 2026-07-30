use serde_json::Value;

/// Classification of one JSON-RPC line read during the handshake, relative to
/// the request id currently being awaited (hardening-plan §4.6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandshakeLineClass {
    /// A JSON-RPC notification (has `method`, no `id`). A real ACP server may
    /// legally send one at any time, including mid-handshake; ignore it and
    /// keep reading — it is never the handshake response.
    Notification,
    /// The response we are waiting for: `id` matches `expected_id` AND the
    /// line carries `result` or `error`. Either satisfies the handshake step
    /// (an `error` still ends the read; the caller surfaces it separately).
    ExpectedResponse,
    /// Neither of the above: a response tagged with a DIFFERENT id, a
    /// malformed/non-JSON line, or a line with no `id` and no `method`. A real
    /// protocol anomaly to surface — never silently treated the same as a
    /// benign `Notification`.
    Anomaly,
}

/// Classify one handshake line against the request id currently awaited
/// (hardening-plan §4.6). Pure — no IO, no async. `perform_handshake` loops
/// reading lines (bounded by ONE overall handshake deadline, not a fresh
/// timeout per skipped line) and acts on the classification: skip
/// `Notification`s, accept `ExpectedResponse`, fail the handshake on
/// `Anomaly`.
pub fn classify_handshake_line(line: &str, expected_id: u64) -> HandshakeLineClass {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return HandshakeLineClass::Anomaly;
    }

    let v: Value = match serde_json::from_str(trimmed) {
        Ok(v) => v,
        Err(_) => return HandshakeLineClass::Anomaly,
    };

    // A genuine notification: `method` is a STRING (not merely present --
    // rejects a malformed non-string `method`), no `id`, and NEITHER
    // `result` nor `error` (a real notification never carries either; a line
    // with `method` + `result`/`error` and no `id` is malformed/theoretical,
    // not a benign notification, so it must NOT be silently accepted here).
    if v.get("method").and_then(Value::as_str).is_some()
        && v.get("id").is_none()
        && v.get("result").is_none()
        && v.get("error").is_none()
    {
        return HandshakeLineClass::Notification;
    }

    let Some(id) = v.get("id").and_then(Value::as_u64) else {
        return HandshakeLineClass::Anomaly;
    };

    if v.get("result").is_some() || v.get("error").is_some() {
        return if id == expected_id {
            HandshakeLineClass::ExpectedResponse
        } else {
            HandshakeLineClass::Anomaly
        };
    }

    HandshakeLineClass::Anomaly
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_notification_with_result_or_error_and_no_id_is_anomaly_not_notification() {
        // Found by holistic code review: a real JSON-RPC notification NEVER
        // carries `result`/`error` (those belong to responses only). A line
        // shaped like `{method, result}` with no `id` is malformed/
        // theoretical, not a benign notification -- it must not be silently
        // skipped as one.
        assert_eq!(
            classify_handshake_line(r#"{"method":"turn/completed","result":{}}"#, 2),
            HandshakeLineClass::Anomaly
        );
        assert_eq!(
            classify_handshake_line(r#"{"method":"turn/completed","error":{"message":"x"}}"#, 2),
            HandshakeLineClass::Anomaly
        );
    }

    #[test]
    fn a_non_string_method_is_not_classified_as_a_notification() {
        // `method` present but not a string is malformed; must not be
        // silently accepted as a benign notification.
        assert_eq!(
            classify_handshake_line(r#"{"method":123,"params":{}}"#, 2),
            HandshakeLineClass::Anomaly
        );
    }

    #[test]
    fn a_well_formed_notification_is_still_classified_correctly() {
        // The hardening above must not regress the ordinary, well-formed case.
        assert_eq!(
            classify_handshake_line(r#"{"method":"thread/started","params":{}}"#, 2),
            HandshakeLineClass::Notification
        );
    }

    #[test]
    fn a_matching_id_response_is_still_the_expected_response() {
        assert_eq!(
            classify_handshake_line(r#"{"id":2,"result":{}}"#, 2),
            HandshakeLineClass::ExpectedResponse
        );
        assert_eq!(
            classify_handshake_line(r#"{"id":2,"error":{"message":"boom"}}"#, 2),
            HandshakeLineClass::ExpectedResponse
        );
    }

    #[test]
    fn a_mismatched_id_response_is_anomaly() {
        assert_eq!(
            classify_handshake_line(r#"{"id":3,"result":{}}"#, 2),
            HandshakeLineClass::Anomaly
        );
    }

    #[test]
    fn empty_or_malformed_lines_are_anomaly() {
        assert_eq!(classify_handshake_line("", 2), HandshakeLineClass::Anomaly);
        assert_eq!(
            classify_handshake_line("not json at all", 2),
            HandshakeLineClass::Anomaly
        );
        assert_eq!(
            classify_handshake_line(r#"{"params":{}}"#, 2),
            HandshakeLineClass::Anomaly
        );
    }
}
