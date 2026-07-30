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

    if v.get("method").is_some() && v.get("id").is_none() {
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
