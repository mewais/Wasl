//! Thin async client for the Runtime Control API (R4).
//!
//! This module is intentionally small and typed around the public R4 wire
//! vocabulary so `wasl-session` can map each command to exactly one control-plane
//! call.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::StreamExt;
use reqwest::{Response, StatusCode};
use serde::de::DeserializeOwned;
use serde::Serialize;
use wasl_proto::GateDecision;
use wasl_proto::RequestId;
use wasl_runtime::{
    BusyPolicy, CancelReason, Causation, ShutdownMode, TargetKey, TurnEvent, TurnId, TurnReceipt,
    TurnSource,
};
use wasl_runtime_control::{
    CancelRequest, CreateTargetRequest, ErrorBody, GateRequest, ShutdownRequest, TargetSnapshot,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    UnknownTarget(String),
    Http(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownTarget(target) => write!(f, "unknown target: {target}"),
            Self::Http(detail) => write!(f, "http error: {detail}"),
        }
    }
}

impl std::error::Error for ClientError {}

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

pub async fn start(
    base_url: &str,
    key: &str,
    protocol: Option<&str>,
) -> Result<TargetSnapshot, ClientError> {
    let req = CreateTargetRequest {
        key: TargetKey(key.to_string()),
        protocol: protocol.map(ToOwned::to_owned),
        context: Default::default(),
    };
    post_json(&endpoint(base_url, "v1/targets"), &req).await
}

pub async fn send(base_url: &str, target_id: &str, text: &str) -> Result<TurnReceipt, ClientError> {
    let req = SubmitTurnBody {
        delivery: DeliveryBody {
            context_id: target_id.to_string(),
            from: "human".to_string(),
            kind: "message".to_string(),
            text: text.to_string(),
            data: Vec::new(),
        },
        source: TurnSource::LocalHuman,
        idempotency_key: None,
        busy_policy: BusyPolicy::Reject,
        causation: Causation {
            root_request: RequestId(fresh_request_id()),
            parent_request: None,
            path: Vec::new(),
            hop: 0,
        },
    };

    post_json(
        &endpoint(base_url, &format!("v1/targets/{target_id}/turns")),
        &req,
    )
    .await
}

pub async fn attach_once(
    base_url: &str,
    target_id: &str,
    timeout: Duration,
) -> Result<Vec<TurnEvent>, ClientError> {
    let response = reqwest::Client::new()
        .get(endpoint(
            base_url,
            &format!("v1/targets/{target_id}/events"),
        ))
        .send()
        .await
        .map_err(http_err)?;
    let response = ensure_success(response).await?;

    let mut events = Vec::new();
    let mut stream = response.bytes_stream();
    let mut buf = String::new();
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        while let Some(idx) = buf.find("\n\n") {
            let block: String = buf.drain(..idx + 2).collect();
            if let Some(frame) = parse_sse_block(&block) {
                if frame.event == "turn_event" {
                    let event: TurnEvent = serde_json::from_str(&frame.data)
                        .map_err(|e| ClientError::Http(e.to_string()))?;
                    events.push(event);
                }
            }
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        let remaining = deadline - now;

        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(Ok(bytes))) => buf.push_str(&String::from_utf8_lossy(&bytes)),
            Ok(Some(Err(e))) => return Err(http_err(e)),
            Ok(None) | Err(_) => break,
        }
    }

    Ok(events)
}

pub async fn interrupt(base_url: &str, target_id: &str, turn_id: &str) -> Result<(), ClientError> {
    let req = CancelRequest {
        turn_id: TurnId(turn_id.to_string()),
        reason: CancelReason::OperatorRequest,
    };

    post_no_content(
        &endpoint(base_url, &format!("v1/targets/{target_id}/cancel")),
        &req,
    )
    .await
}

pub async fn stop(base_url: &str, target_id: &str, drain: bool) -> Result<(), ClientError> {
    let req = ShutdownRequest {
        mode: if drain {
            ShutdownMode::Drain
        } else {
            ShutdownMode::Immediate
        },
    };

    post_no_content(
        &endpoint(base_url, &format!("v1/targets/{target_id}/shutdown")),
        &req,
    )
    .await
}

pub async fn gate(
    base_url: &str,
    target_id: &str,
    turn_id: &str,
    decision: GateDecision,
) -> Result<(), ClientError> {
    let req = GateRequest {
        turn_id: TurnId(turn_id.to_string()),
        decision,
    };

    post_no_content(
        &endpoint(base_url, &format!("v1/targets/{target_id}/gate")),
        &req,
    )
    .await
}

#[derive(Debug, Clone, Serialize)]
struct SubmitTurnBody {
    delivery: DeliveryBody,
    source: TurnSource,
    idempotency_key: Option<String>,
    busy_policy: BusyPolicy,
    causation: Causation,
}

#[derive(Debug, Clone, Serialize)]
struct DeliveryBody {
    context_id: String,
    from: String,
    kind: String,
    text: String,
    data: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SseFrame {
    event: String,
    data: String,
}

fn parse_sse_block(block: &str) -> Option<SseFrame> {
    let mut event = String::new();
    let mut data_lines = Vec::new();

    for raw_line in block.split('\n') {
        let line = raw_line.trim_end_matches('\r');
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("event:") {
            event = rest.trim().to_string();
        } else if let Some(rest) = line.strip_prefix("data:") {
            data_lines.push(rest.trim().to_string());
        }
    }

    if data_lines.is_empty() {
        return None;
    }

    Some(SseFrame {
        event,
        data: data_lines.join("\n"),
    })
}

fn endpoint(base_url: &str, path: &str) -> String {
    format!("{}/{}", base_url.trim_end_matches('/'), path)
}

async fn post_json<TReq, TResp>(url: &str, body: &TReq) -> Result<TResp, ClientError>
where
    TReq: Serialize + ?Sized,
    TResp: DeserializeOwned,
{
    let response = reqwest::Client::new()
        .post(url)
        .json(body)
        .send()
        .await
        .map_err(http_err)?;
    let response = ensure_success(response).await?;

    response.json::<TResp>().await.map_err(http_err)
}

async fn post_no_content<TReq>(url: &str, body: &TReq) -> Result<(), ClientError>
where
    TReq: Serialize + ?Sized,
{
    let response = reqwest::Client::new()
        .post(url)
        .json(body)
        .send()
        .await
        .map_err(http_err)?;
    ensure_success(response).await.map(|_| ())
}

async fn ensure_success(response: Response) -> Result<Response, ClientError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }

    let body_text = response.text().await.unwrap_or_default();
    let parsed: Option<ErrorBody> = serde_json::from_str(&body_text).ok();

    if status == StatusCode::NOT_FOUND {
        let detail = parsed
            .map(|b| b.detail)
            .filter(|s| !s.is_empty())
            .unwrap_or(body_text);
        return Err(ClientError::UnknownTarget(detail));
    }

    let detail = match parsed {
        Some(body) => format!("{} {}: {}", status.as_u16(), body.error, body.detail),
        None if body_text.is_empty() => format!("{} {}", status.as_u16(), status),
        None => format!("{} {}", status.as_u16(), body_text),
    };
    Err(ClientError::Http(detail))
}

fn fresh_request_id() -> String {
    let seq = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("req-r3-{seq}")
}

fn http_err(err: impl std::error::Error) -> ClientError {
    ClientError::Http(err.to_string())
}
