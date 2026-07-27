#![forbid(unsafe_code)]

mod hub;
mod wire;

use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::{stream, Stream, StreamExt};

use hub::{frame_from_stream_item, status_for_error, HubSseFrame};
pub use hub::{EventSubscription, RuntimeControlHub, RuntimeKind};
pub use wire::{
    CancelRequest, CreateTargetRequest, ErrorBody, GateRequest, HelloFrame, InfoResponse,
    ShutdownRequest, SubmitTurnRequest, TargetSnapshot, TargetsSnapshotResponse, TruncatedFrame,
};

#[derive(Clone)]
struct AppState {
    hub: RuntimeControlHub,
}

#[derive(Debug, serde::Deserialize)]
struct EventsQuery {
    after_seq: Option<u64>,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    body: ErrorBody,
}

impl ApiError {
    fn from_body(body: ErrorBody) -> Self {
        Self {
            status: status_for_error(&body.error),
            body,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

pub fn router(hub: RuntimeControlHub) -> Router {
    Router::new()
        .route("/v1/info", get(info_handler))
        .route(
            "/v1/targets",
            get(list_targets_handler).post(create_target_handler),
        )
        .route("/v1/targets/:target_id", get(target_handler))
        .route("/v1/targets/:target_id/events", get(events_handler))
        .route("/v1/targets/:target_id/turns", post(submit_turn_handler))
        .route("/v1/targets/:target_id/cancel", post(cancel_handler))
        .route("/v1/targets/:target_id/gate", post(gate_handler))
        .route("/v1/targets/:target_id/shutdown", post(shutdown_handler))
        .with_state(AppState { hub })
}

pub async fn serve(hub: RuntimeControlHub, addr: SocketAddr) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(hub)).await
}

async fn info_handler(State(state): State<AppState>) -> Json<InfoResponse> {
    Json(state.hub.info())
}

async fn list_targets_handler(
    State(state): State<AppState>,
) -> Result<Json<TargetsSnapshotResponse>, ApiError> {
    state
        .hub
        .list_targets()
        .await
        .map(Json)
        .map_err(ApiError::from_body)
}

async fn create_target_handler(
    State(state): State<AppState>,
    Json(req): Json<CreateTargetRequest>,
) -> Result<Json<TargetSnapshot>, ApiError> {
    state
        .hub
        .create_target(req)
        .await
        .map(Json)
        .map_err(ApiError::from_body)
}

async fn target_handler(
    State(state): State<AppState>,
    Path(target_id): Path<String>,
) -> Result<Json<TargetSnapshot>, ApiError> {
    state
        .hub
        .target_snapshot(&wasl_runtime::TargetId(target_id.clone()))
        .await
        .map(Json)
        .ok_or_else(|| {
            ApiError::from_body(ErrorBody {
                error: "unknown_target".to_string(),
                detail: target_id,
            })
        })
}

async fn events_handler(
    State(state): State<AppState>,
    Path(target_id): Path<String>,
    Query(query): Query<EventsQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let after_seq = query.after_seq.unwrap_or(0);
    let subscription = state
        .hub
        .subscribe_events(
            &wasl_runtime::TargetId(target_id),
            wasl_runtime::EventSeq(after_seq),
        )
        .await
        .map_err(ApiError::from_body)?;

    Ok(subscription_to_sse(subscription))
}

fn subscription_to_sse(
    subscription: EventSubscription,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let hello_json =
        serde_json::to_string(&subscription.hello).expect("hello frame is serializable");
    let hello_event = Event::default().event("hello").data(hello_json);
    let hello_stream = stream::once(async move { Ok::<Event, Infallible>(hello_event) });

    let event_stream = subscription.stream.map(|item| {
        let frame = frame_from_stream_item(item);
        let event = match frame {
            HubSseFrame::TurnEvent { seq, event } => Event::default()
                .event("turn_event")
                .id(seq.0.to_string())
                .data(serde_json::to_string(&event).expect("turn_event payload is serializable")),
            HubSseFrame::Truncated(frame) => Event::default()
                .event("truncated")
                .data(serde_json::to_string(&frame).expect("truncated payload is serializable")),
        };
        Ok::<Event, Infallible>(event)
    });

    Sse::new(hello_stream.chain(event_stream)).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text(": ka"),
    )
}

async fn submit_turn_handler(
    State(state): State<AppState>,
    Path(target_id): Path<String>,
    Json(req): Json<SubmitTurnRequest>,
) -> Result<Json<wasl_runtime::TurnReceipt>, ApiError> {
    state
        .hub
        .submit_turn(&wasl_runtime::TargetId(target_id), req)
        .await
        .map(Json)
        .map_err(ApiError::from_body)
}

async fn cancel_handler(
    State(state): State<AppState>,
    Path(target_id): Path<String>,
    Json(req): Json<CancelRequest>,
) -> Result<StatusCode, ApiError> {
    state
        .hub
        .cancel(&wasl_runtime::TargetId(target_id), req)
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(ApiError::from_body)
}

async fn gate_handler(
    State(state): State<AppState>,
    Path(target_id): Path<String>,
    Json(req): Json<GateRequest>,
) -> Result<StatusCode, ApiError> {
    state
        .hub
        .gate(&wasl_runtime::TargetId(target_id), req)
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(ApiError::from_body)
}

async fn shutdown_handler(
    State(state): State<AppState>,
    Path(target_id): Path<String>,
    Json(req): Json<ShutdownRequest>,
) -> Result<StatusCode, ApiError> {
    state
        .hub
        .shutdown(&wasl_runtime::TargetId(target_id), req)
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(ApiError::from_body)
}
