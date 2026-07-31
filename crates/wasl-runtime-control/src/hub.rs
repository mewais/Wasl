use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::{FutureExt, StreamExt};
use wasl_runtime::{
    DeliveryTarget, EventSeq, RuntimeError, RuntimeProvider, StreamItem, TargetId, TargetKey,
    TurnReceipt, TurnRequest,
};

use crate::wire::{
    CancelRequest, CreateTargetRequest, ErrorBody, GateRequest, HelloFrame, InfoResponse,
    ShutdownRequest, SubmitTurnRequest, TargetSnapshot, TargetsSnapshotResponse, TruncatedFrame,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeKind {
    Ephemeral,
    Managed,
}

impl RuntimeKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ephemeral => "ephemeral",
            Self::Managed => "managed",
        }
    }
}

pub struct EventSubscription {
    pub hello: HelloFrame,
    pub stream: wasl_runtime::TurnEventStream,
}

#[derive(Debug, Clone)]
struct TargetEntry {
    key: TargetKey,
    target: Arc<dyn DeliveryTarget>,
    native_session_id: Option<String>,
}

struct Inner {
    provider: Arc<dyn RuntimeProvider>,
    info: InfoResponse,
    known: Mutex<HashMap<TargetId, TargetEntry>>,
}

#[derive(Clone)]
pub struct RuntimeControlHub(Arc<Inner>);

impl RuntimeControlHub {
    pub fn new(
        provider: Arc<dyn RuntimeProvider>,
        agent_id: String,
        host_id: String,
        version: String,
        runtime_kind: RuntimeKind,
    ) -> Self {
        Self(Arc::new(Inner {
            provider,
            info: InfoResponse {
                agent_id,
                host_id,
                version,
                runtime: runtime_kind.as_str().to_owned(),
            },
            known: Mutex::new(HashMap::new()),
        }))
    }

    pub fn info(&self) -> InfoResponse {
        self.0.info.clone()
    }

    pub async fn list_targets(&self) -> Result<TargetsSnapshotResponse, ErrorBody> {
        let ids = self.0.provider.list_targets().await;
        let entries: Vec<TargetEntry> = {
            let known = self.0.known.lock().unwrap();
            if ids.is_empty() {
                known.values().cloned().collect()
            } else {
                ids.into_iter()
                    .filter_map(|id| known.get(&id).cloned())
                    .collect()
            }
        };

        let mut targets = Vec::with_capacity(entries.len());
        for entry in entries {
            targets.push(self.snapshot_from_entry(&entry).await);
        }

        Ok(TargetsSnapshotResponse { targets })
    }

    pub async fn create_target(
        &self,
        req: CreateTargetRequest,
    ) -> Result<TargetSnapshot, ErrorBody> {
        let effective_protocol = req.context.protocol.clone().or_else(|| req.protocol.clone());
        validate_protocol(effective_protocol.as_deref())?;
        let mut context = req.context;
        context.protocol = effective_protocol;

        let target = self
            .0
            .provider
            .target_for_with_context(&req.key, context)
            .await
            .map_err(map_runtime_error)?;
        let target_id = target.target_id().clone();

        let entry = TargetEntry {
            key: req.key,
            target,
            native_session_id: None,
        };

        {
            self.0
                .known
                .lock()
                .unwrap()
                .insert(target_id, entry.clone());
        }

        Ok(self.snapshot_from_entry(&entry).await)
    }

    pub async fn target_snapshot(&self, target_id: &TargetId) -> Option<TargetSnapshot> {
        let entry = self.0.known.lock().unwrap().get(target_id).cloned()?;
        Some(self.snapshot_from_entry(&entry).await)
    }

    pub async fn submit_turn(
        &self,
        target_id: &TargetId,
        req: SubmitTurnRequest,
    ) -> Result<TurnReceipt, ErrorBody> {
        let target = self.get_target(target_id)?;
        let turn = TurnRequest {
            delivery: req.delivery,
            source: req.source,
            idempotency_key: req.idempotency_key,
            causation: req.causation,
        };
        Ok(target.submit_turn(turn, req.busy_policy).await)
    }

    pub async fn cancel(&self, target_id: &TargetId, req: CancelRequest) -> Result<(), ErrorBody> {
        let target = self.get_target(target_id)?;
        target
            .cancel_turn(req.turn_id, req.reason)
            .await
            .map_err(map_runtime_error)
    }

    pub async fn gate(&self, target_id: &TargetId, req: GateRequest) -> Result<(), ErrorBody> {
        let target = self.get_target(target_id)?;
        target
            .resolve_native_gate(req.turn_id, req.decision)
            .await
            .map_err(map_runtime_error)
    }

    pub async fn shutdown(
        &self,
        target_id: &TargetId,
        req: ShutdownRequest,
    ) -> Result<(), ErrorBody> {
        let target = self.get_target(target_id)?;
        target.shutdown(req.mode).await.map_err(map_runtime_error)
    }

    pub async fn subscribe_events(
        &self,
        target_id: &TargetId,
        after_seq: EventSeq,
    ) -> Result<EventSubscription, ErrorBody> {
        let entry = self
            .0
            .known
            .lock()
            .unwrap()
            .get(target_id)
            .cloned()
            .ok_or_else(|| ErrorBody {
                error: "unknown_target".to_string(),
                detail: target_id.0.clone(),
            })?;

        let meta = probe_target_window(entry.target.clone()).await;
        let hello = HelloFrame {
            api_version: 1,
            agent_id: self.0.info.agent_id.clone(),
            host_id: self.0.info.host_id.clone(),
            version: self.0.info.version.clone(),
            target_id: target_id.clone(),
            last_seq: meta.window.last_seq,
            floor: meta.window.floor,
            resume: "snapshot".to_string(),
        };
        let stream = entry.target.events(after_seq).await;

        Ok(EventSubscription { hello, stream })
    }

    fn get_target(&self, target_id: &TargetId) -> Result<Arc<dyn DeliveryTarget>, ErrorBody> {
        self.0
            .known
            .lock()
            .unwrap()
            .get(target_id)
            .map(|entry| entry.target.clone())
            .ok_or_else(|| ErrorBody {
                error: "unknown_target".to_string(),
                detail: target_id.0.clone(),
            })
    }

    async fn snapshot_from_entry(&self, entry: &TargetEntry) -> TargetSnapshot {
        let status = entry.target.status().await;
        let capabilities = entry.target.capabilities();
        let meta = probe_target_window(entry.target.clone()).await;
        TargetSnapshot {
            target_id: entry.target.target_id().clone(),
            key: entry.key.clone(),
            capabilities,
            status,
            last_seq: meta.window.last_seq,
            queued: 0,
            native_session_id: meta
                .native_session_id
                .or_else(|| entry.native_session_id.clone()),
        }
    }
}

fn validate_protocol(protocol: Option<&str>) -> Result<(), ErrorBody> {
    match protocol {
        None | Some("claude") | Some("opencode-acp") | Some("opencode-serve") => Ok(()),
        Some(other) => Err(ErrorBody {
            error: "malformed".to_string(),
            detail: format!("unknown protocol: {other}"),
        }),
    }
}

fn map_runtime_error(err: RuntimeError) -> ErrorBody {
    match err {
        RuntimeError::AdmissionRefused { detail } => ErrorBody {
            error: "admission_refused".to_string(),
            detail,
        },
        RuntimeError::NotFound { detail } => ErrorBody {
            error: "unknown_target".to_string(),
            detail,
        },
        RuntimeError::Unsupported { operation } => ErrorBody {
            error: "unsupported".to_string(),
            detail: operation,
        },
        RuntimeError::GateUnresolvable { detail } => ErrorBody {
            error: "gate_unresolvable".to_string(),
            detail,
        },
        other => ErrorBody {
            error: "malformed".to_string(),
            detail: other.to_string(),
        },
    }
}

#[derive(Debug, Clone, Copy)]
struct TargetWindow {
    floor: EventSeq,
    last_seq: EventSeq,
}

#[derive(Debug, Clone)]
struct TargetProbe {
    window: TargetWindow,
    native_session_id: Option<String>,
}

async fn probe_target_window(target: Arc<dyn DeliveryTarget>) -> TargetProbe {
    let mut stream = target.events(EventSeq(0)).await;
    let mut earliest_seen: Option<EventSeq> = None;
    let mut latest_seen = EventSeq(0);
    let mut native_session_id = None;

    while let Some(next_item) = stream.next().now_or_never() {
        let Some(item) = next_item else {
            break;
        };

        match item {
            StreamItem::Truncated { earliest, latest } => {
                return TargetProbe {
                    window: TargetWindow {
                        floor: EventSeq(earliest.0.saturating_sub(1)),
                        last_seq: latest,
                    },
                    native_session_id,
                };
            }
            StreamItem::Event { seq, event } => {
                earliest_seen.get_or_insert(seq);
                if seq > latest_seen {
                    latest_seen = seq;
                }
                if let wasl_runtime::TurnEvent::SessionReady {
                    native: Some(native),
                } = event
                {
                    native_session_id = Some(native.0);
                }
            }
        }
    }

    let floor = earliest_seen
        .map(|first| EventSeq(first.0.saturating_sub(1)))
        .unwrap_or(EventSeq(0));

    TargetProbe {
        window: TargetWindow {
            floor,
            last_seq: latest_seen,
        },
        native_session_id,
    }
}

pub(crate) fn status_for_error(error_code: &str) -> axum::http::StatusCode {
    match error_code {
        "admission_refused" => axum::http::StatusCode::TOO_MANY_REQUESTS,
        "unknown_target" => axum::http::StatusCode::NOT_FOUND,
        "unsupported" | "gate_unresolvable" => axum::http::StatusCode::CONFLICT,
        _ => axum::http::StatusCode::BAD_REQUEST,
    }
}

pub(crate) fn frame_from_stream_item(item: StreamItem) -> HubSseFrame {
    match item {
        StreamItem::Event { seq, event } => HubSseFrame::TurnEvent { seq, event },
        StreamItem::Truncated { earliest, latest } => {
            HubSseFrame::Truncated(TruncatedFrame { earliest, latest })
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum HubSseFrame {
    TurnEvent {
        seq: EventSeq,
        event: wasl_runtime::TurnEvent,
    },
    Truncated(TruncatedFrame),
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use futures::stream;
    use wasl_adapter::{Delivery, DeliveryKind};
    use wasl_proto::RequestId;
    use wasl_runtime::{
        BusyPolicy, CancelReason, Causation, DeliveryTarget, IdempotencyKey, RuntimeCapabilities,
        RuntimeProvider, ShutdownMode, TargetStatus, TurnReceipt, TurnRequest, TurnSource,
    };

    use super::*;

    #[derive(Debug)]
    struct FakeTarget {
        target_id: TargetId,
        submitted: AtomicUsize,
    }

    #[async_trait]
    impl DeliveryTarget for FakeTarget {
        fn target_id(&self) -> &TargetId {
            &self.target_id
        }

        fn capabilities(&self) -> RuntimeCapabilities {
            RuntimeCapabilities {
                persistent_process: true,
                persistent_conversation: true,
                structured_turns: true,
                streaming_output: false,
                attachable: true,
                interruptible: false,
                structured_provenance: true,
                resumable: true,
            }
        }

        async fn status(&self) -> TargetStatus {
            TargetStatus::Idle {
                since: "2026-07-02T12:00:00.000Z".to_string(),
            }
        }

        async fn submit_turn(&self, _req: TurnRequest, _busy_policy: BusyPolicy) -> TurnReceipt {
            self.submitted.fetch_add(1, Ordering::SeqCst);
            TurnReceipt::Started {
                turn_id: wasl_runtime::TurnId("turn-1".to_string()),
                first_seq: EventSeq(1),
            }
        }

        async fn events(&self, _after_seq: EventSeq) -> wasl_runtime::TurnEventStream {
            let item = StreamItem::Event {
                seq: EventSeq(1),
                event: wasl_runtime::TurnEvent::SessionReady {
                    native: Some(wasl_runtime::NativeSessionId("ses_test".to_string())),
                },
            };
            Box::pin(stream::iter(vec![item]))
        }

        async fn cancel_turn(
            &self,
            _turn_id: wasl_runtime::TurnId,
            _reason: CancelReason,
        ) -> Result<(), RuntimeError> {
            Ok(())
        }

        async fn shutdown(&self, _mode: ShutdownMode) -> Result<(), RuntimeError> {
            Ok(())
        }
    }

    #[async_trait]
    impl wasl_runtime::NativeGateResolver for FakeTarget {
        async fn resolve_native_gate(
            &self,
            _turn_id: wasl_runtime::TurnId,
            _decision: wasl_proto::GateDecision,
        ) -> Result<(), RuntimeError> {
            Ok(())
        }
    }

    #[async_trait]
    impl wasl_runtime::ToolCallAnswerer for FakeTarget {
        async fn answer_tool_call(
            &self,
            _turn_id: wasl_runtime::TurnId,
            _value: String,
        ) -> Result<(), RuntimeError> {
            Ok(())
        }
    }

    struct FakeProvider {
        target: Arc<FakeTarget>,
    }

    #[async_trait]
    impl RuntimeProvider for FakeProvider {
        async fn target_for(
            &self,
            _key: &TargetKey,
        ) -> Result<Arc<dyn DeliveryTarget>, RuntimeError> {
            Ok(self.target.clone())
        }

        async fn list_targets(&self) -> Vec<TargetId> {
            vec![self.target.target_id.clone()]
        }
    }

    fn make_hub() -> RuntimeControlHub {
        let provider = Arc::new(FakeProvider {
            target: Arc::new(FakeTarget {
                target_id: TargetId("tgt-1".to_string()),
                submitted: AtomicUsize::new(0),
            }),
        });
        RuntimeControlHub::new(
            provider,
            "wasl-a".to_string(),
            "host-1".to_string(),
            "0.1.0".to_string(),
            RuntimeKind::Managed,
        )
    }

    #[tokio::test]
    async fn create_and_snapshot_round_trip() {
        let hub = make_hub();
        let snapshot = hub
            .create_target(CreateTargetRequest {
                key: TargetKey("ctx-42".to_string()),
                protocol: Some("opencode-acp".to_string()),
                context: Default::default(),
            })
            .await
            .expect("create should succeed");

        assert_eq!(snapshot.target_id.0, "tgt-1");
        assert_eq!(snapshot.key.0, "ctx-42");
        assert_eq!(snapshot.last_seq.0, 1);
        assert_eq!(snapshot.native_session_id.as_deref(), Some("ses_test"));

        let got = hub
            .target_snapshot(&TargetId("tgt-1".to_string()))
            .await
            .expect("snapshot should exist");
        assert_eq!(got.target_id.0, "tgt-1");
    }

    #[tokio::test]
    async fn submit_turn_uses_underlying_target() {
        let hub = make_hub();
        hub.create_target(CreateTargetRequest {
            key: TargetKey("ctx-42".to_string()),
            protocol: None,
            context: Default::default(),
        })
        .await
        .expect("create should succeed");

        let receipt = hub
            .submit_turn(
                &TargetId("tgt-1".to_string()),
                SubmitTurnRequest {
                    delivery: Delivery {
                        context_id: "ctx-42".to_string(),
                        from: "human".to_string(),
                        kind: DeliveryKind::Message,
                        text: "hello".to_string(),
                        data: vec![],
                    },
                    source: TurnSource::LocalHuman,
                    idempotency_key: Some(IdempotencyKey("k-1".to_string())),
                    busy_policy: BusyPolicy::Reject,
                    causation: Causation {
                        root_request: RequestId("req-1".to_string()),
                        parent_request: None,
                        path: vec![],
                        hop: 0,
                    },
                },
            )
            .await
            .expect("submit should succeed");

        assert!(matches!(receipt, TurnReceipt::Started { .. }));
    }

    #[tokio::test]
    async fn unknown_target_maps_to_error_body() {
        let hub = make_hub();
        let err = hub
            .shutdown(
                &TargetId("missing".to_string()),
                ShutdownRequest {
                    mode: ShutdownMode::Drain,
                },
            )
            .await
            .expect_err("missing target should fail");
        assert_eq!(err.error, "unknown_target");
    }
}
