use serde::{Deserialize, Serialize};
use wasl_adapter::Delivery;
use wasl_proto::GateDecision;
use wasl_runtime::{
    BusyPolicy, CancelReason, Causation, EventSeq, IdempotencyKey, RuntimeCapabilities,
    ShutdownMode, TargetId, TargetKey, TargetStatus, TurnId, TurnSource,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct InfoResponse {
    pub agent_id: String,
    pub host_id: String,
    pub version: String,
    pub runtime: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TargetSnapshot {
    pub target_id: TargetId,
    pub key: TargetKey,
    pub capabilities: RuntimeCapabilities,
    pub status: TargetStatus,
    pub last_seq: EventSeq,
    pub queued: usize,
    pub native_session_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TargetsSnapshotResponse {
    pub targets: Vec<TargetSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateTargetRequest {
    pub key: TargetKey,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    #[serde(default)]
    pub context: wasl_runtime::SpawnContext,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct HelloFrame {
    pub api_version: u32,
    pub agent_id: String,
    pub host_id: String,
    pub version: String,
    pub target_id: TargetId,
    pub last_seq: EventSeq,
    pub floor: EventSeq,
    pub resume: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TruncatedFrame {
    pub earliest: EventSeq,
    pub latest: EventSeq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitTurnRequest {
    pub delivery: Delivery,
    pub source: TurnSource,
    pub idempotency_key: Option<IdempotencyKey>,
    pub busy_policy: BusyPolicy,
    pub causation: Causation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct DeliveryWire {
    context_id: String,
    from: String,
    kind: wasl_adapter::DeliveryKind,
    text: String,
    data: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct SubmitTurnRequestWire {
    delivery: DeliveryWire,
    source: TurnSource,
    idempotency_key: Option<IdempotencyKey>,
    busy_policy: BusyPolicy,
    causation: Causation,
}

impl From<SubmitTurnRequest> for SubmitTurnRequestWire {
    fn from(value: SubmitTurnRequest) -> Self {
        Self {
            delivery: DeliveryWire {
                context_id: value.delivery.context_id,
                from: value.delivery.from,
                kind: value.delivery.kind,
                text: value.delivery.text,
                data: value.delivery.data,
            },
            source: value.source,
            idempotency_key: value.idempotency_key,
            busy_policy: value.busy_policy,
            causation: value.causation,
        }
    }
}

impl From<SubmitTurnRequestWire> for SubmitTurnRequest {
    fn from(value: SubmitTurnRequestWire) -> Self {
        Self {
            delivery: Delivery {
                context_id: value.delivery.context_id,
                from: value.delivery.from,
                kind: value.delivery.kind,
                text: value.delivery.text,
                data: value.delivery.data,
            },
            source: value.source,
            idempotency_key: value.idempotency_key,
            busy_policy: value.busy_policy,
            causation: value.causation,
        }
    }
}

impl Serialize for SubmitTurnRequest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        SubmitTurnRequestWire::from(self.clone()).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SubmitTurnRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        SubmitTurnRequestWire::deserialize(deserializer).map(Self::from)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CancelRequest {
    pub turn_id: TurnId,
    pub reason: CancelReason,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct GateRequest {
    pub turn_id: TurnId,
    pub decision: GateDecision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ShutdownRequest {
    pub mode: ShutdownMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ErrorBody {
    pub error: String,
    pub detail: String,
}
