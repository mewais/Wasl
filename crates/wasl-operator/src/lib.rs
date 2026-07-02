//! wasl-operator — the node's operator surface (W3): the serving side of the
//! versioned `/v1` operator wire contract in `docs/specs/operator-api.md`.
//!
//! Three pieces: an `OperatorHub` broker (pending decidable-gate table +
//! broadcast fan-out + a blocking decision rendezvous), an `OperatorSurfacer`
//! implementing `wasl_server::Surfacer` (Tier-3: publish the gate, block until
//! an authority POSTs a decision or the timeout dismisses it), and a `TeeSink`
//! wrapping any inner `wasl_audit::Sink` (durable append first, then broadcast
//! — the observation feed). Consumer-agnostic by construction: nothing in this
//! crate names, assumes, or links any consumer.
//!
//! See `docs/specs/operator.md` for the authoritative contract.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::{stream, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;

use wasl_audit::{AuditEntry, AuditError, Sink};
use wasl_gate::{GateMode, GateOutcome};
use wasl_proto::{AgentId, GateRequest, HostId};

// ---------------------------------------------------------------------
// 1. Types
// ---------------------------------------------------------------------

/// The node identity served at `/v1/info` and inside `hello` frames.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub agent_id: AgentId,
    pub host_id: HostId,
    pub version: String,
}

/// Hub tuning. Defaults: `decision_timeout` = 120 s (a human answers in
/// minutes, not ms; W2 config overrides), `channel_capacity` = 256 broadcast
/// frames (see the lag policy in `docs/specs/operator.md`).
#[derive(Debug, Clone, Copy)]
pub struct HubConfig {
    pub decision_timeout: Duration,
    pub channel_capacity: usize,
}

impl Default for HubConfig {
    fn default() -> Self {
        HubConfig {
            decision_timeout: Duration::from_secs(120),
            channel_capacity: 256,
        }
    }
}

/// Injectable wall clock (deterministic tests). Default [`SystemClock`].
pub trait Clock: Send + Sync {
    fn now(&self) -> SystemTime;
}

/// The real wall clock.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// Injectable gate-id source (deterministic tests). Default [`CounterGateIds`]
/// yields `"gate-1"`, `"gate-2"`, ... (the `CounterIds` idiom from
/// `wasl-a2a-server`, single prefix).
pub trait GateIds: Send + Sync {
    fn next(&self) -> String;
}

/// Default [`GateIds`]: a single monotonic counter → `"gate-{n}"`, starting at 1.
#[derive(Default)]
pub struct CounterGateIds(AtomicU64);

impl GateIds for CounterGateIds {
    fn next(&self) -> String {
        let n = self.0.fetch_add(1, Ordering::SeqCst) + 1;
        format!("gate-{n}")
    }
}

/// Who/how a gate resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DecidedBy {
    Operator,
    Elicitation,
    Timeout,
}

/// `POST /v1/gates/{id}/decision` body. `reason` is only meaningful with
/// `Denied` (a `Denied` with no reason records "denied by operator").
#[derive(Debug, Clone, Deserialize)]
pub struct DecisionBody {
    pub outcome: DecisionOutcome,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DecisionOutcome {
    Approved,
    Denied,
    Dismissed,
}

/// `DecisionBody` → `GateOutcome`: `Approved` → `Approved`; `Denied` →
/// `Denied{reason.unwrap_or("denied by operator")}`; `Dismissed` → `Dismissed`.
fn outcome_from_decision(body: &DecisionBody) -> GateOutcome {
    match body.outcome {
        DecisionOutcome::Approved => GateOutcome::Approved,
        DecisionOutcome::Denied => GateOutcome::Denied {
            reason: body
                .reason
                .clone()
                .unwrap_or_else(|| "denied by operator".to_string()),
        },
        DecisionOutcome::Dismissed => GateOutcome::Dismissed,
    }
}

/// The wire `reason` for a `GateDecided` frame: `Some(..)` iff `outcome` is
/// `Denied`, else `None`.
fn frame_reason(outcome: &GateOutcome) -> Option<String> {
    match outcome {
        GateOutcome::Denied { reason } => Some(reason.clone()),
        GateOutcome::Approved | GateOutcome::Dismissed => None,
    }
}

/// `GateOutcome` → the wire `DecisionOutcome` (per `wasl_gate::outcome_of`'s
/// mapping, used by `resolve_announced`).
fn decision_outcome_of(outcome: &GateOutcome) -> DecisionOutcome {
    match outcome {
        GateOutcome::Approved => DecisionOutcome::Approved,
        GateOutcome::Denied { .. } => DecisionOutcome::Denied,
        GateOutcome::Dismissed => DecisionOutcome::Dismissed,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecideError {
    UnknownGate,
    AlreadyDecided,
}

impl fmt::Display for DecideError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecideError::UnknownGate => write!(f, "unknown gate"),
            DecideError::AlreadyDecided => write!(f, "gate already decided"),
        }
    }
}

impl std::error::Error for DecideError {}

/// One broadcastable frame, paired with its node-global sequence via
/// [`SeqFrame`]. Variants serialize to EXACTLY the `data:` payloads in
/// `operator-api.md` (untagged: no variant-name wrapper — the SSE `event:`
/// name comes from [`Frame::event_name`], the payload does not repeat it).
/// `hello` is connection-local, built per subscription in the SSE handler,
/// NOT a `Frame` variant and NOT broadcast.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Frame {
    GateRequested {
        gate_id: String,
        mode: GateMode,
        decidable: bool,
        request: GateRequest,
        requested_at: String,
        timeout_ms: u64,
        deadline: String,
    },
    GateDecided {
        gate_id: String,
        outcome: DecisionOutcome,
        decided_by: DecidedBy,
        reason: Option<String>,
        decided_at: String,
    },
    Audit {
        entry: AuditEntry,
    },
}

impl Frame {
    /// The SSE `event:` name for this frame (the payload never repeats it).
    pub fn event_name(&self) -> &'static str {
        match self {
            Frame::GateRequested { .. } => "gate_requested",
            Frame::GateDecided { .. } => "gate_decided",
            Frame::Audit { .. } => "audit",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SeqFrame {
    pub seq: u64,
    pub frame: Frame,
}

/// The `GET /v1/gates` body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GatesSnapshot {
    pub last_seq: u64,
    pub gates: Vec<PendingGateSnapshot>,
}

/// One entry in [`GatesSnapshot`].
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PendingGateSnapshot {
    pub gate_id: String,
    pub request: GateRequest,
    pub requested_at: String,
    pub age_ms: u64,
    pub deadline: String,
}

// ---------------------------------------------------------------------
// 2. Timestamp formatting — RFC 3339 UTC millis, std-only (no chrono dep).
// ---------------------------------------------------------------------

/// Render `t` as RFC 3339 UTC with millisecond precision, e.g.
/// `"2026-07-02T12:00:00.000Z"`.
fn fmt_rfc3339(t: SystemTime) -> String {
    // `Clock::now()` is always post-epoch in practice (a real wall clock, or
    // a test's fake clock seeded at a post-epoch instant); clamp defensively
    // rather than panic so this function is total.
    let dur = t.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let millis_total = dur.as_millis();
    let secs = (millis_total / 1000) as i64;
    let millis = (millis_total % 1000) as u32;
    let days = secs / 86_400;
    let secs_of_day = secs % 86_400;
    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

/// Convert days-since-`1970-01-01` to a proleptic-Gregorian `(year, month,
/// day)`. `days` is always non-negative here (see `fmt_rfc3339`'s clamp), so
/// the classic algorithm's negative-`era` case never applies and is omitted.
///
/// Branchless port of Howard Hinnant's `civil_from_days`
/// (<https://howardhinnant.github.io/date_algorithms.html#civil_from_days>):
/// the month/year "carry" the reference algorithm expresses as `if`/`else` is
/// folded into integer div/mod so there is no untaken branch to leave
/// uncovered.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11], month index counted from March
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let month = ((mp + 2) % 12 + 1) as u32; // [1, 12]
    let year = y + mp / 10; // +1 iff the date fell in Jan/Feb
    (year, month, day)
}

// ---------------------------------------------------------------------
// 3. OperatorHub
// ---------------------------------------------------------------------

struct PendingGate {
    request: GateRequest,
    requested_at: SystemTime,
    deadline: SystemTime,
    waiter: std::sync::mpsc::SyncSender<GateOutcome>,
}

/// The handle returned by [`OperatorHub::open_gate`] and consumed by
/// [`OperatorHub::wait`]. `gate_id` is public (callers need it, e.g. to log
/// or to call `timeout_gate` directly); the receiving end of the rendezvous
/// channel is private — only `wait` may drain it.
pub struct PendingWait {
    pub gate_id: String,
    rx: std::sync::mpsc::Receiver<GateOutcome>,
}

struct Inner {
    info: NodeInfo,
    cfg: HubConfig,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn GateIds>,
    seq: Mutex<u64>,
    tx: broadcast::Sender<SeqFrame>,
    pending: Mutex<HashMap<String, PendingGate>>,
    resolved: Mutex<HashSet<String>>,
}

impl Inner {
    /// Assign the next frame sequence and broadcast `frame` — a pure
    /// ring-buffer write, synchronous and non-blocking, safe from any
    /// thread. Assigning the sequence and sending happen under one lock so
    /// concurrent publishers can never be observed out of order.
    fn publish(&self, frame: Frame) -> u64 {
        let mut seq = self.seq.lock().unwrap();
        *seq += 1;
        let seq_val = *seq;
        // `send`'s only error is "no active subscribers" — an ordinary state
        // (e.g. before any SSE client has connected), infallible per spec.
        let _ = self.tx.send(SeqFrame {
            seq: seq_val,
            frame,
        });
        seq_val
    }
}

/// Remove `gate_id` from `pending`, or classify why it's absent: `resolved`
/// (the resolved-id set) distinguishes "already decided" (404-vs-409 honesty)
/// from "never existed as a decidable gate". Shared by `decide` and
/// `timeout_gate` — both must classify absence identically.
fn take_pending_or_classify(
    pending: &mut HashMap<String, PendingGate>,
    resolved: &Mutex<HashSet<String>>,
    gate_id: &str,
) -> Result<PendingGate, DecideError> {
    match pending.remove(gate_id) {
        Some(gate) => Ok(gate),
        None => {
            if resolved.lock().unwrap().contains(gate_id) {
                Err(DecideError::AlreadyDecided)
            } else {
                Err(DecideError::UnknownGate)
            }
        }
    }
}

/// The operator hub: a pending decidable-gate table + broadcast fan-out + a
/// blocking decision rendezvous. Cheap to clone (`Arc<Inner>`); every clone
/// shares the same table, sequence counter, and broadcast channel.
#[derive(Clone)]
pub struct OperatorHub(Arc<Inner>);

impl OperatorHub {
    /// A hub using the real [`SystemClock`] and default [`CounterGateIds`].
    pub fn new(info: NodeInfo, cfg: HubConfig) -> Self {
        Self::with_seams(
            info,
            cfg,
            Arc::new(SystemClock),
            Arc::new(CounterGateIds::default()),
        )
    }

    /// A hub with injected clock/id seams (deterministic tests).
    pub fn with_seams(
        info: NodeInfo,
        cfg: HubConfig,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn GateIds>,
    ) -> Self {
        let (tx, _rx) = broadcast::channel(cfg.channel_capacity);
        OperatorHub(Arc::new(Inner {
            info,
            cfg,
            clock,
            ids,
            seq: Mutex::new(0),
            tx,
            pending: Mutex::new(HashMap::new()),
            resolved: Mutex::new(HashSet::new()),
        }))
    }

    pub fn info(&self) -> NodeInfo {
        self.0.info.clone()
    }

    /// `(last_seq, rx)`: `last_seq` is the sequence of the last frame
    /// published before this call (0 if none) — the value a `hello` frame
    /// carries. Locking the sequence across both the read and the receiver's
    /// creation means a concurrent publish can't land in the gap between
    /// them.
    pub fn subscribe(&self) -> (u64, broadcast::Receiver<SeqFrame>) {
        let seq = self.0.seq.lock().unwrap();
        let rx = self.0.tx.subscribe();
        (*seq, rx)
    }

    /// Mint a gate id, open a capacity-1 rendezvous channel, register it as a
    /// pending DECIDABLE gate (inserted before the frame is broadcast, so a
    /// `decide`/timeout can never race ahead of the gate's own visibility),
    /// and broadcast `GateRequested{decidable: true, ..}`.
    pub fn open_gate(&self, mode: GateMode, req: &GateRequest) -> PendingWait {
        let gate_id = self.0.ids.next();
        let (tx, rx) = std::sync::mpsc::sync_channel::<GateOutcome>(1);
        let requested_at = self.0.clock.now();
        let deadline = requested_at + self.0.cfg.decision_timeout;
        {
            let mut pending = self.0.pending.lock().unwrap();
            pending.insert(
                gate_id.clone(),
                PendingGate {
                    request: req.clone(),
                    requested_at,
                    deadline,
                    waiter: tx,
                },
            );
        }
        self.publish_requested(&gate_id, mode, req, true, requested_at, deadline);
        PendingWait { gate_id, rx }
    }

    /// Block on the rendezvous channel until a decision arrives or
    /// `decision_timeout` elapses (`recv_timeout` is monotonic — the wire
    /// timestamps are informational only).
    pub fn wait(&self, w: PendingWait) -> GateOutcome {
        match w.rx.recv_timeout(self.0.cfg.decision_timeout) {
            Ok(outcome) => outcome,
            Err(_timeout_or_disconnected) => {
                // Resolve the gate as a timeout dismissal if it is still
                // pending. `Err(AlreadyDecided)` here means a `decide` call
                // won the race between the timeout firing and this call
                // taking the `pending` lock — ignored, because `decide`
                // sends on the waiter channel and broadcasts `GateDecided`
                // *while holding that same lock* (see `decide` /
                // `take_pending_or_classify`), so "already decided" implies
                // the outcome is already buffered in the channel below.
                let _ = self.timeout_gate(&w.gate_id);
                // RACE ARM (the `Ok(outcome)` case is verified-unreachable
                // under the frozen test suite — the one permitted core
                // exception per operator.md's coverage bar; see AGENTS.md).
                // Either the `timeout_gate` call above just resolved the
                // gate itself (nothing was ever sent — this channel is
                // empty/disconnected) or a decision raced in per the
                // invariant above and is already sitting here; either way
                // this never blocks.
                w.rx.try_recv().unwrap_or(GateOutcome::Dismissed)
            }
        }
    }

    /// Resolve a decidable pending gate. Exactly one concurrent caller wins:
    /// under the `pending` lock, remove the gate (absent → classify via
    /// `resolved`), mark it resolved, hand the outcome to the waiter
    /// (`try_send` on a capacity-1 channel with single-decider-by-removal
    /// never blocks/fills; a `Disconnected` waiter is ignored — the frame
    /// still tells observers), and broadcast `GateDecided{decided_by:
    /// Operator}` — all before releasing the lock.
    pub fn decide(&self, gate_id: &str, body: DecisionBody) -> Result<(), DecideError> {
        let outcome = outcome_from_decision(&body);
        let mut pending = self.0.pending.lock().unwrap();
        let gate = take_pending_or_classify(&mut pending, &self.0.resolved, gate_id)?;
        self.0.resolved.lock().unwrap().insert(gate_id.to_string());
        let reason = frame_reason(&outcome);
        let _ = gate.waiter.try_send(outcome);
        self.publish_decided(gate_id, body.outcome, DecidedBy::Operator, reason);
        Ok(())
    }

    /// `decide`'s twin for the timeout path: same lock discipline, but no
    /// waiter send (the waiter IS the caller — see `wait`).
    pub fn timeout_gate(&self, gate_id: &str) -> Result<(), DecideError> {
        let mut pending = self.0.pending.lock().unwrap();
        let _gate = take_pending_or_classify(&mut pending, &self.0.resolved, gate_id)?;
        self.0.resolved.lock().unwrap().insert(gate_id.to_string());
        self.publish_decided(
            gate_id,
            DecisionOutcome::Dismissed,
            DecidedBy::Timeout,
            None,
        );
        Ok(())
    }

    /// Observe-only mirror hook (W4 uses it for Tier-1 elicitation gates):
    /// mint an id, broadcast `GateRequested{decidable: false}`, do NOT touch
    /// `pending` — it never appears in `/v1/gates` and `decide` on it always
    /// yields `UnknownGate`/404.
    pub fn announce(&self, mode: GateMode, req: &GateRequest) -> String {
        let gate_id = self.0.ids.next();
        let requested_at = self.0.clock.now();
        let deadline = requested_at + self.0.cfg.decision_timeout;
        self.publish_requested(&gate_id, mode, req, false, requested_at, deadline);
        gate_id
    }

    /// Broadcast the `GateDecided` matching an announced gate's resolution
    /// (per `wasl_gate::outcome_of`'s mapping); infallible, no table.
    pub fn resolve_announced(&self, gate_id: &str, outcome: &GateOutcome, decided_by: DecidedBy) {
        self.publish_decided(
            gate_id,
            decision_outcome_of(outcome),
            decided_by,
            frame_reason(outcome),
        );
    }

    /// The `GET /v1/gates` body: pending decidable gates, ascending
    /// `requested_at` then `gate_id`, plus the frame sequence at snapshot
    /// time.
    pub fn snapshot(&self) -> GatesSnapshot {
        let now = self.0.clock.now();
        let pending = self.0.pending.lock().unwrap();
        let mut entries: Vec<(&String, &PendingGate)> = pending.iter().collect();
        entries.sort_by(|a, b| {
            a.1.requested_at
                .cmp(&b.1.requested_at)
                .then_with(|| a.0.cmp(b.0))
        });
        let gates = entries
            .into_iter()
            .map(|(gate_id, gate)| PendingGateSnapshot {
                gate_id: gate_id.clone(),
                request: gate.request.clone(),
                requested_at: fmt_rfc3339(gate.requested_at),
                age_ms: now
                    .duration_since(gate.requested_at)
                    .unwrap_or(Duration::ZERO)
                    .as_millis() as u64,
                deadline: fmt_rfc3339(gate.deadline),
            })
            .collect();
        drop(pending);
        let last_seq = *self.0.seq.lock().unwrap();
        GatesSnapshot { last_seq, gates }
    }

    /// Broadcast an `Audit` frame for `entry`; infallible.
    pub fn publish_audit(&self, entry: &AuditEntry) {
        self.0.publish(Frame::Audit {
            entry: entry.clone(),
        });
    }

    /// Shared `GateRequested` builder for `open_gate`/`announce` (they differ
    /// only in `decidable` and whether the gate enters `pending`).
    fn publish_requested(
        &self,
        gate_id: &str,
        mode: GateMode,
        req: &GateRequest,
        decidable: bool,
        requested_at: SystemTime,
        deadline: SystemTime,
    ) -> u64 {
        let timeout_ms = self.0.cfg.decision_timeout.as_millis() as u64;
        self.0.publish(Frame::GateRequested {
            gate_id: gate_id.to_string(),
            mode,
            decidable,
            request: req.clone(),
            requested_at: fmt_rfc3339(requested_at),
            timeout_ms,
            deadline: fmt_rfc3339(deadline),
        })
    }

    /// Shared `GateDecided` builder for `decide`/`timeout_gate`/`resolve_announced`.
    fn publish_decided(
        &self,
        gate_id: &str,
        outcome: DecisionOutcome,
        decided_by: DecidedBy,
        reason: Option<String>,
    ) -> u64 {
        let decided_at = fmt_rfc3339(self.0.clock.now());
        self.0.publish(Frame::GateDecided {
            gate_id: gate_id.to_string(),
            outcome,
            decided_by,
            reason,
            decided_at,
        })
    }
}

// ---------------------------------------------------------------------
// 4. OperatorSurfacer — the sync <-> async bridge, explicitly.
// ---------------------------------------------------------------------

/// Implements `wasl_server::Surfacer` over an [`OperatorHub`]: publish the
/// gate, then block until an authority POSTs a decision or the timeout
/// dismisses it.
///
/// **Embedder contract (normative):** `surface()` blocks its thread for up to
/// `decision_timeout`. The wait path uses only `std::sync::mpsc`
/// (`recv_timeout`), never a tokio primitive, so `surface()` is
/// runtime-agnostic and cannot hit a `blocking_recv`-in-async panic — but
/// callers running on a tokio runtime (e.g. `wasl-mcp`'s multi-thread
/// runtime) MUST still invoke the gatekeeper's authorize path inside
/// `tokio::task::block_in_place` or from `spawn_blocking`; never from a plain
/// async worker. This crate cannot detect a violation of that contract
/// (`block_in_place` is invisible from here) — the contract lives here and in
/// W4's wiring spec.
pub struct OperatorSurfacer {
    hub: OperatorHub,
}

impl OperatorSurfacer {
    pub fn new(hub: OperatorHub) -> Self {
        OperatorSurfacer { hub }
    }
}

impl wasl_server::Surfacer for OperatorSurfacer {
    fn surface(&mut self, mode: GateMode, req: &GateRequest) -> GateOutcome {
        let w = self.hub.open_gate(mode, req);
        self.hub.wait(w)
    }
}

// ---------------------------------------------------------------------
// 5. TeeSink
// ---------------------------------------------------------------------

/// Wraps any inner `wasl_audit::Sink`: durable append first (on `Err`,
/// nothing is broadcast and the error propagates), then an infallible
/// broadcast of the entry as an `Audit` frame — the observation feed.
///
/// **Never lose audit on slow subscribers:** durability is the inner sink's
/// alone; the broadcast is a lossy-by-design feed (see the hub's lag/drop
/// policy).
pub struct TeeSink<K: Sink> {
    inner: K,
    hub: OperatorHub,
}

impl<K: Sink> TeeSink<K> {
    pub fn new(inner: K, hub: OperatorHub) -> Self {
        TeeSink { inner, hub }
    }

    pub fn into_inner(self) -> K {
        self.inner
    }
}

impl<K: Sink> Sink for TeeSink<K> {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
        self.inner.append(entry)?;
        self.hub.publish_audit(entry);
        Ok(())
    }
}

// ---------------------------------------------------------------------
// 6. HTTP shell (axum, the `wasl-a2a-server` idioms)
// ---------------------------------------------------------------------

/// Build the axum `Router`: `hub` is the shared `State` (cheap `Clone`).
pub fn router(hub: OperatorHub) -> Router {
    Router::new()
        .route("/v1/info", get(info_handler))
        .route("/v1/events", get(events_handler))
        .route("/v1/gates", get(gates_handler))
        .route("/v1/gates/:gate_id/decision", post(decision_handler))
        .with_state(hub)
}

/// Bind `addr` and serve until shutdown (the thin IO main loop).
pub async fn serve(hub: OperatorHub, addr: SocketAddr) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(hub)).await
}

async fn info_handler(State(hub): State<OperatorHub>) -> Json<NodeInfo> {
    Json(hub.info())
}

async fn gates_handler(State(hub): State<OperatorHub>) -> Json<GatesSnapshot> {
    Json(hub.snapshot())
}

/// The `hello` frame body: connection-local, built per subscription, never
/// broadcast — see `Frame`'s doc.
#[derive(Serialize)]
struct HelloFrame<'a> {
    api_version: u32,
    agent_id: &'a AgentId,
    host_id: &'a HostId,
    version: &'a str,
    last_seq: u64,
    resume: &'static str,
}

async fn events_handler(
    State(hub): State<OperatorHub>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (last_seq, rx) = hub.subscribe();
    let info = hub.info();

    let hello_json = serde_json::to_string(&HelloFrame {
        api_version: 1,
        agent_id: &info.agent_id,
        host_id: &info.host_id,
        version: &info.version,
        last_seq,
        resume: "snapshot",
    })
    .expect("HelloFrame is serializable");
    // No `.id()`: `hello` is connection-local and never carries a sequence.
    let hello_event = Event::default().event("hello").data(hello_json);
    let hello_stream = stream::once(async move { Ok::<Event, Infallible>(hello_event) });

    let frame_stream = BroadcastStream::new(rx).filter_map(|item| async move {
        match item {
            Ok(seq_frame) => {
                let data = serde_json::to_string(&seq_frame.frame).expect("Frame is serializable");
                Some(Ok::<Event, Infallible>(
                    Event::default()
                        .event(seq_frame.frame.event_name())
                        .id(seq_frame.seq.to_string())
                        .data(data),
                ))
            }
            // The only error variant, `Lagged`, is the documented
            // skip-and-continue policy: drop it, keep the stream open — the
            // gap in `id:` is the consumer's signal to re-fetch `/v1/gates`.
            Err(BroadcastStreamRecvError::Lagged(_)) => None,
        }
    });

    Sse::new(hello_stream.chain(frame_stream)).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ka"),
    )
}

async fn decision_handler(
    State(hub): State<OperatorHub>,
    Path(gate_id): Path<String>,
    body: Bytes,
) -> Response {
    // A custom, direct `serde_json` parse (rather than axum's `Json`
    // extractor) so the rejection shape is OURS — matching
    // `error_malformed.json` byte-for-byte, including the raw serde_json
    // parser message as `detail`.
    let decision: DecisionBody = match serde_json::from_slice(&body) {
        Ok(decision) => decision,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, "malformed", err.to_string()),
    };
    match hub.decide(&gate_id, decision) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(DecideError::UnknownGate) => {
            error_response(StatusCode::NOT_FOUND, "unknown_gate", gate_id)
        }
        Err(DecideError::AlreadyDecided) => {
            error_response(StatusCode::CONFLICT, "already_decided", gate_id)
        }
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: &'static str,
    detail: String,
}

fn error_response(status: StatusCode, error: &'static str, detail: String) -> Response {
    (status, Json(ErrorBody { error, detail })).into_response()
}
