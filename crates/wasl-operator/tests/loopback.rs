//! `tests/loopback.rs` — INTEGRATION tests over real HTTP+SSE for
//! `wasl_operator`. Binds `wasl_operator::router()` to an ephemeral
//! `127.0.0.1:0` port (bound synchronously before `axum::serve` is spawned —
//! no readiness race) and drives it with real `reqwest` + a hand-rolled SSE
//! reader (`common::SseReader`). Mirrors the spec's §Test plan L1–L8
//! (`docs/specs/operator.md`), plus a concurrent-POST race test.
//!
//! Test-first: this MUST fail to compile until `src/` defines the public API
//! (`router`, `OperatorHub`, `OperatorSurfacer`, `TeeSink`, ...).

mod common;

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use common::{
    at, client, filler_entry, fresh_hub_with_clock, hub_with_cfg, node_info, req_call9,
    spawn_router, FakeClock, RecordingSink, SseReader, T0_MS,
};

use wasl_audit::{AuditEntry, EntryKind, Sink};
use wasl_gate::{GateMode, GateOutcome};
use wasl_proto::AgentId;
use wasl_server::Surfacer;

use wasl_operator::{
    CounterGateIds, DecisionBody, DecisionOutcome, HubConfig, OperatorHub, OperatorSurfacer,
    SystemClock, TeeSink,
};

const SSE_TIMEOUT: Duration = Duration::from_secs(5);

// ── L1: GET /v1/info ────────────────────────────────────────────────────────

#[tokio::test]
async fn l1_info_endpoint_returns_node_info() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let addr = spawn_router(hub).await;

    let resp = client()
        .get(format!("http://{addr}/v1/info"))
        .send()
        .await
        .expect("GET /v1/info");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let body: Value = resp.json().await.expect("json body");
    assert_eq!(
        body,
        json!({ "agent_id": "wasl-a", "host_id": "host-1", "version": "0.1.0" })
    );
}

// ── L2: hello is the first SSE frame, no id:, schema fields ────────────────

#[tokio::test]
async fn l2_events_hello_frame_first_with_no_id() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let addr = spawn_router(hub).await;

    let resp = client()
        .get(format!("http://{addr}/v1/events"))
        .send()
        .await
        .expect("GET /v1/events");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .expect("content-type header")
        .to_string();
    assert!(
        content_type.starts_with("text/event-stream"),
        "expected SSE content-type, got {content_type:?}"
    );

    let mut sse = SseReader::new(resp);
    let hello = sse.next_event(SSE_TIMEOUT).await.expect("hello frame");
    assert_eq!(hello.event, "hello");
    assert!(hello.id.is_none(), "hello must carry no id:");
    assert_eq!(hello.data["api_version"], json!(1));
    assert_eq!(hello.data["agent_id"], json!("wasl-a"));
    assert_eq!(hello.data["host_id"], json!("host-1"));
    assert_eq!(hello.data["version"], json!("0.1.0"));
    assert_eq!(hello.data["last_seq"], json!(0));
    assert_eq!(hello.data["resume"], json!("snapshot"));
}

// ── L3: the full rendezvous over HTTP ───────────────────────────────────────

#[tokio::test]
async fn l3_full_rendezvous_subscribe_open_decide_resolves() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let addr = spawn_router(hub.clone()).await;

    let resp = client()
        .get(format!("http://{addr}/v1/events"))
        .send()
        .await
        .expect("GET /v1/events");
    let mut sse = SseReader::new(resp);
    let hello = sse.next_event(SSE_TIMEOUT).await.expect("hello");
    assert_eq!(hello.event, "hello");

    let surfacer_hub = hub.clone();
    let req = req_call9();
    let handle = tokio::task::spawn_blocking(move || {
        let mut surfacer = OperatorSurfacer::new(surfacer_hub);
        surfacer.surface(GateMode::External, &req)
    });

    let requested = sse
        .next_event(SSE_TIMEOUT)
        .await
        .expect("gate_requested frame");
    assert_eq!(requested.event, "gate_requested");
    assert_eq!(requested.id, Some(1));
    assert_eq!(requested.data["gate_id"], json!("gate-1"));
    assert_eq!(requested.data["decidable"], json!(true));

    // GET /v1/gates shows it, with age/deadline present.
    let snap: Value = client()
        .get(format!("http://{addr}/v1/gates"))
        .send()
        .await
        .expect("GET /v1/gates")
        .json()
        .await
        .expect("gates json");
    let gates = snap["gates"].as_array().expect("gates array");
    assert_eq!(gates.len(), 1);
    assert_eq!(gates[0]["gate_id"], json!("gate-1"));
    assert!(gates[0]["age_ms"].as_u64().is_some());
    assert!(gates[0]["deadline"].as_str().is_some());

    // POST approved -> 204 empty body.
    let post = client()
        .post(format!("http://{addr}/v1/gates/gate-1/decision"))
        .json(&json!({ "outcome": "approved" }))
        .send()
        .await
        .expect("POST decision");
    assert_eq!(post.status(), reqwest::StatusCode::NO_CONTENT);
    let post_body = post.bytes().await.expect("post body");
    assert!(post_body.is_empty(), "204 must have an empty body");

    let decided = sse
        .next_event(SSE_TIMEOUT)
        .await
        .expect("gate_decided frame");
    assert_eq!(decided.event, "gate_decided");
    assert_eq!(decided.data["gate_id"], json!("gate-1"));
    assert_eq!(decided.data["outcome"], json!("approved"));
    assert_eq!(decided.data["decided_by"], json!("operator"));

    let outcome = handle.await.expect("surface task");
    assert_eq!(outcome, GateOutcome::Approved);

    let snap2: Value = client()
        .get(format!("http://{addr}/v1/gates"))
        .send()
        .await
        .expect("GET /v1/gates #2")
        .json()
        .await
        .expect("gates json #2");
    assert!(snap2["gates"].as_array().unwrap().is_empty());
}

// ── L4: timeout over HTTP, then a late POST conflicts ──────────────────────

#[tokio::test]
async fn l4_timeout_over_http_then_late_post_conflicts() {
    let cfg = HubConfig {
        decision_timeout: Duration::from_millis(50),
        channel_capacity: 256,
    };
    let hub = OperatorHub::with_seams(
        node_info(),
        cfg,
        Arc::new(SystemClock),
        Arc::new(CounterGateIds::default()),
    );
    let addr = spawn_router(hub.clone()).await;

    let resp = client()
        .get(format!("http://{addr}/v1/events"))
        .send()
        .await
        .expect("GET /v1/events");
    let mut sse = SseReader::new(resp);
    sse.next_event(SSE_TIMEOUT).await.expect("hello");

    let surfacer_hub = hub.clone();
    let req = req_call9();
    let handle = tokio::task::spawn_blocking(move || {
        let mut surfacer = OperatorSurfacer::new(surfacer_hub);
        surfacer.surface(GateMode::External, &req)
    });

    sse.next_event(SSE_TIMEOUT)
        .await
        .expect("gate_requested frame");

    let outcome = handle.await.expect("surface task");
    assert_eq!(outcome, GateOutcome::Dismissed);

    let decided = sse
        .next_event(SSE_TIMEOUT)
        .await
        .expect("gate_decided frame");
    assert_eq!(decided.data["outcome"], json!("dismissed"));
    assert_eq!(decided.data["decided_by"], json!("timeout"));

    let late = client()
        .post(format!("http://{addr}/v1/gates/gate-1/decision"))
        .json(&json!({ "outcome": "approved" }))
        .send()
        .await
        .expect("late POST");
    assert_eq!(late.status(), reqwest::StatusCode::CONFLICT);
    let body: Value = late.json().await.expect("409 body");
    assert_eq!(body["error"], json!("already_decided"));
}

// ── L5: multi-subscriber sees identical frames with equal ids ─────────────

#[tokio::test]
async fn l5_multi_subscriber_same_frames_same_ids() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let addr = spawn_router(hub.clone()).await;

    let r1 = client()
        .get(format!("http://{addr}/v1/events"))
        .send()
        .await
        .expect("subscriber 1");
    let r2 = client()
        .get(format!("http://{addr}/v1/events"))
        .send()
        .await
        .expect("subscriber 2");
    let mut s1 = SseReader::new(r1);
    let mut s2 = SseReader::new(r2);
    s1.next_event(SSE_TIMEOUT).await.expect("hello 1");
    s2.next_event(SSE_TIMEOUT).await.expect("hello 2");

    let surfacer_hub = hub.clone();
    let req = req_call9();
    let handle = tokio::task::spawn_blocking(move || {
        let mut surfacer = OperatorSurfacer::new(surfacer_hub);
        surfacer.surface(GateMode::External, &req)
    });

    let e1 = s1.next_event(SSE_TIMEOUT).await.expect("s1 gate_requested");
    let e2 = s2.next_event(SSE_TIMEOUT).await.expect("s2 gate_requested");
    assert_eq!(e1.id, e2.id);
    assert_eq!(e1.data, e2.data);

    hub.decide(
        "gate-1",
        DecisionBody {
            outcome: DecisionOutcome::Approved,
            reason: None,
        },
    )
    .expect("decide ok");

    let d1 = s1.next_event(SSE_TIMEOUT).await.expect("s1 gate_decided");
    let d2 = s2.next_event(SSE_TIMEOUT).await.expect("s2 gate_decided");
    assert_eq!(d1.id, d2.id);
    assert_eq!(d1.data, d2.data);

    assert_eq!(handle.await.expect("surface task"), GateOutcome::Approved);
}

// ── L6: slow subscriber lags (id gap, skip-and-continue); inner sink is whole ──
//
// A companion, actively-draining subscriber seeing every frame is already
// covered by L5 above. This test forces GENUINE `tokio::sync::broadcast`
// receiver lag (not a timing simulation): `#[tokio::test]` defaults to the
// current-thread runtime, so a burst of publishes with NO `.await` between
// them cannot be interleaved with the SSE connection task's own polling —
// cooperative scheduling only switches tasks at an `.await` point. That
// guarantees the connection genuinely falls behind a channel_capacity of 2
// (verified directly against `tokio::sync::broadcast` semantics: 20 sends
// past a capacity-2 receiver that never polled yield `Lagged(18)`, then
// recover to the two still-buffered items).
#[tokio::test]
async fn l6_slow_subscriber_sees_gap_stream_stays_open_and_inner_has_everything() {
    let cfg = HubConfig {
        decision_timeout: Duration::from_secs(120),
        channel_capacity: 2,
    };
    let hub = hub_with_cfg(Arc::new(FakeClock::new(at(T0_MS))), cfg);
    let addr = spawn_router(hub.clone()).await;

    let resp = client()
        .get(format!("http://{addr}/v1/events"))
        .send()
        .await
        .expect("connect");
    let mut reader = SseReader::new(resp);
    reader.next_event(SSE_TIMEOUT).await.expect("hello");

    let mut tee = TeeSink::new(RecordingSink::default(), hub.clone());
    for seq in 1..=20u64 {
        tee.append(&filler_entry(seq)).expect("append ok"); // no `.await` here — see comment above
    }

    // Yield so the (now-lagged) connection task gets to observe it.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let received = reader.next_events(10, Duration::from_millis(500)).await;
    assert!(
        !received.is_empty(),
        "the stream must still be open and deliver frames after lagging"
    );
    let ids: Vec<u64> = received.iter().filter_map(|e| e.id).collect();
    let first_id_skipped_ahead = *ids.first().expect("at least one id") > 1;
    let has_internal_gap = ids.windows(2).any(|w| w[1].saturating_sub(w[0]) > 1);
    assert!(
        first_id_skipped_ahead || has_internal_gap,
        "a lagged subscriber must observe an id gap (skip-and-continue), got {ids:?}"
    );

    // The connection is genuinely alive (not just draining a pre-lag
    // backlog): a freshly-published frame after the gap is still delivered.
    tee.append(&filler_entry(21)).expect("append ok");
    let fresh = reader
        .next_event(SSE_TIMEOUT)
        .await
        .expect("frame after the gap");
    assert_eq!(fresh.id, Some(21));

    // The durable inner sink has every entry regardless of broadcast lag.
    let inner = tee.into_inner();
    assert_eq!(inner.len(), 21);
}

// ── L7: error responses — 404 / 400 / 409 ───────────────────────────────────

#[tokio::test]
async fn l7_error_responses_404_400_409() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let addr = spawn_router(hub.clone()).await;
    let http = client();

    // 404: unknown gate id.
    let r404 = http
        .post(format!("http://{addr}/v1/gates/gate-nope/decision"))
        .json(&json!({ "outcome": "approved" }))
        .send()
        .await
        .expect("404 POST");
    assert_eq!(r404.status(), reqwest::StatusCode::NOT_FOUND);
    let b404: Value = r404.json().await.expect("404 body");
    assert_eq!(b404["error"], json!("unknown_gate"));

    let _w = hub.open_gate(GateMode::External, &req_call9()); // gate-1, pending

    // 400: not JSON at all.
    let r400a = http
        .post(format!("http://{addr}/v1/gates/gate-1/decision"))
        .header("content-type", "application/json")
        .body("not json at all")
        .send()
        .await
        .expect("400 POST (bad syntax)");
    assert_eq!(r400a.status(), reqwest::StatusCode::BAD_REQUEST);
    let b400a: Value = r400a.json().await.expect("400 body (bad syntax)");
    assert_eq!(b400a["error"], json!("malformed"));

    // 400: missing `outcome`.
    let r400b = http
        .post(format!("http://{addr}/v1/gates/gate-1/decision"))
        .json(&json!({}))
        .send()
        .await
        .expect("400 POST (missing outcome)");
    assert_eq!(r400b.status(), reqwest::StatusCode::BAD_REQUEST);
    let b400b: Value = r400b.json().await.expect("400 body (missing outcome)");
    assert_eq!(b400b["error"], json!("malformed"));

    // 400: unknown outcome value.
    let r400c = http
        .post(format!("http://{addr}/v1/gates/gate-1/decision"))
        .json(&json!({ "outcome": "maybe" }))
        .send()
        .await
        .expect("400 POST (bad outcome)");
    assert_eq!(r400c.status(), reqwest::StatusCode::BAD_REQUEST);
    let b400c: Value = r400c.json().await.expect("400 body (bad outcome)");
    assert_eq!(b400c["error"], json!("malformed"));

    // 204 then 409: already decided.
    let ok = http
        .post(format!("http://{addr}/v1/gates/gate-1/decision"))
        .json(&json!({ "outcome": "approved" }))
        .send()
        .await
        .expect("204 POST");
    assert_eq!(ok.status(), reqwest::StatusCode::NO_CONTENT);

    let conflict = http
        .post(format!("http://{addr}/v1/gates/gate-1/decision"))
        .json(&json!({ "outcome": "denied" }))
        .send()
        .await
        .expect("409 POST");
    assert_eq!(conflict.status(), reqwest::StatusCode::CONFLICT);
    let bconf: Value = conflict.json().await.expect("409 body");
    assert_eq!(bconf["error"], json!("already_decided"));
}

// ── L7b: concurrent POSTs race safely — exactly one 204 ────────────────────

#[tokio::test]
async fn l7b_concurrent_decision_posts_race_exactly_one_204() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let addr = spawn_router(hub.clone()).await;
    let _w = hub.open_gate(GateMode::External, &req_call9());

    let mut handles = Vec::new();
    for _ in 0..8 {
        handles.push(tokio::spawn(async move {
            client()
                .post(format!("http://{addr}/v1/gates/gate-1/decision"))
                .json(&json!({ "outcome": "approved" }))
                .send()
                .await
                .expect("concurrent POST")
                .status()
                .as_u16()
        }));
    }

    let mut statuses = Vec::new();
    for h in handles {
        statuses.push(h.await.expect("join concurrent POST task"));
    }

    let ok_count = statuses.iter().filter(|&&s| s == 204).count();
    let conflict_count = statuses.iter().filter(|&&s| s == 409).count();
    assert_eq!(
        ok_count, 1,
        "exactly one concurrent POST must win with 204, got {statuses:?}"
    );
    assert_eq!(
        conflict_count, 7,
        "the rest must lose with 409, got {statuses:?}"
    );
}

// ── L8: audit tee over HTTP ──────────────────────────────────────────────

#[tokio::test]
async fn l8_audit_tee_over_http_matches_inner_sink_entry() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let addr = spawn_router(hub.clone()).await;

    let resp = client()
        .get(format!("http://{addr}/v1/events"))
        .send()
        .await
        .expect("GET /v1/events");
    let mut sse = SseReader::new(resp);
    sse.next_event(SSE_TIMEOUT).await.expect("hello");

    let mut tee = TeeSink::new(RecordingSink::default(), hub.clone());
    let entry = AuditEntry {
        seq: 1,
        kind: EntryKind::Message,
        actor: AgentId("wasl-a".to_string()),
        targets: vec![AgentId("wasl-b".to_string())],
        summary: "message to 1 peer(s)".to_string(),
        decision: None,
        ..Default::default()
    };
    tee.append(&entry).expect("tee append ok");

    let audit_ev = sse.next_event(SSE_TIMEOUT).await.expect("audit frame");
    assert_eq!(audit_ev.event, "audit");
    assert_eq!(
        audit_ev.data["entry"],
        serde_json::to_value(&entry).unwrap()
    );

    let inner = tee.into_inner();
    assert_eq!(inner.snapshot(), vec![entry]);
}
