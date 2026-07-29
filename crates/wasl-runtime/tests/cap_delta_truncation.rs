#![allow(clippy::items_after_statements)]
//! Tests for the `cap_delta` UTF-8 truncation panic (§3.1).
mod support;

use support::*;

use std::sync::Arc;
use tokio::runtime::Runtime;

use wasl_proto::RequestId;
use wasl_runtime::{BusyPolicy, EventSeq, ManagedConfig, ProtocolEvent, RuntimeError, TurnEvent};

#[test]
fn cap_delta_truncates_multi_byte_utf8_char_panics_today() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let clock = TestClock::new();
        let sink = sink_arc(RecordingSink::default());

        // Set delta_max_bytes to 1, forcing truncation inside a 4-byte UTF-8 char.
        let config = ManagedConfig {
            delta_max_bytes: 1, // Will panic on "🎉" (4 bytes)
            ..ManagedConfig::default()
        };

        let protocol = ScriptedProtocol::new();
        let _shared_counter = protocol.shared_counter();
        let provider = provider(protocol.clone(), config, clock, sink.clone());

        let context_id = "test_target";

        // Assistant Delta with a multi-byte UTF-8 character at the beginning
        let original_delta_text = "🎉test_suffix"; // 🎉 is 4 bytes
        let script = ProtocolScript::new()
            .ready(Some("native-session-id"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .emit(ProtocolEvent::AssistantDelta {
                text: original_delta_text.to_string(),
            })
            .emit(ProtocolEvent::TurnCompleted {
                reply: "final reply".to_string(),
            });

        // Fix: Install script BEFORE calling target_for
        protocol.install(context_id, script);

        let target = provider.target_for(&key(context_id)).await.unwrap();

        let stream = target.events(EventSeq(0)).await;

        let turn_receipt = target
            .submit_turn(consult(context_id, "Hello"), BusyPolicy::Reject)
            .await;

        let _turn_id = match turn_receipt {
            // §7.1 row 582: a submit racing the transport's `SessionReady` may
            // land either `Started` (session was already ready) or `Queued`
            // (session was still `Starting`, promoted once ready arrives) —
            // both are spec-correct and carry a `turn_id`.
            wasl_runtime::TurnReceipt::Started { turn_id, .. }
            | wasl_runtime::TurnReceipt::Queued { turn_id, .. } => turn_id,
            _ => panic!(
                "Expected TurnReceipt::Started or Queued, got {:?}",
                turn_receipt
            ),
        };

        // Drain events and expect the AssistantDelta. The real event sequence per
        // turn is SessionReady, TurnAccepted, StatusChanged(Dispatching),
        // StatusChanged(Generating), AssistantDelta, ... -- AssistantDelta is the
        // 5th event, not among the first 3. Drain generously and search by
        // content rather than assuming a fixed position/count (robust to any
        // legitimate change in the exact intermediate StatusChanged events).
        //
        // This assertion will only be reached if the program doesn't panic.
        // Against the unfixed code, this line should not be reached (the actor
        // panics inside cap_delta while producing the AssistantDelta event).
        let events = drain_items(stream, 10).await;
        let events = events_of(&events);

        let assistant_delta_event = events
            .iter()
            .find(|e| matches!(e, TurnEvent::AssistantDelta { .. }))
            .expect("Expected AssistantDelta event");

        if let TurnEvent::AssistantDelta { text, .. } = assistant_delta_event {
            // Assert that the text is valid UTF-8 and contains the truncation marker
            assert!(
                text.is_char_boundary(text.len()),
                "Resulting text is not valid UTF-8"
            );
            assert!(
                text.ends_with("…[truncated]"),
                "Text did not end with truncation marker, got: {:?}",
                text
            );
            // Assert that the original problematic character is not partially included
            // With delta_max_bytes=1, a 4-byte char cannot fit, so it should be just the marker
            assert_eq!(
                text, "…[truncated]",
                "Expected only truncation marker, as original char cannot fit within 1 byte"
            );
        } else {
            panic!("Unexpected event type: {:?}", assistant_delta_event);
        }
    });
}
