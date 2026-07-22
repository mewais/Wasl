//! Phase-2A: the bounded event journal — a ring of `(EventSeq, TurnEvent)` plus a
//! `tokio::sync::broadcast` live tail, with the exact §4.3 atomic
//! replay-to-live handoff.
//!
//! The journal is owned behind a `std::sync::Mutex` shared between the actor
//! (which appends under the lock) and `events()` (which subscribes under the
//! same lock). §4.3 pins the ordering so the tail→live seam has neither a gap
//! nor a duplicate:
//!
//!   1. take the lock ONCE,
//!   2. subscribe a live broadcast receiver FIRST,
//!   3. THEN copy the retained tail,
//!   4. release the lock,
//!   5. yield the tail, then the live receiver, dropping any live item whose
//!      `seq <= last_tail_seq`.

use std::collections::VecDeque;

use tokio::sync::broadcast;

use crate::{EventSeq, StreamItem, TurnEvent};

/// A journalled `(seq, event)` pair, broadcast to live subscribers and retained
/// in the ring.
#[derive(Debug, Clone)]
pub(crate) struct SeqEvent {
    seq: EventSeq,
    event: TurnEvent,
}

/// The bounded ring + live broadcast tail for one target. Held behind a mutex by
/// the actor; `subscribe` performs the §4.3 handoff.
pub(crate) struct Journal {
    /// Retained ring (bounded by `capacity`), oldest at the front.
    ring: VecDeque<SeqEvent>,
    /// The next sequence to mint (per-target monotonic, starts at 1).
    next_seq: u64,
    /// Ring capacity (events retained).
    capacity: usize,
    /// The live broadcast sender; a fresh subscriber gets everything published
    /// after it subscribes. Dropped (set to `None`) on `close()` so live streams
    /// end when the session goes terminal.
    tx: Option<broadcast::Sender<SeqEvent>>,
}

impl Journal {
    /// Build an empty journal with the given ring capacity.
    pub(crate) fn new(capacity: usize) -> Self {
        // The broadcast buffer is sized generously relative to the ring so a
        // subscriber that keeps up never spuriously lags; a slow subscriber that
        // does lag surfaces as `Truncated` (§4.3, identical recovery to
        // below-floor). `broadcast::channel` requires capacity >= 1.
        let buf = capacity.max(1).saturating_mul(4).max(16);
        let (tx, _rx) = broadcast::channel(buf);
        Journal {
            ring: VecDeque::new(),
            next_seq: 1,
            capacity: capacity.max(1),
            tx: Some(tx),
        }
    }

    /// Close the live broadcast (session terminal): existing subscribers get
    /// `Closed` and their streams end; the ring still serves late subscribers a
    /// finite replay that ends cleanly.
    pub(crate) fn close(&mut self) {
        self.tx = None;
    }

    /// The highest sequence journalled so far (0 if none).
    pub(crate) fn last_seq(&self) -> EventSeq {
        EventSeq(self.next_seq.saturating_sub(1))
    }

    /// The lowest retained sequence (the ring floor); the last seq when empty.
    pub(crate) fn floor(&self) -> EventSeq {
        self.earliest_retained()
    }

    /// The lowest retained sequence (the ring floor), or the last seq when empty.
    fn earliest_retained(&self) -> EventSeq {
        self.ring
            .front()
            .map(|e| e.seq)
            .unwrap_or_else(|| self.last_seq())
    }

    /// Append `event`, mint its seq, push to the ring (evicting the oldest if at
    /// capacity), broadcast it live, and return the minted seq.
    pub(crate) fn append(&mut self, event: TurnEvent) -> EventSeq {
        let seq = EventSeq(self.next_seq);
        self.next_seq += 1;
        let item = SeqEvent {
            seq,
            event: event.clone(),
        };
        if self.ring.len() >= self.capacity {
            self.ring.pop_front();
        }
        self.ring.push_back(item.clone());
        // A broadcast send fails only when there are no live receivers, which is
        // fine — the ring retains the event for a future subscriber. After
        // `close()` there is no sender (the session is terminal).
        if let Some(tx) = self.tx.as_ref() {
            let _ = tx.send(item);
        }
        seq
    }

    /// Perform the §4.3 atomic replay-to-live handoff for a subscriber resuming
    /// after `after_seq`. Returns the copied tail (ascending), a fresh live
    /// receiver, and — if the cursor is below the retained floor — a `Truncated`
    /// control item the caller must yield alone before stopping.
    ///
    /// This runs entirely under the actor lock (the caller holds it), so the ring
    /// and `next_seq` cannot advance between the subscribe and the tail copy.
    pub(crate) fn subscribe(&self, after_seq: EventSeq) -> Handoff {
        // Step 2 — subscribe a live receiver FIRST (before copying the tail), so
        // everything published after this instant is captured by this receiver.
        // If the session is already terminal (`tx` dropped), hand back an
        // already-closed receiver so the stream ends after the retained tail.
        let rx = match self.tx.as_ref() {
            Some(tx) => tx.subscribe(),
            None => {
                let (closed_tx, closed_rx) = broadcast::channel(1);
                drop(closed_tx);
                closed_rx
            }
        };
        let last = self.last_seq();
        let earliest = self.earliest_retained();

        // Below-floor: the cursor fell out of the retained ring. Emit a single
        // Truncated{earliest, latest} and stop (§4.3 line 352). A cursor of 0 (or
        // anything >= earliest-1) is in-window; only a strictly-below cursor with
        // a real floor > 1 truncates.
        if !self.ring.is_empty() && after_seq.0 + 1 < earliest.0 {
            return Handoff {
                tail: Vec::new(),
                rx,
                truncated: Some(StreamItem::Truncated {
                    earliest,
                    latest: last,
                }),
                last_tail_seq: after_seq.0,
            };
        }

        // Step 3 — copy the retained tail `after_seq+1..=last` out of the ring.
        // The tail is built EXCLUSIVELY from `StreamItem::Event` (never
        // `Truncated`), so `last_tail_seq` is derived directly from the ring's
        // `EventSeq`s — no `StreamItem` match with a dead `Truncated` arm.
        let last_tail_seq = self
            .ring
            .iter()
            .map(|e| e.seq.0)
            .filter(|s| *s > after_seq.0)
            .max()
            .unwrap_or(after_seq.0);
        let tail: Vec<StreamItem> = self
            .ring
            .iter()
            .filter(|e| e.seq.0 > after_seq.0)
            .map(|e| StreamItem::Event {
                seq: e.seq,
                event: e.event.clone(),
            })
            .collect();

        Handoff {
            tail,
            rx,
            truncated: None,
            last_tail_seq,
        }
    }
}

/// The result of a §4.3 subscribe: the copied tail, a live receiver, an optional
/// leading `Truncated`, and the highest seq already emitted from the tail (the
/// dedupe boundary for step 5).
pub(crate) struct Handoff {
    pub(crate) tail: Vec<StreamItem>,
    pub(crate) rx: broadcast::Receiver<SeqEvent>,
    pub(crate) truncated: Option<StreamItem>,
    pub(crate) last_tail_seq: u64,
}

impl Handoff {
    /// Turn the handoff into the public stream (§4.3 step 5): yield any leading
    /// `Truncated` alone (then stop), else yield the tail, then the live
    /// receiver deduped by dropping any item whose `seq <= last_tail_seq`; a
    /// broadcast lag surfaces as `Truncated` and stops the stream.
    pub(crate) fn into_stream(self, window: (EventSeq, EventSeq)) -> crate::TurnEventStream {
        // `last_tail_seq` is already computed correctly by `subscribe` (directly
        // from the ring, handling the empty-tail case via `unwrap_or(after_seq)`),
        // so it is used as-is here — no redundant recompute over the all-`Event`
        // tail (which previously carried a dead `Truncated` match arm).
        let last_tail_seq = self.last_tail_seq;

        let state = SubState {
            truncated: self.truncated,
            tail: self.tail.into_iter().collect(),
            rx: self.rx,
            last_tail_seq,
            window,
            done: false,
        };

        let s = futures::stream::unfold(state, |mut st| async move {
            if st.done {
                return None;
            }
            // 1. A leading below-floor Truncated: yield it alone, then stop.
            if let Some(t) = st.truncated.take() {
                st.done = true;
                return Some((t, st));
            }
            // 2. Drain the copied tail (already ascending, gap-free).
            if let Some(item) = st.tail.pop_front() {
                return Some((item, st));
            }
            // 3. Drain the live receiver, deduping the boundary.
            loop {
                match st.rx.recv().await {
                    Ok(item) => {
                        // SAFETY-CRITICAL DEFENSE-IN-DEPTH for the exactly-once
                        // replay guarantee (§4.3). Under the correct subscribe-
                        // then-copy-under-ONE-lock discipline (see `subscribe`),
                        // a live broadcast item ALWAYS has `seq > last_tail_seq`:
                        // the live receiver is subscribed BEFORE the tail copy and
                        // no `append` can interleave (both hold the journal lock),
                        // so there is no tail↔live overlap. This branch is thus
                        // unreachable today. It is deliberately KEPT LIVE (not
                        // deleted) so a FUTURE regression in the locking discipline
                        // cannot silently emit a duplicate in release builds, and a
                        // `debug_assert` trips it loudly in debug/test builds.
                        // Documented allowed residual (see AGENTS.md).
                        if item.seq.0 <= st.last_tail_seq {
                            debug_assert!(
                                false,
                                "§4.3 exactly-once invariant broken: live seq {} <= last_tail_seq {}",
                                item.seq.0, st.last_tail_seq
                            );
                            continue;
                        }
                        st.last_tail_seq = item.seq.0;
                        let out = StreamItem::Event {
                            seq: item.seq,
                            event: item.event,
                        };
                        return Some((out, st));
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return None;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let window = st.window;
                        st.done = true;
                        let out = StreamItem::Truncated {
                            earliest: window.0,
                            latest: window.1,
                        };
                        return Some((out, st));
                    }
                }
            }
        });
        Box::pin(s)
    }
}

/// The `unfold` state for one subscription stream.
struct SubState {
    truncated: Option<StreamItem>,
    tail: VecDeque<StreamItem>,
    rx: broadcast::Receiver<SeqEvent>,
    last_tail_seq: u64,
    window: (EventSeq, EventSeq),
    done: bool,
}
