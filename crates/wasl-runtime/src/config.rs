//! Phase-2A: [`ManagedConfig`] — the admission-control + bounds config the
//! managed provider and per-target actor validate against (runtime.md §10/§11).
//!
//! Phase-3 node-config wiring only SETS these values; the TYPE + `Default`
//! (locked defaults) live here so the core validates against them now.

use std::time::Duration;

/// Admission-control + retained-state bounds for the managed runtime
/// (runtime.md §10 lines 753-757, §11 lines 768-784).
///
/// All fields are `pub` so a test / Phase-3 wiring can override selectively via
/// struct-update syntax over [`ManagedConfig::default`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedConfig {
    /// Global cap on live targets (untrusted `context_id` must not DoS via
    /// target explosion). Exceeding ⇒ `AdmissionRefused` (§10).
    pub max_targets_global: usize,
    /// Per-sender cap on live targets (§10). Enforcement hook; the full A2A
    /// sharing threat model is Phase-3.
    pub max_targets_per_sender: usize,
    /// Event-journal ring capacity (events retained per target). Below-floor
    /// ⇒ `Truncated` (§4.3 / §11).
    pub journal_capacity: usize,
    /// Idempotency-map entry cap (§11); LRU/TTL eviction.
    pub idempotency_cap: usize,
    /// Idempotency-map entry TTL (§11).
    pub idempotency_ttl: Duration,
    /// FIFO queue length bound; over ⇒ `RejectedBusy` (§11).
    pub queue_max_len: usize,
    /// FIFO queue total-bytes bound; over ⇒ `RejectedBusy` (§11).
    pub queue_max_bytes: usize,
    /// Transcript projection total-bytes bound (§4.5 / §11).
    pub transcript_max_bytes: usize,
    /// Per-`AssistantDelta` byte cap; over ⇒ truncated + a flag (§11).
    pub delta_max_bytes: usize,
    /// Idle-eviction timeout: an `Idle` target unused past this is
    /// `Drain`-shut and removed (§10).
    pub idle_eviction: Duration,
    /// Bounded actor-mailbox capacity (§4.6).
    pub mailbox_capacity: usize,
    /// Causation max-hop; `hop > max_hop` ⇒ rejected (§8.2, locked default 8).
    pub max_hop: u32,
}

impl Default for ManagedConfig {
    fn default() -> Self {
        // Locked defaults (runtime.md §10/§11/§8.2). Generous where a bound is a
        // safety net rather than a tuning knob; MAX_HOP = 8 is the pinned value.
        ManagedConfig {
            max_targets_global: 1024,
            max_targets_per_sender: 64,
            journal_capacity: 4096,
            idempotency_cap: 4096,
            idempotency_ttl: Duration::from_secs(3600),
            queue_max_len: 64,
            queue_max_bytes: 1 << 20,
            transcript_max_bytes: 1 << 20,
            delta_max_bytes: 64 * 1024,
            idle_eviction: Duration::from_secs(600),
            mailbox_capacity: 256,
            max_hop: 8,
        }
    }
}
