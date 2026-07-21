//! Behavioral security tests for the Tier-2 approval-token generator
//! ([`wasl_mcp::RandomTokenIds`] via the [`wasl_mcp::TokenIds`] seam).
//!
//! M1 hardens `RandomTokenIds` to draw its 128 bits from a real CSPRNG
//! (`getrandom`) instead of hashing wall-clock nanoseconds through a SipHash
//! `RandomState` (documented HashDoS-resistant, explicitly NOT cryptographic,
//! fed a low-entropy/predictable input). These tests pin the *observable*
//! properties a correct CSPRNG-backed generator must satisfy against the
//! PUBLIC API only (`TokenIds` / `RandomTokenIds`), never the private
//! `random_u64`. The public API is unchanged by the fix: `RandomTokenIds`
//! still `impl TokenIds`, and `next()` still returns `"tok-mcp-" + 32`
//! lowercase hex chars.
//!
//! HONEST RED/GREEN STATE (test-first discipline):
//!   * FORMAT (`format_is_tok_mcp_prefix_plus_32_lowercase_hex`) and the
//!     `CounterTokenIds` contrast (`counter_token_ids_are_sequential`) PASS on
//!     the current (weak) impl too — they pin the shape/seam, not the entropy.
//!   * UNIQUENESS / HIGH-CARDINALITY-OF-HALVES / NO-SIMPLE-SEQUENCE /
//!     CONCURRENCY are the discriminating net: a genuine CSPRNG passes them
//!     trivially; the timestamp-hash generator MAY fail them under a tight,
//!     no-work-between-draws loop, because many draws land in the same
//!     nanosecond bucket and collide. This is the WHOLE POINT of the tight
//!     loop below — but it is inherently machine/OS-timer-dependent, so on a
//!     machine with a coarse or fast clock the weak impl can *coincidentally*
//!     pass. We do NOT fabricate a guaranteed failure or fake a flaky
//!     assertion.
//!
//!   Therefore: the M1 security contract is PRIMARILY a SOURCE-LEVEL property
//!   — the impl must use a documented CSPRNG — which the dev-manager verifies
//!   by reading `server.rs`. These black-box tests are a SUPPORTING net that a
//!   CSPRNG passes and a weak generator MAY (but is not guaranteed to) fail
//!   under load. The tester's run against the current unfixed impl is recorded
//!   in the delivered report.

use std::collections::HashSet;
use std::sync::Arc;
use std::thread;

use wasl_mcp::{CounterTokenIds, RandomTokenIds, TokenIds};

/// Prefix every token must carry, per the public contract.
const PREFIX: &str = "tok-mcp-";
/// Length of the hex payload after the prefix (two u64 halves = 32 hex chars).
const PAYLOAD_LEN: usize = 32;
/// Total token length: prefix + 32 hex payload chars.
const TOTAL_LEN: usize = PREFIX.len() + PAYLOAD_LEN;

/// Split a token into its 32-char hex payload, asserting the prefix/shape.
/// Returns `(first_half_hex, second_half_hex)` — the two rendered u64 halves.
fn payload_halves(token: &str) -> (&str, &str) {
    assert!(
        token.starts_with(PREFIX),
        "token must start with {PREFIX:?}, got {token:?}"
    );
    assert_eq!(
        token.len(),
        TOTAL_LEN,
        "token must be prefix + {PAYLOAD_LEN} hex chars, got {token:?} (len {})",
        token.len()
    );
    let payload = &token[PREFIX.len()..];
    (&payload[..16], &payload[16..])
}

// =======================================================================
// 1. FORMAT — `^tok-mcp-[0-9a-f]{32}$`
// =======================================================================

/// PROPERTY: every token is exactly `"tok-mcp-"` followed by 32 LOWERCASE hex
/// digits — no uppercase, no non-hex, no length drift. A CSPRNG rendered with
/// `format!("{:016x}{:016x}", ..)` satisfies this exactly; so does the weak
/// impl (shape is unchanged by M1), so this test is GREEN on both — it pins
/// the public contract that must survive the fix.
#[test]
fn format_is_tok_mcp_prefix_plus_32_lowercase_hex() {
    let ids = RandomTokenIds;
    for _ in 0..1_000 {
        let token = ids.next();
        assert!(
            token.starts_with(PREFIX),
            "missing {PREFIX:?} prefix: {token:?}"
        );
        assert_eq!(
            token.len(),
            TOTAL_LEN,
            "wrong total length: {token:?} (len {})",
            token.len()
        );
        let payload = &token[PREFIX.len()..];
        assert_eq!(
            payload.len(),
            PAYLOAD_LEN,
            "payload must be {PAYLOAD_LEN} chars: {payload:?}"
        );
        for c in payload.chars() {
            assert!(
                c.is_ascii_hexdigit() && !c.is_ascii_uppercase(),
                "payload char {c:?} in {token:?} is not lowercase hex [0-9a-f]"
            );
        }
    }
}

// =======================================================================
// 2. UNIQUENESS — 10_000 draws, all distinct (tightest loop, discriminating)
// =======================================================================

/// PROPERTY: 10_000 tokens drawn back-to-back from ONE `RandomTokenIds` are
/// all distinct. Deliberately the tightest possible loop — NO work between
/// draws, high volume — to maximize the chance the timestamp-hash impl lands
/// multiple draws in the same nanosecond bucket and collides. A 128-bit
/// CSPRNG has a negligible birthday-collision probability over 10^4 draws
/// (~10^-31), so a correct impl passes with certainty; the weak impl MAY fail
/// here under a coarse/fast clock. DISCRIMINATING.
#[test]
fn ten_thousand_draws_are_all_distinct() {
    let ids = RandomTokenIds;
    let mut set: HashSet<String> = HashSet::with_capacity(10_000);
    for _ in 0..10_000 {
        set.insert(ids.next());
    }
    assert_eq!(
        set.len(),
        10_000,
        "expected 10_000 distinct tokens; got {} — a CSPRNG never collides at \
         this scale, so duplicates indict a low-entropy generator",
        set.len()
    );
}

// =======================================================================
// 3. HIGH CARDINALITY OF HALVES — each u64 half is near-fully distinct
// =======================================================================

/// PROPERTY: splitting each 32-hex payload into its two 16-hex u64 halves and
/// collecting each half across 10_000 draws yields a near-fully-distinct set
/// per half (> 9_900 of 10_000). A CSPRNG produces independent, uniform u64s,
/// so each half-set is ~fully distinct; the timestamp-hash impl, under a tight
/// loop, tends to produce runs of identical/near-identical halves (same nanos
/// bucket -> same hash), collapsing cardinality. DISCRIMINATING.
#[test]
fn each_u64_half_has_high_cardinality() {
    let ids = RandomTokenIds;
    let mut firsts: HashSet<String> = HashSet::with_capacity(10_000);
    let mut seconds: HashSet<String> = HashSet::with_capacity(10_000);
    for _ in 0..10_000 {
        let token = ids.next();
        let (a, b) = payload_halves(&token);
        firsts.insert(a.to_string());
        seconds.insert(b.to_string());
    }
    assert!(
        firsts.len() > 9_900,
        "first-half cardinality too low: {} distinct of 10_000 — a CSPRNG \
         yields ~fully-distinct halves; low cardinality indicts a predictable \
         generator",
        firsts.len()
    );
    assert!(
        seconds.len() > 9_900,
        "second-half cardinality too low: {} distinct of 10_000 — a CSPRNG \
         yields ~fully-distinct halves; low cardinality indicts a predictable \
         generator",
        seconds.len()
    );
}

// =======================================================================
// 4. NO SIMPLE SEQUENCE — consecutive tokens differ, no fixed stride
// =======================================================================

/// PROPERTY: consecutive tokens are never equal, AND the numeric values of
/// their halves do not advance by a single fixed stride (as a sequential
/// counter or a linear generator would). We parse each half as a `u64`,
/// compute wrapping diffs between consecutive draws, and assert the diffs are
/// NOT all identical. A CSPRNG's consecutive diffs are essentially random (all
/// identical is astronomically unlikely); a `+1` counter or fixed-stride LCG
/// would produce a constant diff. DISCRIMINATING against any sequential/linear
/// scheme.
#[test]
fn tokens_are_not_a_fixed_stride_sequence() {
    let ids = RandomTokenIds;
    const N: usize = 2_000;

    let mut prev_token: Option<String> = None;
    let mut first_vals: Vec<u64> = Vec::with_capacity(N);
    let mut second_vals: Vec<u64> = Vec::with_capacity(N);

    for _ in 0..N {
        let token = ids.next();
        if let Some(prev) = &prev_token {
            assert_ne!(
                prev, &token,
                "consecutive tokens must never be equal (got a repeat: {token:?})"
            );
        }
        let (a, b) = payload_halves(&token);
        first_vals.push(u64::from_str_radix(a, 16).expect("first half is hex u64"));
        second_vals.push(u64::from_str_radix(b, 16).expect("second half is hex u64"));
        prev_token = Some(token);
    }

    // Diffs must not be a single constant across the whole run.
    assert!(
        !diffs_all_constant(&first_vals),
        "first-half values advance by a fixed stride — that is a sequential / \
         linear generator, not a CSPRNG"
    );
    assert!(
        !diffs_all_constant(&second_vals),
        "second-half values advance by a fixed stride — that is a sequential / \
         linear generator, not a CSPRNG"
    );
}

/// True iff every consecutive wrapping diff in `vals` is identical (i.e. the
/// series is an exact fixed-stride arithmetic progression). Trivially false
/// for a CSPRNG; true for a `+k` counter.
fn diffs_all_constant(vals: &[u64]) -> bool {
    if vals.len() < 3 {
        return true; // too short to distinguish — treat as "looks constant"
    }
    let stride = vals[1].wrapping_sub(vals[0]);
    vals.windows(2).all(|w| w[1].wrapping_sub(w[0]) == stride)
}

// =======================================================================
// 5. CONCURRENCY — no cross-thread collisions (small adaptive thread count)
// =======================================================================

/// PROPERTY: with a SMALL, machine-adaptive number of threads sharing one
/// `Arc<RandomTokenIds>`, each drawing a few thousand tokens, the GLOBAL union
/// of every token across every thread is all-distinct (zero cross-thread
/// collisions). A CSPRNG is collision-free regardless of interleaving; a
/// clock-derived generator is most likely to collide precisely when many
/// threads draw concurrently in the same nanosecond window.
///
/// PORTABILITY: the thread count is `available_parallelism().min(8)` (fallback
/// 4) and each thread draws only a few thousand tokens — deliberately small
/// (learning from M3) so the test itself stays portable and does not become a
/// resource hog or a non-deterministic flake on constrained CI.
#[test]
fn concurrent_draws_have_no_cross_thread_collisions() {
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get().min(8));
    const PER_THREAD: usize = 3_000;

    let ids = Arc::new(RandomTokenIds);
    let mut handles = Vec::with_capacity(threads);
    for _ in 0..threads {
        let ids = Arc::clone(&ids);
        handles.push(thread::spawn(move || {
            let mut local = Vec::with_capacity(PER_THREAD);
            for _ in 0..PER_THREAD {
                local.push(ids.next());
            }
            local
        }));
    }

    let mut union: HashSet<String> = HashSet::with_capacity(threads * PER_THREAD);
    let mut total = 0usize;
    for handle in handles {
        let local = handle.join().expect("worker thread must not panic");
        total += local.len();
        union.extend(local);
    }

    assert_eq!(
        total,
        threads * PER_THREAD,
        "each thread must draw its full quota"
    );
    assert_eq!(
        union.len(),
        total,
        "expected {total} globally-distinct tokens across {threads} threads; \
         got {} — cross-thread collisions indict a non-CSPRNG source that \
         repeats under concurrent draws",
        union.len()
    );
}

// =======================================================================
// 6. CONTRAST — `CounterTokenIds` is the deterministic test seam
// =======================================================================

/// PROPERTY (test-seam sanity, NOT a security property): `CounterTokenIds`
/// yields `"tok-mcp-0"`, `"tok-mcp-1"`, ... in order. Confirms the injectable
/// `TokenIds` seam exists and that the DETERMINISTIC arm is exactly the
/// pre-fix sequence — the deliberate foil to `RandomTokenIds`. GREEN on the
/// current impl (this arm is unchanged by M1); its presence documents WHY the
/// production arm must NOT be enumerable like this one.
#[test]
fn counter_token_ids_are_sequential() {
    let ids = CounterTokenIds::default();
    for expected in 0..1_000u64 {
        assert_eq!(ids.next(), format!("tok-mcp-{expected}"));
    }
}
