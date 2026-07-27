# External managed-runtime seam — design sketch (Phase 6, doc-only)

**Status:** sketch only. **Nothing in this document is built or scheduled.** Per
`docs/design/live-runtime-plan.md` §8 and `docs/design/live-runtime-impl-plan.md`'s Phase 6 entry, this is
explicitly a follow-on document written once phases 0′–5 of the live-runtime plan shipped and went green
(`wasl-runtime`, `wasl-runtime-protocol`'s `FakeLineProtocol`/`ClaudeStreamProtocol`/`OpenCodeAcpProtocol`,
`wasl-runtime-control`, `wasl-session`) — so that this sketch can be checked against two REAL, independently
different managed drivers instead of only the paper design. It designs/builds nothing; a specific external
integration is separate follow-on work, to be scoped only if/when a real external supervisor is identified.

**Process note:** the impl-plan asked for a compile-only `ExternalManagedRuntimeProvider` stub to land in
**Phase 1**, specifically so a trait-surface problem would surface early rather than late. That did not
happen — Phase 1 shipped without it. It is closed out now, in Phase 6, as
`crates/wasl-runtime/examples/external_seam_stub.rs` (see §3 below), rather than left undone. Late, not
skipped.

---

## 1. The question this sketch answers

Every driver built so far (`FakeLineProtocol`, `ClaudeStreamProtocol`, `OpenCodeAcpProtocol`) is a **local
process** Wasl itself spawns and owns end-to-end: Wasl holds the `Child` handle, owns stdin/stdout/stderr,
and runs the single-writer actor directly against that handle. The question this sketch is required to
answer, per council #3, is narrower and sharper than "could Wasl talk to something external": **can the
EXISTING `RuntimeProvider` / `DeliveryTarget` / `NativeGateResolver` / `ToolCallAnswerer` trait surface —
frozen since Phase 1, unchanged through Phases 2A–5 — be implemented by something that owns NO
process/PTY/OS handle at all, without adding a method, a generic parameter, or leaking any CLI-specific
concept into a signature?**

The answer, proven by compilation (§3), is **yes** — with zero changes to the frozen surface. This matters
because it means the two real drivers built in Phases 4–5 did NOT accidentally shape the trait surface around
"a thing with a `Child` handle" even though every existing implementor happens to have one.

## 2. The seam: contract boundary, never a handle boundary

The correct shape, unchanged from the original plan sketch, is **"same target contract, different provider
implementation."** An external supervisor — something that spawns/owns/runs an agent session on infrastructure
Wasl doesn't control — implements `RuntimeProvider` the exact same way `WaslManagedRuntimeProvider` and
`EphemeralRuntimeProvider` already do, and hands back an `Arc<dyn DeliveryTarget>` the generic
actor/scheduler/R3 control surface cannot distinguish from a locally-spawned one:

```rust
/// FUTURE / NOT PART OF ANY SHIPPED PLAN. Fronts a target OWNED and RUN by an
/// external supervisor, spoken to over a GENERIC transport (HTTP / JSON-RPC /
/// gRPC — deliberately unpicked here) using only GENERIC terms: target_id,
/// capability flags, turn requests/receipts/events. NO consumer-specific
/// field, type, or assumption. Returns a `DeliveryTarget` like any other; the
/// generic scheduler cannot tell the difference.
pub struct ExternalManagedRuntimeProvider { /* generic endpoint config only */ }
```

**Hard rule, locked even for this future seam (unchanged from the original plan, re-affirmed here after
5 real phases of building against it):** Wasl must **NEVER** be handed a raw process / PTY / OS handle by an
external party. Handle-passing creates ownership/ordering ambiguity, dueling stdin writers, and invisible
human-input races — precisely the failure modes the §4 single-writer invariant exists to prevent, and
precisely the "Model B" (attach to a human's own terminal) shape this whole feature already forecloses
(`live-runtime-plan.md` §9). The seam is a **contract boundary** (`DeliveryTarget` over a generic wire), never
a handle boundary. An external supervisor registers/owns the underlying session **on its side**; Wasl only
ever sees the generic target contract — `submit_turn`, `events`, `cancel_turn`, `shutdown`,
`resolve_native_gate`, `answer_tool_call`. This mirrors Wasl's existing external-integration philosophy
exactly: the `Surfacer` / audit `Sink` seams are generic, consumer-named-by-nobody, and optional.

### What Phases 2–5 taught us that the original v1 sketch (written before any real driver existed) could not know

- **Every real driver's hardest problem — process supervision — simply DISAPPEARS on this seam.** Both
  `ClaudeStreamProtocol` and `OpenCodeAcpProtocol` spend the bulk of their code (and the bulk of their honest
  coverage residual, per their own `AGENTS.md` sections) on stderr draining, the hang watchdog, the bounded
  interrupt→SIGTERM→SIGKILL escalation ladder, and the M2 EPIPE-benign write discipline — all of it exists
  ONLY because Wasl owns a raw OS child. An `ExternalManagedRuntimeProvider` owns none of that: the external
  supervisor's OWN infrastructure is responsible for its process lifecycle, exactly the same way `wasl-audit`'s
  `Sink` or a `Surfacer` never has to know how its consumer stores or renders what it's handed.
- **The R4 control-plane API (`wasl-runtime-control`, Phase 3.5) is a live, running proof that the generic
  wire shape sketched in §680–689 of the v1 plan actually works end-to-end**, not just on paper: R4's own
  `router()`/`serve()` already expose a target over HTTP + SSE — `GET /v1/targets/{id}/events` (snapshot +
  live tail + `Truncated`), `POST /v1/targets/{id}/turns` (submit), `POST .../cancel`, `POST .../gate`,
  `POST .../shutdown` — using EXACTLY the generic vocabulary (`TurnEvent`, `TargetSnapshot`,
  `SubmitTurnRequest`, `TurnReceipt`, `RuntimeCapabilities`) an external supervisor's own API would need to
  expose on ITS side for Wasl to consume as a **client**. In other words: Wasl already ships a server that
  proves the shape; a hypothetical `ExternalManagedRuntimeProvider` is structurally the SAME shape used as a
  **client of someone else's R4-equivalent**, not a new invention.
- **`Causation`/cycle-rejection (§8.2) crosses the seam unchanged.** An external supervisor calling back INTO
  Wasl (the receive side — i.e. Wasl as the target of ANOTHER system's managed runtime) is out of scope here
  (this sketch is about Wasl as a *caller* of an external provider, not the reverse), but the `Causation`
  struct's wire pinning (`root_request`/`parent_request`/`path`/`hop`, already `Serialize`/`Deserialize` with
  bare-string request ids) was designed R1-side specifically so it crosses ANY wire, generic or not — nothing
  about it assumes a local process.
- **Every honest ⛔ UNKNOWN/🅰 ASSUMED capability cell the real drivers carry (interruptibility; the
  tool/gate wire shape for both claude and opencode) has a DIFFERENT, cleaner answer over a generic wire: an
  external supervisor's `RuntimeCapabilities` report is just as honest, but is no longer bounded by "what did
  the Phase-0′ spike capture from a specific CLI" — it's bounded only by what THAT supervisor's own API
  contract promises. This sketch takes no position on what a real one would report; the point is only that
  `RuntimeCapabilities`'s 8-independent-boolean shape (`runtime.md` §2) already has room for it.**

## 3. The compile-only proof

`crates/wasl-runtime/examples/external_seam_stub.rs` implements, using ONLY `wasl-runtime`'s public API (no
`wasl-runtime-protocol` dependency, no `tokio::process`, no `PathBuf`, no `Command`, no CLI concept of any
kind):

- `ExternalTarget: DeliveryTarget + NativeGateResolver + ToolCallAnswerer + Debug` — every method stubbed
  (fixed/default return values, or `RuntimeError::Unsupported` where a real generic-wire round-trip would be
  needed), backed by an intentionally-undesigned `Arc<dyn ExternalWireClient>` seam (a marker trait, left
  EMPTY on purpose — designing that wire client's own API is real follow-on work this stub does not do).
- `ExternalManagedRuntimeProvider: RuntimeProvider` — `target_for`/`list_targets`, constructing an
  `ExternalTarget` per key, holding nothing but the generic wire seam.
- A trivial `main()` that actually CONSTRUCTS a real `ExternalManagedRuntimeProvider` and upcasts it to
  `&dyn RuntimeProvider` (proving the whole assembly compiles end to end as an object-safe trait object, not
  merely that the impl blocks parse in isolation) — deliberately never invoking the async methods (no
  executor is pulled into this example; type-checking the trait bounds is the entire point).

**Result:** `cargo build -p wasl-runtime --example external_seam_stub` and
`cargo clippy -p wasl-runtime --example external_seam_stub -- -D warnings` both pass clean, with ZERO changes
to `wasl-runtime/src/**`. The frozen Phase-1 trait surface hosts this seam exactly as-is. No method needed
adding, no generic parameter, no leaked CLI-ism. Council #3's concern is answered: negatively for "does the
surface need to change," which is the good outcome — the abstraction that already shipped is general enough.

## 4. Non-goals (unchanged from the original plan, re-affirmed)

- No cross-restart durability crosses this seam either — an external supervisor's OWN target state
  survives however IT chooses to persist it; Wasl's own in-memory posture (`live-runtime-plan.md` §9) is
  unaffected either way.
- No Model B (shared raw input path) — restated in §2's hard rule.
- No specific external integration is designed, scoped, or committed to by this document. If/when a real
  external supervisor is identified, THAT is the follow-on document this one predicted would be needed.
