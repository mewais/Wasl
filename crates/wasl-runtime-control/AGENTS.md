# R4 — `wasl-runtime-control` — working contract

**Purpose:** Runtime Control API v1 serving crate over `wasl_runtime::RuntimeProvider`: wire types, hub orchestration, and axum HTTP+SSE router/`serve()`.
**Spec:** `../../docs/specs/runtime-control-api.md`.

- **code-agent:** edit ONLY `src/**` and `Cargo.toml`.
- **test-agent:** owns `tests/**` and `fixtures/**` (frozen acceptance corpus); code-agent must never edit them.

## Ownership split

- `src/**`, `Cargo.toml`: code-agent
- `tests/**`, `fixtures/**`: test-agent (frozen)

## Coverage honesty posture

- Wire types (`src/wire.rs`) are pure data-contract code and should be held to full line+region coverage by fixture tests.
- Router/`serve()` and SSE plumbing are IO shell and loopback-oriented; they are not held to 100% line/region so long as wire-contract behavior remains pinned by fixtures and hub unit tests.
