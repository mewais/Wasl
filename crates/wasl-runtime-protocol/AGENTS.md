# R2 — `wasl-runtime-protocol` — working contract

**Purpose:** the concrete interactive drivers (`FakeLineProtocol`, `ClaudeStreamProtocol`,
`OpenCodeAcpProtocol`/`OpenCodeServeProtocol`) that map each substrate's native wire onto R1's generic
`ProtocolEvent`. The one place CLI-/ACP-/opencode-specific wire knowledge is allowed. **Depends on R1 and
never the reverse** (the driver-seam traits + the internal `ProtocolEvent` live in R1; only concrete drivers
live here).
**Tier:** L2 (IO shell over the tested R1 core; real CLIs behind `--features live-cli`).
**Full spec (read first):** `../../docs/specs/runtime-protocol.md`.
**Architecture:** `../../ARCHITECTURE.md`

## What you may edit
- **code-agent:** write ONLY `crates/wasl-runtime-protocol/src/**` and `crates/wasl-runtime-protocol/Cargo.toml`.
  Read-only: `crates/wasl-runtime-protocol/tests/**`, the spec above, and the public API of `../wasl-runtime`.
- **test-agent:** write ONLY `crates/wasl-runtime-protocol/tests/**`. Read-only: the spec above.

## Phase-1 scope (THIS phase) — SKELETON ONLY
- In Phase-1 this crate has NO type surface of its own: it merely EXISTS and DEPENDS ON R1, proving the
  R2 → R1 dependency edge and that R1 owns the driver-seam traits + `ProtocolEvent`. The crate root re-exports
  the R1-owned generic contract to make the edge explicit.
- The concrete drivers are LATER phases (2B / 4 / 5), NOT Phase-1.

## Definition of done (Phase-1)
- `cargo test -p wasl-runtime-protocol` green (skeleton_smoke). `cargo build -p wasl-runtime-protocol` compiles.
- No `unsafe`. `cargo fmt` clean; `cargo clippy -p wasl-runtime-protocol --all-targets -- -D warnings` clean.

## Rules
- Tests are written BEFORE code and are read-only to the code-agent. Do not edit tests to pass.
- Implement the spec EXACTLY; invent no type. If a test references something not in the spec, STOP and report.
- No test may be `#[ignore]`d/`#[cfg]`-disabled/stubbed/a tautology.
- Deps: `wasl-runtime` (+ `serde`/`serde_json`/`async-trait`/`tokio`); later `uuid` for the claude driver.

## Coverage honesty (Phase-1 — skeleton)
Phase-1 is a re-export/dep-edge skeleton with no driver logic; it is exercised only by `skeleton_smoke.rs`.
The mapping/framing/supervision LINE+REGION-100% bar (spec §5) applies from the driver phases (2B/4/5) when
the concrete drivers land and are proven against byte-exact fixtures and stand-in commands. Report honestly;
do not game a type-only re-export with fake tests.
