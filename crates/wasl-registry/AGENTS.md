# WR — `wasl-registry` — working contract

**Purpose:** peer registry + agent cards + mesh route resolution (Local vs Remote). Pure; mesh IO is the
daemon block. **Tier:** L0/L1 (no IO, no `unsafe`).
**Spec:** `../../docs/specs/registry.md`

## What you may edit
- **code-agent:** ONLY `crates/wasl-registry/src/**` + `Cargo.toml`. Read-only: `tests/**`, the spec, `../wasl-proto/src/lib.rs`.
- **test-agent:** ONLY `crates/wasl-registry/tests/**`. Read-only: the spec + `../wasl-proto/src/lib.rs`.

## Definition of done
- API in `docs/specs/registry.md` implemented exactly; `cargo test -p wasl-registry` green;
  `cargo llvm-cov -p wasl-registry --fail-under-lines 100 --fail-under-regions 100` passes; no unsafe/IO;
  `cargo fmt` clean; `cargo clippy -p wasl-registry --all-targets -- -D warnings` clean.

## Rules
- Tests before code, read-only to code-agent. No `#[ignore]`/`#[cfg]`-disable/stub/tautology. Unreachable
  branches: delete. Deps: `wasl-proto` + `serde` (+ `serde_json` dev).
