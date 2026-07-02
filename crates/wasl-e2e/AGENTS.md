# E2E — `wasl-e2e` — working contract
**Purpose:** prove the Wasl crates compose end-to-end. Integration-test-only crate (empty src) that wires
`wasl-server` (Gatekeeper), `wasl-daemon` (dispatch), `wasl-audit`, `wasl-registry`, `wasl-mailbox`,
`wasl-transfer` over their trait seams with fakes, exercising full flows. **Spec:** `../../docs/specs/e2e.md`
- **code/test:** this crate is TESTS-ONLY — everything lives in `tests/**`. No production src beyond the doc stub.
- Done: `cargo test -p wasl-e2e` green; scenarios cover approve/deny/Tier-2/Tier-3/transfer/partial-multicast end to end; no unsafe; fmt + `clippy --all-targets -- -D warnings` clean. NOT a 100%-coverage unit (it has no src) — but every test is real, asserting the composed behavior. No `#[ignore]`/stub/tautology.
