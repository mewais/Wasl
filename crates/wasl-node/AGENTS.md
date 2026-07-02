# D2 — `wasl-node` — working contract
**Purpose:** the per-host send/receive unit — compose gate (`wasl-server`) + send dispatch (`wasl-dispatch`)
+ A2A receive server (`wasl-a2a-server` + `wasl-adapter`). NO new decision/routing logic. **Spec:** `../../docs/specs/node.md`
- **code-agent:** ONLY `src/**` + `Cargo.toml`. **test-agent:** ONLY `tests/**`. Read-only: spec + the dep crates' `src/lib.rs`.
- Done: `cargo test -p wasl-node` green — INCLUDING the two-node loopback e2e (two real A2A nodes on 127.0.0.1, agent A gates+dispatches consult/command/message to agent B and gets replies; partial multicast; blocked/pending). `cargo llvm-cov -p wasl-node --fail-under-lines 100` (region residual only from generic HTTP/serde monomorphization, documented). No unsafe; fmt + `clippy --all-targets -- -D warnings` clean.
- Tests before code, read-only to code-agent. No `#[ignore]`/stub/tautology. The real-CLI `AgentAdapter` last inch stays a fake here (UNTESTED HERE); everything else is real over loopback.

## Coverage note (honest)
Genuine **100% LINE** coverage (177/177) + 100% functions; **REGION 99.51%** (1 residual). The single
unreached region is NOT an uncovered source line (the merged code-region view is fully covered) — it is an
unexecuted monomorphized instantiation of the generic send/dispatch path expanding into `wasl-a2a-client`'s
reqwest/serde generics (the same documented artifact as `wasl-a2a-client`). No dead code/panics were added to
force it. Gate for this crate: `--fail-under-lines 100`.
