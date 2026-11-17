# MCP — `wasl-mcp` (`wasl-mcpd`) — working contract
**Purpose:** the MCP server facade — expose consult/command/message/transfer/commit as MCP tools, run the
REAL three-tier gate (elicitation / operator / prepare-commit) per a configured authority, dispatch over the
tested Wasl cores, audit every decision (mirrored to an operator feed when one is bound). IO shell; NO new
decision logic. Also owns the node **config** module (identity/binds/audit path/adapter selection/peers/gate
authority), which is the one genuinely-pure, fully-tested surface in this crate.
**Spec (current):** `../../docs/specs/mcp-gating.md` (W4 — real three-tier gating: `src/gating.rs`, operator
wiring, Tier-2 prepare/commit). Also: `../../docs/specs/mcp-a2a.md` (F1 — dispatch over REAL A2A, superseded
in one pinned arm by W4 §7), `../../docs/specs/node-config.md` (W2 — node config: `src/config.rs`). Original:
`../../docs/specs/mcp.md`.
- Wires rmcp 0.9 (server + transport-io) + tokio over the tested crates. **v2:** the SEND path dispatches
  over REAL A2A (`wasl-dispatch` + `reqwest`) instead of the retired canned `LocalMesh`; audit defaults to the
  durable `wasl-filesink::FileSink`; transfer uses the real `wasl-procrunner::ProcessRunner`; `run()` also
  wires the A2A RECEIVE server (`wasl_node::receiver` over a `wasl_adapter_claude::ClaudeAdapter`).
- **W2 (`src/config.rs`):** a TOML config file (`toml` dep, direct in this crate's `Cargo.toml` — NOT
  workspace-hoisted) overlaid by the four legacy env vars, overlaid on built-in defaults, per
  `docs/specs/node-config.md`. Public API re-exported from the crate root: `Config`/`IdentityConfig`/
  `A2aConfig`/`OperatorConfig`/`AuditConfig`/`AdapterConfig`/`AdapterKind`/`PeerEntry`/`ConfigError`/
  `EnvOverrides`, plus the two consumption seams `registry_from_peers`/`build_adapter`. `run()` now calls
  `Config::load()` FIRST and fails loud (`wasl-mcpd: config error: {e}` on stderr, exit code 2) before any
  IO starts; `self_identity()`/`audit_path()` are deleted (their behavior lives in `EnvOverrides`/`apply_env`).
  `build_handler` takes `&Config` (identity/registry/audit path all config-fed; `registry_from_peers`
  replaces the always-empty `PeerRegistry::new()`); the receive adapter is built via `build_adapter(&cfg.adapter)`
  instead of being hard-wired to `ClaudeAdapter::new(ProcessCliInvoker)`.
- **W4 (`src/gating.rs` + `config`/`server`/`handle`/`lib` changes, `docs/specs/mcp-gating.md`):** the MCP
  facade now runs the REAL three-tier gate end to end instead of a hard-coded Tier-1-only shim.
  - `src/gating.rs` (new, pure, re-exported): `GateAuthority` (`Elicitation` default / `ElicitationFirst` /
    `ExternalFirst` / `PrepareCommit`), `detect_caps(peer_info, operator_attached) -> ClientCaps` (the REAL
    rmcp 0.9 `ClientCapabilities::elicitation` handshake, not a hard-coded `true`), `select_mode(authority,
    caps) -> Option<GateMode>` (the authority×caps truth table; `None` = auto-deny), `proto_action` (rmcp ->
    `wasl_proto` `ElicitationAction` type conversion ONLY — composed with the already-tested
    `wasl_gate::outcome_of` for the semantics, killing the hand-rolled duplicate mapping `server.rs` used to
    carry), `ledger_decision_of` (the Tier-2 ledger's outcome->decision bookkeeping mapping, distinct from the
    audit decision).
  - `config.rs`: `Config` gains `pub gate: GateConfig { pub authority: GateAuthority }` (`[gate] authority =
    "…"` TOML key, `#[serde(deny_unknown_fields, default)]`, validated as the exact 4 strings after adapter
    kind and before peer urls); `ConfigError::UnknownGateAuthority(String)` (Display `"unknown gate authority:
    {value}"`); `operator.bind` is unchanged shape but now CONSUMED by `run()` (§ below) instead of "RESERVED
    FOR W3".
  - `server.rs`: `WaslMcp` gains `authority`/`hub: Option<wasl_operator::OperatorHub>` fields and a
    `with_gating(.., authority, hub)` constructor (`new(..) == with_gating(.., Elicitation, None)`, byte-for-
    byte backward compat); `WaslState` gains the Tier-2 tables (`ledger: wasl_gate::GateLedger`,
    `pending_calls: HashMap<ApprovalToken, ToolCall>`, `decisions: HashMap<ApprovalToken, GateOutcome>`, in
    lockstep — inserted/removed together), constructed empty by the unchanged `WaslState::new` signature.
    `gated_call` now: `detect_caps` -> `select_mode` -> branch on `None` (pinned deny reason, unchanged
    string) / `Elicitation` (mirrored observe-only via `hub.announce`/`resolve_announced` when a hub is
    attached — never decidable, never enters `/v1/gates`) / `External` (a fresh `OperatorSurfacer` built
    per-gate inside `tokio::task::spawn_blocking`, never a plain async worker — the embedder contract) /
    `PrepareCommit` (prepare the ledger + `pending_calls`, spawn a background decision waiter when a hub is
    bound, return the pinned `Pending` reply). New `commit` tool (5th tool, always advertised) redeems a
    token: a landed decision commits the ledger + dispatches (the ONE audit record for that gate, written at
    commit); no decision yet + still pending -> the pinned `pending` JSON again (idempotent poll); neither ->
    `classify_not_pending` -> the pinned `blocked: unknown/already resolved` reply. New public
    `WaslMcp::resolve_pending(token, outcome)` is the embedder's decision inlet when no operator is bound.
  - `handle.rs`: `ToolReply` gains `Pending { token: ApprovalToken }` (a SUCCESS content block, not a block —
    `is_blocked()` stays `Blocked`-only). `dispatch_approved`'s `TransferError::NoDestinations` arm no longer
    returns an empty-success `TransferResult` — it now falls into the same `Blocked` arm as every other
    transfer failure (`"blocked: transfer has no destination hosts"`); the gate decision is still audited.
    This supersedes `mcp-a2a.md`'s earlier pinned arm — see §9 below.
  - `lib.rs`: new `RunSink` (`Plain(DefaultSink)` / `Teed(TeeSink<DefaultSink>)`, delegating `Sink::append`)
    so `build_handler` stays one concrete handler type regardless of whether an operator is bound.
    `build_handler(cfg, hub)` now: opens the audit sink as before, wraps it `Teed` (hub present) or `Plain`
    (absent), and constructs via `WaslMcp::with_gating(.., cfg.gate.authority, hub)`. `run()`, after the
    (unchanged) A2A receive wiring, ALSO parses `cfg.operator.bind` (same lenient warn-and-skip idiom as
    `a2a.bind`), builds an `OperatorHub` + spawns `wasl_operator::serve` when it's a valid socket address, and
    passes the hub into `build_handler`.
  - **Backward compat (no config / no `[operator]`/`[gate]`):** `gate.authority = Elicitation`, no hub,
    `RunSink::Plain`, `caps.external = false` — `select_mode` reproduces the v1 elicitation-or-deny behavior
    exactly (identical message, identical outcome mapping, identical deny reason string). The ONLY two
    externally-visible deviations, both pinned by `docs/specs/mcp-gating.md` §9 and both required test-file
    edits (see below): (a) the `NoDestinations` fix above; (b) the tool list gains `commit` (five tools).
  - **Frozen-suite deviations (the two edits `mcp-gating.md` §9 explicitly authorizes):**
    `tests/smoke.rs`'s advertised-tools assertion (now asserts the five names, `lists_the_five_tools`) and
    `tests/handle.rs`'s `NoDestinations` test (now asserts the `Blocked` reply, renamed
    `transfer_with_no_destinations_is_blocked_not_an_empty_success`) were updated to match — these are the
    ONLY edits made to previously-existing test files for this block. `tests/config_pure.rs`'s
    `full_parse_every_field_exact` literal additionally needed a `gate: GateConfig::default()` field (a new
    public `Config` field breaks any exhaustive struct-literal construction of it — a compile-level, not
    behavioral, necessity). `crates/wasl-mcp/Cargo.toml` gained a crate-wide
    `clippy::field_reassign_with_default = "allow"` (alongside the existing `doc_lazy_continuation` one) for
    the frozen `tests/gating_unit.rs`'s `ClientInfo::default(); info.capabilities = …` idiom.
- **W7b (`src/config.rs`, `Cargo.toml`, `docs/specs/adapter-codex.md`'s "Node-config integration" delta on
  top of `docs/specs/node-config.md`):** the codex adapter is now selectable in `wasl-mcpd`'s node config.
  - `AdapterKind` gains `Codex` (`adapter.kind = "codex"`, exact case-sensitive match, same second-pass
    validation as `"claude"`; anything else, incl. `"Codex"`, is still `UnknownAdapterKind`).
  - `build_adapter`'s return type is no longer the bare `ClaudeAdapter<ProcessCliInvoker>` — it is now
    `NodeAdapter`, an exhaustive-match delegating enum (`Claude(ClaudeAdapter<ProcessCliInvoker>)` /
    `Codex(CodexAdapter<ProcessCliInvoker>)`) implementing `AgentAdapter` by delegation, re-exported from the
    crate root. An enum, not `Box<dyn AgentAdapter>`, so adding a THIRD kind later still forces a match-arm
    decision at compile time (the same forcing function W2 wanted for the original single-arm match).
    `adapter.bin`'s meaning is unchanged for both arms: absent → the adapter's own default bin
    (`"claude"`/`"codex"`), present → `with_bin`.
  - `Cargo.toml` gains `wasl-adapter-codex` (already workspace-hoisted) plus a direct `async-trait` dependency
    in `[dependencies]` (previously only a dev-dependency) — `NodeAdapter`'s `impl AgentAdapter` needs the same
    `#[async_trait::async_trait]` macro the trait itself is defined with.
  - `src/lib.rs`'s `run()` A2A-receive doc comment is updated to say "the config-selected `NodeAdapter`" rather
    than naming `ClaudeAdapter` specifically; `run()`'s actual wiring (`build_adapter(&cfg.adapter)` →
    `wasl_node::receiver(card, adapter)`) is UNCHANGED — it already worked generically against
    `impl AgentAdapter`.
  - **Frozen-suite deviation (the one edit this block makes to a previously-existing test file):**
    `tests/config_pure.rs`'s `unknown_adapter_kind_is_rejected` originally asserted `kind = "codex"` was
    unknown (written against the pre-W7 world, per `node-config.md`'s own W2-era test plan). Now that W7
    makes `"codex"` a real kind, that assertion is no longer true; the test was updated in place to probe a
    kind that is STILL unknown (`"gpt"`), same string substituted throughout (Display assertion + preserved-
    verbatim assertion) — same precedent `docs/specs/mcp-gating.md` §9 already established for two other
    frozen-file edits in this crate. No other previously-existing test file was touched. New coverage of
    `"codex"` itself (now valid, incl. a `bin` override) plus a fresh still-unknown-kind probe and the
    `NodeAdapter`/`build_adapter` behavior lives in the new `tests/config_codex.rs`, including a real loopback
    A2A proof: a `NodeAdapter::Codex` (built via `build_adapter` from a config pointing at a stub `codex`
    binary that emits the spec's captured JSONL fixture) wired as the RECEIVE side of a real `A2aServer`, with
    the SEND side driven through `Config::from_toml_str` → `registry_from_peers` → `resolve_and_dispatch`,
    proving the whole config → registry → receive chain composes for the codex arm exactly as it already did
    for claude.
- **code-agent:** ONLY `src/**` + `Cargo.toml`. **test-agent:** ONLY `tests/**`.
- Done: `cargo build -p wasl-mcp` + the `wasl-mcpd` bin compile; the async `handle`/`resolve_and_dispatch`
  heart is integration-tested green over **loopback A2A** (real a2a-server + real AdapterExecutor + fake
  adapter — NOT fakes-only); the config module (`from_toml_str`/`apply_env`/`resolve_path`/`load_with`/`load`/
  `registry_from_peers`/`build_adapter`, now incl. `[gate]` and, since W7b, `AdapterKind::Codex`/
  `NodeAdapter`) is black-box tested
  (`tests/config_{pure,resolve,wiring,bin,codex}.rs`); the real three-tier gate (`gating.rs`'s pure
  seams + operator-decided/mirrored-elicitation/prepare-commit end to end over real loopback HTTP+SSE + rmcp
  duplex) is tested in `tests/gating_{unit,loopback}.rs`; `cargo test -p wasl-mcp` is 93 tests green (52
  pre-W4 + 34 W4: 20 `gating_unit` + 14 `gating_loopback`, + 7 W7b `config_codex`); `clippy -p wasl-mcp --all-targets -D warnings` + fmt clean; `cargo test --workspace`
  green; no unsafe; no leftover `unimplemented!`/`todo!`; the gate is never bypassed (non-Approved never
  dispatches, and Tier-2 audits nothing until `commit`).
- **Testability honesty:** ✅ the async send heart end-to-end over loopback A2A (gate→audit→A2A→receive→reply)
  + block/deny/dismiss + audit-failure abort + transfer; ✅ (W2) the config module's parse/validate/precedence/
  path-resolution semantics, registry population from a config file all the way through a real loopback A2A
  dispatch, adapter bin selection through a real child process, and the binary's fail-loud startup on bad
  config (spawned-process exit-code-2 assertions in `tests/config_bin.rs`); ✅ (W4) real rmcp capability
  detection (unit + a real handshake over duplex), the full `GateAuthority`×caps mode table, operator-decided
  sends (approve/deny/timeout) over REAL loopback HTTP+SSE, Tier-1 mirroring (observe-only, non-decidable,
  incl. the transport-error deny), Tier-2 prepare→commit through the tools via both the operator POST and the
  embedder API, the deduplicated `proto_action`+`outcome_of` mapping, the `NoDestinations` blocked reply,
  byte-for-byte default (no-config) behavior, and `RunSink`'s `Plain`/`Teed` delegation incl. the
  append-before-broadcast / no-frame-on-error contract. ❌ UNTESTED HERE (unchanged from before W2/W4): the
  rmcp stdio serve loop, the A2A receive serve loop in `run()`, the LIVE `wasl_operator::serve()` bind inside
  `run()` (its router IS loopback-tested via `wasl-mcp`'s own `gating_loopback.rs` plus `wasl-operator`'s own
  suite), driving a real Claude CLI, and true cross-host over SSH/WAN. Do NOT claim a coverage % for this
  pre-existing IO shell (rmcp/stdio + serve wiring in `lib.rs`) — it is verified by compile + the rmcp smoke
  test + (for the config-error exit path only) the spawned-binary tests.
- **Coverage honesty (verified, W2):** `src/config.rs` — the module W2 added — is genuinely **100%**
  LINE/FUNCTION/REGION. Getting there required one deliberate, non-gamed restructure: `Config::load_with`'s
  "no resolvable path" case (`resolve_path` -> `None`, e.g. `WASL_CONFIG`/`XDG_CONFIG_HOME`/`HOME` all unset —
  a real but, given the frozen `tests/config_resolve.rs` fixtures, untested-in-isolation combination for
  `load_with` specifically, as opposed to `resolve_path` itself which the same suite tests exhaustively) is
  folded into the SAME already-covered "default path exists-but-missing" arm via
  `Config::resolve_path(env).unwrap_or_default()`: `std::fs::read_to_string("")` reliably returns
  `ErrorKind::NotFound` (verified empirically), so both cases hit one physical branch. No dead code, no
  `#[allow]`-for-coverage, no panics — this is a real behavioral equivalence ("nothing to read" either way),
  not a coverage trick.
- **Coverage honesty (verified, W4):** `src/gating.rs` — the pure seams this block added (`detect_caps`,
  `select_mode`, `proto_action`, `ledger_decision_of`) — and `src/config.rs`'s `[gate]` additions are
  genuinely **100%** LINE/FUNCTION/REGION together:
  `cargo llvm-cov -p wasl-mcp --ignore-filename-regex 'lib\.rs|server\.rs|handle\.rs|main\.rs' --fail-under-lines 100 --fail-under-regions 100`
  exits 0 (304/304 regions, 25/25 functions, 252/252 lines across the two files, pre-W7b). The ONE permitted
  verified-unreachable region in the tier logic outside that gate is `server.rs`'s
  `classify_not_pending`'s `Ok(_)` arm (`unreachable!(..)`): it is only ever called when
  `ledger.is_pending(token) == false`, and `wasl_gate::GateLedger::commit` only ever returns `Ok` when the
  token IS still pending — the two preconditions can never both hold, so the arm is structurally dead, not
  merely untested. Documented in-code (not gamed with dead code or a silent default), the same precedent
  `wasl-filesink`'s AGENTS.md documents for its own never-taken `serde_json::to_string` error branch.
  - **Whole-package total (verified, W4):** `cargo llvm-cov -p wasl-mcp --summary-only` reports TOTAL
    **86.37% lines** / 82.86% regions (up from the pre-W4 baseline of ~51% lines) — `handle.rs` 92.86% line /
    91.27% region and `server.rs` 95.80% line / 92.87% region are now genuinely exercised by the loopback
    suites (previously 90.10%/7.47%). The residual gaps in both are honest, not gamed:
    `handle.rs`'s `ToolReply::is_blocked` and `HandleError`'s `Display` impl (2 pre-existing missed functions,
    unchanged by this block — `git diff` on their bodies is empty); `server.rs`'s elicitation
    transport-error deny arm (`surface_elicitation`, requires forcing the rmcp duplex transport itself to
    fail mid-call — not exercised by any loopback fixture, same status as the identical arm in the pre-W4
    `surface_gate`), `handle_error_to_mcp` and `truncate`'s >120-char branch (no fixture drives an audit-sink
    failure or an over-length preview through the rmcp layer specifically — both ARE exercised at the
    `handle.rs`/pure-logic level), and `reply_to_result`'s `Command`/`Message`/`Transfer` JSON-success arms
    (only `Consult`'s successful-dispatch shape is decoded by name in the loopback fixtures; the other three
    tools' gate/dispatch/audit PATHS are all still exercised, just not their JSON rendering specifically).
    `lib.rs` remains the pre-existing, spec-sanctioned-untestable IO shell (20.33% region / 19.64% line): the
    rmcp-stdio/A2A-receive-serve/operator-serve glue in `run()` — same "compile-only shell" status as before
    W2/W4 (the config-error exit path is exercised via the spawned-binary `tests/config_bin.rs`). This crate
    has never carried a whole-package `--fail-under-lines 100` gate (see the pre-W2 stance, carried forward
    unchanged); W4 substantially narrows the gap (51% -> 86%) rather than closing it, and does not attempt to
    newly test the rmcp stdio loop / live A2A or operator receive loops / a real Claude CLI to force the raw
    command to pass.
- **Coverage honesty (verified, W7b):** `src/config.rs`'s `AdapterKind::Codex` arm, the new `NodeAdapter` enum
  (both match arms of its `AgentAdapter::deliver` delegation), and `build_adapter`'s new `Codex` match arm are
  genuinely **100%** LINE/FUNCTION/REGION, together with the unchanged `gating.rs`:
  `cargo llvm-cov -p wasl-mcp --ignore-filename-regex 'lib\.rs|server\.rs|handle\.rs|main\.rs' --fail-under-lines 100 --fail-under-regions 100`
  still exits 0, now 312/312 regions, 26/26 functions, 264/264 lines across the two files (up from 304/25/252
  pre-W7b — the delta is exactly the new `NodeAdapter` type + its `Codex` arms, exercised by
  `tests/config_codex.rs`'s `build_adapter_selects_the_codex_variant_for_adapter_kind_codex` and the real
  loopback receive test, which drives `NodeAdapter::Codex`'s `deliver` delegation through a real child
  process). No new dead code, no `#[allow]`-for-coverage. Whole-package total (`--summary-only`) ticks up
  correspondingly to **86.57% lines** / 82.98% regions (from 86.37%/82.86%) — `lib.rs`'s residual 20.33%
  region / 19.64% line untested-IO-shell status is unchanged (the doc-comment edit there carries no new
  executable code), and no other file's numbers moved.
- **M1 (`src/server.rs`, `Cargo.toml`, security hardening):** `RandomTokenIds` — the Tier-2 approval-token
  generator — now draws its 128 bits from the **OS CSPRNG via `getrandom`** (`getrandom::getrandom(&mut [u8;8])`
  per draw, two draws per token), replacing the old `random_u64` that hashed `SystemTime::now()` nanoseconds
  through a `std::collections::hash_map::RandomState` (SipHash — stdlib-documented as HashDoS-resistant,
  explicitly NOT cryptographic, and fed a low-entropy/predictable wall-clock input). The public contract is
  byte-for-byte unchanged: `RandomTokenIds: TokenIds`, `next()` still returns `"tok-mcp-"` + 32 lowercase hex
  (`{:016x}{:016x}`); `CounterTokenIds` (the deterministic test seam) is untouched. `getrandom` is fail-closed
  (`.expect` on OS-entropy failure — a machine that cannot supply cryptographic entropy must not mint bearer
  approval tokens). `getrandom = "0.2"` is a DIRECT dep (pinned to the `0.2.17` already resolved in the build
  graph via `ring`, so `Cargo.lock` did not change and no duplicate/forked getrandom major was introduced); NOT
  `rand` (heavier, unnecessary).
- **Testability honesty (M1):** the CSPRNG switch is a **SOURCE-LEVEL** security property — the correctness of
  M1 is that `server.rs`'s `random_u64` now reads from a documented OS CSPRNG, which is verified by reading the
  source (dev-manager review), NOT by a red→green test flip. The black-box `tests/token_ids.rs` suite (6 tests:
  format, 10k-uniqueness, per-half high-cardinality, no-fixed-stride, portable concurrency, `CounterTokenIds`
  contrast) is a **REGRESSION NET, not a CSPRNG proof**: statistically a genuine CSPRNG passes all six with
  certainty, but these tests cannot themselves prove the source is a CSPRNG (the tester honestly reported the
  weak timestamp-hash impl also passed them — they can't distinguish the two statistically under a normal
  clock). All six stay green on the hardened impl. No coverage % is claimed for `server.rs` here (it is NOT
  under a 100% gate per this crate's coverage-honesty stance above); the new `getrandom` call sits inside the
  already-exercised token path and does not change any of the stated 100%-gated `config.rs`/`gating.rs` numbers.
