# live-substrate — Phase 0′ feasibility SPIKE

**Throwaway diagnostic. NOT production code. NOT part of the cargo workspace.**
Nothing here is referenced by any `Cargo.toml`; it does not affect the build. Delete the
whole `spikes/live-substrate/` directory at any time with zero impact on Wasl.

## What this proves

The GO/NO-GO gate for the "live / managed-runtime" feature
(`docs/design/live-runtime-plan.md` §6 Phase 0; `docs/design/live-runtime-impl-plan.md`
§0.2 Phase 0′). The whole feature rests on ONE fact: **can any of Wasl's target coding-agent
CLIs be driven SAME-PROCESS, MULTI-TURN?** — i.e. one long-lived process that serves turn 2
using the context from turn 1, without being respawned and without closing its stdin between
turns. If no CLI can, the generic runtime core must **not** be built speculatively.

For each candidate CLI the spike proves or disproves three facts, **all without EOF-ing stdin
between turns**:

| Fact | Meaning |
|---|---|
| **P** `persistent_process` | the **same OS pid** serves turn 2 as turn 1 (no respawn), still alive after turn 1 |
| **C** `context_retained`   | turn 2 recalls a token (`VIOLET-7F29`) planted in turn 1 |
| **B** `turn_boundary`      | a machine-detectable per-turn completion signal exists for **both** turns |

A CLI **PASSES** only if P **and** C **and** B are all `verified-true`. Needing an stdin
close (EOF) to get a result == one-turn-per-process == **FAIL for persistence**.

### What is probed, per CLI

- **claude (primary):** stream-json duplex — one `claude -p --input-format stream-json
  --output-format stream-json --verbose --session-id <uuid>` process, user messages written
  one-JSON-per-line to a **persistent, never-closed stdin**, reading `result` events as turn
  boundaries. This is the strongest near-term candidate.
- **opencode (secondary, two variants, launched with an ISOLATED config):**
  `opencode serve` (headless HTTP server) and `opencode acp` (ACP JSON-RPC over stdio). The
  per-turn `opencode run` is explicitly **not** the live substrate and is not probed.
  The `serve` HTTP message-API shape is not documented by `--help`, so the probe **discovers
  it at runtime**; if it can't, it records `API-shape-unknown, needs follow-up` rather than
  guessing.
- **codex (brief):** `codex exec`/`exec resume` is per-turn → `persistent_process` via the
  contracted interface is `unsupported-by-documented-interface`, `persistent_conversation` is
  true. The probe notes that experimental `app-server`/`exec-server`/`mcp-server` subcommands
  exist (left `unknown` — a Phase-0 follow-up, out of 0′ scope).

## Prerequisites

1. **`LLM_GATEWAY_KEY`** must be exported (the real model turns go through the AMD gateway).
   The script hard-fails prereqs if it is unset.
2. **`python3`** (stdlib only — no `pip`, no venv).
3. At least one of the CLIs on `PATH` (or at `~/.opencode/bin/opencode`). Absent CLIs are
   SKIPPED, not failed. On this machine: `claude` ✓, `codex` ✓, `opencode` ✓ (v1.18.4).
4. **Memory for opencode:** opencode OOMs under ~2 GB and its MCP catalog bloats context. The
   spike already launches it with an isolated config (own `XDG_CONFIG_HOME`/`XDG_DATA_HOME`,
   `OPENCODE_DISABLE_PROJECT_CONFIG=1`, `OPENCODE_DISABLE_AUTOCOMPACT=1`, minimal
   `opencode.json` → gateway + `Claude-Haiku-4.5`). Give the host **≥ ~4 GB free** so the
   opencode variants aren't `env-blocked`.

## How to run

One command:

```bash
bash spikes/live-substrate/run.sh
```

(Run it from the repo root, or anywhere — it locates itself.) It is **idempotent**: each run
wipes and recreates `out/`. It can **never hang your shell or leave zombies** — every spawned
CLI runs in its own process group under a hard `timeout`, and a `trap` sweeps all groups and
removes temp dirs on exit/interrupt/timeout.

Optional environment overrides:

```bash
WASL_SPIKE_PROBE_TIMEOUT=240   # per-probe wall-clock cap (default 180s)
WASL_SPIKE_TURN_TIMEOUT=120    # per-turn cap handed to the drivers (default 90s)
WASL_SPIKE_OPENCODE_BIN=/path/to/opencode
WASL_SPIKE_CLAUDE_BIN=claude
WASL_SPIKE_CODEX_BIN=codex
```

## Expected runtime

Roughly **1–4 minutes**. Real model turns take ~10–60 s each; claude runs 2 turns, opencode
runs up to 2 turns × 2 variants. Each probe is capped at `WASL_SPIKE_PROBE_TIMEOUT` (default
180 s) so a wedged CLI cannot stall the run.

## How to read the result

1. **Live console output** — a prereq block, one `[<cli>] VERDICT: PASS|FAIL|SKIP|ENV-BLOCKED|
   UNKNOWN` line per CLI, then the final banner:
   - `GATE: PASS  (>=1 CLI proven same-process multi-turn)` → **GO** (exit code 0): proceed to
     build the generic live-runtime core.
   - `GATE: FAIL  (no live substrate ...)` → **NO-GO** (exit code 1): do **not** build the
     generic core; ship the capability-matrix finding.

2. **`out/capability-matrix.md`** — the durable artifact. One row per CLI with P / C / B /
   native session id / interruptible, each cell an explicit state:
   `verified-true` · `verified-false` · `unsupported-by-documented-interface` · `env-blocked`
   · `not-installed` · `unknown`. Every `verified-true` links its captured transcript. There
   are **no ambiguous "candidate" cells** — inconclusive probes read `unknown` (re-runnable).

3. **`out/<cli>/`** — raw evidence per CLI:
   - `out/claude/`: `verdict.json`, `events.jsonl` (every stream-json object incl. the
     ready / result / session-id events), `probe.log`, `argv.txt`, `stderr.txt`.
   - `out/opencode/serve/` and `out/opencode/acp/`: per-variant `verdict-*.json`, discovered
     endpoints, per-turn request/response captures, `messages.jsonl` (ACP JSON-RPC),
     `server.log`, the isolated `opencode.json`.
   - `out/codex/`: `--help` capture + a short `summary.txt`.

> **Credentials are redacted from all captured output.** The probes run every HTTP/config
> response body and captured log through a `redact()` helper before writing to disk (any
> `apiKey` / `authorization` / `*-Subscription-Key` / `token` / `secret` / `key` /
> `password` value becomes `<redacted>`), and the gateway key is only ever passed to the
> child via the environment (the generated `opencode.json` uses the `{env:LLM_GATEWAY_KEY}`
> placeholder, never the resolved value). `out/` may therefore be safely inspected and shared.

## What was validated where

- **Syntax + non-model logic validated in the authoring sandbox:** `bash -n run.sh`,
  `python3 -m py_compile` of both drivers, the `run_bounded`/trap timeout logic, isolated
  opencode-config generation, the SKIP/GATE/matrix flow (via fake bins), and **both drivers
  end-to-end against local stream-json / HTTP / ACP JSON-RPC emulators** — the claude driver
  proven to report `pass=true` (P/C/B verified-true, token recalled) on a passing emulator and
  `verified-false` on a single-shot emulator; the opencode `serve` and `acp` variants proven to
  pass on a good emulator and to record `API-shape-unknown` (not crash) when the turn endpoint
  is absent.
- **Only runs on your box (needs real CLIs + real model turns + memory + approval):** the
  actual model calls to claude / opencode through the AMD gateway. That is exactly what
  `bash spikes/live-substrate/run.sh` performs on your machine.
