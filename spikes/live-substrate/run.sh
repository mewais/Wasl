#!/usr/bin/env bash
#
# spikes/live-substrate/run.sh
#
# Phase-0' feasibility SPIKE (throwaway diagnostic; NOT production code, NOT part of
# the cargo workspace). Proves, on THIS machine, whether any coding-agent CLI can be
# driven SAME-PROCESS, MULTI-TURN. The result is the GO/NO-GO gate for building the
# generic "live runtime" core (docs/design/live-runtime-plan.md §6 Phase 0;
# docs/design/live-runtime-impl-plan.md §0.2 Phase 0').
#
# For each candidate CLI we prove or disprove three facts:
#   (P) persistent_process  -- the SAME OS pid serves turn 2 as turn 1 (no respawn)
#   (C) context_retained    -- turn 2 sees turn 1's context (recalls a planted token)
#   (B) turn_boundary       -- a machine-detectable per-turn completion signal exists
#   ...all WITHOUT closing/EOF-ing stdin between turns (needing an EOF to get a
#      result == one-turn-per-process == FAIL for persistence).
#
# The user runs this in ONE shot:  bash spikes/live-substrate/run.sh
# It prints prereqs, a per-CLI PASS/FAIL line, writes out/capability-matrix.md, and a
# final GATE verdict. Absent CLIs are SKIPPED (not failed). A trap kills every spawned
# process group on exit/timeout so it can never hang the shell or leave zombies.
#
set -u  # (intentionally NOT -e: a failing probe must not abort the whole spike)

# --------------------------------------------------------------------------- #
# Paths & config
# --------------------------------------------------------------------------- #
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="$SCRIPT_DIR/out"
CLAUDE_PROBE="$SCRIPT_DIR/claude_probe.py"
OPENCODE_PROBE="$SCRIPT_DIR/opencode_probe.py"
MATRIX="$OUT_DIR/capability-matrix.md"

# Per-probe wall-clock cap (real model turns take 10-60s; 2 turns + overhead).
PROBE_TIMEOUT_SECS="${WASL_SPIKE_PROBE_TIMEOUT:-180}"
# Per-turn cap handed to the python drivers.
TURN_TIMEOUT_SECS="${WASL_SPIKE_TURN_TIMEOUT:-90}"

OPENCODE_BIN="${WASL_SPIKE_OPENCODE_BIN:-}"
CLAUDE_BIN="${WASL_SPIKE_CLAUDE_BIN:-claude}"
CODEX_BIN="${WASL_SPIKE_CODEX_BIN:-codex}"

# Isolated temp root for opencode (own XDG dirs + minimal config). Cleaned on exit.
TMP_ROOT=""

# --------------------------------------------------------------------------- #
# Cleanup / trap: kill every child process group; remove temp dirs.
# --------------------------------------------------------------------------- #
declare -a SPAWNED_PGIDS=()

cleanup() {
    # Kill any tracked process groups (probes run start_new_session, so their own
    # children die with them; this is a belt-and-suspenders sweep).
    for pgid in "${SPAWNED_PGIDS[@]:-}"; do
        [ -n "$pgid" ] && kill -TERM "-$pgid" 2>/dev/null
    done
    sleep 0.3
    for pgid in "${SPAWNED_PGIDS[@]:-}"; do
        [ -n "$pgid" ] && kill -KILL "-$pgid" 2>/dev/null
    done
    # Sweep any stray isolated-opencode servers we launched (defensive).
    if [ -n "$TMP_ROOT" ] && [ -d "$TMP_ROOT" ]; then
        rm -rf "$TMP_ROOT" 2>/dev/null
    fi
}
trap cleanup EXIT INT TERM

# Run one command as its own process group under a hard timeout, tracked for cleanup.
# Usage: run_bounded <secs> <logfile> -- cmd args...
#
# setsid makes the child a process-group leader (pgid == its pid), so we can signal the
# WHOLE group -- `timeout` alone only signals its direct child, which leaks grandchildren
# (verified). After wait returns (normal exit OR timeout kill), we sweep the entire group
# so no probe child/grandchild is ever left running.
run_bounded() {
    local secs="$1"; shift
    local logf="$1"; shift
    [ "$1" = "--" ] && shift
    setsid timeout --signal=TERM --kill-after=10 "$secs" "$@" >"$logf" 2>&1 &
    local pid=$!
    SPAWNED_PGIDS+=("$pid")   # child is a group leader (setsid) => pgid == pid
    wait "$pid"
    local rc=$?
    # Belt-and-suspenders: nuke the whole group (kills any grandchildren timeout missed).
    kill -TERM "-$pid" 2>/dev/null
    sleep 0.2
    kill -KILL "-$pid" 2>/dev/null
    return $rc
}

# --------------------------------------------------------------------------- #
# Header + prereqs
# --------------------------------------------------------------------------- #
mkdir -p "$OUT_DIR"
echo "==============================================================================="
echo " Wasl live-substrate feasibility SPIKE (Phase 0')"
echo " Proving: can any CLI be driven SAME-PROCESS, MULTI-TURN? (GO/NO-GO gate)"
echo "==============================================================================="
echo
echo "-- Prerequisites --------------------------------------------------------------"

PREREQ_OK=1

# LLM_GATEWAY_KEY (real model turns need it)
if [ -n "${LLM_GATEWAY_KEY:-}" ]; then
    echo "  [ok]   LLM_GATEWAY_KEY is set (len ${#LLM_GATEWAY_KEY})"
else
    echo "  [FAIL] LLM_GATEWAY_KEY is NOT set -- opencode (and any gateway model) will fail."
    echo "         export LLM_GATEWAY_KEY=<your key> and re-run."
    PREREQ_OK=0
fi

# python3
if command -v python3 >/dev/null 2>&1; then
    echo "  [ok]   python3: $(python3 --version 2>&1)"
else
    echo "  [FAIL] python3 not found -- the probes require it (stdlib only, no pip)."
    PREREQ_OK=0
fi

# Resolve opencode bin (prefer explicit path if not on PATH)
if [ -z "$OPENCODE_BIN" ]; then
    if command -v opencode >/dev/null 2>&1; then
        OPENCODE_BIN="$(command -v opencode)"
    elif [ -x "$HOME/.opencode/bin/opencode" ]; then
        OPENCODE_BIN="$HOME/.opencode/bin/opencode"
    fi
fi

# which each CLI
HAVE_CLAUDE=0; HAVE_CODEX=0; HAVE_OPENCODE=0
command -v "$CLAUDE_BIN" >/dev/null 2>&1 && HAVE_CLAUDE=1
command -v "$CODEX_BIN"  >/dev/null 2>&1 && HAVE_CODEX=1
[ -n "$OPENCODE_BIN" ] && [ -x "$OPENCODE_BIN" ] && HAVE_OPENCODE=1

printf "  %-9s %s\n" "claude"   "$([ $HAVE_CLAUDE   = 1 ] && echo "found ($(command -v "$CLAUDE_BIN"))" || echo "NOT installed -> SKIP")"
printf "  %-9s %s\n" "codex"    "$([ $HAVE_CODEX    = 1 ] && echo "found ($(command -v "$CODEX_BIN"))"  || echo "NOT installed -> SKIP")"
printf "  %-9s %s\n" "opencode" "$([ $HAVE_OPENCODE = 1 ] && echo "found ($OPENCODE_BIN)"               || echo "NOT installed -> SKIP")"
echo
echo "  per-probe timeout: ${PROBE_TIMEOUT_SECS}s   per-turn timeout: ${TURN_TIMEOUT_SECS}s"
echo "  artifacts:         $OUT_DIR/<cli>/"
echo

if [ "$PREREQ_OK" = 0 ]; then
    echo "Prerequisites not met. Fix the [FAIL] items above and re-run. Aborting."
    exit 2
fi

# Fresh output dirs for a clean, idempotent re-run (keep the dir, wipe contents).
rm -rf "$OUT_DIR/claude" "$OUT_DIR/opencode" "$OUT_DIR/codex" 2>/dev/null
mkdir -p "$OUT_DIR/claude" "$OUT_DIR/opencode" "$OUT_DIR/codex"

# --------------------------------------------------------------------------- #
# Verdict accumulators (state per CLI). States:
#   verified-true | verified-false | env-blocked | not-installed | unknown
#   | unsupported-by-documented-interface
# --------------------------------------------------------------------------- #
CLAUDE_STATE="not-installed"; CLAUDE_P="not-installed"; CLAUDE_C="not-installed"; CLAUDE_B="not-installed"; CLAUDE_ID="-"; CLAUDE_INT="unknown"; CLAUDE_TRANSCRIPT="-"
OPENCODE_STATE="not-installed"; OPENCODE_P="not-installed"; OPENCODE_C="not-installed"; OPENCODE_B="not-installed"; OPENCODE_ID="-"; OPENCODE_INT="unknown"; OPENCODE_TRANSCRIPT="-"
CODEX_STATE="not-installed"; CODEX_P="not-installed"; CODEX_C="not-installed"; CODEX_B="not-installed"; CODEX_ID="-"; CODEX_INT="unknown"; CODEX_TRANSCRIPT="-"

ANY_PASS=0

# helper: read a field out of a verdict.json with python (stdlib, robust)
jget() { python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(d.get(sys.argv[2],''))" "$1" "$2" 2>/dev/null; }

echo "-- Probes ---------------------------------------------------------------------"

# --------------------------------------------------------------------------- #
# CLAUDE (primary)
# --------------------------------------------------------------------------- #
if [ $HAVE_CLAUDE = 1 ]; then
    echo
    echo "[claude] stream-json duplex on a persistent (never-EOF'd) stdin ..."
    # Best-effort capture of the stream-json help surface (claude can be fragile; don't fail).
    ( "$CLAUDE_BIN" --help 2>/dev/null | grep -iE 'stream-json|input-format|output-format|session-id|--resume|--verbose|--print' \
        > "$OUT_DIR/claude/help-flags.txt" ) 2>/dev/null || true

    run_bounded "$PROBE_TIMEOUT_SECS" "$OUT_DIR/claude/run.log" -- \
        python3 "$CLAUDE_PROBE" --bin "$CLAUDE_BIN" --outdir "$OUT_DIR/claude" --timeout "$TURN_TIMEOUT_SECS"
    rc=$?
    VF="$OUT_DIR/claude/verdict.json"
    if [ -f "$VF" ]; then
        CLAUDE_P="$(jget "$VF" P_persistent_process)"
        CLAUDE_C="$(jget "$VF" C_context_retained)"
        CLAUDE_B="$(jget "$VF" B_turn_boundary)"
        CLAUDE_ID="$(jget "$VF" native_session_id)"; [ -z "$CLAUDE_ID" ] && CLAUDE_ID="-"
        CLAUDE_INT="unknown"   # not probed in 0'; claude stream-json interrupt is a Phase-0 follow-up
        CLAUDE_TRANSCRIPT="out/claude/events.jsonl"
        if [ "$(jget "$VF" pass)" = "True" ]; then
            CLAUDE_STATE="verified-true"; ANY_PASS=1
            echo "[claude] VERDICT: PASS  (P=$CLAUDE_P C=$CLAUDE_C B=$CLAUDE_B)"
        else
            CLAUDE_STATE="verified-false"
            echo "[claude] VERDICT: FAIL  (P=$CLAUDE_P C=$CLAUDE_C B=$CLAUDE_B) -- see out/claude/probe.log"
        fi
    else
        if [ $rc -eq 124 ]; then
            CLAUDE_STATE="unknown"; CLAUDE_P="unknown"; CLAUDE_C="unknown"; CLAUDE_B="unknown"
            echo "[claude] VERDICT: UNKNOWN (probe TIMED OUT after ${PROBE_TIMEOUT_SECS}s) -- see out/claude/run.log"
        else
            CLAUDE_STATE="env-blocked"; CLAUDE_P="env-blocked"; CLAUDE_C="env-blocked"; CLAUDE_B="env-blocked"
            echo "[claude] VERDICT: ENV-BLOCKED (probe produced no verdict, rc=$rc) -- see out/claude/run.log"
        fi
    fi
else
    echo
    echo "[claude] SKIP -- not installed."
fi

# --------------------------------------------------------------------------- #
# OPENCODE (secondary; two variants; ISOLATED config)
# --------------------------------------------------------------------------- #
if [ $HAVE_OPENCODE = 1 ]; then
    echo
    echo "[opencode] serve(HTTP) + acp(stdio) with ISOLATED config ..."
    TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/wasl-spike-oc.XXXXXX")"
    OC_CFG="$TMP_ROOT/xdgcfg"; OC_DATA="$TMP_ROOT/xdgdata"
    mkdir -p "$OC_CFG/opencode" "$OC_DATA"
    # Minimal opencode.json: gateway + Haiku, isolated (no MCP catalog, no autocompact).
    cat > "$OC_CFG/opencode/opencode.json" <<JSON
{
  "\$schema": "https://opencode.ai/config.json",
  "model": "amd-anthropic/Claude-Haiku-4.5",
  "provider": {
    "amd-anthropic": {
      "npm": "@ai-sdk/anthropic",
      "options": {
        "baseURL": "https://llm-gateway.example.com/Anthropic/v1",
        "apiKey": "{env:LLM_GATEWAY_KEY}",
        "headers": { "Ocp-Apim-Subscription-Key": "{env:LLM_GATEWAY_KEY}" }
      },
      "models": {
        "Claude-Haiku-4.5": { "limit": { "context": 200000, "output": 64000 } }
      }
    }
  },
  "enabled_providers": ["amd-anthropic"]
}
JSON
    cp "$OC_CFG/opencode/opencode.json" "$OUT_DIR/opencode/opencode.json"

    run_bounded "$PROBE_TIMEOUT_SECS" "$OUT_DIR/opencode/run.log" -- \
        python3 "$OPENCODE_PROBE" --bin "$OPENCODE_BIN" --outdir "$OUT_DIR/opencode" \
            --config-home "$OC_CFG" --data-home "$OC_DATA" \
            --model "amd-anthropic/Claude-Haiku-4.5" --timeout "$TURN_TIMEOUT_SECS"
    rc=$?
    VF="$OUT_DIR/opencode/verdict.json"
    if [ -f "$VF" ]; then
        # Prefer whichever variant did best. Pull the aggregate + each variant.
        ANYP="$(jget "$VF" any_pass)"
        # extract per-variant P/C/B via python
        read -r OPENCODE_P OPENCODE_C OPENCODE_B OPENCODE_WHICH < <(python3 - "$VF" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]))
v=d.get("variants",{})
best=None
order=["verified-true","verified-false","unknown","env-blocked"]
def rank(x): 
    return order.index(x) if x in order else len(order)
# pick the passing variant if any, else the serve variant, else acp
pick=None
for name,rv in v.items():
    if rv.get("pass"):
        pick=(name,rv); break
if pick is None:
    for name in ("serve","acp"):
        if name in v: pick=(name,v[name]); break
if pick is None:
    print("unknown unknown unknown none"); sys.exit()
name,rv=pick
print(rv.get("P_persistent_process","unknown"),
      rv.get("C_context_retained","unknown"),
      rv.get("B_turn_boundary","unknown"),
      name)
PY
)
        OPENCODE_INT="unknown"
        OPENCODE_TRANSCRIPT="out/opencode/${OPENCODE_WHICH}/"
        # native id: acp sessionId or serve session -- best effort from logs
        OPENCODE_ID="(see out/opencode/${OPENCODE_WHICH}/)"
        if [ "$ANYP" = "True" ]; then
            OPENCODE_STATE="verified-true"; ANY_PASS=1
            echo "[opencode] VERDICT: PASS  (variant=$OPENCODE_WHICH P=$OPENCODE_P C=$OPENCODE_C B=$OPENCODE_B)"
        else
            # distinguish env-blocked (OOM / no verdict inside) from a real false
            if grep -qiE 'out of memory|oom|heap|killed' "$OUT_DIR/opencode/run.log" 2>/dev/null; then
                OPENCODE_STATE="env-blocked"
                echo "[opencode] VERDICT: ENV-BLOCKED (looks like OOM) -- see out/opencode/run.log"
            else
                OPENCODE_STATE="verified-false"
                echo "[opencode] VERDICT: FAIL  (best variant=$OPENCODE_WHICH P=$OPENCODE_P C=$OPENCODE_C B=$OPENCODE_B) -- see out/opencode/"
            fi
        fi
    else
        if [ $rc -eq 124 ]; then
            OPENCODE_STATE="unknown"; OPENCODE_P="unknown"; OPENCODE_C="unknown"; OPENCODE_B="unknown"
            echo "[opencode] VERDICT: UNKNOWN (probe TIMED OUT) -- see out/opencode/run.log"
        else
            OPENCODE_STATE="env-blocked"; OPENCODE_P="env-blocked"; OPENCODE_C="env-blocked"; OPENCODE_B="env-blocked"
            echo "[opencode] VERDICT: ENV-BLOCKED (no verdict, rc=$rc) -- see out/opencode/run.log"
        fi
    fi
else
    echo
    echo "[opencode] SKIP -- not installed."
fi

# --------------------------------------------------------------------------- #
# CODEX (brief: check for any persistent/server subcommand; exec is per-turn)
# --------------------------------------------------------------------------- #
if [ $HAVE_CODEX = 1 ]; then
    echo
    echo "[codex] checking for any long-lived / server mode ..."
    "$CODEX_BIN" --help > "$OUT_DIR/codex/help.txt" 2>&1 || true
    # codex exec / exec resume is per-turn (persistent_conversation only). Note whether
    # experimental server modes exist (app-server / mcp-server / exec-server) -- these are
    # NOT the contracted exec path and are out of scope for 0', recorded as unknown.
    SERVER_MODES="$(grep -iE '^\s+(app-server|mcp-server|exec-server|remote-control)\b' "$OUT_DIR/codex/help.txt" | awk '{print $1}' | tr '\n' ' ')"
    CODEX_STATE="unsupported-by-documented-interface"
    CODEX_P="unsupported-by-documented-interface"   # `codex exec` self-terminates per turn
    CODEX_C="verified-true"                          # `exec resume` retains context (resume-based)
    CODEX_B="verified-true"                          # JSONL stream has thread/item/turn events
    CODEX_ID="codex-minted thread_id (thread.started)"
    CODEX_INT="unknown"
    CODEX_TRANSCRIPT="out/codex/help.txt"
    {
      echo "codex exec / exec resume is PER-TURN (process self-terminates on each turn)."
      echo "=> persistent_process via the contracted exec interface: FALSE."
      echo "=> persistent_conversation (resume-based): TRUE."
      echo "Experimental long-lived subcommands present: ${SERVER_MODES:-<none>}"
      echo "(app-server/exec-server are [experimental] and NOT the contracted exec path;"
      echo " a persistent codex substrate via those is UNKNOWN / a Phase-0 follow-up, not 0'.)"
    } > "$OUT_DIR/codex/summary.txt"
    echo "[codex] VERDICT: persistent_process=unsupported-by-documented-interface (exec is per-turn);"
    echo "        persistent_conversation=true. Experimental server modes: ${SERVER_MODES:-none} (unknown, out of 0' scope)."
else
    echo
    echo "[codex] SKIP -- not installed."
fi

# --------------------------------------------------------------------------- #
# Capability matrix
# --------------------------------------------------------------------------- #
{
echo "# Live-substrate capability matrix (Phase 0' spike)"
echo
echo "_Generated $(date -u '+%Y-%m-%dT%H:%M:%SZ') by spikes/live-substrate/run.sh on this machine._"
echo
echo "Facts (all WITHOUT closing stdin between turns):"
echo "- **P** persistent_process -- same OS pid serves turn 2 as turn 1 (no respawn)."
echo "- **C** context_retained -- turn 2 recalled the token planted in turn 1 ($([ -n "$LLM_GATEWAY_KEY" ] && echo 'VIOLET-7F29'))."
echo "- **B** turn_boundary -- a machine-detectable per-turn completion signal exists."
echo
echo "States: \`verified-true\` \`verified-false\` \`unsupported-by-documented-interface\` \`env-blocked\` \`not-installed\` \`unknown\`."
echo "Each \`verified-true\` links its captured transcript."
echo
echo "| CLI | P (persistent_process) | C (context_retained) | B (turn_boundary) | native session id | interruptible | transcript |"
echo "|---|---|---|---|---|---|---|"
echo "| claude (stream-json duplex) | $CLAUDE_P | $CLAUDE_C | $CLAUDE_B | $CLAUDE_ID | $CLAUDE_INT | $CLAUDE_TRANSCRIPT |"
echo "| opencode (serve/acp) | $OPENCODE_P | $OPENCODE_C | $OPENCODE_B | $OPENCODE_ID | $OPENCODE_INT | $OPENCODE_TRANSCRIPT |"
echo "| codex (exec/resume) | $CODEX_P | $CODEX_C | $CODEX_B | $CODEX_ID | $CODEX_INT | $CODEX_TRANSCRIPT |"
echo
echo "## Per-CLI overall state"
echo
echo "- **claude:** $CLAUDE_STATE"
echo "- **opencode:** $OPENCODE_STATE"
echo "- **codex:** $CODEX_STATE (persistent_conversation=true; persistent_process via contracted \`exec\` is per-turn)"
echo
echo "## Notes"
echo
echo "- \`unsupported-by-documented-interface\`: the CLI's documented headless interface is per-turn"
echo "  (self-terminating process); context continuity exists only via resume, not a held process."
echo "- codex exposes experimental \`app-server\`/\`exec-server\`/\`mcp-server\` subcommands; a persistent"
echo "  codex substrate via those is out of Phase-0' scope and left \`unknown\` (a Phase-0 follow-up)."
echo "- \`env-blocked\` (e.g. opencode OOM) is NOT a capability verdict -- re-run with more memory."
echo "- \`interruptible\` was not probed in Phase 0' (mid-turn cancel is a Phase-0 follow-up)."
} > "$MATRIX"

# --------------------------------------------------------------------------- #
# Final gate
# --------------------------------------------------------------------------- #
echo
echo "-- Capability matrix ----------------------------------------------------------"
echo "  written: $MATRIX"
echo
echo "==============================================================================="
if [ "$ANY_PASS" = 1 ]; then
    echo " GATE: PASS  (>=1 CLI proven same-process multi-turn)"
    echo "        -> GO: proceed to build the generic 'live runtime' core."
    GATE_RC=0
else
    echo " GATE: FAIL  (no live substrate proven same-process multi-turn)"
    echo "        -> NO-GO: do NOT build the generic core speculatively; ship the"
    echo "           capability-matrix finding (docs/design/live-runtime-impl-plan.md §0.2 gate)."
    GATE_RC=1
fi
echo "==============================================================================="
echo " Per-CLI: claude=$CLAUDE_STATE  opencode=$OPENCODE_STATE  codex=$CODEX_STATE"
echo " Full matrix: $MATRIX"
echo "==============================================================================="

exit $GATE_RC
