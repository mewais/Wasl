#!/usr/bin/env python3
"""
ONE-SHOT tool-call wire-shape capture for `claude` stream-json duplex.

Phase-0'-follow-up for Issue A (docs/design/live-runtime-hardening-plan.md §1):
map_claude_line() in crates/wasl-runtime-protocol/src/claude_stream.rs never
recognizes a real tool_use / tool_result wire shape (both left ASSUMED-unmapped).
This spike drives ONE real claude turn that forces a built-in tool invocation,
captures EVERY raw stream-json line, redacts credentials, and saves the redacted
transcript so a mapping-code author can pin the concrete shapes.

Reuses claude_probe.py's EXACT pattern: same argv shape, same persistent-child
spawn (own process group), same redact() logic, same process-group-kill cleanup.
Differs only in: single turn, a tool-triggering prompt, a safe temp cwd, and it
archives every line verbatim (redacted) rather than adjudicating P/C/B.

stdlib only. Bounded by a hard wall-clock deadline; never loops on the model.
"""

import argparse
import json
import os
import re
import selectors
import signal
import subprocess
import sys
import time
import uuid

# --------------------------------------------------------------------------- #
# Redaction (SECURITY) -- MIRRORED VERBATIM from claude_probe.py so the same
# discipline scrubs any credential that could leak into the transcript before
# ANYTHING is written to disk or looked at.
# --------------------------------------------------------------------------- #
_SECRET_KEY_RE = re.compile(
    r"(?i)(api[_-]?key|authorization|[\w-]*subscription-key|password|token|secret|\bkey\b)"
)
_SECRET_LINE_RE = re.compile(
    r"(?i)([\"']?(?:api[_-]?key|authorization|[\w-]*subscription-key|password|token|secret|key)[\"']?\s*[:=]\s*[\"']?)"
    r"[^\s\"',}]+"
)


def _redact_obj(obj):
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if isinstance(k, str) and _SECRET_KEY_RE.search(k) and not isinstance(v, (dict, list)):
                out[k] = "<redacted>"
            else:
                out[k] = _redact_obj(v)
        return out
    if isinstance(obj, list):
        return [_redact_obj(x) for x in obj]
    return obj


def redact(raw):
    if raw is None:
        return raw
    j = None
    try:
        j = json.loads(raw)
    except Exception:
        j = None
    if j is not None:
        try:
            return json.dumps(_redact_obj(j))
        except Exception:
            pass
    return _SECRET_LINE_RE.sub(r"\1<redacted>", raw)


def user_message_line(text):
    obj = {
        "type": "user",
        "message": {"role": "user", "content": [{"type": "text", "text": text}]},
    }
    return json.dumps(obj) + "\n"


def build_argv(bin_name, session_id):
    return [
        bin_name,
        "-p",
        "--input-format", "stream-json",
        "--output-format", "stream-json",
        "--verbose",
        "--session-id", session_id,
    ]


def capture_one(bin_name, prompt, cwd, deadline_ts, raw_out_path):
    """Drive ONE turn to a `result` event OR the wall-clock deadline.
    Returns (result_seen: bool, tool_seen: bool, raw_lines: list[str])."""
    session_id = str(uuid.uuid4())
    argv = build_argv(bin_name, session_id)
    print(f"[capture] cwd={cwd}", flush=True)
    print(f"[capture] argv={' '.join(argv[:-1])} <uuid>", flush=True)
    proc = subprocess.Popen(
        argv,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,   # own process group -> group-kill on cleanup
        bufsize=0,
        cwd=cwd,
    )
    sel = selectors.DefaultSelector()
    os.set_blocking(proc.stdout.fileno(), False)
    os.set_blocking(proc.stderr.fileno(), False)
    sel.register(proc.stdout, selectors.EVENT_READ, "stdout")
    sel.register(proc.stderr, selectors.EVENT_READ, "stderr")

    raw_lines = []          # redacted, verbatim stdout lines (one JSON per line)
    stderr_chunks = []
    stdout_buf = b""
    result_seen = False
    tool_seen = False

    # write the ONE user turn; stdin left open (matches probe discipline)
    try:
        proc.stdin.write(user_message_line(prompt).encode())
        proc.stdin.flush()
    except Exception as e:
        print(f"[capture] stdin write failed: {e}", flush=True)

    try:
        while time.time() < deadline_ts:
            if proc.poll() is not None:
                # drain remaining stderr and stop
                try:
                    c = proc.stderr.read()
                    if c:
                        stderr_chunks.append(c)
                except (BlockingIOError, TypeError):
                    pass
                print(f"[capture] process EXITED rc={proc.returncode}", flush=True)
                break
            for key, _ in sel.select(timeout=0.5):
                if key.data == "stderr":
                    try:
                        c = proc.stderr.read()
                        if c:
                            stderr_chunks.append(c)
                    except (BlockingIOError, TypeError):
                        pass
                    continue
                try:
                    chunk = proc.stdout.read()
                except BlockingIOError:
                    chunk = None
                if not chunk:
                    continue
                stdout_buf += chunk
                while b"\n" in stdout_buf:
                    raw, stdout_buf = stdout_buf.split(b"\n", 1)
                    raw = raw.strip()
                    if not raw:
                        continue
                    line_text = raw.decode("utf-8", "replace")
                    # SECURITY: redact BEFORE storing/printing/inspecting.
                    red = redact(line_text)
                    raw_lines.append(red)
                    # classify only for progress logging (best-effort)
                    try:
                        obj = json.loads(red)
                    except json.JSONDecodeError:
                        obj = None
                    t = obj.get("type") if isinstance(obj, dict) else None
                    if isinstance(obj, dict):
                        msg = obj.get("message")
                        content = msg.get("content") if isinstance(msg, dict) else None
                        if isinstance(content, list):
                            for blk in content:
                                if isinstance(blk, dict) and blk.get("type") in ("tool_use", "tool_result"):
                                    tool_seen = True
                    print(f"[capture] <- {t}"
                          + (f"/{obj.get('subtype')}" if isinstance(obj, dict) and obj.get('subtype') else ""),
                          flush=True)
                    if t == "result":
                        result_seen = True
                if result_seen:
                    break
            if result_seen:
                break
        else:
            print("[capture] DEADLINE reached before result", flush=True)
    finally:
        sel.close()
        if proc.poll() is None:
            print("[capture] killing process group", flush=True)
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except OSError:
                pass
        # archive stderr (redacted) next to the raw capture, for diagnostics
        try:
            c = proc.stderr.read()
            if c:
                stderr_chunks.append(c)
        except (BlockingIOError, TypeError):
            pass
        if stderr_chunks:
            stderr_text = b"".join(stderr_chunks).decode("utf-8", "replace")
            with open(raw_out_path + ".stderr.txt", "w") as f:
                f.write(redact(stderr_text))

    return result_seen, tool_seen, raw_lines


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", default=os.environ.get("WASL_SPIKE_CLAUDE_BIN", "claude"))
    ap.add_argument("--out", required=True, help="path for the redacted .jsonl capture")
    ap.add_argument("--wall", type=int, default=120, help="hard wall-clock cap (s) for the WHOLE capture")
    ap.add_argument("--turn", type=int, default=90, help="per-attempt cap (s)")
    args = ap.parse_args()

    if not os.environ.get("LLM_GATEWAY_KEY"):
        print("PREREQ FAIL: LLM_GATEWAY_KEY unset", file=sys.stderr)
        sys.exit(2)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)

    # safe, harmless cwd: a fresh empty temp dir (nothing of ours to read)
    import tempfile
    cwd = tempfile.mkdtemp(prefix="wasl-toolcap-")

    prompts = [
        "List the files in the current directory using your Bash tool, then tell me how many there are.",
        "Use the Bash tool to run `ls /tmp` right now.",
    ]

    overall_deadline = time.time() + args.wall
    all_lines = []
    tool_seen = False
    result_seen = False
    attempts = []

    for i, prompt in enumerate(prompts):
        remaining = overall_deadline - time.time()
        if remaining <= 5:
            print("[capture] out of wall-clock budget; stopping", flush=True)
            break
        this_deadline = min(overall_deadline, time.time() + args.turn)
        print(f"\n[capture] === attempt {i+1}/{len(prompts)} (budget {remaining:.0f}s) ===", flush=True)
        r_seen, t_seen, lines = capture_one(args.bin, prompt, cwd, this_deadline, args.out)
        attempts.append({"prompt": prompt, "result_seen": r_seen, "tool_seen": t_seen, "lines": len(lines)})
        # keep the FIRST attempt's lines that actually triggered a tool; else keep last
        if t_seen and not tool_seen:
            all_lines = lines
        elif not all_lines:
            all_lines = lines
        result_seen = result_seen or r_seen
        tool_seen = tool_seen or t_seen
        if t_seen:
            print(f"[capture] TOOL CALL observed on attempt {i+1}; not retrying.", flush=True)
            break
        else:
            print(f"[capture] no tool call on attempt {i+1}.", flush=True)

    # write the redacted capture (verbatim redacted lines, one per line)
    with open(args.out, "w") as f:
        for ln in all_lines:
            f.write(ln + "\n")

    summary = {
        "tool_call_observed": tool_seen,
        "result_event_observed": result_seen,
        "attempts": attempts,
        "out": args.out,
        "cwd": cwd,
    }
    print("\nSUMMARY " + json.dumps(summary))
    # non-zero exit if no tool call, so caller knows honestly
    sys.exit(0 if tool_seen else 1)


if __name__ == "__main__":
    main()
