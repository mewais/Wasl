#!/usr/bin/env python3
"""
Follow-up feasibility probe: does `thread/resume` (RPC method + request/response
shape confirmed authoritative via `codex app-server generate-json-schema`) ACTUALLY
work as a cross-process resume -- i.e. can a genuinely NEW, FRESH OS process
resume a thread whose ORIGINAL process has already exited, and recall context
from before that exit?

This is the one fact `codex_appserver_probe.py` did NOT establish (it only
proved same-process multi-turn persistence). `CodexAppServerProtocol::resume`
implements `thread/resume` per the schema but its doc comment marks the
runtime round-trip "ASSUMED... until a dedicated live resume capture is
archived" -- this script is that capture.

Facts under test:
    (R1) resume_succeeds   -- thread/resume against a FRESH process, after the
                              ORIGINAL process has fully exited, returns a
                              result (not an error) whose result.thread.id
                              matches the original thread id.
    (R2) context_retained  -- a turn run on the resumed thread (fresh process)
                              recalls a token planted in a turn run on the
                              original thread (now-dead process).

Reuses `codex_appserver_probe.py`'s `Probe` class (handshake/start_thread/
run_turn/cleanup) unmodified; only orchestrates two SEPARATE Probe instances
(A then B) instead of one, plus a new `resume_thread` step B needs that A's
flow never exercises.

Usage:
    python3 codex_appserver_resume_probe.py --bin codex \
        --outdir out/codex_appserver_resume [--codex-home DIR]
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import codex_appserver_probe as base  # noqa: E402

TOKEN = "MAGENTA-3D81"
TURN1_TEXT = (
    f"Remember the token {TOKEN}. Do not use any tools. Reply with just: OK"
)
TURN2_TEXT = (
    "What token did I ask you to remember? Do not use any tools. "
    "Reply with just the token, nothing else."
)


def resume_thread(probe, thread_id):
    """Send `thread/resume` on a freshly-handshaken Probe (post-`initialize`,
    pre-`thread/start`) and return (resumed_thread_id_or_None, response_obj)."""
    rid = probe.next_id
    probe.next_id += 1
    cwd = os.getcwd()
    line = base.jsonrpc_request(rid, "thread/resume", {"threadId": thread_id, "cwd": cwd})
    probe.write_line(line, "thread/resume")
    obj, timed_out = probe._pump(
        lambda o: o.get("id") == rid and ("result" in o or "error" in o),
        "thread/resume", timeout=probe.per_turn_timeout,
    )
    if obj is None or "error" in (obj or {}):
        return None, obj
    result = obj.get("result", {})
    thread = result.get("thread", {})
    probe.thread_id = thread.get("id")
    return probe.thread_id, obj


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", default=os.environ.get("WASL_SPIKE_CODEX_BIN", "codex"))
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--timeout", type=int, default=120)
    ap.add_argument("--codex-home", default=os.environ.get("CODEX_HOME"))
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    verdict = {
        "cli": "codex",
        "probe": "thread/resume across a FRESH process, after original process exit",
        "token": TOKEN,
        "R1_resume_succeeds": "not-run",
        "R2_context_retained": "not-run",
        "pass": False,
        "notes": [],
    }

    probe_a = base.Probe(args.bin, args.outdir, args.timeout, codex_home=args.codex_home)
    thread_id = None
    try:
        probe_a.start()
        ok, obj = probe_a.handshake()
        if not ok:
            verdict["notes"].append(f"probe A initialize failed: {obj}")
            raise SystemExit(_finish(verdict, args.outdir))
        thread_id, obj = probe_a.start_thread()
        if thread_id is None:
            verdict["notes"].append(f"probe A thread/start failed: {obj}")
            raise SystemExit(_finish(verdict, args.outdir))
        completed, reply, status, timed_out, tool_seen = probe_a.run_turn(TURN1_TEXT, "A-turn1")
        if not completed or status != "completed":
            verdict["notes"].append(
                f"probe A turn1 did not complete cleanly: completed={completed} status={status} timed_out={timed_out}"
            )
            raise SystemExit(_finish(verdict, args.outdir))
        verdict["notes"].append(f"probe A: thread_id={thread_id} pid={probe_a.pid} turn1 reply={reply!r}")
    finally:
        # Fully tear down process A -- this is the whole point: process B must
        # resume a thread whose ORIGINAL process is genuinely gone, not still
        # running (the schema explicitly distinguishes "running" vs "non-running"
        # thread resume semantics; the deployment scenario we care about is
        # "process A already exited").
        probe_a.cleanup()

    if thread_id is None:
        raise SystemExit(_finish(verdict, args.outdir))

    # Give the OS a moment to fully reap process A before B starts, so there is
    # no ambiguity about whether A was still "running" when B resumes.
    deadline = time.time() + 5
    while probe_a.proc.poll() is None and time.time() < deadline:
        time.sleep(0.05)
    verdict["notes"].append(f"probe A final poll() after cleanup: {probe_a.proc.poll()}")

    probe_b = base.Probe(args.bin, args.outdir + "_B", args.timeout, codex_home=args.codex_home)
    os.makedirs(args.outdir + "_B", exist_ok=True)
    try:
        probe_b.start()
        ok, obj = probe_b.handshake()
        if not ok:
            verdict["notes"].append(f"probe B initialize failed: {obj}")
            raise SystemExit(_finish(verdict, args.outdir))

        resumed_id, obj = resume_thread(probe_b, thread_id)
        if resumed_id is None:
            verdict["R1_resume_succeeds"] = False
            verdict["notes"].append(f"probe B thread/resume failed: {obj}")
            raise SystemExit(_finish(verdict, args.outdir))
        verdict["R1_resume_succeeds"] = resumed_id == thread_id
        verdict["resumed_thread_id"] = resumed_id
        verdict["original_thread_id"] = thread_id
        verdict["notes"].append(f"probe B: resumed_id={resumed_id} pid={probe_b.pid}")

        completed, reply, status, timed_out, tool_seen = probe_b.run_turn(TURN2_TEXT, "B-turn2")
        verdict["notes"].append(
            f"probe B turn2: completed={completed} status={status} timed_out={timed_out} reply={reply!r}"
        )
        verdict["R2_context_retained"] = bool(completed and status == "completed" and TOKEN in reply)
    finally:
        probe_b.cleanup()

    verdict["pass"] = bool(verdict["R1_resume_succeeds"] and verdict["R2_context_retained"])
    raise SystemExit(_finish(verdict, args.outdir))


def _finish(verdict, outdir):
    with open(os.path.join(outdir, "resume_verdict.json"), "w") as f:
        json.dump(verdict, f, indent=2)
    print("RESUME_VERDICT " + json.dumps({
        k: verdict[k] for k in ("pass", "R1_resume_succeeds", "R2_context_retained")
    }))
    return 0 if verdict["pass"] else 1


if __name__ == "__main__":
    main()
