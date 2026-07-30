#!/usr/bin/env python3
"""
Follow-up feasibility probe: does `turn/interrupt` (schema-confirmed shape,
`{threadId, turnId}`) ACTUALLY stop a genuinely in-flight codex turn, well
before its natural completion?

`CodexAppServerTransport::interrupt()` already sends `turn/interrupt` over the
wire, but the capability is deliberately locked `interruptible: false` because
this exact round-trip was never exercised live -- R1's actor (`actor.rs`)
gates `cancel_turn` behind `capabilities().interruptible`, so today this code
is UNREACHABLE from any real caller (only a direct transport-level call could
ever exercise it). This script is the live proof needed before that lock can
be lifted.

Method: submit a turn whose PROMPT asks codex to run a real, genuinely slow
shell command (`sleep 30`) via its shell tool, then say "done" -- naturally
takes >=30s to complete. Shortly after observing the tool call START
(`item/started`, `commandExecution`), send `turn/interrupt {threadId, turnId}`
and keep reading. If interrupt genuinely works, `turn/completed` arrives with
`status:"interrupted"` WELL BEFORE the natural ~30s completion time (the
elapsed-time gap is the real proof, not just the presence of the status
string, which a driver could theoretically report even if it just quietly
waited out the full 30s).

Facts under test:
    (I1) interrupt_accepted   -- `turn/interrupt` is accepted (no JSON-RPC
                                  error response).
    (I2) turn_actually_stopped_early -- `turn/completed` arrives with
                                  `status == "interrupted"` in significantly
                                  less than the ~30s the sleep would naturally
                                  take (bounded check: < 20s elapsed since the
                                  interrupt was sent).

Usage:
    python3 codex_appserver_interrupt_probe.py --bin codex \
        --outdir out/codex_appserver_interrupt [--codex-home DIR]
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import codex_appserver_probe as base  # noqa: E402


def turn_with_interrupt(probe, text, interrupt_after_tool_start=True):
    """Submit a turn, wait for the tool call to START, send turn/interrupt,
    then keep reading to turn/completed. Returns a dict of facts."""
    rid = probe.next_id
    probe.next_id += 1
    line = base.jsonrpc_request(rid, "turn/start", {
        "threadId": probe.thread_id,
        "input": [{"type": "text", "text": text}],
    })
    turn_start_sent_at = time.time()
    probe.write_line(line, "turn/start (interrupt-target)")

    turn_id = {"v": None}
    tool_started_at = {"v": None}
    interrupt_sent_at = {"v": None}
    completed = {"obj": None}

    def capture_turn_id(o):
        if turn_id["v"] is not None:
            return
        if o.get("id") == rid and isinstance(o.get("result"), dict):
            t = (o["result"].get("turn") or {}).get("id")
            if t:
                turn_id["v"] = t
                return
        if o.get("method") == "turn/started":
            t = ((o.get("params") or {}).get("turn") or {}).get("id")
            if t:
                turn_id["v"] = t

    def pred(o):
        capture_turn_id(o)
        meth = o.get("method")
        params = o.get("params") or {}
        if meth == "item/started":
            item = params.get("item") or {}
            if item.get("type") == "commandExecution" and tool_started_at["v"] is None:
                tool_started_at["v"] = time.time()
                if interrupt_after_tool_start and turn_id["v"] and interrupt_sent_at["v"] is None:
                    irid = probe.next_id
                    probe.next_id += 1
                    iline = base.jsonrpc_request(irid, "turn/interrupt", {
                        "threadId": probe.thread_id,
                        "turnId": turn_id["v"],
                    })
                    probe.write_line(iline, "turn/interrupt")
                    interrupt_sent_at["v"] = time.time()
        elif meth == "turn/completed":
            if params.get("threadId") == probe.thread_id or params.get("turn"):
                completed["obj"] = o
                return True
        elif meth == "error":
            completed["obj"] = o
            return True
        return False

    obj, timed_out = probe._pump(pred, "interrupt-target", timeout=45)

    turn_status = None
    if completed["obj"] is not None:
        params = completed["obj"].get("params") or {}
        turn = params.get("turn") or {}
        turn_status = turn.get("status")

    now = time.time()
    return {
        "turn_id": turn_id["v"],
        "tool_started_at_rel": (tool_started_at["v"] - turn_start_sent_at) if tool_started_at["v"] else None,
        "interrupt_sent_at_rel": (interrupt_sent_at["v"] - turn_start_sent_at) if interrupt_sent_at["v"] else None,
        "completed_at_rel": (now - turn_start_sent_at) if completed["obj"] is not None else None,
        "elapsed_since_interrupt": (now - interrupt_sent_at["v"]) if interrupt_sent_at["v"] else None,
        "turn_status": turn_status,
        "timed_out": timed_out,
        "got_completed": completed["obj"] is not None and completed["obj"].get("method") == "turn/completed",
        "got_error": completed["obj"] is not None and "error" in completed["obj"],
        "raw_completed": completed["obj"],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", default=os.environ.get("WASL_SPIKE_CODEX_BIN", "codex"))
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--codex-home", default=os.environ.get("CODEX_HOME"))
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    verdict = {
        "cli": "codex",
        "probe": "turn/interrupt genuinely stops an in-flight (sleep 30) tool-running turn",
        "I1_interrupt_accepted": "not-run",
        "I2_turn_actually_stopped_early": "not-run",
        "pass": False,
        "notes": [],
    }

    probe = base.Probe(args.bin, args.outdir, args.timeout, codex_home=args.codex_home)
    try:
        probe.start()
        ok, obj = probe.handshake()
        if not ok:
            verdict["notes"].append(f"initialize failed: {obj}")
            raise SystemExit(_finish(verdict, args.outdir))
        thread_id, obj = probe.start_thread()
        if thread_id is None:
            verdict["notes"].append(f"thread/start failed: {obj}")
            raise SystemExit(_finish(verdict, args.outdir))

        prompt = (
            "Run the shell command `sleep 30 && echo woke-up` using your shell "
            "tool, then reply with just: done"
        )
        result = turn_with_interrupt(probe, prompt)
        verdict["notes"].append(f"result={json.dumps({k: v for k, v in result.items() if k != 'raw_completed'})}")

        if result["interrupt_sent_at_rel"] is None:
            verdict["notes"].append(
                "never observed a commandExecution item/started -- could not send "
                "turn/interrupt at all (tool call may not have started in time)"
            )
            verdict["I1_interrupt_accepted"] = False
            raise SystemExit(_finish(verdict, args.outdir))

        verdict["I1_interrupt_accepted"] = not result["got_error"]
        if result["got_error"]:
            verdict["notes"].append(f"turn/interrupt or the turn itself errored: {result['raw_completed']}")

        # The real proof: completed well before the natural ~30s sleep would
        # finish, AND the reported status says interrupted/failed (not
        # completed as if nothing happened).
        stopped_early = (
            result["completed_at_rel"] is not None
            and result["completed_at_rel"] < 20.0
        )
        status_reflects_interrupt = result["turn_status"] in ("interrupted", "failed")
        verdict["I2_turn_actually_stopped_early"] = bool(stopped_early and status_reflects_interrupt)
        verdict["notes"].append(
            f"stopped_early(<20s)={stopped_early} status={result['turn_status']!r} "
            f"completed_at_rel={result['completed_at_rel']}"
        )
    finally:
        probe.cleanup()

    verdict["pass"] = bool(verdict["I1_interrupt_accepted"] and verdict["I2_turn_actually_stopped_early"])
    raise SystemExit(_finish(verdict, args.outdir))


def _finish(verdict, outdir):
    with open(os.path.join(outdir, "interrupt_verdict.json"), "w") as f:
        json.dump(verdict, f, indent=2)
    print("INTERRUPT_VERDICT " + json.dumps({
        k: verdict[k] for k in ("pass", "I1_interrupt_accepted", "I2_turn_actually_stopped_early")
    }))
    return 0 if verdict["pass"] else 1


if __name__ == "__main__":
    main()
