#!/usr/bin/env python3
"""
Phase-0-follow-up feasibility probe for `codex app-server --stdio` as a
SAME-PROCESS, MULTI-TURN live substrate (the codex analog of claude_probe.py).

Hypothesis under test (docs/design/live-runtime-impl-plan.md §0.2, extended to
codex): drive ONE persistent `codex app-server --stdio` process over a single
never-closed stdin/stdout pipe, JSON-RPC (one JSON object per line), and get
MULTIPLE turns out of it WITHOUT EOF-ing stdin between turns.

The three facts that matter (proven or disproven, per probe):
    (P) persistent_process   -- the SAME OS pid serves turn 2 as turn 1 (no
                                respawn), still alive after turn 1.
    (C) context_retained     -- turn 2's reply text contains the token planted
                                in turn 1 (the process actually remembered).
    (B) turn_boundary        -- a machine-detectable per-turn completion signal
                                exists (the `turn/completed` notification, whose
                                `turn.status == "completed"`) for BOTH turns.

Protocol (confirmed OFFLINE from `codex app-server generate-json-schema`, then
VERIFIED live by this probe):
    - handshake:  request  method="initialize", params={clientInfo:{name,version},
                  capabilities:{experimentalApi:true}} -> response (result:
                  {codexHome, userAgent, platformOs, ...})
    - session:    request  method="thread/start", params={cwd, ...} ->
                  notification method="thread/started" params.thread.id
                  + response result.thread.id  (the native thread id, UUIDv7)
    - turn:       request  method="turn/start", params={threadId, input:[{type:
                  "text", text:"..."}]} -> streaming notifications
                  method="item/agentMessage/delta" params={delta,itemId,threadId,
                  turnId}, then item/started + item/completed, then notification
                  method="turn/completed" params={threadId, turn:{status,items,..}}
                  + response result.turn
    - interrupt:  request  method="turn/interrupt", params={threadId, turnId}
                  (capability present; NOT triggered live by the P/C/B proof).

Everything archived to disk is run through redact() first (mirrors
claude_probe.py's discipline). stdlib only. Writes a machine-readable
verdict JSON + a redacted events.jsonl transcript. Exit 0 iff PASS (P and C and B).

Usage:
    python3 codex_appserver_probe.py --bin codex --outdir out/codex_appserver \
        --timeout 120 [--tool-capture]
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

TOKEN = "VIOLET-7F29"
TURN1_TEXT = (
    f"Remember the token {TOKEN}. Do not use any tools. "
    f"Reply with just: OK"
)
TURN2_TEXT = (
    "What token did I ask you to remember? Do not use any tools. "
    "Reply with just the token, nothing else."
)
# Tool-triggering prompt (only used with --tool-capture), a real shell tool call.
TOOL_TEXT = (
    "Run the shell command `ls -a` in the current working directory using your "
    "shell tool, then tell me how many entries it listed."
)

# --------------------------------------------------------------------------- #
# Redaction (SECURITY) -- MIRRORED from claude_probe.py so the same discipline
# scrubs any credential before ANYTHING is written to disk or inspected.
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


def log(outdir, msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(os.path.join(outdir, "probe.log"), "a") as f:
        f.write(line + "\n")


def pid_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def jsonrpc_request(req_id, method, params):
    obj = {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}
    return json.dumps(obj) + "\n"


class Probe:
    def __init__(self, bin_name, outdir, per_turn_timeout, codex_home=None):
        self.bin = bin_name
        self.outdir = outdir
        self.per_turn_timeout = per_turn_timeout
        self.codex_home = codex_home
        self.proc = None
        self.sel = selectors.DefaultSelector()
        self.raw_events = []       # every parsed stdout JSON object (redacted on write)
        self.stdout_buf = b""
        self.stderr_chunks = []
        self.next_id = 1
        self.thread_id = None

    def start(self):
        # `codex app-server --stdio` -- JSON-RPC over a persistent stdio pipe.
        argv = [self.bin, "app-server", "--stdio"]
        log(self.outdir, f"spawn: {' '.join(argv)}")
        with open(os.path.join(self.outdir, "argv.txt"), "w") as f:
            f.write(" ".join(argv) + "\n")
        env = dict(os.environ)
        # codex's own internal check requires OPENAI_API_KEY be present (any
        # value); the REAL auth is a hardcoded Ocp-Apim-Subscription-Key in
        # ~/.codex/config.toml. Never write this value to disk.
        env.setdefault("OPENAI_API_KEY", os.environ.get("OPENAI_API_KEY", "x"))
        # Relocate codex's writable state (sqlite state/logs/goals DBs) to an
        # ISOLATED, writable CODEX_HOME so the real ~/.codex is never mutated and
        # the app-server can init its sqlite state runtime under the sandbox.
        if self.codex_home:
            env["CODEX_HOME"] = self.codex_home
        # start_new_session=True => own process group; the run.sh trap and our
        # own cleanup can kill the whole group, never orphaning a codex child.
        self.proc = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
            bufsize=0,
            env=env,
        )
        self.pid = self.proc.pid
        log(self.outdir, f"pid={self.pid}")
        os.set_blocking(self.proc.stdout.fileno(), False)
        os.set_blocking(self.proc.stderr.fileno(), False)
        self.sel.register(self.proc.stdout, selectors.EVENT_READ, "stdout")
        self.sel.register(self.proc.stderr, selectors.EVENT_READ, "stderr")

    def _drain_stderr(self):
        try:
            chunk = self.proc.stderr.read()
            if chunk:
                self.stderr_chunks.append(chunk)
        except (BlockingIOError, TypeError):
            pass

    def write_line(self, line, label):
        """Write ONE JSON-RPC line to the persistent stdin. Never closes stdin."""
        log(self.outdir, f"-> {label} ({len(line)} bytes), stdin stays OPEN")
        self.proc.stdin.write(line.encode())
        self.proc.stdin.flush()

    def _pump(self, predicate, label, timeout=None):
        """Read stdout JSON-RPC lines until predicate(obj) is True or timeout.
        Returns (matched_obj_or_None, timed_out: bool). Every parsed object is
        appended to self.raw_events. Streaming deltas are collected by the caller
        via raw_events inspection."""
        deadline = time.time() + (timeout if timeout is not None else self.per_turn_timeout)
        while time.time() < deadline:
            if self.proc.poll() is not None:
                self._drain_stderr()
                log(self.outdir, f"[{label}] process EXITED mid-read rc={self.proc.returncode}")
                return None, False
            for key, _ in self.sel.select(timeout=0.5):
                if key.data == "stderr":
                    self._drain_stderr()
                    continue
                try:
                    chunk = self.proc.stdout.read()
                except BlockingIOError:
                    chunk = None
                if not chunk:
                    continue
                self.stdout_buf += chunk
                while b"\n" in self.stdout_buf:
                    raw, self.stdout_buf = self.stdout_buf.split(b"\n", 1)
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        obj = json.loads(raw.decode("utf-8", "replace"))
                    except json.JSONDecodeError:
                        log(self.outdir, f"[{label}] non-JSON stdout line: {raw[:120]!r}")
                        continue
                    self.raw_events.append(obj)
                    meth = obj.get("method")
                    if meth:
                        # progress log for notifications
                        log(self.outdir, f"[{label}] <- notif {meth}")
                    elif "result" in obj:
                        log(self.outdir, f"[{label}] <- response id={obj.get('id')}")
                    elif "error" in obj:
                        log(self.outdir, f"[{label}] <- ERROR id={obj.get('id')}: "
                                         f"{str(obj.get('error'))[:200]}")
                    if predicate(obj):
                        return obj, False
        return None, True

    def handshake(self):
        rid = self.next_id
        self.next_id += 1
        line = jsonrpc_request(rid, "initialize", {
            "clientInfo": {"name": "wasl-spike", "version": "0.1.0"},
            "capabilities": {"experimentalApi": True},
        })
        self.write_line(line, "initialize")
        obj, to = self._pump(
            lambda o: o.get("id") == rid and ("result" in o or "error" in o),
            "initialize", timeout=self.per_turn_timeout)
        if obj is None or "error" in (obj or {}):
            return False, obj
        return True, obj

    def start_thread(self):
        rid = self.next_id
        self.next_id += 1
        cwd = os.getcwd()
        line = jsonrpc_request(rid, "thread/start", {"cwd": cwd})
        self.write_line(line, "thread/start")
        obj, to = self._pump(
            lambda o: o.get("id") == rid and ("result" in o or "error" in o),
            "thread/start", timeout=self.per_turn_timeout)
        if obj is None or "error" in (obj or {}):
            return None, obj
        result = obj.get("result", {})
        thread = result.get("thread", {})
        self.thread_id = thread.get("id")
        return self.thread_id, obj

    def run_turn(self, text, label):
        """Submit one user turn on the SAME thread, read to turn/completed.
        Returns (completed: bool, reply_text: str, turn_status: str,
                 timed_out: bool, tool_seen: bool)."""
        rid = self.next_id
        self.next_id += 1
        line = jsonrpc_request(rid, "turn/start", {
            "threadId": self.thread_id,
            "input": [{"type": "text", "text": text}],
        })
        self.write_line(line, f"turn/start ({label})")

        # Collect streaming deltas + detect the turn/completed notification.
        delta_parts = []
        agent_msg_texts = []
        tool_seen = {"v": False}
        completed = {"obj": None}

        def pred(o):
            meth = o.get("method")
            params = o.get("params") or {}
            if meth == "item/agentMessage/delta":
                d = params.get("delta")
                if isinstance(d, str):
                    delta_parts.append(d)
            elif meth == "item/completed":
                item = params.get("item") or {}
                if item.get("type") == "agentMessage":
                    t = item.get("text")
                    if isinstance(t, str):
                        agent_msg_texts.append(t)
                if item.get("type") in ("commandExecution", "mcpToolCall",
                                        "dynamicToolCall", "fileChange"):
                    tool_seen["v"] = True
            elif meth == "item/started":
                item = params.get("item") or {}
                if item.get("type") in ("commandExecution", "mcpToolCall",
                                        "dynamicToolCall", "fileChange"):
                    tool_seen["v"] = True
            elif meth == "turn/completed":
                # matched only if it's for OUR thread
                if params.get("threadId") == self.thread_id or params.get("turn"):
                    completed["obj"] = o
                    return True
            elif meth == "turn/failed" or (meth == "error"):
                completed["obj"] = o
                return True
            return False

        obj, timed_out = self._pump(pred, f"turn ({label})")
        turn_status = ""
        if completed["obj"] is not None:
            params = completed["obj"].get("params") or {}
            turn = params.get("turn") or {}
            turn_status = turn.get("status", "")
        # Prefer the authoritative agentMessage item text; fall back to deltas.
        reply = "".join(agent_msg_texts) if agent_msg_texts else "".join(delta_parts)
        got_completed = completed["obj"] is not None and \
            (completed["obj"].get("method") == "turn/completed")
        return got_completed, reply, turn_status, timed_out, tool_seen["v"]

    def cleanup(self):
        try:
            self.sel.close()
        except Exception:
            pass
        if self.proc and self.proc.poll() is None:
            log(self.outdir, "cleanup: killing process group")
            try:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
            except OSError:
                pass
        self._drain_stderr()
        if self.stderr_chunks:
            stderr_text = b"".join(self.stderr_chunks).decode("utf-8", "replace")
            with open(os.path.join(self.outdir, "stderr.txt"), "w") as f:
                f.write(redact(stderr_text))
        # SECURITY: redact each archived event before writing.
        with open(os.path.join(self.outdir, "events.jsonl"), "w") as f:
            for obj in self.raw_events:
                f.write(redact(json.dumps(obj)) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", default=os.environ.get("WASL_SPIKE_CODEX_BIN", "codex"))
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--timeout", type=int, default=120, help="per-turn timeout seconds")
    ap.add_argument("--tool-capture", action="store_true",
                    help="after the P/C/B proof, run ONE extra tool-triggering turn "
                         "and archive its wire (a separate tool_capture.jsonl)")
    ap.add_argument("--codex-home", default=os.environ.get("CODEX_HOME"),
                    help="isolated writable CODEX_HOME (sqlite state relocation)")
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    verdict = {
        "cli": "codex",
        "interface": "app-server --stdio (JSON-RPC over persistent stdio)",
        "probe": "thread/start + 2x turn/start on ONE never-EOF'd stdio pipe",
        "token": TOKEN,
        "P_persistent_process": "unknown",
        "C_context_retained": "unknown",
        "B_turn_boundary": "unknown",
        "native_thread_id": None,
        "pid_turn1": None,
        "pid_turn2": None,
        "turn1_completed": False,
        "turn2_completed": False,
        "turn1_status": "",
        "turn2_status": "",
        "turn2_reply": "",
        "interruptible": "advertised-turn/interrupt (not triggered live)",
        "pass": False,
        "notes": [],
    }

    probe = Probe(args.bin, args.outdir, args.timeout, codex_home=args.codex_home)
    try:
        probe.start()

        ok, init_obj = probe.handshake()
        if not ok:
            verdict["notes"].append(
                "initialize failed or errored: "
                + redact(json.dumps(init_obj))[:400])
            raise SystemExit(_finish(probe, verdict))
        log(args.outdir, "initialize OK")

        tid, ts_obj = probe.start_thread()
        if not tid:
            verdict["notes"].append(
                "thread/start returned no thread id: "
                + redact(json.dumps(ts_obj))[:400])
            raise SystemExit(_finish(probe, verdict))
        verdict["native_thread_id"] = tid
        log(args.outdir, f"thread/start OK thread_id={tid}")

        # --- TURN 1 (plant token) ---
        c1, r1, s1, to1, _ = probe.run_turn(TURN1_TEXT, "turn1")
        verdict["pid_turn1"] = probe.pid if pid_alive(probe.pid) else None
        verdict["turn1_completed"] = c1
        verdict["turn1_status"] = s1
        alive_after_t1 = pid_alive(probe.pid) and probe.proc.poll() is None

        if not c1:
            verdict["notes"].append(
                "turn 1 produced NO turn/completed notification"
                + (" (timed out)" if to1 else " (process exited)")
                + " while stdin was held OPEN.")
            verdict["B_turn_boundary"] = "verified-false"
            verdict["P_persistent_process"] = "verified-false" if not alive_after_t1 else "unknown"
            raise SystemExit(_finish(probe, verdict))

        if not alive_after_t1:
            verdict["notes"].append(
                "process EXITED after turn 1 even though stdin was never closed"
                " => cannot serve a second turn => FAIL-for-persistence.")
            verdict["P_persistent_process"] = "verified-false"
            verdict["B_turn_boundary"] = "verified-true"
            raise SystemExit(_finish(probe, verdict))

        log(args.outdir, "turn 1 completed WITHOUT closing stdin; process alive -> turn 2")

        # --- TURN 2 (recall token; stdin still OPEN, never EOF'd) ---
        c2, r2, s2, to2, _ = probe.run_turn(TURN2_TEXT, "turn2")
        verdict["pid_turn2"] = probe.pid if pid_alive(probe.pid) else None
        verdict["turn2_completed"] = c2
        verdict["turn2_status"] = s2
        verdict["turn2_reply"] = r2.strip()[:2000]

        same_pid = (
            verdict["pid_turn1"] is not None
            and verdict["pid_turn2"] is not None
            and verdict["pid_turn1"] == verdict["pid_turn2"])
        verdict["P_persistent_process"] = "verified-true" if same_pid else "verified-false"

        context_ok = TOKEN in r2
        verdict["C_context_retained"] = "verified-true" if context_ok else (
            "verified-false" if c2 else "unknown")

        both_boundaries = c1 and c2
        verdict["B_turn_boundary"] = "verified-true" if both_boundaries else "verified-false"

        if not c2:
            verdict["notes"].append(
                "turn 2 produced no turn/completed"
                + (" (timed out)" if to2 else " (process exited)"))

        verdict["pass"] = bool(same_pid and context_ok and both_boundaries)
        if verdict["pass"]:
            verdict["notes"].append(
                "SAME pid served both turns over one never-closed stdio pipe, turn 2"
                " recalled the turn-1 token, and both turns had a machine-detectable"
                " turn/completed boundary. codex app-server IS a same-process"
                " multi-turn live substrate.")

        # --- OPTIONAL: one tool-call capture turn (same thread, still alive) ---
        if args.tool_capture and verdict["pass"] and \
                pid_alive(probe.pid) and probe.proc.poll() is None:
            log(args.outdir, "tool-capture: running ONE tool-triggering turn")
            mark = len(probe.raw_events)
            c3, r3, s3, to3, tool_seen = probe.run_turn(TOOL_TEXT, "toolcap")
            tool_lines = probe.raw_events[mark:]
            with open(os.path.join(args.outdir, "codex_toolcall_capture.jsonl"), "w") as f:
                for obj in tool_lines:
                    f.write(redact(json.dumps(obj)) + "\n")
            verdict["tool_capture"] = {
                "tool_seen": tool_seen,
                "turn_completed": c3,
                "turn_status": s3,
                "timed_out": to3,
                "lines": len(tool_lines),
                "out": "codex_toolcall_capture.jsonl",
            }
            verdict["notes"].append(
                f"tool-capture turn: tool_seen={tool_seen} completed={c3} "
                f"status={s3} ({len(tool_lines)} wire lines archived)")

        raise SystemExit(_finish(probe, verdict))
    except SystemExit:
        raise
    except Exception as e:
        verdict["notes"].append(f"probe raised {type(e).__name__}: {e}")
        raise SystemExit(_finish(probe, verdict))
    finally:
        probe.cleanup()


def _finish(probe, verdict):
    with open(os.path.join(probe.outdir, "verdict.json"), "w") as f:
        json.dump(verdict, f, indent=2)
    print("VERDICT " + json.dumps({k: verdict[k] for k in
          ("pass", "P_persistent_process", "C_context_retained", "B_turn_boundary")}))
    return 0 if verdict["pass"] else 1


if __name__ == "__main__":
    main()
