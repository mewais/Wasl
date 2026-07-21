#!/usr/bin/env python3
"""
Phase-0' feasibility probe for `claude` as a SAME-PROCESS, MULTI-TURN live substrate.

Hypothesis under test (from live-runtime-impl-plan.md §0.2):
    Feed stream-json input on a PERSISTENT (never-closed) stdin to ONE `claude`
    process and get MULTIPLE turns out of it -- WITHOUT EOF-ing stdin between turns.

The three facts that matter (proven or disproven, per probe):
    (P) persistent_process   -- the SAME OS pid serves turn 2 as turn 1 (no respawn),
                                and the process is still alive after turn 1.
    (C) context_retained     -- turn 2's reply text contains the token established in
                                turn 1 (the process actually remembered).
    (B) turn_boundary        -- a machine-detectable per-turn completion signal exists
                                (the stream-json `result` event) for BOTH turns.

CRITICAL: stdin is opened once and never closed until AFTER turn 2 has completed.
If claude only ever emits a `result` once stdin is closed (EOF), that is
one-turn-per-process => FAIL-for-persistence, and we record it with evidence.

stdlib only. No pip. Writes a machine-readable verdict JSON to <outdir>/verdict.json
and human-readable artifacts alongside it. Exit code 0 iff PASS (P and C and B).

SECURITY / REDACTION RULE (do NOT undo this):
    claude's stream-json events are its own protocol (init/assistant/result) and do not
    carry the gateway credential, but as a matter of discipline EVERYTHING this probe
    archives to disk (events.jsonl, stderr.txt) is run through redact() so a future
    claude output that echoes a credential (apiKey / authorization / *-Subscription-Key /
    token / secret / key / password) can never leak to disk. The credential is only ever
    passed to the child via the environment; it is never written to any file.

Usage:
    python3 claude_probe.py --bin claude --outdir out/claude --timeout 180
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

TOKEN = "VIOLET-7F29"

# --------------------------------------------------------------------------- #
# Redaction (SECURITY): scrub credentials from anything we archive to disk.
# --------------------------------------------------------------------------- #
_SECRET_KEY_RE = re.compile(
    r"(?i)(api[_-]?key|authorization|[\w-]*subscription-key|password|token|secret|\bkey\b)"
)
_SECRET_LINE_RE = re.compile(
    r"(?i)([\"']?(?:api[_-]?key|authorization|[\w-]*subscription-key|password|token|secret|key)[\"']?\s*[:=]\s*[\"']?)"
    r"[^\s\"',}]+"
)


def _redact_obj(obj):
    """Recursively replace credential values in a parsed JSON structure."""
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
    """Return a copy of a body/log with any credential VALUES replaced by "<redacted>".
    JSON is scrubbed key-by-key; non-JSON text line-by-line. Apply to EVERYTHING
    archived to disk so no future output can leak a secret."""
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
TURN1_TEXT = f"Remember the token {TOKEN} and reply with just: OK"
TURN2_TEXT = "What token did I ask you to remember? Reply with just the token, nothing else."


def log(outdir, msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(os.path.join(outdir, "probe.log"), "a") as f:
        f.write(line + "\n")


def user_message_line(text):
    """The stream-json INPUT shape claude expects (Claude Code SDK convention):
    one JSON object per line, type=user, message with role+content."""
    obj = {
        "type": "user",
        "message": {"role": "user", "content": [{"type": "text", "text": text}]},
    }
    return json.dumps(obj) + "\n"


def build_argv(bin_name, session_id):
    # Duplex stream-json on --print; persistent stdin carries multiple turns.
    # --verbose is required by claude when output-format=stream-json with --print.
    return [
        bin_name,
        "-p",
        "--input-format", "stream-json",
        "--output-format", "stream-json",
        "--verbose",
        "--session-id", session_id,
    ]


def pid_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def classify_event(obj):
    """Return one of: 'ready', 'turn_end', 'assistant', 'other' and pull useful bits."""
    t = obj.get("type")
    if t == "system" and obj.get("subtype") == "init":
        return "ready"
    if t == "result":
        return "turn_end"
    if t == "assistant":
        return "assistant"
    return "other"


def extract_assistant_text(obj):
    """Pull text out of an assistant event (content blocks) or a result event."""
    texts = []
    msg = obj.get("message")
    if isinstance(msg, dict):
        content = msg.get("content")
        if isinstance(content, list):
            for blk in content:
                if isinstance(blk, dict) and blk.get("type") == "text":
                    texts.append(blk.get("text", ""))
        elif isinstance(content, str):
            texts.append(content)
    # result events carry the final text in "result"
    if obj.get("type") == "result" and isinstance(obj.get("result"), str):
        texts.append(obj["result"])
    return "".join(texts)


class Probe:
    def __init__(self, bin_name, outdir, per_turn_timeout):
        self.bin = bin_name
        self.outdir = outdir
        self.per_turn_timeout = per_turn_timeout
        self.proc = None
        self.sel = selectors.DefaultSelector()
        self.raw_events = []          # every parsed stdout JSON object
        self.stdout_buf = b""
        self.stderr_chunks = []

    def start(self):
        session_id = str(uuid.uuid4())
        argv = build_argv(self.bin, session_id)
        log(self.outdir, f"spawn: {' '.join(argv)}")
        with open(os.path.join(self.outdir, "argv.txt"), "w") as f:
            f.write(" ".join(argv) + "\n")
        # start_new_session=True => own process group, so the trap in run.sh
        # (and our own cleanup) can kill the whole group, never orphaning.
        self.proc = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
            bufsize=0,
        )
        self.session_id = session_id
        self.pid = self.proc.pid
        log(self.outdir, f"pid={self.pid} session-id={session_id}")
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

    def write_turn(self, text):
        """Write ONE user message to the persistent stdin. Does NOT close stdin."""
        line = user_message_line(text)
        log(self.outdir, f"-> turn input ({len(line)} bytes), stdin stays OPEN")
        self.proc.stdin.write(line.encode())
        self.proc.stdin.flush()

    def read_until_turn_end(self, label):
        """Read stdout events until a `result` (turn_end) event or timeout.
        Returns (turn_end_seen: bool, assistant_text: str, timed_out: bool)."""
        deadline = time.time() + self.per_turn_timeout
        assistant_text_parts = []
        turn_end_obj = None
        while time.time() < deadline:
            if self.proc.poll() is not None:
                # process died mid-turn: record and stop
                self._drain_stderr()
                log(self.outdir, f"[{label}] process EXITED mid-read rc={self.proc.returncode}")
                break
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
                    kind = classify_event(obj)
                    if kind == "ready":
                        log(self.outdir, f"[{label}] READY event: session_id={obj.get('session_id')}")
                    elif kind == "assistant":
                        assistant_text_parts.append(extract_assistant_text(obj))
                    elif kind == "turn_end":
                        turn_end_obj = obj
                        assistant_text_parts.append(extract_assistant_text(obj))
                        log(self.outdir, f"[{label}] TURN-END (result) subtype={obj.get('subtype')}")
                if turn_end_obj is not None:
                    return True, "".join(assistant_text_parts), False
        # timeout or death
        return (turn_end_obj is not None), "".join(assistant_text_parts), True

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
        # persist any stderr we saw -- SECURITY: redact before writing.
        self._drain_stderr()
        if self.stderr_chunks:
            stderr_text = b"".join(self.stderr_chunks).decode("utf-8", "replace")
            with open(os.path.join(self.outdir, "stderr.txt"), "w") as f:
                f.write(redact(stderr_text))
        # SECURITY: redact each archived event in case an output ever echoes a credential.
        with open(os.path.join(self.outdir, "events.jsonl"), "w") as f:
            for obj in self.raw_events:
                f.write(redact(json.dumps(obj)) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", default="claude")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--timeout", type=int, default=180, help="per-turn timeout seconds")
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    verdict = {
        "cli": "claude",
        "probe": "stream-json duplex, persistent stdin (never EOF between turns)",
        "token": TOKEN,
        "P_persistent_process": "unknown",
        "C_context_retained": "unknown",
        "B_turn_boundary": "unknown",
        "native_session_id": None,
        "pid_turn1": None,
        "pid_turn2": None,
        "turn1_end_seen": False,
        "turn2_end_seen": False,
        "turn2_reply": "",
        "pass": False,
        "notes": [],
    }

    probe = Probe(args.bin, args.outdir, args.timeout)
    try:
        probe.start()
        verdict["native_session_id"] = probe.session_id

        # --- TURN 1 ---
        probe.write_turn(TURN1_TEXT)
        t1_end, t1_text, t1_timeout = probe.read_until_turn_end("turn1")
        verdict["pid_turn1"] = probe.pid if pid_alive(probe.pid) else None
        verdict["turn1_end_seen"] = t1_end
        alive_after_t1 = pid_alive(probe.pid) and probe.proc.poll() is None
        # capture the ready session_id if claude reported one
        for obj in probe.raw_events:
            if classify_event(obj) == "ready" and obj.get("session_id"):
                verdict["native_session_id"] = obj["session_id"]
                break

        if not t1_end:
            verdict["notes"].append(
                "turn 1 produced NO `result` event"
                + (" (timed out)" if t1_timeout else " (process exited without result)")
                + " while stdin was held OPEN. If claude needs stdin-close to emit a result,"
                  " this is one-turn-per-process => FAIL-for-persistence."
            )
            verdict["B_turn_boundary"] = "verified-false"
            verdict["P_persistent_process"] = "verified-false" if not alive_after_t1 else "unknown"
            raise SystemExit(_finish(probe, verdict))

        if not alive_after_t1:
            verdict["notes"].append(
                "process EXITED after turn 1 even though stdin was never closed"
                " => cannot serve a second turn => FAIL-for-persistence."
            )
            verdict["P_persistent_process"] = "verified-false"
            verdict["B_turn_boundary"] = "verified-true"  # boundary existed, but single-shot
            raise SystemExit(_finish(probe, verdict))

        log(args.outdir, "turn 1 completed WITHOUT closing stdin; process still alive -> writing turn 2")

        # --- TURN 2 (stdin still OPEN, never EOF'd) ---
        probe.write_turn(TURN2_TEXT)
        t2_end, t2_text, t2_timeout = probe.read_until_turn_end("turn2")
        verdict["pid_turn2"] = probe.pid if pid_alive(probe.pid) else None
        verdict["turn2_end_seen"] = t2_end
        verdict["turn2_reply"] = t2_text.strip()[:2000]

        # --- adjudicate P / C / B ---
        same_pid = (
            verdict["pid_turn1"] is not None
            and verdict["pid_turn2"] is not None
            and verdict["pid_turn1"] == verdict["pid_turn2"]
        )
        verdict["P_persistent_process"] = "verified-true" if same_pid else "verified-false"

        context_ok = TOKEN in t2_text
        verdict["C_context_retained"] = "verified-true" if context_ok else (
            "verified-false" if t2_end else "unknown"
        )

        both_boundaries = t1_end and t2_end
        verdict["B_turn_boundary"] = "verified-true" if both_boundaries else "verified-false"

        if not t2_end:
            verdict["notes"].append(
                "turn 2 produced no `result`"
                + (" (timed out)" if t2_timeout else " (process exited)")
                + "; a second turn over the same stdin did not complete."
            )

        verdict["pass"] = bool(same_pid and context_ok and both_boundaries)
        if verdict["pass"]:
            verdict["notes"].append(
                "SAME pid served both turns over one never-closed stdin, turn 2 recalled the"
                " turn-1 token, and both turns had detectable completion events. claude IS a"
                " same-process multi-turn live substrate."
            )
        raise SystemExit(_finish(probe, verdict))
    except SystemExit:
        raise
    except Exception as e:  # any unexpected failure -> honest unknown, not a crash
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
