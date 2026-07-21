#!/usr/bin/env python3
"""
Phase-0' feasibility probe for `opencode` as a SAME-PROCESS, MULTI-TURN live substrate.

`opencode run --format json` self-terminates per turn -> NOT the live substrate.
The candidate live substrates are:
    (a) `opencode serve`  -- a headless HTTP server that stays up and serves many turns.
    (b) `opencode acp`    -- an ACP JSON-RPC server over stdio (persistent stdin/stdout).

Both are probed for the same three facts:
    (P) the SAME server/OS pid serves turn 2 as turn 1 (no respawn),
    (C) turn 2 sees turn 1's context (recall of the token),
    (B) a machine-detectable turn-boundary/completion signal exists.

opencode is launched with an ISOLATED config (own XDG dirs, project-config disabled,
autocompact disabled, a minimal opencode.json pointing at the AMD gateway + Haiku) so
it does not OOM on the MCP catalog and does not bloat context.

The exact HTTP API shape of `opencode serve` is NOT documented by `--help`, so variant
(a) DISCOVERS it at runtime by probing candidate endpoints. If it cannot determine the
shape, it records "API-shape-unknown, needs follow-up" rather than guessing.

stdlib only (urllib for HTTP, subprocess for stdio). No pip. Writes verdict.json per
variant plus an aggregate. Exit 0 iff at least one variant PASSES (P and C and B).

SECURITY / REDACTION RULE (do NOT undo this):
    This probe talks to a server that is configured with the AMD gateway credential
    (LLM_GATEWAY_KEY). Endpoints such as `GET /config` echo the RESOLVED credential
    (fields like apiKey / api_key / authorization / *-Subscription-Key / token / secret /
    password / key). Any HTTP body or server log this probe ARCHIVES to disk MUST first be
    run through redact() so credentials are replaced with "<redacted>". The credential is
    ALWAYS passed to the child opencode process via the environment (see isolated_env);
    it is NEVER substituted as a literal into any file. The isolated opencode.json written
    by run.sh uses the "{env:LLM_GATEWAY_KEY}" placeholder (resolved by opencode at
    runtime), never the literal value. Keep it that way.

Usage:
    python3 opencode_probe.py --bin <opencode> --outdir out/opencode \
        --config-home <tmp/xdgcfg> --data-home <tmp/xdgdata> --timeout 180
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
import urllib.request
import urllib.error

TOKEN = "VIOLET-7F29"
TURN1_TEXT = f"Remember the token {TOKEN} and reply with just: OK"
TURN2_TEXT = "What token did I ask you to remember? Reply with just the token, nothing else."


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


def killpg(proc):
    if proc and proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except OSError:
            pass


def isolated_env(config_home, data_home):
    env = dict(os.environ)
    env["XDG_CONFIG_HOME"] = config_home
    env["XDG_DATA_HOME"] = data_home
    env["OPENCODE_DISABLE_PROJECT_CONFIG"] = "1"
    env["OPENCODE_DISABLE_AUTOCOMPACT"] = "1"
    return env


# --------------------------------------------------------------------------- #
# HTTP helpers (stdlib only)
# --------------------------------------------------------------------------- #

def http(method, url, body=None, timeout=30):
    data = None
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
            return resp.status, raw
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except Exception as e:
        return None, f"__error__:{type(e).__name__}:{e}"


def try_json(raw):
    try:
        return json.loads(raw)
    except Exception:
        return None


# --------------------------------------------------------------------------- #
# Redaction (SECURITY): scrub credentials from anything we archive to disk.
# --------------------------------------------------------------------------- #

# Key names (case-insensitive) whose VALUES are credential material and must never
# be persisted. Matches whole keys and the *-Subscription-Key family (e.g.
# "Ocp-Apim-Subscription-Key") as well as substring hits like "apiKey" / "api_key".
_SECRET_KEY_RE = re.compile(
    r"(?i)(api[_-]?key|authorization|[\w-]*subscription-key|password|token|secret|\bkey\b)"
)
# Line-level scrub for free-form text (logs): "field: value" or "field=value" or
# "field":"value" -> keep the field name, replace the value with <redacted>.
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
    """Return a copy of an HTTP body / log text with any credential VALUES replaced
    by "<redacted>". Structured (JSON) bodies are parsed and scrubbed key-by-key;
    non-JSON text is scrubbed line-by-line by field name. Apply to EVERYTHING archived
    to disk (all response bodies, all captured logs) so no endpoint can leak a secret."""
    if raw is None:
        return raw
    j = None
    try:
        j = json.loads(raw)
    except Exception:
        j = None
    if j is not None:
        try:
            return json.dumps(_redact_obj(j), indent=2)
        except Exception:
            pass
    # Fall back to line-based scrubbing for non-JSON (or unserialisable) text.
    return _SECRET_LINE_RE.sub(r"\1<redacted>", raw)


# --------------------------------------------------------------------------- #
# Variant (a): opencode serve (HTTP)
# --------------------------------------------------------------------------- #

def probe_serve(bin_name, outdir, env, model, per_turn_timeout):
    vd = {
        "variant": "serve-http",
        "P_persistent_process": "unknown",
        "C_context_retained": "unknown",
        "B_turn_boundary": "unknown",
        "server_pid": None,
        "base_url": None,
        "pass": False,
        "api_shape": "unknown",
        "notes": [],
    }
    os.makedirs(outdir, exist_ok=True)
    argv = [bin_name, "serve", "--hostname", "127.0.0.1", "--port", "0", "--print-logs"]
    log(outdir, f"spawn: {' '.join(argv)}")
    proc = subprocess.Popen(
        argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        start_new_session=True, bufsize=1, universal_newlines=True, env=env,
    )
    vd["server_pid"] = proc.pid
    server_log = open(os.path.join(outdir, "server.log"), "w")
    base_url = None
    try:
        # Parse the server URL/port from stdout (opencode prints e.g. "server listening on
        # http://127.0.0.1:PORT" -- match any http URL or a bare port).
        deadline = time.time() + 60
        url_re = re.compile(r"https?://[0-9.]+:\d+")
        port_re = re.compile(r"\b(?:port|listening)\D{0,12}(\d{3,5})\b", re.I)
        while time.time() < deadline and base_url is None:
            if proc.poll() is not None:
                vd["notes"].append(f"serve exited early rc={proc.returncode} before URL seen")
                break
            line = proc.stdout.readline()
            if not line:
                time.sleep(0.1)
                continue
            # SECURITY: --print-logs may echo the effective config (incl. the resolved
            # credential). Scrub every captured log line before it touches disk.
            server_log.write(redact(line))
            server_log.flush()
            m = url_re.search(line)
            if m:
                base_url = m.group(0)
                break
            m = port_re.search(line)
            if m:
                base_url = f"http://127.0.0.1:{m.group(1)}"
                break
        if base_url is None:
            vd["notes"].append("could not determine serve URL/port from stdout within 60s")
            vd["api_shape"] = "API-shape-unknown, needs follow-up (no URL on stdout)"
            return _finish_variant(outdir, "serve", vd)
        vd["base_url"] = base_url
        log(outdir, f"server URL: {base_url}")

        # Confirm the server is reachable and try to discover an OpenAPI / doc surface.
        # SECURITY: /config (and any endpoint) can echo the resolved credential, so every
        # archived body AND every logged preview is run through redact() first.
        for path in ("/doc", "/openapi.json", "/config", "/session"):
            st, raw = http("GET", base_url + path, timeout=10)
            safe = redact(raw)
            log(outdir, f"GET {path} -> {st} ({safe[:80]!r})")
            with open(os.path.join(outdir, "discover" + path.replace("/", "_") + ".txt"), "w") as f:
                f.write(f"status={st}\n{safe[:20000]}")

        # Attempt the documented-by-convention opencode server API:
        #   POST /session            -> create a session, returns {"id": "ses_..."} (shape varies)
        #   POST /session/{id}/message  (or /session/{id}/prompt) -> run a turn
        # Because the exact shape is version-specific and undocumented in --help, we
        # try a small set and record what worked. If none work -> API-shape-unknown.
        st, raw = http("POST", base_url + "/session", body={}, timeout=30)
        log(outdir, f"POST /session -> {st}")
        sess = try_json(raw)
        session_id = None
        if isinstance(sess, dict):
            session_id = sess.get("id") or sess.get("sessionID") or sess.get("session_id")
        if not session_id:
            vd["notes"].append(
                f"POST /session did not yield a session id (status={st}); "
                "server HTTP message API shape not confirmed."
            )
            vd["api_shape"] = "API-shape-unknown, needs follow-up (session create shape differs)"
            return _finish_variant(outdir, "serve", vd)
        log(outdir, f"session id: {session_id}")

        def run_turn(text, label):
            payload = {
                "parts": [{"type": "text", "text": text}],
                "model": {"providerID": "amd-anthropic", "modelID": "Claude-Haiku-4.5"},
            }
            # try /message then /prompt
            for suffix in ("/message", "/prompt"):
                url = f"{base_url}/session/{session_id}{suffix}"
                st, raw = http("POST", url, body=payload, timeout=per_turn_timeout)
                log(outdir, f"POST {suffix} [{label}] -> {st}")
                # SECURITY: redact before archiving in case a turn body ever echoes config.
                with open(os.path.join(outdir, f"turn_{label}{suffix.replace('/', '_')}.txt"), "w") as f:
                    f.write(f"status={st}\n{redact(raw)[:40000]}")
                if st and 200 <= st < 300:
                    return st, raw, suffix
            return st, raw, None

        st1, raw1, used1 = run_turn(TURN1_TEXT, "1")
        alive1 = pid_alive(proc.pid) and proc.poll() is None
        if used1 is None:
            vd["notes"].append("no working /message or /prompt endpoint on the session")
            vd["api_shape"] = "API-shape-unknown, needs follow-up (no turn endpoint)"
            return _finish_variant(outdir, "serve", vd)
        vd["api_shape"] = f"serve: POST /session + POST /session/{{id}}{used1}"
        vd["B_turn_boundary"] = "verified-true"  # HTTP response completes = turn boundary

        st2, raw2, used2 = run_turn(TURN2_TEXT, "2")
        alive2 = pid_alive(proc.pid) and proc.poll() is None

        # P: server pid unchanged and still alive across both turns
        vd["P_persistent_process"] = "verified-true" if (alive1 and alive2) else "verified-false"

        # C: turn-2 response text contains the token
        text2 = raw2 or ""
        j2 = try_json(raw2)
        if isinstance(j2, (dict, list)):
            text2 = json.dumps(j2)
        vd["C_context_retained"] = "verified-true" if TOKEN in text2 else "verified-false"

        vd["pass"] = (
            vd["P_persistent_process"] == "verified-true"
            and vd["C_context_retained"] == "verified-true"
            and vd["B_turn_boundary"] == "verified-true"
        )
        if vd["pass"]:
            vd["notes"].append("serve HTTP: same server pid across 2 turns, turn 2 recalled the token.")
        return _finish_variant(outdir, "serve", vd)
    except Exception as e:
        vd["notes"].append(f"serve probe raised {type(e).__name__}: {e}")
        return _finish_variant(outdir, "serve", vd)
    finally:
        killpg(proc)
        try:
            server_log.close()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Variant (b): opencode acp (JSON-RPC over stdio)
# --------------------------------------------------------------------------- #

def probe_acp(bin_name, outdir, env, model, per_turn_timeout):
    vd = {
        "variant": "acp-stdio",
        "P_persistent_process": "unknown",
        "C_context_retained": "unknown",
        "B_turn_boundary": "unknown",
        "pid": None,
        "pass": False,
        "notes": [],
    }
    os.makedirs(outdir, exist_ok=True)
    argv = [bin_name, "acp"]
    log(outdir, f"spawn: {' '.join(argv)}")
    proc = subprocess.Popen(
        argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        start_new_session=True, bufsize=0, env=env,
    )
    vd["pid"] = proc.pid
    sel = selectors.DefaultSelector()
    os.set_blocking(proc.stdout.fileno(), False)
    os.set_blocking(proc.stderr.fileno(), False)
    sel.register(proc.stdout, selectors.EVENT_READ, "stdout")
    sel.register(proc.stderr, selectors.EVENT_READ, "stderr")
    buf = b""
    stderr_chunks = []
    rpc_id = [0]
    all_msgs = []

    def send(method, params, is_request=True):
        rpc_id[0] += 1
        msg = {"jsonrpc": "2.0", "method": method, "params": params}
        if is_request:
            msg["id"] = rpc_id[0]
        line = json.dumps(msg) + "\n"
        log(outdir, f"-> {method} (id={msg.get('id')}), stdin stays OPEN")
        proc.stdin.write(line.encode())
        proc.stdin.flush()
        return msg.get("id")

    def pump(want_id=None, want_result_method=None, timeout=per_turn_timeout):
        """Read JSON-RPC messages until we see the response to want_id, or a
        notification matching want_result_method, or timeout. Returns list of msgs read."""
        nonlocal buf
        deadline = time.time() + timeout
        got = []
        while time.time() < deadline:
            if proc.poll() is not None:
                try:
                    c = proc.stderr.read()
                    if c:
                        stderr_chunks.append(c)
                except (BlockingIOError, TypeError):
                    pass
                log(outdir, f"acp process EXITED rc={proc.returncode}")
                return got
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
                buf += chunk
                while b"\n" in buf:
                    raw, buf = buf.split(b"\n", 1)
                    raw = raw.strip()
                    if not raw:
                        continue
                    obj = None
                    try:
                        obj = json.loads(raw.decode("utf-8", "replace"))
                    except json.JSONDecodeError:
                        log(outdir, f"non-JSON acp line: {raw[:120]!r}")
                        continue
                    all_msgs.append(obj)
                    got.append(obj)
                    m = obj.get("method")
                    if m:
                        log(outdir, f"<- notif/req {m}")
                    elif "result" in obj or "error" in obj:
                        log(outdir, f"<- response id={obj.get('id')}")
                    if want_id is not None and obj.get("id") == want_id and (
                        "result" in obj or "error" in obj):
                        return got
                    if want_result_method and obj.get("method") == want_result_method:
                        return got
        return got

    try:
        # ACP handshake: initialize
        init_id = send("initialize", {
            "protocolVersion": 1,
            "clientCapabilities": {},
            "clientInfo": {"name": "wasl-spike", "version": "0"},
        })
        pump(want_id=init_id, timeout=45)

        # session/new
        newsess_id = send("session/new", {"cwd": os.getcwd(), "mcpServers": []})
        got = pump(want_id=newsess_id, timeout=45)
        session_id = None
        for obj in got:
            if obj.get("id") == newsess_id and isinstance(obj.get("result"), dict):
                r = obj["result"]
                session_id = r.get("sessionId") or r.get("sessionID") or r.get("id")
        if not session_id:
            vd["notes"].append(
                "session/new returned no sessionId; ACP request shape not confirmed for this build."
            )
            vd["B_turn_boundary"] = "unknown"
            return _finish_acp(outdir, vd, proc, all_msgs, stderr_chunks, sel)
        log(outdir, f"acp sessionId={session_id}")

        def prompt(text, label):
            pid_ = send("session/prompt", {
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": text}],
            })
            got = pump(want_id=pid_, timeout=per_turn_timeout)
            end_seen = any(o.get("id") == pid_ and "result" in o for o in got)
            # concat any text from session/update notifications
            text_seen = []
            for o in got:
                if o.get("method") in ("session/update", "session/updated"):
                    text_seen.append(json.dumps(o.get("params", {})))
                if o.get("id") == pid_ and isinstance(o.get("result"), (dict, list)):
                    text_seen.append(json.dumps(o["result"]))
            return end_seen, "".join(text_seen)

        t1_end, _ = prompt(TURN1_TEXT, "1")
        alive1 = pid_alive(proc.pid) and proc.poll() is None
        vd["B_turn_boundary"] = "verified-true" if t1_end else "verified-false"
        if not t1_end or not alive1:
            vd["notes"].append(
                "turn 1 did not complete over persistent stdio"
                + ("" if alive1 else " and process exited")
                + "."
            )
            vd["P_persistent_process"] = "verified-false" if not alive1 else "unknown"
            return _finish_acp(outdir, vd, proc, all_msgs, stderr_chunks, sel)

        t2_end, t2_text = prompt(TURN2_TEXT, "2")
        alive2 = pid_alive(proc.pid) and proc.poll() is None
        vd["P_persistent_process"] = "verified-true" if (alive1 and alive2) else "verified-false"
        vd["C_context_retained"] = "verified-true" if TOKEN in t2_text else (
            "verified-false" if t2_end else "unknown")
        vd["B_turn_boundary"] = "verified-true" if (t1_end and t2_end) else "verified-false"
        vd["pass"] = (
            vd["P_persistent_process"] == "verified-true"
            and vd["C_context_retained"] == "verified-true"
            and vd["B_turn_boundary"] == "verified-true"
        )
        if vd["pass"]:
            vd["notes"].append("acp stdio: same pid across 2 turns over never-closed stdin, token recalled.")
        return _finish_acp(outdir, vd, proc, all_msgs, stderr_chunks, sel)
    except Exception as e:
        vd["notes"].append(f"acp probe raised {type(e).__name__}: {e}")
        return _finish_acp(outdir, vd, proc, all_msgs, stderr_chunks, sel)
    finally:
        killpg(proc)


def _finish_acp(outdir, vd, proc, all_msgs, stderr_chunks, sel):
    try:
        sel.close()
    except Exception:
        pass
    # SECURITY: redact every archived ACP message and any stderr before writing to disk.
    with open(os.path.join(outdir, "messages.jsonl"), "w") as f:
        for o in all_msgs:
            f.write(redact(json.dumps(o)) + "\n")
    if stderr_chunks:
        stderr_text = b"".join(stderr_chunks).decode("utf-8", "replace")
        with open(os.path.join(outdir, "stderr.txt"), "w") as f:
            f.write(redact(stderr_text))
    return _finish_variant(outdir, "acp", vd)


def _finish_variant(outdir, name, vd):
    with open(os.path.join(outdir, f"verdict-{name}.json"), "w") as f:
        json.dump(vd, f, indent=2)
    return vd


# --------------------------------------------------------------------------- #

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--config-home", required=True)
    ap.add_argument("--data-home", required=True)
    ap.add_argument("--model", default="amd-anthropic/Claude-Haiku-4.5")
    ap.add_argument("--timeout", type=int, default=180)
    ap.add_argument("--variant", choices=["serve", "acp", "both"], default="both")
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)
    env = isolated_env(args.config_home, args.data_home)

    results = {}
    if args.variant in ("serve", "both"):
        results["serve"] = probe_serve(
            args.bin, os.path.join(args.outdir, "serve"), env, args.model, args.timeout)
    if args.variant in ("acp", "both"):
        results["acp"] = probe_acp(
            args.bin, os.path.join(args.outdir, "acp"), env, args.model, args.timeout)

    any_pass = any(r.get("pass") for r in results.values())
    agg = {
        "cli": "opencode",
        "any_pass": any_pass,
        "variants": results,
    }
    with open(os.path.join(args.outdir, "verdict.json"), "w") as f:
        json.dump(agg, f, indent=2)
    print("VERDICT " + json.dumps({
        "any_pass": any_pass,
        "serve": results.get("serve", {}).get("pass"),
        "acp": results.get("acp", {}).get("pass"),
    }))
    return 0 if any_pass else 1


if __name__ == "__main__":
    sys.exit(main())
