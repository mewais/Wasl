# E2 — `wasl-procrunner` — real process `Runner` (spawns scp/rsync) — spec (authoritative)

**Tier:** L1 (real process exec; the plumbing is fully testable in-env with stand-in commands). **Deps:**
`wasl-transfer` (the `Runner` trait). Dev: none beyond std (stand-in commands like `sh`/`true`/`false`).
No `unsafe`. This is the concrete `Runner` that actually runs a transfer's argv as a child process. It adds
NO transfer logic — `wasl-transfer` already plans the argv and drives `execute`; this only spawns/waits.

**Testability (honesty):**
- ✅ TESTABLE HERE: the PROCESS PLUMBING — spawn a program, wait, map success→`Ok(())`, map a non-zero exit
  (with captured stderr) and a spawn failure (program not found) → `Err(detail)`, and the empty-argv guard.
  Exercised with stand-in commands (`sh -c 'exit 0'`, `sh -c '...>&2; exit N'`, a nonexistent program).
- ❌ UNTESTED HERE: a REAL `scp -3` / `rsync` transfer between TWO hosts — needs a second host + SSH + scp,
  which this environment does not have. The argv it runs is exactly what `wasl_transfer::{scp_argv,rsync_argv}`
  build (already unit-tested); running that argv against real remote hosts is the same code path, UNTESTED
  HERE only for lack of a second machine. State this in `AGENTS.md`.

## API
```rust
use wasl_transfer::Runner;

/// A `Runner` that executes each transfer argv as a child process and reports the outcome.
#[derive(Debug, Default, Clone)]
pub struct ProcessRunner;

impl ProcessRunner {
    pub fn new() -> Self;   // == ProcessRunner
}

impl Runner for ProcessRunner {
    /// `argv[0]` is the program; `argv[1..]` the arguments. Runs it to completion capturing output.
    /// - empty `argv`            -> `Err("empty argv")`
    /// - spawn fails (e.g. ENOENT program not found) -> `Err(format!("failed to spawn {prog}: {e}"))`
    /// - runs, exit == success   -> `Ok(())`
    /// - runs, exit != success   -> `Err(format!("{prog} {status}: {stderr}"))` where `status` is the
    ///                              child's `ExitStatus` Display (covers both code and signal) and `stderr`
    ///                              is the trimmed captured stderr.
    fn run(&mut self, argv: &[String]) -> Result<(), String>;
}
```

### `run` (shape — keep branches minimal + fully coverable)
```rust
fn run(&mut self, argv: &[String]) -> Result<(), String> {
    let (prog, rest) = argv.split_first().ok_or_else(|| "empty argv".to_string())?;
    let output = std::process::Command::new(prog)
        .args(rest)
        .output()
        .map_err(|e| format!("failed to spawn {prog}: {e}"))?;
    if output.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!("{prog} {}: {}", output.status, stderr.trim()))
    }
}
```
NOTE: do NOT branch on `ExitStatus::code()` (its `None` signal case is awkward to test deterministically) —
use the `ExitStatus` `Display` directly, which renders both the exit-code and the signal case. Every branch
above is reachable with stand-in commands → genuine 100% line+region.

## Test plan (`tests/*.rs`; real child processes; line+region 100)
Use only programs guaranteed present on a POSIX/Linux CI (`sh` is safe; `true`/`false` may not be on PATH in
all shells, so prefer `sh -c`). Build argv as `Vec<String>`.
- **success:** `run(&["sh","-c","exit 0"])` → `Ok(())`.
- **non-zero exit + stderr capture:** `run(&["sh","-c","echo boom 1>&2; exit 7"])` → `Err(detail)` where
  `detail` contains `"boom"` (proves stderr captured) and reflects failure (non-empty, mentions the program).
- **spawn failure (program not found):** `run(&["this-program-does-not-exist-wasl-xyz"])` →
  `Err(detail)` containing `"failed to spawn"`.
- **empty argv:** `run(&[])` → `Err("empty argv")`.
- (optional) **integration via `wasl_transfer::execute`** with a `ProcessRunner`: a `TransferParams` whose
  argv is overridden is not possible (execute builds real scp argv), so instead directly assert the `Runner`
  contract as above. Do NOT attempt a real scp — that is UNTESTED HERE.

No `#[ignore]`, no `todo!`, no tautologies. Every test asserts a concrete value. Tests compile ONLY against
the public API (`ProcessRunner`) + `wasl_transfer::Runner`.
