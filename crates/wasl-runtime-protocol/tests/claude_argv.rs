//! Black-box tests for `wasl_runtime_protocol::claude_argv`.
//!
//! Contract (from `src/claude_stream.rs` doc + todo!() banner):
//!   result[0] == bin
//!   base flags: ["-p", "--input-format", "stream-json", "--output-format",
//!                "stream-json", "--verbose"]
//!   session flag (resume=false): ["--session-id", session_id]
//!   session flag (resume=true):  ["--resume",     session_id]
//!   iff context.model = Some(m):       append ["--model", m]
//!   iff context.posture = Some(p):     append ["--permission-mode", p]
//!   iff !context.disallowed_tools.empty: append ["--disallowedTools", <comma-join>]
//!
//! All tests are RED until the coder fills in the `todo!()` body.

use wasl_runtime_protocol::{SpawnContext, claude_argv};

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

/// All-default context (no model, no posture, no disallowed_tools, no cwd, etc.)
fn default_ctx() -> SpawnContext {
    SpawnContext::default()
}

// ------------------------------------------------------------------
// 1. All-default context, resume=false  →  exact today's argv
// ------------------------------------------------------------------

#[test]
fn default_context_resume_false_produces_canonical_argv() {
    let sid = "test-session-id";
    let argv = claude_argv("claude", sid, false, &default_ctx());

    // result[0] is the binary name
    assert_eq!(argv[0], "claude", "argv[0] must be the bin argument");

    // The full expected vector for all-default, resume=false
    let expected: Vec<String> = vec![
        "claude",
        "-p",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--verbose",
        "--session-id",
        sid,
    ]
    .into_iter()
    .map(String::from)
    .collect();

    assert_eq!(
        argv, expected,
        "all-default context + resume=false must produce the canonical argv"
    );
}

// ------------------------------------------------------------------
// 2. resume=true uses --resume instead of --session-id
// ------------------------------------------------------------------

#[test]
fn resume_true_uses_resume_flag() {
    let sid = "abc-resume-id";
    let argv = claude_argv("claude", sid, true, &default_ctx());

    // must contain "--resume" at some position
    let resume_pos = argv
        .iter()
        .position(|a| a == "--resume")
        .expect("argv must contain --resume when resume=true");

    // the value after "--resume" must be the session_id
    assert_eq!(
        argv[resume_pos + 1], sid,
        "--resume must be immediately followed by the session_id"
    );

    // must NOT contain "--session-id"
    assert!(
        !argv.contains(&"--session-id".to_string()),
        "resume=true must not emit --session-id"
    );
}

#[test]
fn resume_false_uses_session_id_flag() {
    let sid = "abc-session-id";
    let argv = claude_argv("claude", sid, false, &default_ctx());

    let pos = argv
        .iter()
        .position(|a| a == "--session-id")
        .expect("argv must contain --session-id when resume=false");

    assert_eq!(
        argv[pos + 1], sid,
        "--session-id must be immediately followed by the session_id"
    );

    assert!(
        !argv.contains(&"--resume".to_string()),
        "resume=false must not emit --resume"
    );
}

// ------------------------------------------------------------------
// 3. model only
// ------------------------------------------------------------------

#[test]
fn model_only_appends_model_flag() {
    let ctx = SpawnContext {
        model: Some("claude-sonnet-4.6".to_string()),
        ..SpawnContext::default()
    };
    let argv = claude_argv("claude", "sid", false, &ctx);

    let pos = argv
        .iter()
        .position(|a| a == "--model")
        .expect("argv must contain --model when context.model is Some");

    assert_eq!(
        argv[pos + 1], "claude-sonnet-4.6",
        "--model must be followed by the model name"
    );
}

#[test]
fn no_model_omits_model_flag() {
    let argv = claude_argv("claude", "sid", false, &default_ctx());
    assert!(
        !argv.contains(&"--model".to_string()),
        "--model must be absent when context.model is None"
    );
}

// ------------------------------------------------------------------
// 4. posture only
// ------------------------------------------------------------------

#[test]
fn posture_only_appends_permission_mode_flag() {
    let ctx = SpawnContext {
        posture: Some("bypassPermissions".to_string()),
        ..SpawnContext::default()
    };
    let argv = claude_argv("claude", "sid", false, &ctx);

    let pos = argv
        .iter()
        .position(|a| a == "--permission-mode")
        .expect("argv must contain --permission-mode when context.posture is Some");

    assert_eq!(
        argv[pos + 1], "bypassPermissions",
        "--permission-mode must be followed by the posture value"
    );
}

#[test]
fn no_posture_omits_permission_mode_flag() {
    let argv = claude_argv("claude", "sid", false, &default_ctx());
    assert!(
        !argv.contains(&"--permission-mode".to_string()),
        "--permission-mode must be absent when context.posture is None"
    );
}

// ------------------------------------------------------------------
// 5. disallowed_tools: one tool
// ------------------------------------------------------------------

#[test]
fn single_disallowed_tool_appends_flag_with_exact_name() {
    let ctx = SpawnContext {
        disallowed_tools: vec!["Bash".to_string()],
        ..SpawnContext::default()
    };
    let argv = claude_argv("claude", "sid", false, &ctx);

    let pos = argv
        .iter()
        .position(|a| a == "--disallowedTools")
        .expect("argv must contain --disallowedTools when disallowed_tools is non-empty");

    assert_eq!(
        argv[pos + 1], "Bash",
        "single disallowed tool must appear verbatim after --disallowedTools"
    );
}

// ------------------------------------------------------------------
// 6. disallowed_tools: multiple → exact comma join (no spaces)
// ------------------------------------------------------------------

#[test]
fn multiple_disallowed_tools_comma_joined_no_spaces() {
    let ctx = SpawnContext {
        disallowed_tools: vec!["Bash".to_string(), "Read".to_string(), "Write".to_string()],
        ..SpawnContext::default()
    };
    let argv = claude_argv("claude", "sid", false, &ctx);

    let pos = argv
        .iter()
        .position(|a| a == "--disallowedTools")
        .expect("argv must contain --disallowedTools for multiple tools");

    assert_eq!(
        argv[pos + 1], "Bash,Read,Write",
        "multiple disallowed tools must be comma-joined (no spaces) into a single argv value"
    );

    // only ONE --disallowedTools flag (not one per tool)
    let count = argv.iter().filter(|a| *a == "--disallowedTools").count();
    assert_eq!(count, 1, "there must be exactly one --disallowedTools flag");
}

#[test]
fn empty_disallowed_tools_omits_flag() {
    let argv = claude_argv("claude", "sid", false, &default_ctx());
    assert!(
        !argv.contains(&"--disallowedTools".to_string()),
        "--disallowedTools must be absent when disallowed_tools is empty"
    );
}

// ------------------------------------------------------------------
// 7. Combined: model + posture + disallowed_tools, correct order
// ------------------------------------------------------------------

#[test]
fn combined_model_posture_disallowed_all_present_in_order() {
    let ctx = SpawnContext {
        model: Some("claude-haiku".to_string()),
        posture: Some("default".to_string()),
        disallowed_tools: vec!["Bash".to_string(), "Edit".to_string()],
        ..SpawnContext::default()
    };
    let argv = claude_argv("claude", "the-sid", false, &ctx);

    // All three optional sections must be present
    let model_pos = argv
        .iter()
        .position(|a| a == "--model")
        .expect("--model must be present");
    let posture_pos = argv
        .iter()
        .position(|a| a == "--permission-mode")
        .expect("--permission-mode must be present");
    let tools_pos = argv
        .iter()
        .position(|a| a == "--disallowedTools")
        .expect("--disallowedTools must be present");

    assert_eq!(argv[model_pos + 1], "claude-haiku");
    assert_eq!(argv[posture_pos + 1], "default");
    assert_eq!(argv[tools_pos + 1], "Bash,Edit");

    // Order: model before permission-mode before disallowed-tools
    // (matches the documented append order in the todo!() banner)
    assert!(
        model_pos < posture_pos,
        "--model must appear before --permission-mode; got model@{model_pos} posture@{posture_pos}"
    );
    assert!(
        posture_pos < tools_pos,
        "--permission-mode must appear before --disallowedTools; got posture@{posture_pos} tools@{tools_pos}"
    );
}

// ------------------------------------------------------------------
// 8. Custom bin as result[0]
// ------------------------------------------------------------------

#[test]
fn custom_bin_is_result_zero() {
    let argv = claude_argv("/usr/local/bin/claude", "sid", false, &default_ctx());
    assert_eq!(
        argv[0], "/usr/local/bin/claude",
        "argv[0] must be exactly the bin argument, including path"
    );
}

#[test]
fn empty_string_bin_is_result_zero() {
    // Edge: the function takes the bin as-is; even an empty string is faithfully placed at [0].
    let argv = claude_argv("", "sid", false, &default_ctx());
    assert_eq!(argv[0], "", "argv[0] must be the bin arg even if empty");
}

// ------------------------------------------------------------------
// 9. Base flags are always present and in documented order
// ------------------------------------------------------------------

#[test]
fn base_flags_always_present_in_order() {
    let argv = claude_argv("claude", "s", false, &default_ctx());
    // After argv[0]=bin the documented base flags must appear contiguously:
    // ["-p", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose"]
    let base = &argv[1..];
    assert!(
        base.starts_with(&[
            "-p".to_string(),
            "--input-format".to_string(),
            "stream-json".to_string(),
            "--output-format".to_string(),
            "stream-json".to_string(),
            "--verbose".to_string(),
        ]),
        "base flags must be ['-p','--input-format','stream-json','--output-format','stream-json','--verbose'] \
         at argv[1..]; got {base:?}"
    );
}

// ------------------------------------------------------------------
// 10. resume=true + combined context
// ------------------------------------------------------------------

#[test]
fn resume_true_combined_context_uses_resume_not_session_id() {
    let ctx = SpawnContext {
        model: Some("claude-opus".to_string()),
        posture: Some("bypassPermissions".to_string()),
        disallowed_tools: vec!["Task".to_string()],
        ..SpawnContext::default()
    };
    let argv = claude_argv("claude", "resume-sid-xyz", true, &ctx);

    // --resume present, --session-id absent
    assert!(argv.contains(&"--resume".to_string()));
    assert!(!argv.contains(&"--session-id".to_string()));

    // session_id value follows --resume
    let pos = argv.iter().position(|a| a == "--resume").unwrap();
    assert_eq!(argv[pos + 1], "resume-sid-xyz");

    // optional flags still present
    assert!(argv.contains(&"--model".to_string()));
    assert!(argv.contains(&"--permission-mode".to_string()));
    assert!(argv.contains(&"--disallowedTools".to_string()));
}
