//! wasl-session binary entry point (Phase-1 skeleton).
//!
//! Phase-1 is a crate/bin skeleton: `main` prints usage and exits. The command
//! behavior (control-plane client calls, rendering) is Phase-2 and is NOT
//! implemented here.

fn main() {
    print!("{}", wasl_session::USAGE);
}
