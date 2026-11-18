//! The scriptable fake agent CLI of the tests, built from this workspace under the name
//! `wasit-fake-cli` (where `wasit_fake_cli::fake_cli_path()` looks for it).

fn main() {
    std::process::exit(wasit_fake_cli::cli::main_with(
        std::env::args().skip(1).collect(),
    ));
}
