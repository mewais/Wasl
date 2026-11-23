//! The `wasl` package is the `wasl` binary (`src/main.rs`). This empty library exists so that
//! the package can depend on itself in `[dev-dependencies]` to turn its `panic-test` feature on
//! for its own tests (a bin-only package cannot be a dependency); no build for use has it.
