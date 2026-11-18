//! Every process Wasl starts goes through wasit-process (no console windows on Windows).

use std::path::Path;

#[test]
fn no_process_is_created_outside_wasit_process() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let found = wasit_process::scan::raw_spawns(&root, &[]);
    assert!(
        found.is_empty(),
        "create processes with wasit_process::{{command, std_command, detached_command}}, not Command::new:\n{}",
        found.join("\n")
    );
}
