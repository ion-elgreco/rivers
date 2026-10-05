use std::process::exit;

use rivers_runtime::workspace_sync;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    // Run through the `/workspace/.git-ssh` symlink, this binary is git's
    // GIT_SSH.
    if args
        .first()
        .is_some_and(|argv0| argv0.ends_with(workspace_sync::GIT_SSH_HELPER))
    {
        exit(workspace_sync::git_ssh(&args[1..]));
    }
    match args.get(1).map(String::as_str) {
        Some("workspace-sync") => exit(workspace_sync::run()),
        _ => {
            eprintln!("usage: rivers-runtime workspace-sync");
            exit(2);
        }
    }
}
