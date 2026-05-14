use std::collections::HashMap;
use std::process::Command;

#[derive(Clone, Debug)]
pub enum BashCommand {
    Shell(String),
    Exec(Vec<String>),
}

/// Execute a bash command and return its trimmed stdout.
pub fn execute_bash_command(
    command: &BashCommand,
    env: Option<&HashMap<String, String>>,
    cwd: Option<&str>,
) -> Result<String, String> {
    let mut cmd = match command {
        BashCommand::Shell(s) => {
            let mut c = Command::new("sh");
            c.args(["-c", s]);
            c
        }
        BashCommand::Exec(args) => {
            let mut c = Command::new(&args[0]);
            c.args(&args[1..]);
            c
        }
    };
    if let Some(env) = env {
        cmd.envs(env);
    }
    if let Some(cwd) = cwd {
        cmd.current_dir(cwd);
    }
    let output = cmd.output().map_err(|e| e.to_string())?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "Command failed with {}: {}",
            output.status,
            stderr.trim()
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.trim_end_matches('\n').to_string())
}
