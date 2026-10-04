//! Child processes under the build's deadline.

use std::io::{self, Read};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const POLL: Duration = Duration::from_millis(50);
/// After SIGTERM, how long a child gets before SIGKILL.
#[cfg(unix)]
const TERM_GRACE: Duration = Duration::from_secs(10);

/// When the build must be done.
pub(crate) struct Deadline(Instant);

impl Deadline {
    pub fn after(budget: Duration) -> Self {
        let now = Instant::now();
        Self(
            now.checked_add(budget)
                .unwrap_or_else(|| now + Duration::from_secs(u64::from(u32::MAX))),
        )
    }

    fn passed(&self) -> bool {
        Instant::now() >= self.0
    }
}

/// What a child's streams do.
#[derive(Clone, Copy)]
pub(crate) enum Output {
    /// Both go to the pod log.
    Inherit,
    /// Both kept, stderr first — git's error for the termination message.
    Capture,
    /// Stdout kept, stderr goes to the pod log — uv queries.
    Stdout,
}

pub(crate) struct Finished {
    pub status: ExitStatus,
    pub captured: String,
}

#[derive(Debug)]
pub(crate) enum Error {
    Spawn { program: String, source: io::Error },
    TimedOut,
}

/// Runs `cmd` in its own process group; past the deadline the whole group
/// gets SIGTERM, as `timeout(1)` does.
pub(crate) fn run(
    cmd: &mut Command,
    deadline: &Deadline,
    output: Output,
) -> Result<Finished, Error> {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        cmd.process_group(0);
    }
    cmd.stdin(Stdio::null());
    match output {
        Output::Inherit => {}
        Output::Capture => {
            cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        }
        Output::Stdout => {
            cmd.stdout(Stdio::piped());
        }
    }
    let program = cmd.get_program().to_string_lossy().into_owned();
    let mut child = cmd
        .spawn()
        .map_err(|source| Error::Spawn { program, source })?;
    let stdout = child
        .stdout
        .take()
        .map(|stream| thread::spawn(move || read_all(stream)));
    let stderr = child
        .stderr
        .take()
        .map(|stream| thread::spawn(move || read_all(stream)));
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) if deadline.passed() => {
                terminate(&mut child);
                break None;
            }
            Ok(None) => thread::sleep(POLL),
            Err(_) => break child.wait().ok(),
        }
    };
    let collect = |reader: Option<JoinHandle<String>>| {
        reader
            .and_then(|reader| reader.join().ok())
            .unwrap_or_default()
    };
    let (stdout, stderr) = (collect(stdout), collect(stderr));
    let Some(status) = status else {
        return Err(Error::TimedOut);
    };
    let captured = match output {
        Output::Inherit => String::new(),
        Output::Capture => stderr + &stdout,
        Output::Stdout => stdout,
    };
    Ok(Finished { status, captured })
}

/// The shell's view of an exit status: the code, or 128 + the signal.
pub(crate) fn exit_code(status: &ExitStatus) -> i32 {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        if let Some(signal) = status.signal() {
            return 128 + signal;
        }
    }
    status.code().unwrap_or(1)
}

fn read_all(mut stream: impl Read) -> String {
    let mut bytes = Vec::new();
    let _ = stream.read_to_end(&mut bytes);
    String::from_utf8_lossy(&bytes).into_owned()
}

/// SIGTERM to the child's process group, SIGKILL after the grace period.
fn terminate(child: &mut Child) {
    #[cfg(unix)]
    {
        let group = child.id() as libc::pid_t;
        // SAFETY: a plain syscall on our own child's process group.
        unsafe { libc::killpg(group, libc::SIGTERM) };
        let grace = Instant::now() + TERM_GRACE;
        while Instant::now() < grace {
            if matches!(child.try_wait(), Ok(Some(_))) {
                return;
            }
            thread::sleep(POLL);
        }
        // SAFETY: as above.
        unsafe { libc::killpg(group, libc::SIGKILL) };
    }
    #[cfg(not(unix))]
    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn sh(script: &str) -> Command {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", script]);
        cmd
    }

    #[cfg(unix)]
    #[test]
    fn deadline_terminates_the_process_group() {
        let started = Instant::now();
        let result = run(
            &mut sh("sleep 30"),
            &Deadline::after(Duration::from_millis(200)),
            Output::Inherit,
        );
        assert!(matches!(result, Err(Error::TimedOut)));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[cfg(unix)]
    #[test]
    fn captured_output_holds_stderr_then_stdout() {
        let done = run(
            &mut sh("echo out; echo err >&2; exit 3"),
            &Deadline::after(Duration::from_secs(5)),
            Output::Capture,
        )
        .unwrap();
        assert_eq!(done.captured, "err\nout\n");
        assert_eq!(exit_code(&done.status), 3);
    }

    #[cfg(unix)]
    #[test]
    fn stdout_mode_keeps_only_stdout() {
        let done = run(
            &mut sh("echo out; echo err >&2"),
            &Deadline::after(Duration::from_secs(5)),
            Output::Stdout,
        )
        .unwrap();
        assert_eq!(done.captured, "out\n");
        assert!(done.status.success());
    }

    #[cfg(unix)]
    #[test]
    fn a_signal_exits_as_128_plus_the_signal() {
        let done = run(
            &mut sh("kill -TERM $$"),
            &Deadline::after(Duration::from_secs(5)),
            Output::Inherit,
        )
        .unwrap();
        assert_eq!(exit_code(&done.status), 143);
    }

    #[test]
    fn missing_program_is_a_spawn_error() {
        let result = run(
            &mut Command::new("rivers-no-such-program"),
            &Deadline::after(Duration::from_secs(1)),
            Output::Inherit,
        );
        assert!(
            matches!(result, Err(Error::Spawn { ref program, .. }) if program == "rivers-no-such-program")
        );
    }
}
