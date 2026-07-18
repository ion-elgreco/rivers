//! Pod termination → [`FailureReason`] classification.

use rivers_core::execution::retry::FailureReason;

/// Classify one terminated container state; `None` = clean exit.
///
/// Only an explicit `OOMKilled` marks memory exhaustion — a bare kill signal
/// (137/143, whatever the runtime's generic reason string) is infrastructure:
/// evictions, node drains, and preemptions kill with SIGKILL/SIGTERM, and a
/// real cgroup OOM sets the reason.
pub fn classify_termination(reason: Option<&str>, exit_code: i32) -> Option<FailureReason> {
    match reason {
        Some("OOMKilled") => return Some(FailureReason::OutOfMemory),
        Some("DeadlineExceeded") => return Some(FailureReason::Timeout),
        _ => {}
    }
    match exit_code {
        0 => None,
        137 | 143 => Some(FailureReason::Infrastructure),
        _ => Some(FailureReason::Error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oom_requires_the_explicit_reason() {
        assert_eq!(
            classify_termination(Some("OOMKilled"), 137),
            Some(FailureReason::OutOfMemory)
        );
        // A bare SIGKILL (eviction, preemption) is not a memory failure —
        // escalating memory for it would be wrong.
        assert_eq!(
            classify_termination(None, 137),
            Some(FailureReason::Infrastructure)
        );
        assert_eq!(
            classify_termination(Some("Error"), 137),
            Some(FailureReason::Infrastructure)
        );
    }

    #[test]
    fn sigterm_is_infrastructure() {
        assert_eq!(
            classify_termination(Some("Error"), 143),
            Some(FailureReason::Infrastructure)
        );
    }

    #[test]
    fn deadline_maps_to_timeout() {
        assert_eq!(
            classify_termination(Some("DeadlineExceeded"), 1),
            Some(FailureReason::Timeout)
        );
    }

    #[test]
    fn ordinary_exits() {
        assert_eq!(
            classify_termination(Some("Error"), 1),
            Some(FailureReason::Error)
        );
        assert_eq!(classify_termination(None, 0), None);
        assert_eq!(classify_termination(Some("Completed"), 0), None);
    }
}
