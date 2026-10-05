use crate::types::{RunStatus, StaleStatus};

/// Map a `RunStatus` to a CSS class suffix for `.grid-row-rail--*`.
pub fn run_status_class(status: &RunStatus) -> &'static str {
    match status {
        RunStatus::Success => "success",
        RunStatus::Failure => "failure",
        RunStatus::Started => "running",
        RunStatus::Queued => "queued",
        RunStatus::NotStarted => "pending",
        RunStatus::Canceled => "canceled",
    }
}

/// Whether a run is still in flight (queued, starting, or running) and can
/// therefore be canceled.
pub fn run_is_active(status: &RunStatus) -> bool {
    matches!(
        status,
        RunStatus::Started | RunStatus::NotStarted | RunStatus::Queued
    )
}

/// Chip kinds recognized by `StatusChip` — each maps to a `.dot-{kind}` CSS
/// class and is displayed verbatim as the chip label.
///
/// If you add a kind here, ensure a matching `.dot-{kind}` rule exists in
/// `rust/rivers-ui/style/rivers-widgets.css`. The `chip_kinds_are_recognized` test
/// guards `run_status_kind` / `backfill_status_kind` outputs against this list.
pub const CHIP_KINDS: &[&str] = &[
    "success",
    "running",
    "failed",
    "queued",
    "pending",
    "skipped",
    "canceled",
    "up-to-date",
    "stale",
    "missing",
];

/// Chip-vocabulary kind for a `RunStatus`. Exhaustive — adding a new variant
/// becomes a compile error so the UI can't silently fall back to "queued".
pub fn run_status_kind(status: &RunStatus) -> &'static str {
    match status {
        RunStatus::Success => "success",
        RunStatus::Failure => "failed",
        RunStatus::Started => "running",
        RunStatus::Queued => "queued",
        RunStatus::NotStarted => "pending",
        RunStatus::Canceled => "canceled",
    }
}

/// Chip-vocabulary kind for a `StaleStatus`. Exhaustive — keeps every
/// asset-status surface (chips, rails, sidebar labels, sort keys) on the same
/// vocabulary as the storage-side enum.
pub fn stale_status_kind(status: &StaleStatus) -> &'static str {
    match status {
        StaleStatus::UpToDate => "up-to-date",
        StaleStatus::Stale => "stale",
        StaleStatus::Missing => "missing",
    }
}

/// Chip-vocabulary kind for a backfill-status string (the `{:?}`-formatted
/// `BackfillStatus` variant name). Returns `"queued"` for unknown inputs so
/// the chip still renders something visible.
pub fn backfill_status_kind(status: &str) -> &'static str {
    match status {
        "Requested" => "queued",
        "InProgress" => "running",
        "CompletedSuccess" => "success",
        "CompletedFailed" => "failed",
        "Canceled" => "canceled",
        _ => "queued",
    }
}

/// Colour for a backfill's progress bar and rail, matching its status chip.
pub fn backfill_status_color(status: &str) -> &'static str {
    match status {
        "CompletedFailed" => "var(--error)",
        "Canceled" => "var(--text-muted)",
        "InProgress" | "Requested" => "var(--secondary)",
        _ => "var(--success)",
    }
}

/// Status-chip kind for a schedule or sensor tick.
pub fn tick_status_kind(status: &str) -> &'static str {
    match status {
        "Success" | "Requested" => "success",
        "Failure" => "failed",
        "Skipped" => "skipped",
        _ => "pending",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_status_kind() {
        assert_eq!(run_status_kind(&RunStatus::Success), "success");
        assert_eq!(run_status_kind(&RunStatus::Failure), "failed");
        assert_eq!(run_status_kind(&RunStatus::Started), "running");
        assert_eq!(run_status_kind(&RunStatus::Queued), "queued");
        assert_eq!(run_status_kind(&RunStatus::NotStarted), "pending");
        assert_eq!(run_status_kind(&RunStatus::Canceled), "canceled");
    }

    #[test]
    fn test_backfill_status_kind() {
        assert_eq!(backfill_status_kind("Requested"), "queued");
        assert_eq!(backfill_status_kind("InProgress"), "running");
        assert_eq!(backfill_status_kind("CompletedSuccess"), "success");
        assert_eq!(backfill_status_kind("CompletedFailed"), "failed");
        assert_eq!(backfill_status_kind("Canceled"), "canceled");
        assert_eq!(backfill_status_kind("anything-else"), "queued");
    }

    /// Guards against a kind function emitting a string that has no matching
    /// `.dot-{kind}` rule in `style/rivers-widgets.css`. If a new `CHIP_KINDS` entry is added,
    /// its CSS rule must be added too — this test won't catch that half, but
    /// it ensures the kind helpers never drift from the documented set.
    #[test]
    fn chip_kinds_are_recognized() {
        let run_variants = [
            RunStatus::Success,
            RunStatus::Failure,
            RunStatus::Started,
            RunStatus::Queued,
            RunStatus::NotStarted,
            RunStatus::Canceled,
        ];
        for s in &run_variants {
            let kind = run_status_kind(s);
            assert!(
                CHIP_KINDS.contains(&kind),
                "run_status_kind({s:?}) = {kind:?} is not in CHIP_KINDS"
            );
        }
        for s in [
            "Requested",
            "InProgress",
            "CompletedSuccess",
            "CompletedFailed",
            "Canceled",
        ] {
            let kind = backfill_status_kind(s);
            assert!(
                CHIP_KINDS.contains(&kind),
                "backfill_status_kind({s:?}) = {kind:?} is not in CHIP_KINDS"
            );
        }
        for s in [
            StaleStatus::UpToDate,
            StaleStatus::Stale,
            StaleStatus::Missing,
        ] {
            let kind = stale_status_kind(&s);
            assert!(
                CHIP_KINDS.contains(&kind),
                "stale_status_kind({s:?}) = {kind:?} is not in CHIP_KINDS"
            );
        }
    }
}
