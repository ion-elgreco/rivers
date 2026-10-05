use crate::types::{CodeLocationEntry, LaunchedBy};

/// Convert a nanosecond timestamp to a UTC instant.
pub fn nanos_to_datetime(ts: i64) -> Option<jiff::Timestamp> {
    jiff::Timestamp::from_nanosecond(ts as i128).ok()
}

/// Format an optional nanosecond timestamp as "YYYY-MM-DD HH:MM:SS" or "—".
pub fn format_timestamp(ts: Option<i64>) -> String {
    ts.and_then(nanos_to_datetime)
        .map(|d| d.strftime("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "—".to_string())
}

/// Format a nanosecond timestamp (non-optional) as "YYYY-MM-DD HH:MM:SS" or "—".
pub fn format_timestamp_nanos(ts: i64) -> String {
    nanos_to_datetime(ts)
        .map(|d| d.strftime("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "—".to_string())
}

/// Format a count of seconds as a compact human-readable duration.
pub fn format_seconds(secs: i64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    }
}

/// Format a duration between two optional nanosecond timestamps.
pub fn format_duration(start: Option<i64>, end: Option<i64>) -> String {
    match (start, end) {
        (Some(s), Some(e)) if e - s < 1_000_000_000 => "<1s".to_string(),
        (Some(s), Some(e)) => format_seconds((e - s) / 1_000_000_000),
        (Some(_), None) => "Running…".to_string(),
        _ => "—".to_string(),
    }
}

/// Live elapsed counter: duration from `start_ns` to `end_ns` if set, else
/// to `now_secs` (unix seconds). Returns "—" when start is missing. Pair
/// with [`crate::now::use_now`] in a reactive scope so the rendered string
/// re-evaluates each clock tick — the running-step companion to
/// [`format_duration`], which freezes at "Running…" once end is None.
pub fn format_elapsed(start_ns: Option<i64>, end_ns: Option<i64>, now_secs: i64) -> String {
    match start_ns {
        Some(s) => {
            let start_secs = s / 1_000_000_000;
            let end_secs = end_ns.map(|e| e / 1_000_000_000).unwrap_or(now_secs);
            format_seconds((end_secs - start_secs).max(0))
        }
        None => "—".to_string(),
    }
}

/// Format a nanosecond timestamp as relative time (e.g. "2 hours ago").
///
/// `now` is unix seconds; pass [`crate::now::use_now`]`().get()` from a
/// reactive scope so the label re-renders on each clock tick. Tests/non-
/// reactive callers can pass `jiff::Timestamp::now().as_second()` directly.
pub fn format_relative_time(ts: i64, now: i64) -> String {
    let secs = ts / 1_000_000_000;
    let diff = now - secs;
    if diff < 0 {
        return "just now".to_string();
    }
    if diff < 60 {
        return format!("{}s ago", diff);
    }
    if diff < 3600 {
        return format!("{}m ago", diff / 60);
    }
    if diff < 86400 {
        return format!("{}h ago", diff / 3600);
    }
    if diff < 86400 * 30 {
        return format!("{}d ago", diff / 86400);
    }
    nanos_to_datetime(ts)
        .map(|d| d.strftime("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "—".to_string())
}

/// Badge text for a job executor: "InProcess" → "in-process",
/// "Parallel(4)" → "parallel · 4".
pub fn executor_label(raw: &str) -> String {
    let (name, arg) = match raw.split_once('(') {
        Some((name, rest)) => (name, rest.strip_suffix(')')),
        None => (raw, None),
    };
    match (name, arg) {
        ("InProcess", _) => "in-process".to_string(),
        ("Parallel", Some(n)) => format!("parallel · {n}"),
        ("Parallel", None) => "parallel".to_string(),
        ("Kubernetes", _) => "kubernetes".to_string(),
        _ => raw.to_string(),
    }
}

/// "TimeWindow" → "time window", "Multi" → "multi".
pub fn partition_kind_label(kind: &str) -> String {
    match kind {
        "TimeWindow" => "time window".to_string(),
        other => other.to_ascii_lowercase(),
    }
}

/// Button text for an action verb: "optimize" → "Optimize",
/// "rebuild_index" → "Rebuild index". Tooltips keep the declared name.
pub fn verb_label(verb: &str) -> String {
    let spaced = verb.replace('_', " ");
    let mut chars = spaced.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

/// "1 run", "3 runs".
pub fn plural(n: u64, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

/// Error text for the UI, without server_fn's "error running server
/// function:" prefix on `ServerFnError::ServerError`.
pub fn err_text(e: &impl std::fmt::Display) -> String {
    let s = e.to_string();
    match s.strip_prefix("error running server function: ") {
        Some(msg) => msg.to_string(),
        None => s,
    }
}

/// Truncate an id-like string for compact display. Returns an owned String
/// so callers can push it straight into views.
pub fn short_id(id: &str, max_len: usize) -> String {
    if id.len() > max_len {
        id[..max_len].to_string()
    } else {
        id.to_string()
    }
}

/// Up-to-two uppercase initials for the avatar ("John Doe" → "JD",
/// "admin" → "A", all-symbol names → "?").
pub fn initials(display: &str) -> String {
    let mut firsts = display
        .split_whitespace()
        .filter_map(|w| w.chars().find(|c| c.is_alphanumeric()));
    match (firsts.next(), firsts.next_back()) {
        (Some(f), Some(l)) => f.to_uppercase().chain(l.to_uppercase()).collect(),
        (Some(f), None) => f.to_uppercase().collect(),
        _ => "?".to_string(),
    }
}

/// Stable per-user avatar hue. FNV-1a by hand: `DefaultHasher` is not
/// stable across builds, and the server-rendered color must match what
/// the client recomputes at hydration.
pub fn avatar_hue(seed: &str) -> u16 {
    let mut h: u32 = 0x811c9dc5;
    for b in seed.bytes() {
        h ^= u32::from(b);
        h = h.wrapping_mul(0x0100_0193);
    }
    (h % 360) as u16
}

/// Compact "N run · M backfill" summary for a tick's runs/backfills. Returns
/// `None` when both lists are empty so callers can fall back to their own
/// placeholder.
pub fn tick_counts_summary(run_ids: &[String], backfill_ids: &[String]) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();
    if !run_ids.is_empty() {
        parts.push(format!(
            "{} run{}",
            run_ids.len(),
            if run_ids.len() == 1 { "" } else { "s" },
        ));
    }
    if !backfill_ids.is_empty() {
        parts.push(format!(
            "{} backfill{}",
            backfill_ids.len(),
            if backfill_ids.len() == 1 { "" } else { "s" },
        ));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" · "))
    }
}

/// Resolve a stored `code_location_id` (the registry's `identity` UUID) to a
/// human-readable label using the supplied registry snapshot. Falls back to a
/// truncated id when no entry matches — typical when a code location was
/// removed but its records are still in storage.
///
/// Includes the namespace prefix only when the snapshot spans multiple
/// namespaces, mirroring the location-switcher's display rule.
pub fn code_location_label(id: &str, entries: &[CodeLocationEntry]) -> String {
    if id.is_empty() {
        return "—".to_string();
    }
    let ns_count = entries
        .iter()
        .map(|e| &e.namespace)
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    match entries.iter().find(|e| e.identity == id) {
        Some(e) if ns_count > 1 => format!("{}/{}", e.namespace, e.name),
        Some(e) => e.name.clone(),
        None => short_id(id, 8),
    }
}

/// Sub-line under a run row's launched-by label. For a manual run both the
/// acting user *and* the job name are shown together (`job · user`), so an
/// authenticated job launch reveals who triggered it — passing only the job
/// name would hide the user. `None` lets `LaunchedByCell` fall back to its own
/// default sub-line (schedule/sensor/backfill name).
pub fn launched_by_sub_line(l: &LaunchedBy, job_name: Option<&str>) -> Option<String> {
    // Non-manual origins fall back to the cell's own default sub-line; for
    // manual runs, reuse `launched_by_display`'s user payload (single source of
    // the user-display rule) and layer the job name on top.
    if !matches!(l, LaunchedBy::Manual { .. }) {
        return None;
    }
    let (_, _, _, user) = launched_by_display(l);
    match (job_name, user) {
        (Some(job), Some(user)) => Some(format!("{job} · {user}")),
        (Some(job), None) => Some(job.to_string()),
        (None, sub) => sub,
    }
}

/// Display metadata for a `LaunchedBy` origin: `(glyph, color, label, default_sub_line)`.
/// Shared by `LaunchedByCell` and the run-detail header so the glyph/label set
/// stays in one place.
pub fn launched_by_display(
    l: &LaunchedBy,
) -> (&'static str, &'static str, &'static str, Option<String>) {
    match l {
        LaunchedBy::Manual { user } => (
            "◉",
            "var(--text)",
            "manual",
            user.as_ref().map(|u| u.display().to_string()),
        ),
        LaunchedBy::Schedule { name } => ("⏱", "var(--warning)", "schedule", Some(name.clone())),
        LaunchedBy::Sensor { name } => ("⚡", "var(--secondary)", "sensor", Some(name.clone())),
        LaunchedBy::Backfill { backfill_id } => (
            "↻",
            "var(--accent)",
            "backfill",
            Some(short_id(backfill_id, 8)),
        ),
        LaunchedBy::Condition => ("✦", "var(--accent)", "condition", None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initials_take_first_and_last_word() {
        assert_eq!(initials("John Doe"), "JD");
        assert_eq!(initials("John Ronald Reuel Tolkien"), "JT");
        assert_eq!(initials("admin"), "A");
        assert_eq!(initials("john.doe@example.com"), "J");
        // Symbol-led words fall through to their first alphanumeric.
        assert_eq!(initials("(ion) k"), "IK");
        assert_eq!(initials("---"), "?");
        assert_eq!(initials(""), "?");
    }

    #[test]
    fn avatar_hue_is_stable_and_bounded() {
        // Pinned value: SSR and hydration must agree across builds/targets.
        assert_eq!(avatar_hue("john.doe"), avatar_hue("john.doe"));
        assert_eq!(avatar_hue("john.doe"), 34);
        assert!(avatar_hue("") < 360);
        assert_ne!(avatar_hue("alice"), avatar_hue("bob"));
    }

    #[test]
    fn launched_by_sub_line_manual_shows_job_and_user() {
        use crate::types::{LaunchedBy, UserRef};
        let user = Some(UserRef {
            subject: "sub-1".into(),
            email: None,
            name: Some("Ada".into()),
        });
        // Authenticated job launch: both job and user, not just the job.
        assert_eq!(
            launched_by_sub_line(&LaunchedBy::Manual { user: user.clone() }, Some("nightly")),
            Some("nightly · Ada".to_string())
        );
        // Auth disabled: job only.
        assert_eq!(
            launched_by_sub_line(&LaunchedBy::Manual { user: None }, Some("nightly")),
            Some("nightly".to_string())
        );
        // Asset materialization by a user: user only.
        assert_eq!(
            launched_by_sub_line(&LaunchedBy::Manual { user }, None),
            Some("Ada".to_string())
        );
        // Non-manual origins fall back to the cell's own sub-line.
        assert_eq!(
            launched_by_sub_line(&LaunchedBy::Schedule { name: "s".into() }, Some("j")),
            None
        );
    }

    #[test]
    fn test_format_timestamp_some() {
        assert_eq!(format_timestamp(Some(0)), "1970-01-01 00:00:00");
        // 1700000000 seconds in nanoseconds
        assert_eq!(
            format_timestamp(Some(1_700_000_000_000_000_000)),
            "2023-11-14 22:13:20"
        );
    }

    #[test]
    fn test_format_timestamp_none() {
        assert_eq!(format_timestamp(None), "—");
    }

    #[test]
    fn test_format_duration_completed() {
        let s = 1_000_000_000i64;
        assert_eq!(format_duration(Some(0), Some(s / 2)), "<1s");
        assert_eq!(format_duration(Some(0), Some(30 * s)), "30s");
        assert_eq!(format_duration(Some(0), Some(125 * s)), "2m 5s");
        assert_eq!(format_duration(Some(0), Some(7265 * s)), "2h 1m");
    }

    #[test]
    fn test_format_duration_running() {
        assert_eq!(format_duration(Some(100), None), "Running…");
    }

    #[test]
    fn test_format_duration_no_start() {
        assert_eq!(format_duration(None, None), "—");
        assert_eq!(format_duration(None, Some(100)), "—");
    }
}
