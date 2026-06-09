//! Shared helpers for URL query state persistence and timestamp formatting.

use leptos::prelude::*;
use leptos_router::hooks::use_location;
use leptos_router::params::ParamsMap;

use crate::types::{AssetDefinitionInfo, CodeLocationEntry, LaunchedBy, RunStatus, StaleStatus};

/// Shared buffer so multiple `use_query_param` setters called in the same
/// tick accumulate into a single navigation instead of clobbering each other.
#[derive(Clone, Copy)]
struct PendingQueryParams(RwSignal<Option<ParamsMap>>);

fn query_params_ctx() -> PendingQueryParams {
    if let Some(ctx) = use_context::<PendingQueryParams>() {
        return ctx;
    }
    let sig = RwSignal::new(None::<ParamsMap>);
    let ctx = PendingQueryParams(sig);
    provide_context(ctx);

    let location = use_location();
    // Effect fires once per tick — collapses multiple setter calls into one navigate
    Effect::new(move |_| {
        if let Some(params) = sig.get() {
            let qs = params.to_query_string();
            let path = location.pathname.get_untracked();
            let new_url = format!("{path}{qs}");
            let current_path = location.pathname.get_untracked();
            let current_qs = location.query.get_untracked().to_query_string();
            if format!("{current_path}{current_qs}") != new_url {
                let navigate = leptos_router::hooks::use_navigate();
                let _ = navigate(&new_url, Default::default());
            }
            sig.update_untracked(|v| *v = None);
        }
    });

    ctx
}

/// Read a query parameter as a String, defaulting to `default` if absent.
pub fn use_query_param(
    key: &'static str,
    default: &str,
) -> (Signal<String>, impl Fn(String) + Clone + 'static + use<>) {
    let location = use_location();
    let pending = query_params_ctx();
    let default = default.to_string();
    let default_for_read = default.clone();

    let value = Signal::derive(move || {
        // Read from pending buffer first (for same-tick consistency),
        // then fall back to the actual location query
        if let Some(ref params) = pending.0.get_untracked() {
            params.get(key).unwrap_or(default_for_read.clone())
        } else {
            location
                .query
                .read()
                .get(key)
                .unwrap_or(default_for_read.clone())
        }
    });

    let set_value = move |new_val: String| {
        // navigate() requires window() — skip during SSR
        if cfg!(not(target_arch = "wasm32")) {
            return;
        }
        let base = pending
            .0
            .get_untracked()
            .unwrap_or_else(|| location.query.get_untracked());
        let mut new_map = ParamsMap::new();
        for (k, v) in base.into_iter() {
            if k.as_ref() != key {
                new_map.insert(k, v);
            }
        }
        if !new_val.is_empty() && new_val != default {
            new_map.insert(key, new_val);
        }
        // Effect will navigate at end of tick.
        pending.0.set(Some(new_map));
    };

    (value, set_value)
}

/// Read a query parameter as a comma-separated list of strings.
pub fn use_query_param_list(
    key: &'static str,
) -> (Signal<Vec<String>>, impl Fn(Vec<String>) + Clone + 'static) {
    let (raw, set_raw) = use_query_param(key, "");

    let value = Signal::derive(move || {
        let s = raw.get();
        if s.is_empty() {
            Vec::new()
        } else {
            s.split(',').map(|s| s.to_string()).collect()
        }
    });

    let set_value = move |vals: Vec<String>| {
        set_raw(vals.join(","));
    };

    (value, set_value)
}

/// Read a query parameter as usize, defaulting to `default` if absent.
pub fn use_query_param_usize(
    key: &'static str,
    default: usize,
) -> (Signal<usize>, impl Fn(usize) + Clone + 'static) {
    let (raw, set_raw) = use_query_param(key, &default.to_string());

    let value = Signal::derive(move || raw.get().parse::<usize>().unwrap_or(default));

    let set_value = move |n: usize| {
        set_raw(if n == default {
            String::new()
        } else {
            n.to_string()
        });
    };

    (value, set_value)
}

/// Convert a nanosecond timestamp to a chrono DateTime.
pub fn nanos_to_datetime(ts: i64) -> Option<chrono::DateTime<chrono::Utc>> {
    let secs = ts / 1_000_000_000;
    let nanos = (ts % 1_000_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, nanos)
}

/// Format an optional nanosecond timestamp as "YYYY-MM-DD HH:MM:SS" or "-".
pub fn format_timestamp(ts: Option<i64>) -> String {
    ts.and_then(nanos_to_datetime)
        .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "-".to_string())
}

/// Format a nanosecond timestamp (non-optional) as "YYYY-MM-DD HH:MM:SS" or "-".
pub fn format_timestamp_nanos(ts: i64) -> String {
    nanos_to_datetime(ts)
        .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "-".to_string())
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
        (Some(s), Some(e)) => format_seconds((e - s) / 1_000_000_000),
        (Some(_), None) => "Running...".to_string(),
        _ => "-".to_string(),
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
        None => "-".to_string(),
    }
}

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

/// Map a tick status string to a CSS class suffix.
pub fn tick_status_class(status: &str) -> &'static str {
    match status {
        "Success" | "Requested" => "success",
        "Failure" => "failure",
        "Skipped" => "muted",
        _ => "muted",
    }
}

/// Format a nanosecond timestamp as relative time (e.g. "2 hours ago").
///
/// `now` is unix seconds; pass [`crate::now::use_now`]`().get()` from a
/// reactive scope so the label re-renders on each clock tick. Tests/non-
/// reactive callers can pass `chrono::Utc::now().timestamp()` directly.
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
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "-".to_string())
}

/// Format a function-call-style value like `PerDimension(multi_run=["a"], single_run=["b"])`
/// into a multi-line display form when it has at least two top-level, comma-separated args.
/// Single-arg or unparenthesized strings are returned unchanged. Brackets inside arguments
/// are respected (top-level comma split only).
pub fn format_call_multiline(s: &str) -> String {
    let Some(open) = s.find('(') else {
        return s.to_string();
    };
    if !s.ends_with(')') {
        return s.to_string();
    }
    let prefix = &s[..open];
    let inner = &s[open + 1..s.len() - 1];
    let mut parts: Vec<&str> = Vec::new();
    let mut depth: i32 = 0;
    let mut start = 0usize;
    for (i, b) in inner.bytes().enumerate() {
        match b {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            b',' if depth == 0 => {
                parts.push(&inner[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&inner[start..]);
    if parts.len() < 2 {
        return s.to_string();
    }
    let body = parts
        .iter()
        .map(|p| format!("  {},", p.trim()))
        .collect::<Vec<_>>()
        .join("\n");
    format!("{prefix}(\n{body}\n)")
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

/// What the partition picker should render for a job's selection.
///
/// Single source of truth for the dialog: callers route on the variant
/// instead of reasoning about partition kinds + key shapes themselves.
/// Resolve-time validation already rejects user-defined jobs whose
/// partitioned assets have disjoint definitions, so for any UI-visible
/// job this is guaranteed to expose every key the dialog will show.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobPartitionPicker {
    /// No partitioned assets in the selection — submit with no key.
    None,
    /// Static / TimeWindow / etc. — flat list of keys, intersection
    /// across the job's partitioned assets in first-encounter order.
    /// `truncated` is set when any contributing key list was a bounded
    /// window, i.e. shared keys beyond the window exist but can't be shown.
    SingleDim { keys: Vec<String>, truncated: bool },
    /// Single-dim with more partitions than fit in the inline key window —
    /// paged on demand by `asset_key` as the user scrolls (`total` sizes the
    /// scrollbar). Avoids ever shipping the full key list to the browser.
    SingleDimPaged { asset_key: String, total: u64 },
    /// Storage-managed keys (not in the in-memory def), paged from storage by
    /// `dynamic_name`; `total` sizes the scrollbar. Single-dim-shaped.
    Dynamic { dynamic_name: String, total: u64 },
    /// Multi — per-dimension selectors; each dimension's `keys` is the
    /// intersection across the job's Multi assets, in the first asset's order.
    /// `asset_key` is `Some` only for a single-Multi-asset job, enabling
    /// per-dimension paging; `None` (several Multi assets) keeps the intersected
    /// windows inline — same cross-asset guard as `SingleDimPaged`.
    Multi {
        dimensions: Vec<crate::types::PartitionDimensionInfo>,
        asset_key: Option<String>,
    },
}

/// Compute the picker shape for a job's partitioned assets.
///
/// `SingleDim` keys = intersection of `partition_def.keys` across all
/// partitioned assets in encounter order, dropping unpartitioned assets.
/// `Multi` dimensions = per-dimension intersection across every
/// Multi-partitioned asset (resolve-time validator guarantees the
/// dimension name sets match). `Dynamic` when every partitioned asset shares one
/// Dynamic namespace. Mixed kinds are rejected at resolve time so the helper
/// picks whichever kind the partitioned assets share.
pub fn partition_picker_for_assets(
    assets: &[String],
    asset_info_by_key: &std::collections::HashMap<String, AssetDefinitionInfo>,
) -> JobPartitionPicker {
    let defs: Vec<&crate::types::PartitionDefinitionInfo> = assets
        .iter()
        .filter_map(|asset| {
            asset_info_by_key
                .get(asset)
                .and_then(|info| info.partition_def.as_ref())
        })
        .collect();
    if defs.is_empty() {
        return JobPartitionPicker::None;
    }
    let multi_count = defs.iter().filter(|d| !d.dimensions.is_empty()).count();
    if multi_count > 0 {
        // Multi: per-dimension intersection. Use the first def's
        // dimension order; later defs are guaranteed to carry the same
        // dimension names by the resolve-time validator.
        let first = defs[0];
        let mut dims: Vec<crate::types::PartitionDimensionInfo> = first.dimensions.clone();
        for other in defs.iter().skip(1) {
            // Dimension-name sets must match exactly — a key carrying a dim
            // the other asset lacks (or missing one it has) can never
            // validate for both, so there is nothing to offer. Jobs are
            // guarded at resolve time; ad-hoc selections reach here.
            let dims_match = other.dimensions.len() == dims.len()
                && dims
                    .iter()
                    .all(|d| other.dimensions.iter().any(|od| od.name == d.name));
            if !dims_match {
                return JobPartitionPicker::None;
            }
            for dim in dims.iter_mut() {
                let Some(other_dim) = other.dimensions.iter().find(|d| d.name == dim.name) else {
                    continue;
                };
                let next: std::collections::HashSet<&str> =
                    other_dim.keys.iter().map(String::as_str).collect();
                dim.keys.retain(|k| next.contains(k.as_str()));
            }
        }
        if dims.iter().all(|d| d.keys.is_empty()) {
            return JobPartitionPicker::None;
        }
        // Page dimensions only for a single Multi asset (no cross-asset
        // intersection to honor); else `asset_key` None → inline windows.
        let asset_key = (multi_count == 1)
            .then(|| {
                assets.iter().find(|a| {
                    asset_info_by_key
                        .get(*a)
                        .and_then(|i| i.partition_def.as_ref())
                        .is_some_and(|pd| !pd.dimensions.is_empty())
                })
            })
            .flatten()
            .cloned();
        return JobPartitionPicker::Multi {
            dimensions: dims,
            asset_key,
        };
    }
    // Only when every partitioned asset shares one Dynamic namespace — a mixed
    // job can't share a key (validator rejects it), so it falls to single-dim below.
    let dynamic_names: Vec<&str> = defs.iter().filter_map(|d| d.dynamic_namespace()).collect();
    if !dynamic_names.is_empty() {
        let first = dynamic_names[0];
        let all_dynamic_one_namespace =
            dynamic_names.len() == defs.len() && dynamic_names.iter().all(|n| *n == first);
        if all_dynamic_one_namespace {
            // Emit even at 0 so the dialog shows an empty state instead of the
            // button silently firing a keyless (rejected) materialize.
            let total = defs.iter().map(|d| d.total_count).max().unwrap_or(0);
            return JobPartitionPicker::Dynamic {
                dynamic_name: first.to_string(),
                total,
            };
        }
    }
    // Single-dim (Static / TimeWindow). If the driving asset has more
    // partitions than fit in the inline key window, page it on demand;
    // otherwise show the intersection across the job's single-dim assets.
    // Dynamic defs excluded here: `dynamic_namespace()` is `Some`, `keys` empty.
    let first_single = assets.iter().find_map(|a| {
        let pd = asset_info_by_key.get(a)?.partition_def.as_ref()?;
        (pd.dimensions.is_empty()
            && pd.dynamic_namespace().is_none()
            && (pd.total_count > 0 || !pd.keys.is_empty()))
        .then(|| (a.clone(), pd))
    });
    let Some((asset_key, first_def)) = first_single else {
        return JobPartitionPicker::None;
    };
    let needs_paging =
        first_def.keys_truncated || first_def.total_count as usize > first_def.keys.len();
    // Page only when every single-dim asset shares the same key space (same
    // total + window). The paged endpoint serves one asset's keys, so paging a
    // merely-overlapping job (the validator only requires a non-empty
    // intersection) could offer a key invalid for another; differing defs fall
    // through to the intersection path, which only shows shared keys.
    let same_key_space = defs.iter().all(|d| {
        d.dimensions.is_empty()
            && d.total_count == first_def.total_count
            && d.keys == first_def.keys
    });
    if needs_paging && same_key_space {
        return JobPartitionPicker::SingleDimPaged {
            asset_key,
            total: first_def.total_count,
        };
    }
    let contributing: Vec<&&crate::types::PartitionDefinitionInfo> =
        defs.iter().filter(|d| !d.keys.is_empty()).collect();
    let Some((first, rest)) = contributing.split_first() else {
        return JobPartitionPicker::None;
    };
    let mut intersection: Vec<String> = first.keys.to_vec();
    for d in rest {
        let next: std::collections::HashSet<&str> = d.keys.iter().map(String::as_str).collect();
        intersection.retain(|k| next.contains(k.as_str()));
    }
    // Intersecting bounded windows can only see the shared keys inside them
    // — surface that so the dialog doesn't present the list as exhaustive.
    let truncated = contributing
        .iter()
        .any(|d| d.keys_truncated || d.total_count as usize > d.keys.len());
    if intersection.is_empty() {
        JobPartitionPicker::None
    } else {
        JobPartitionPicker::SingleDim {
            keys: intersection,
            truncated,
        }
    }
}

/// Cartesian product of per-dimension selections — used by the Multi
/// dialog flow to expand the user's `{color: [r,g], size: [s]}` choices
/// into individual partition keys, one per concrete combination. Each
/// output carries `(dim_name, value)` pairs sorted alphabetically by
/// dimension name so the wire form is deterministic.
///
/// The server-side enumerator that produces the same shape lives in
/// `python/src/partitions/definition.rs::cartesian_product`. Both must
/// stay aligned on dimension ordering since the partition key shows up
/// in display strings (`py_partition_key_display`) and equality checks
/// downstream.
pub fn cartesian_partition_keys(
    selections: &[(String, Vec<String>)],
) -> Vec<crate::types::SubmitPartitionKey> {
    if selections.is_empty() || selections.iter().any(|(_, vs)| vs.is_empty()) {
        return Vec::new();
    }
    let mut sorted = selections.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out: Vec<Vec<(String, String)>> = vec![vec![]];
    for (dim, vals) in &sorted {
        let mut next: Vec<Vec<(String, String)>> = Vec::with_capacity(out.len() * vals.len());
        for combo in &out {
            for val in vals {
                let mut extended = combo.clone();
                extended.push((dim.clone(), val.clone()));
                next.push(extended);
            }
        }
        out = next;
    }
    out.into_iter()
        .map(crate::types::SubmitPartitionKey::Multi)
        .collect()
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

/// Display metadata for a `LaunchedBy` origin: `(glyph, color, label, default_sub_line)`.
/// Shared by `LaunchedByCell` and the run-detail header so the glyph/label set
/// stays in one place.
pub fn launched_by_display(
    l: &LaunchedBy,
) -> (&'static str, &'static str, &'static str, Option<String>) {
    match l {
        LaunchedBy::Manual => ("◉", "var(--text)", "manual", None),
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
        assert_eq!(format_timestamp(None), "-");
    }

    #[test]
    fn test_format_duration_completed() {
        let s = 1_000_000_000i64;
        assert_eq!(format_duration(Some(0), Some(30 * s)), "30s");
        assert_eq!(format_duration(Some(0), Some(125 * s)), "2m 5s");
        assert_eq!(format_duration(Some(0), Some(7265 * s)), "2h 1m");
    }

    #[test]
    fn test_format_duration_running() {
        assert_eq!(format_duration(Some(100), None), "Running...");
    }

    #[test]
    fn test_format_duration_no_start() {
        assert_eq!(format_duration(None, None), "-");
        assert_eq!(format_duration(None, Some(100)), "-");
    }

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

    use std::collections::HashMap;

    use crate::types::{AssetDefinitionInfo, PartitionDefinitionInfo, PartitionDimensionInfo};

    fn make_info(asset_key: &str, keys: Option<&[&str]>) -> AssetDefinitionInfo {
        AssetDefinitionInfo {
            asset_key: asset_key.to_string(),
            description: None,
            partition_def: keys.map(|ks| PartitionDefinitionInfo {
                kind: "Static".to_string(),
                keys: ks.iter().map(|s| s.to_string()).collect(),
                dimensions: vec![],
                total_count: ks.len() as u64,
                keys_truncated: false,
                dynamic_name: String::new(),
            }),
            hooks: vec![],
            io_handler: None,
            has_self_dependency: false,
            is_external: false,
            automation_condition: None,
            tags: vec![],
            kinds: vec![],
            group: None,
            code_version: None,
            asset_type: "asset".to_string(),
        }
    }

    fn make_multi(asset_key: &str, dims: &[(&str, &[&str])]) -> AssetDefinitionInfo {
        let mut info = make_info(asset_key, None);
        info.partition_def = Some(PartitionDefinitionInfo {
            kind: "Multi".to_string(),
            keys: vec![],
            dimensions: dims
                .iter()
                .map(|(name, ks)| PartitionDimensionInfo {
                    name: name.to_string(),
                    keys: ks.iter().map(|s| s.to_string()).collect(),
                    total_count: ks.len() as u64,
                    keys_truncated: false,
                })
                .collect(),
            total_count: 0,
            keys_truncated: false,
            dynamic_name: String::new(),
        });
        info
    }

    fn make_map(items: Vec<AssetDefinitionInfo>) -> HashMap<String, AssetDefinitionInfo> {
        items
            .into_iter()
            .map(|i| (i.asset_key.clone(), i))
            .collect()
    }

    fn picker(assets: &[&str], infos: &HashMap<String, AssetDefinitionInfo>) -> JobPartitionPicker {
        let assets_owned: Vec<String> = assets.iter().map(|s| s.to_string()).collect();
        partition_picker_for_assets(&assets_owned, infos)
    }

    #[test]
    fn picker_none_when_no_partitioned_assets() {
        let infos = make_map(vec![make_info("a", None), make_info("b", None)]);
        assert_eq!(picker(&["a", "b"], &infos), JobPartitionPicker::None);
    }

    #[test]
    fn picker_singledim_intersects_overlapping_keys() {
        // Common key is "y"; "x" only in a, "z" only in b — neither
        // belongs in the intersection.
        let infos = make_map(vec![
            make_info("a", Some(&["x", "y"])),
            make_info("b", Some(&["y", "z"])),
        ]);
        assert_eq!(
            picker(&["a", "b"], &infos),
            JobPartitionPicker::SingleDim {
                keys: vec!["y".into()],
                truncated: false,
            }
        );
    }

    #[test]
    fn picker_none_when_assets_have_no_overlap() {
        // Defended against by the resolve-time validator, but pin the
        // helper's collapse-to-None behaviour.
        let infos = make_map(vec![
            make_info("a", Some(&["x"])),
            make_info("b", Some(&["y"])),
        ]);
        assert_eq!(picker(&["a", "b"], &infos), JobPartitionPicker::None);
    }

    #[test]
    fn picker_preserves_first_asset_order() {
        // Iteration order follows the first partitioned asset's keys.
        let infos = make_map(vec![
            make_info("first", Some(&["b", "a"])),
            make_info("second", Some(&["a", "b"])),
        ]);
        assert_eq!(
            picker(&["first", "second"], &infos),
            JobPartitionPicker::SingleDim {
                keys: vec!["b".into(), "a".into()],
                truncated: false,
            }
        );
    }

    #[test]
    fn picker_skips_assets_missing_from_map() {
        // The jobs list page may render before `assets_info` resolves;
        // the helper must yield the known asset's keys instead of
        // collapsing to None.
        let infos = make_map(vec![make_info("known", Some(&["x", "y"]))]);
        assert_eq!(
            picker(&["known", "missing"], &infos),
            JobPartitionPicker::SingleDim {
                keys: vec!["x".into(), "y".into()],
                truncated: false,
            }
        );
    }

    #[test]
    fn picker_skips_unpartitioned_assets() {
        // Mixed partitioned + unpartitioned: unpartitioned doesn't
        // constrain the intersection.
        let infos = make_map(vec![
            make_info("part", Some(&["x", "y"])),
            make_info("plain", None),
        ]);
        assert_eq!(
            picker(&["part", "plain"], &infos),
            JobPartitionPicker::SingleDim {
                keys: vec!["x".into(), "y".into()],
                truncated: false,
            }
        );
    }

    /// A Dynamic asset: empty `keys`/`dimensions`, storage-sourced `total_count`.
    fn make_dynamic(asset_key: &str, namespace: &str, total: u64) -> AssetDefinitionInfo {
        let mut info = make_info(asset_key, None);
        info.partition_def = Some(PartitionDefinitionInfo {
            kind: "Dynamic".to_string(),
            keys: vec![],
            dimensions: vec![],
            total_count: total,
            keys_truncated: false,
            dynamic_name: namespace.to_string(),
        });
        info
    }

    #[test]
    fn picker_dynamic_single_asset_pages_from_namespace() {
        let infos = make_map(vec![make_dynamic("dyn", "customers", 42)]);
        assert_eq!(
            picker(&["dyn"], &infos),
            JobPartitionPicker::Dynamic {
                dynamic_name: "customers".into(),
                total: 42,
            }
        );
    }

    #[test]
    fn picker_dynamic_zero_total_still_emits_dynamic() {
        // Zero keys → still emit (empty state), not None.
        let infos = make_map(vec![make_dynamic("dyn", "customers", 0)]);
        assert_eq!(
            picker(&["dyn"], &infos),
            JobPartitionPicker::Dynamic {
                dynamic_name: "customers".into(),
                total: 0,
            }
        );
    }

    #[test]
    fn picker_dynamic_multiple_assets_same_namespace() {
        // Two assets backed by the same Dynamic definition share one key space.
        let infos = make_map(vec![
            make_dynamic("a", "customers", 42),
            make_dynamic("b", "customers", 42),
        ]);
        assert_eq!(
            picker(&["a", "b"], &infos),
            JobPartitionPicker::Dynamic {
                dynamic_name: "customers".into(),
                total: 42,
            }
        );
    }

    #[test]
    fn picker_dynamic_mixed_with_static_falls_through_to_singledim() {
        // Mixed kinds can't share a key; helper skips Dynamic, uses the static keys.
        let infos = make_map(vec![
            make_dynamic("dyn", "customers", 42),
            make_info("stat", Some(&["x", "y"])),
        ]);
        assert_eq!(
            picker(&["dyn", "stat"], &infos),
            JobPartitionPicker::SingleDim {
                keys: vec!["x".into(), "y".into()],
                truncated: false,
            }
        );
    }

    #[test]
    fn picker_dynamic_distinct_namespaces_do_not_merge() {
        // Distinct namespaces share no keys → None (don't page one for both).
        let infos = make_map(vec![
            make_dynamic("a", "customers", 42),
            make_dynamic("b", "orders", 7),
        ]);
        assert_eq!(picker(&["a", "b"], &infos), JobPartitionPicker::None);
    }

    #[test]
    fn picker_none_for_empty_asset_selection() {
        let infos = make_map(vec![make_info("a", Some(&["x"]))]);
        assert_eq!(picker(&[], &infos), JobPartitionPicker::None);
    }

    #[test]
    fn picker_multi_returns_per_dimension_keys() {
        let infos = make_map(vec![make_multi(
            "m",
            &[("color", &["r", "g"]), ("size", &["s", "m"])],
        )]);
        let picker = picker(&["m"], &infos);
        let JobPartitionPicker::Multi { dimensions, .. } = picker else {
            panic!("expected Multi, got {picker:?}");
        };
        assert_eq!(dimensions.len(), 2);
        assert_eq!(dimensions[0].name, "color");
        assert_eq!(dimensions[0].keys, vec!["r", "g"]);
        assert_eq!(dimensions[1].name, "size");
        assert_eq!(dimensions[1].keys, vec!["s", "m"]);
    }

    #[test]
    fn picker_multi_intersects_per_dimension_across_assets() {
        // Two Multi-partitioned assets in the same job — per-dim
        // intersection follows the first asset's order.
        let infos = make_map(vec![
            make_multi("a", &[("color", &["r", "g", "b"]), ("size", &["s", "m"])]),
            make_multi("b", &[("color", &["g", "b", "y"]), ("size", &["m", "l"])]),
        ]);
        let picker = picker(&["a", "b"], &infos);
        let JobPartitionPicker::Multi { dimensions, .. } = picker else {
            panic!("expected Multi");
        };
        assert_eq!(dimensions[0].keys, vec!["g", "b"]);
        assert_eq!(dimensions[1].keys, vec!["m"]);
    }

    #[test]
    fn picker_multi_collapses_to_none_when_every_dim_disjoint() {
        // Pathological — resolve-time check should reject this, but pin
        // the helper's behaviour.
        let infos = make_map(vec![
            make_multi("a", &[("color", &["r"]), ("size", &["s"])]),
            make_multi("b", &[("color", &["g"]), ("size", &["m"])]),
        ]);
        assert_eq!(picker(&["a", "b"], &infos), JobPartitionPicker::None);
    }

    /// A single-dim asset with more partitions than fit in the key window — the
    /// `keys` field is a truncated window, `total_count` the true size.
    fn make_paged(asset_key: &str, window: &[&str], total: u64) -> AssetDefinitionInfo {
        let mut info = make_info(asset_key, Some(window));
        if let Some(pd) = info.partition_def.as_mut() {
            pd.total_count = total;
            pd.keys_truncated = true;
        }
        info
    }

    #[test]
    fn picker_single_large_asset_pages() {
        let infos = make_map(vec![make_paged("big", &["k0", "k1", "k2"], 10_000)]);
        assert_eq!(
            picker(&["big"], &infos),
            JobPartitionPicker::SingleDimPaged {
                asset_key: "big".into(),
                total: 10_000,
            }
        );
    }

    #[test]
    fn picker_identical_large_assets_page() {
        // Same key space (same total + same window) → safe to page one asset.
        let infos = make_map(vec![
            make_paged("a", &["k0", "k1", "k2"], 10_000),
            make_paged("b", &["k0", "k1", "k2"], 10_000),
        ]);
        assert_eq!(
            picker(&["a", "b"], &infos),
            JobPartitionPicker::SingleDimPaged {
                asset_key: "a".into(),
                total: 10_000,
            }
        );
    }

    #[test]
    fn picker_divergent_large_assets_do_not_page() {
        // Different key spaces must NOT page one asset's keys (could offer a key
        // invalid for the other). Fall back to intersecting the visible windows
        // — and say so: shared keys beyond the windows exist but can't be shown.
        let infos = make_map(vec![
            make_paged("a", &["k0", "k1", "k2"], 10_000),
            make_paged("b", &["k1", "k2", "k3"], 12_000),
        ]);
        assert_eq!(
            picker(&["a", "b"], &infos),
            JobPartitionPicker::SingleDim {
                keys: vec!["k1".into(), "k2".into()],
                truncated: true,
            }
        );
    }

    #[test]
    fn picker_mismatched_multi_dims_returns_none() {
        // a has {region,date}, b has {region} only: a key with `date` is
        // invalid for b, a key without it is invalid for a — no shared key
        // exists, so the picker must offer nothing instead of a's full
        // `date` keyset.
        let infos = make_map(vec![
            make_multi("a", &[("region", &["us", "eu"]), ("date", &["d1", "d2"])]),
            make_multi("b", &[("region", &["us", "eu"])]),
        ]);
        assert_eq!(picker(&["a", "b"], &infos), JobPartitionPicker::None);
    }

    use crate::types::SubmitPartitionKey;

    fn multi(dims: &[(&str, &str)]) -> SubmitPartitionKey {
        SubmitPartitionKey::Multi(
            dims.iter()
                .map(|(d, v)| (d.to_string(), v.to_string()))
                .collect(),
        )
    }

    #[test]
    fn cartesian_single_dim_single_value() {
        let out = cartesian_partition_keys(&[("color".into(), vec!["r".into()])]);
        assert_eq!(out, vec![multi(&[("color", "r")])]);
    }

    #[test]
    fn cartesian_two_dims_two_values_each_produces_four_combinations() {
        let out = cartesian_partition_keys(&[
            ("color".into(), vec!["r".into(), "g".into()]),
            ("size".into(), vec!["s".into(), "m".into()]),
        ]);
        // Dims are sorted alphabetically; values iterate in order.
        assert_eq!(
            out,
            vec![
                multi(&[("color", "r"), ("size", "s")]),
                multi(&[("color", "r"), ("size", "m")]),
                multi(&[("color", "g"), ("size", "s")]),
                multi(&[("color", "g"), ("size", "m")]),
            ]
        );
    }

    #[test]
    fn cartesian_empty_when_any_dim_has_no_selection() {
        // User must pick at least one value per dimension; the helper
        // returns nothing if any dim is empty.
        let out = cartesian_partition_keys(&[
            ("color".into(), vec!["r".into()]),
            ("size".into(), vec![]),
        ]);
        assert!(out.is_empty());
    }

    #[test]
    fn cartesian_empty_when_no_dims_supplied() {
        assert!(cartesian_partition_keys(&[]).is_empty());
    }
}
