use croner::Cron;
use jiff::{SignedDuration, civil};
use pyo3::prelude::*;

use crate::errors::PartitionDefinitionError;
use rivers_core::util::parse_key_datetime;

pub(super) fn parse_cron(expr: &str) -> PyResult<Cron> {
    rivers_core::timegrid::parse_cron(expr)
        .map_err(|e| PartitionDefinitionError::new_err(e.to_string()))
}

/// A TimeWindow fmt must round-trip the grid's window starts.
pub(super) fn validate_time_window_fmt(
    cron_schedule: &Option<String>,
    interval_seconds: &Option<f64>,
    start: &civil::DateTime,
    end: &Option<civil::DateTime>,
    fmt: &str,
) -> PyResult<()> {
    if cron_schedule.is_some() {
        if start.subsec_nanosecond() != 0 {
            return Err(PartitionDefinitionError::new_err(format!(
                "cron-gridded time windows require a start on a whole second, got {start}"
            )));
        }
    }
    const MAX_TICKS: usize = 1024;
    let horizon = start
        .checked_add(SignedDuration::from_hours(35064))
        .unwrap_or(civil::DateTime::MAX);
    let bound = match end {
        Some(e) => (*e).min(horizon),
        None => horizon,
    };
    fn round_trip_tick(t: civil::DateTime, fmt: &str, failure: &mut Option<PyErr>) -> bool {
        let key = t.strftime(fmt).to_string();
        if let Some(ch) = rivers_core::storage::PartitionKey::reserved_display_char(&key) {
            *failure = Some(PartitionDefinitionError::new_err(format!(
                "fmt '{fmt}' produces keys containing reserved character '{ch}' \
                 (used by the canonical display form): '{key}'"
            )));
            return false;
        }
        match parse_key_datetime(&key, fmt) {
            Ok(parsed) if parsed == t => true,
            Ok(parsed) => {
                *failure = Some(PartitionDefinitionError::new_err(format!(
                    "fmt '{fmt}' cannot represent the partition grid: window start {t} \
                     formats to '{key}', which parses back to {parsed}; \
                     use a format at least as fine as the grid"
                )));
                false
            }
            Err(e) => {
                *failure = Some(PartitionDefinitionError::new_err(format!(
                    "fmt '{fmt}' cannot represent the partition grid: window start {t} \
                     formats to '{key}', which does not parse back: {e}"
                )));
                false
            }
        }
    }
    let mut checked = 0usize;
    let mut failure: Option<PyErr> = None;
    if let Some(secs) = interval_seconds {
        for_each_interval_tick(*secs, start, bound, &mut |t| {
            checked += 1;
            round_trip_tick(t, fmt, &mut failure) && checked < MAX_TICKS
        });
    } else if let Some(expr) = cron_schedule {
        for_each_cron_tick(expr, start, bound, &mut |t| {
            checked += 1;
            round_trip_tick(t, fmt, &mut failure) && checked < MAX_TICKS
        })?;
    }
    if failure.is_none() && checked < 2 {
        let true_end = match end {
            Some(e) => *e,
            None => civil::DateTime::MAX,
        };
        let mut taken = 0usize;
        if let Some(secs) = interval_seconds {
            for_each_interval_tick(*secs, start, true_end, &mut |t| {
                taken += 1;
                round_trip_tick(t, fmt, &mut failure) && taken < 2
            });
        } else if let Some(expr) = cron_schedule {
            for_each_cron_tick(expr, start, true_end, &mut |t| {
                taken += 1;
                round_trip_tick(t, fmt, &mut failure) && taken < 2
            })?;
        }
    }
    match failure {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

/// Check if all key strings fall on valid time window boundaries within [start, end).
pub(super) fn validate_time_window_key(
    key: &[String],
    cron_schedule: &Option<String>,
    interval_seconds: &Option<f64>,
    start: &civil::DateTime,
    end: &Option<civil::DateTime>,
    fmt: &str,
) -> PyResult<bool> {
    let now = jiff::Zoned::now().datetime();
    let end_dt = end.unwrap_or(now);
    if key.is_empty() {
        return Ok(false);
    }
    for k in key {
        let dt = match parse_key_datetime(k, fmt) {
            Ok(dt) => dt,
            Err(_) => return Ok(false),
        };
        if dt < *start || dt >= end_dt {
            return Ok(false);
        }
        if let Some(secs) = interval_seconds {
            let from_start = dt.duration_since(*start);
            let interval_ns = (*secs * 1_000_000_000.0) as i64;
            if interval_ns <= 0 {
                return Ok(false);
            }
            if i64::try_from(from_start.as_nanos()).is_ok_and(|n| n % interval_ns != 0) {
                return Ok(false);
            }
        } else if let Some(expr) = cron_schedule {
            let window_start = (dt - SignedDuration::from_hours(26)).max(*start);
            let window_end = (dt + SignedDuration::from_hours(26)).min(end_dt);
            let mut found = false;
            for_each_cron_tick(expr, &window_start, window_end, |naive| {
                if naive.strftime(fmt).to_string() == *k {
                    found = true;
                    return false;
                }
                true
            })?;
            if !found {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Enumerate all time window partition keys in `[start, end)` (or now).
pub(super) fn enumerate_time_windows(
    cron_schedule: &Option<String>,
    interval_seconds: &Option<f64>,
    start: &civil::DateTime,
    end: &Option<civil::DateTime>,
    fmt: &str,
) -> PyResult<Vec<String>> {
    let end_dt = time_window_end(end);
    let mut keys = Vec::new();
    if let Some(secs) = interval_seconds {
        for_each_interval_tick(*secs, start, end_dt, |dt| {
            keys.push(dt.strftime(fmt).to_string());
            true
        });
    } else if let Some(expr) = cron_schedule {
        for_each_cron_tick(expr, start, end_dt, |naive| {
            keys.push(naive.strftime(fmt).to_string());
            true
        })?;
    } else {
        return Err(PartitionDefinitionError::new_err(
            "TimeWindow requires either cron_schedule or interval_seconds",
        ));
    }
    Ok(keys)
}

/// Effective end bound for a TimeWindow: the explicit `end`, else now.
pub(super) fn time_window_end(end: &Option<civil::DateTime>) -> civil::DateTime {
    end.unwrap_or_else(|| jiff::Zoned::now().datetime())
}

/// Count of interval windows in `[start, end)`.
pub(super) fn interval_window_count(
    secs: f64,
    start: &civil::DateTime,
    end_dt: civil::DateTime,
) -> usize {
    if end_dt <= *start {
        return 0;
    }
    let interval_ns = (secs * 1_000_000_000.0) as i128;
    if interval_ns <= 0 {
        return 0;
    }
    let span_ns = end_dt.duration_since(*start).as_nanos();
    let count = ((span_ns - 1) / interval_ns) + 1;
    count.clamp(0, usize::MAX as i128) as usize
}

/// Up to `limit` interval keys from index `offset`, via arithmetic seek.
pub(super) fn interval_window(
    secs: f64,
    start: &civil::DateTime,
    end_dt: civil::DateTime,
    fmt: &str,
    offset: usize,
    limit: usize,
) -> Vec<String> {
    let interval_ns = (secs * 1_000_000_000.0) as i64;
    if interval_ns <= 0 || limit == 0 {
        return Vec::new();
    }
    let step = SignedDuration::from_nanos(interval_ns);
    let Some(off_ns) = (offset as i64).checked_mul(interval_ns) else {
        return Vec::new();
    };
    let Ok(mut current) = start.checked_add(SignedDuration::from_nanos(off_ns)) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(limit.min(1024));
    while current < end_dt && out.len() < limit {
        out.push(current.strftime(fmt).to_string());
        current = match current.checked_add(step) {
            Ok(c) => c,
            Err(_) => break,
        };
    }
    out
}

/// Index of `key` if it's an aligned interval window in `[start, end)`, else `None`.
pub(super) fn interval_index(
    secs: f64,
    start: &civil::DateTime,
    end_dt: civil::DateTime,
    fmt: &str,
    key: &str,
) -> Option<usize> {
    let interval_ns = (secs * 1_000_000_000.0) as i64;
    if interval_ns <= 0 {
        return None;
    }
    let dt = parse_key_datetime(key, fmt).ok()?;
    if dt < *start || dt >= end_dt {
        return None;
    }
    let delta = i64::try_from(dt.duration_since(*start).as_nanos()).ok()?;
    if delta % interval_ns != 0 {
        return None;
    }
    Some((delta / interval_ns) as usize)
}

/// Walk interval windows in `[start, end)` lazily; `f` returns false to stop.
pub(crate) fn for_each_interval_tick(
    secs: f64,
    start: &civil::DateTime,
    end_dt: civil::DateTime,
    mut f: impl FnMut(civil::DateTime) -> bool,
) {
    let interval_ns = (secs * 1_000_000_000.0) as i64;
    if interval_ns <= 0 {
        return;
    }
    let step = SignedDuration::from_nanos(interval_ns);
    let mut current = *start;
    while current < end_dt {
        if !f(current) {
            break;
        }
        current = match current.checked_add(step) {
            Ok(c) => c,
            Err(_) => break,
        };
    }
}

/// Walk cron occurrences in `[start, end)` lazily; `f` returns false to stop.
pub(crate) fn for_each_cron_tick(
    cron_expr: &str,
    start: &civil::DateTime,
    end_dt: civil::DateTime,
    mut f: impl FnMut(civil::DateTime) -> bool,
) -> PyResult<()> {
    let cron = parse_cron(cron_expr)?;
    for tick in cron.iter_from(*start, croner::Direction::Forward) {
        if tick >= end_dt {
            break;
        }
        if !f(tick) {
            break;
        }
    }
    Ok(())
}

/// Whether `t` falls exactly on the cron grid (cron is second-granular).
pub(crate) fn cron_grid_contains(cron_expr: &str, t: civil::DateTime) -> PyResult<bool> {
    if t.subsec_nanosecond() != 0 {
        return Ok(false);
    }
    let probe_end = t
        .checked_add(SignedDuration::from_secs(1))
        .unwrap_or(civil::DateTime::MAX);
    let mut hit = false;
    for_each_cron_tick(cron_expr, &t, probe_end, |tick| {
        hit = tick == t;
        false
    })?;
    Ok(hit)
}
