//! Wall-clock time-window grid — the shared timeline convention for
//! time-partition keys: a cron or fixed-interval grid over the naive
//! timeline interpreted as UTC. No DST gaps or duplicates, identical on
//! every host timezone, and croner cannot stall on it (its `Local`
//! iteration spins re-resolving the DST fall-back hour).

use anyhow::{Result, bail};
use chrono::{Local, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

/// The time parameters of a TimeWindow partition definition, detached from
/// any language binding so both the PyO3 layer and core condition eval can
/// shift keys identically.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TimeGrid {
    pub cron_schedule: Option<String>,
    pub interval_seconds: Option<f64>,
    pub start: NaiveDateTime,
    pub end: Option<NaiveDateTime>,
    pub fmt: String,
}

impl TimeGrid {
    /// Effective end bound: the explicit `end`, else now.
    fn end_bound(&self) -> NaiveDateTime {
        self.end.unwrap_or_else(|| Local::now().naive_local())
    }

    /// Shift `key` by `offset` windows (negative = earlier). Interval grids
    /// shift by exact nanosecond arithmetic; cron grids walk the tick grid.
    /// Errors when the shifted window falls outside `[start, end)`.
    pub fn shift_key(&self, key: &str, offset: i64) -> Result<String> {
        let dt = crate::util::parse_key_datetime(key, &self.fmt)
            .map_err(|e| anyhow::anyhow!("Invalid time window key '{key}': {e}"))?;
        if offset == 0 {
            return Ok(key.to_string());
        }
        let end_dt = self.end_bound();
        let out_of_range = || {
            anyhow::anyhow!(
                "time_window(offset={offset}) maps '{key}' outside the partition range \
                 [{}, {end_dt})",
                self.start
            )
        };
        let shifted = if let Some(secs) = self.interval_seconds {
            let interval_ns = (secs * 1_000_000_000.0) as i64;
            let total = interval_ns.checked_mul(offset).ok_or_else(|| {
                anyhow::anyhow!("time_window offset {offset} overflows for interval {secs}s")
            })?;
            dt.checked_add_signed(chrono::Duration::nanoseconds(total))
                .ok_or_else(out_of_range)?
        } else if let Some(expr) = &self.cron_schedule {
            let cron = parse_cron(expr)?;
            let anchor = Utc.from_utc_datetime(&dt);
            let direction = if offset > 0 {
                croner::Direction::Forward
            } else {
                croner::Direction::Backward
            };
            let mut remaining = offset.unsigned_abs();
            let mut shifted = None;
            for tick in cron.iter_from(anchor, direction) {
                if tick == anchor {
                    continue;
                }
                remaining -= 1;
                if remaining == 0 {
                    shifted = Some(tick.naive_utc());
                    break;
                }
            }
            // The iterator ends at croner's internal year limits — that far
            // out is by definition outside the partition range.
            shifted.ok_or_else(out_of_range)?
        } else {
            bail!("TimeWindow requires either cron_schedule or interval_seconds");
        };
        if shifted < self.start || shifted >= end_dt {
            return Err(out_of_range());
        }
        Ok(shifted.format(&self.fmt).to_string())
    }

    /// Enumerate the grid keys in `[from, to]` (inclusive) without touching
    /// the rest of the universe — O(range), not O(universe). Callers pass
    /// endpoints already validated as on-grid keys, so every tick between
    /// them is inside `[start, end)` by construction.
    pub fn keys_in_range(&self, from: NaiveDateTime, to: NaiveDateTime) -> Result<Vec<String>> {
        let mut out = Vec::new();
        if to < from {
            return Ok(out);
        }
        if let Some(secs) = self.interval_seconds {
            let interval_ns = (secs * 1_000_000_000.0) as i64;
            if interval_ns <= 0 {
                bail!("TimeWindow interval must be positive");
            }
            let step = chrono::Duration::nanoseconds(interval_ns);
            let mut t = from;
            while t <= to {
                out.push(t.format(&self.fmt).to_string());
                match t.checked_add_signed(step) {
                    Some(next) => t = next,
                    None => break,
                }
            }
        } else if let Some(expr) = &self.cron_schedule {
            let cron = parse_cron(expr)?;
            // Anchor one second below `from` (cron's resolution) so an
            // on-tick `from` is included — a sub-second anchor makes croner
            // carry the fraction into every yielded tick.
            let anchor = from
                .checked_sub_signed(chrono::Duration::seconds(1))
                .unwrap_or(from);
            for tick in cron.iter_from(Utc.from_utc_datetime(&anchor), croner::Direction::Forward) {
                let t = cron_tick_naive(tick);
                if t < from {
                    continue;
                }
                if t > to {
                    break;
                }
                out.push(t.format(&self.fmt).to_string());
            }
        } else {
            bail!("TimeWindow requires either cron_schedule or interval_seconds");
        }
        Ok(out)
    }

    /// Nearest valid keys around `dt` — the latest grid tick `<= dt` and the
    /// earliest tick `> dt`, both clamped to `[start, end)`. Best-effort for
    /// diagnostics: a side with no representable tick is `None`.
    pub fn nearest_keys(&self, dt: NaiveDateTime) -> (Option<String>, Option<String>) {
        let end_dt = self.end_bound();
        let key = |t: NaiveDateTime| t.format(&self.fmt).to_string();
        if let Some(secs) = self.interval_seconds {
            let interval_ns = (secs * 1_000_000_000.0) as i64;
            if interval_ns <= 0 {
                return (None, None);
            }
            let tick = |idx: i64| {
                idx.checked_mul(interval_ns).and_then(|ns| {
                    self.start
                        .checked_add_signed(chrono::Duration::nanoseconds(ns))
                })
            };
            let prev = end_dt
                .checked_sub_signed(chrono::Duration::nanoseconds(1))
                .map(|last| std::cmp::min(dt, last))
                .filter(|cap| *cap >= self.start)
                .and_then(|cap| (cap - self.start).num_nanoseconds())
                .map(|ns| ns.div_euclid(interval_ns))
                .and_then(tick);
            let next = if dt < self.start {
                Some(self.start)
            } else {
                (dt - self.start)
                    .num_nanoseconds()
                    .and_then(|ns| ns.div_euclid(interval_ns).checked_add(1))
                    .and_then(tick)
            }
            .filter(|t| *t < end_dt);
            (prev.map(key), next.map(key))
        } else if let Some(expr) = &self.cron_schedule {
            let Ok(cron) = parse_cron(expr) else {
                return (None, None);
            };
            // Anchors are clamped to the range so the walk finds its tick
            // within a step or two instead of crossing dt→range tick by tick.
            let prev = {
                let anchor = Utc.from_utc_datetime(&std::cmp::min(dt, end_dt));
                cron.iter_from(anchor, croner::Direction::Backward)
                    .map(cron_tick_naive)
                    .find(|t| *t <= dt && *t < end_dt)
                    .filter(|t| *t >= self.start)
            };
            let next = {
                let anchor = Utc.from_utc_datetime(&std::cmp::max(dt, self.start));
                cron.iter_from(anchor, croner::Direction::Forward)
                    .map(cron_tick_naive)
                    .find(|t| *t > dt)
                    .filter(|t| *t < end_dt)
            };
            (prev.map(key), next.map(key))
        } else {
            (None, None)
        }
    }
}

/// croner carries a sub-second anchor fraction into every yielded tick;
/// cron schedules are second-granular, so normalize to whole seconds.
fn cron_tick_naive(tick: chrono::DateTime<Utc>) -> NaiveDateTime {
    use chrono::Timelike;
    let naive = tick.naive_utc();
    naive.with_nanosecond(0).unwrap_or(naive)
}

fn parse_cron(expr: &str) -> Result<croner::Cron> {
    croner::parser::CronParser::builder()
        .seconds(croner::parser::Seconds::Optional)
        .build()
        .parse(expr)
        .map_err(|e| anyhow::anyhow!("Invalid cron expression '{expr}': {e}"))
}

#[cfg(test)]
mod tests {
    use super::TimeGrid;
    use chrono::NaiveDate;

    fn daily_jan() -> TimeGrid {
        TimeGrid {
            cron_schedule: Some("0 0 * * *".into()),
            interval_seconds: None,
            start: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap().into(),
            end: Some(NaiveDate::from_ymd_opt(2024, 2, 1).unwrap().into()),
            fmt: "%Y-%m-%d".into(),
        }
    }

    #[test]
    fn cron_shift_backward_and_forward() {
        let g = daily_jan();
        assert_eq!(g.shift_key("2024-01-05", -1).unwrap(), "2024-01-04");
        assert_eq!(g.shift_key("2024-01-05", 1).unwrap(), "2024-01-06");
        assert_eq!(g.shift_key("2024-01-05", 0).unwrap(), "2024-01-05");
    }

    #[test]
    fn cron_shift_outside_range_errors() {
        let g = daily_jan();
        assert!(g.shift_key("2024-01-01", -1).is_err());
        assert!(g.shift_key("2024-01-31", 1).is_err());
    }

    #[test]
    fn interval_shift_near_datetime_bounds_errors_instead_of_panicking() {
        let g = TimeGrid {
            cron_schedule: None,
            interval_seconds: Some(3600.0),
            start: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap().into(),
            end: None,
            fmt: "%Y-%m-%dT%H:%M:%S".into(),
        };
        // Parses to chrono's minimum date; one window earlier is unrepresentable.
        assert!(g.shift_key("-262143-01-01T00:00:00", -1).is_err());
    }

    #[test]
    fn cron_walk_exhaustion_reports_the_partition_range() {
        let g = daily_jan();
        let err = g.shift_key("2024-01-05", -10_000_000).unwrap_err();
        assert!(
            err.to_string().contains("outside the partition range"),
            "got: {err}"
        );
    }

    #[test]
    fn interval_shift() {
        let g = TimeGrid {
            cron_schedule: None,
            interval_seconds: Some(21600.0),
            start: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap().into(),
            end: Some(NaiveDate::from_ymd_opt(2024, 1, 3).unwrap().into()),
            fmt: "%Y-%m-%dT%H:%M:%S".into(),
        };
        assert_eq!(
            g.shift_key("2024-01-01T12:00:00", -1).unwrap(),
            "2024-01-01T06:00:00"
        );
    }

    fn hourly_jan1() -> TimeGrid {
        TimeGrid {
            cron_schedule: None,
            interval_seconds: Some(3600.0),
            start: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap().into(),
            end: Some(NaiveDate::from_ymd_opt(2024, 1, 2).unwrap().into()),
            fmt: "%Y-%m-%dT%H:%M:%S".into(),
        }
    }

    fn dt(s: &str) -> chrono::NaiveDateTime {
        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").unwrap()
    }

    #[test]
    fn interval_keys_in_range_walks_only_the_window() {
        let keys = hourly_jan1()
            .keys_in_range(dt("2024-01-01T07:00:00"), dt("2024-01-01T09:00:00"))
            .unwrap();
        assert_eq!(
            keys,
            vec![
                "2024-01-01T07:00:00",
                "2024-01-01T08:00:00",
                "2024-01-01T09:00:00",
            ]
        );
    }

    #[test]
    fn cron_keys_in_range_includes_on_tick_endpoints() {
        let g = daily_jan();
        let keys = g
            .keys_in_range(dt("2024-01-05T00:00:00"), dt("2024-01-07T00:00:00"))
            .unwrap();
        assert_eq!(keys, vec!["2024-01-05", "2024-01-06", "2024-01-07"]);
    }

    #[test]
    fn keys_in_range_inverted_is_empty() {
        let g = hourly_jan1();
        let keys = g
            .keys_in_range(dt("2024-01-01T09:00:00"), dt("2024-01-01T07:00:00"))
            .unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn interval_nearest_keys_brackets_off_grid_datetime() {
        let (prev, next) = hourly_jan1().nearest_keys(dt("2024-01-01T07:30:00"));
        assert_eq!(prev.as_deref(), Some("2024-01-01T07:00:00"));
        assert_eq!(next.as_deref(), Some("2024-01-01T08:00:00"));
    }

    #[test]
    fn interval_nearest_keys_clamps_to_range() {
        let g = hourly_jan1();
        // Before start: nothing earlier; the first key is the suggestion.
        let (prev, next) = g.nearest_keys(dt("2023-12-30T05:00:00"));
        assert_eq!(prev, None);
        assert_eq!(next.as_deref(), Some("2024-01-01T00:00:00"));
        // At/after the exclusive end: only the last key survives.
        let (prev, next) = g.nearest_keys(dt("2024-01-02T05:00:00"));
        assert_eq!(prev.as_deref(), Some("2024-01-01T23:00:00"));
        assert_eq!(next, None);
    }

    #[test]
    fn cron_nearest_keys_brackets_off_grid_datetime() {
        let g = daily_jan();
        let (prev, next) = g.nearest_keys(dt("2024-01-05T13:30:00"));
        assert_eq!(prev.as_deref(), Some("2024-01-05"));
        assert_eq!(next.as_deref(), Some("2024-01-06"));
    }

    #[test]
    fn cron_nearest_keys_clamps_to_range() {
        let g = daily_jan();
        let (prev, next) = g.nearest_keys(dt("2023-11-20T00:00:00"));
        assert_eq!(prev, None);
        assert_eq!(next.as_deref(), Some("2024-01-01"));
        let (prev, next) = g.nearest_keys(dt("2024-03-15T00:00:00"));
        assert_eq!(prev.as_deref(), Some("2024-01-31"));
        assert_eq!(next, None);
    }
}
