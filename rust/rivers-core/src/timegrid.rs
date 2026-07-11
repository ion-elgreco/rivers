//! Wall-clock time-window grid for time-partition keys.

use anyhow::{Result, bail};
use chrono::{Local, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

/// The time parameters of a TimeWindow partition definition.
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

    /// Shift `key` by `offset` windows (negative = earlier).
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
            if interval_ns <= 0 {
                bail!("TimeWindow interval must be positive");
            }
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
            shifted.ok_or_else(out_of_range)?
        } else {
            bail!("TimeWindow requires either cron_schedule or interval_seconds");
        };
        if shifted < self.start || shifted >= end_dt {
            return Err(out_of_range());
        }
        Ok(shifted.format(&self.fmt).to_string())
    }

    /// Enumerate the grid keys in `[from, to]` (inclusive).
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
            // Grid ticks are start + n*step; snap `from` up to the next tick
            // so a raw endpoint can't shift the whole walk off-grid (the cron
            // branch snaps inherently).
            let mut t = if from <= self.start {
                self.start
            } else {
                let offset_ns = (from - self.start).num_nanoseconds().unwrap_or(0);
                let rem = offset_ns.rem_euclid(interval_ns);
                if rem == 0 {
                    from
                } else {
                    match self.start.checked_add_signed(chrono::Duration::nanoseconds(
                        offset_ns - rem + interval_ns,
                    )) {
                        Some(next) => next,
                        None => return Ok(out),
                    }
                }
            };
            while t <= to {
                out.push(t.format(&self.fmt).to_string());
                match t.checked_add_signed(step) {
                    Some(next) => t = next,
                    None => break,
                }
            }
        } else if let Some(expr) = &self.cron_schedule {
            let cron = parse_cron(expr)?;
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

    /// Window starts in the half-open `[from, to)` for arbitrary endpoints.
    pub fn window_starts_in(&self, from: NaiveDateTime, to: NaiveDateTime) -> Result<Vec<String>> {
        let from = from.max(self.start);
        let to = to.min(self.end_bound());
        let mut out = Vec::new();
        if to <= from {
            return Ok(out);
        }
        if let Some(secs) = self.interval_seconds {
            let interval_ns = (secs * 1_000_000_000.0) as i64;
            if interval_ns <= 0 {
                bail!("TimeWindow interval must be positive");
            }
            let Some(offset_ns) = (from - self.start).num_nanoseconds() else {
                return Ok(out);
            };
            let first_idx = offset_ns.div_euclid(interval_ns)
                + if offset_ns.rem_euclid(interval_ns) == 0 {
                    0
                } else {
                    1
                };
            let step = chrono::Duration::nanoseconds(interval_ns);
            let mut t = match first_idx.checked_mul(interval_ns).and_then(|ns| {
                self.start
                    .checked_add_signed(chrono::Duration::nanoseconds(ns))
            }) {
                Some(t) => t,
                None => return Ok(out),
            };
            while t < to {
                out.push(t.format(&self.fmt).to_string());
                match t.checked_add_signed(step) {
                    Some(next) => t = next,
                    None => break,
                }
            }
        } else if let Some(expr) = &self.cron_schedule {
            let cron = parse_cron(expr)?;
            let anchor = from
                .checked_sub_signed(chrono::Duration::seconds(1))
                .unwrap_or(from);
            for tick in cron.iter_from(Utc.from_utc_datetime(&anchor), croner::Direction::Forward) {
                let t = cron_tick_naive(tick);
                if t < from {
                    continue;
                }
                if t >= to {
                    break;
                }
                out.push(t.format(&self.fmt).to_string());
            }
        } else {
            bail!("TimeWindow requires either cron_schedule or interval_seconds");
        }
        Ok(out)
    }

    /// Nearest valid keys around `dt` — the latest tick `<= dt` and earliest `> dt`.
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

/// Normalize a cron tick to whole seconds (croner yields sub-second fractions).
fn cron_tick_naive(tick: chrono::DateTime<Utc>) -> NaiveDateTime {
    use chrono::Timelike;
    let naive = tick.naive_utc();
    naive.with_nanosecond(0).unwrap_or(naive)
}

/// Parse a cron expression in the ONE dialect rivers accepts (5 or 6 fields,
/// seconds optional). Every cron consumer must go through this so conditions,
/// schedules, and time-window grids can never diverge on what parses.
pub fn parse_cron(expr: &str) -> Result<croner::Cron> {
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
        assert!(g.shift_key("-262143-01-01T00:00:00", -1).is_err());
    }

    #[test]
    fn interval_shift_rejects_non_positive_interval() {
        let g = TimeGrid {
            cron_schedule: None,
            interval_seconds: Some(1e-12),
            start: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap().into(),
            end: Some(NaiveDate::from_ymd_opt(2024, 12, 31).unwrap().into()),
            fmt: "%Y-%m-%dT%H:%M:%S".into(),
        };
        assert!(g.shift_key("2024-06-01T00:00:00", 1).is_err());
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
    fn interval_window_starts_snap_off_grid_from_forward() {
        let g = hourly_jan1();
        let keys = g
            .window_starts_in(dt("2024-01-01T06:30:00"), dt("2024-01-01T09:00:00"))
            .unwrap();
        assert_eq!(keys, vec!["2024-01-01T07:00:00", "2024-01-01T08:00:00"]);
        let keys = g
            .window_starts_in(dt("2024-01-01T07:00:00"), dt("2024-01-01T08:00:00"))
            .unwrap();
        assert_eq!(keys, vec!["2024-01-01T07:00:00"]);
    }

    #[test]
    fn cron_window_starts_half_open_and_clamped() {
        let g = daily_jan();
        let keys = g
            .window_starts_in(dt("2024-01-05T12:00:00"), dt("2024-01-08T00:00:00"))
            .unwrap();
        assert_eq!(keys, vec!["2024-01-06", "2024-01-07"]);
        let keys = g
            .window_starts_in(dt("2024-01-31T00:00:01"), dt("2024-03-01T00:00:00"))
            .unwrap();
        assert!(keys.is_empty());
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
    fn interval_keys_in_range_snaps_off_grid_from_to_the_grid() {
        // A raw `from` between ticks must not shift the whole walk off-grid.
        let keys = hourly_jan1()
            .keys_in_range(dt("2024-01-01T07:30:00"), dt("2024-01-01T10:00:00"))
            .unwrap();
        assert_eq!(
            keys,
            vec![
                "2024-01-01T08:00:00",
                "2024-01-01T09:00:00",
                "2024-01-01T10:00:00",
            ]
        );
    }

    #[test]
    fn interval_keys_in_range_clamps_from_before_grid_start() {
        let keys = hourly_jan1()
            .keys_in_range(dt("2023-12-31T22:15:00"), dt("2024-01-01T01:00:00"))
            .unwrap();
        assert_eq!(keys, vec!["2024-01-01T00:00:00", "2024-01-01T01:00:00"]);
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
        let (prev, next) = g.nearest_keys(dt("2023-12-30T05:00:00"));
        assert_eq!(prev, None);
        assert_eq!(next.as_deref(), Some("2024-01-01T00:00:00"));
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
