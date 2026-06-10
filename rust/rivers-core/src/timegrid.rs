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
}
