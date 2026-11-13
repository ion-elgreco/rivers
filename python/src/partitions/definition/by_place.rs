//! The keys of a definition with one dimension, each found by its place.

use jiff::civil;
use pyo3::prelude::*;
use rivers_core::execution::partition_outline::{Keys, cuts, time_cuts};
use rivers_core::timegrid::{DayGrid, Found};
use rivers_core::util::parse_key_datetime;

use super::time_window::{
    enumerate_time_windows, for_each_interval_tick, interval_index, interval_window,
    interval_window_count,
};
use super::{OrderedKeySet, PartitionsDefinition};
use crate::errors::PartitionDefinitionError;

/// The keys of a definition with one dimension, in the order of the
/// definition. A key is found by its place, with no walk from the first key.
pub(crate) enum KeysByPlace<'a> {
    Static(&'a OrderedKeySet),
    /// A cron grid: each of its days has ticks at the same times.
    Days {
        grid: DayGrid,
        fmt: &'a str,
    },
    /// A grid with ticks a fixed time apart.
    Interval {
        secs: f64,
        start: civil::DateTime,
        end: civil::DateTime,
        fmt: &'a str,
    },
    /// The keys of a cron grid that is not read as days and times, listed by
    /// a walk.
    Listed {
        keys: Vec<String>,
        fmt: &'a str,
    },
}

/// The ticks of a time window grid before `end` as days and times of day.
pub(super) fn day_grid(
    cron_schedule: &Option<String>,
    interval_seconds: &Option<f64>,
    start: &civil::DateTime,
    end: civil::DateTime,
) -> PyResult<Option<DayGrid>> {
    DayGrid::of(cron_schedule.as_deref(), *interval_seconds, *start, end)
        .map_err(|e| PartitionDefinitionError::new_err(e.to_string()))
}

impl<'a> KeysByPlace<'a> {
    /// `None` for a definition with more dimensions, and for a dynamic set.
    pub(super) fn of(def: &'a PartitionsDefinition) -> PyResult<Option<Self>> {
        let PartitionsDefinition::TimeWindow {
            cron_schedule,
            interval_seconds,
            start,
            end,
            fmt,
        } = def
        else {
            return Ok(def.static_keys().map(Self::Static));
        };
        let end = super::time_window_end(end);

        // An interval gives a key by its place with no table of its ticks.
        if let Some(secs) = interval_seconds {
            return Ok(Some(Self::Interval {
                secs: *secs,
                start: *start,
                end,
                fmt,
            }));
        }
        Ok(Some(
            match day_grid(cron_schedule, interval_seconds, start, end)? {
                Some(grid) => Self::Days { grid, fmt },
                None => Self::Listed {
                    keys: enumerate_time_windows(
                        cron_schedule,
                        interval_seconds,
                        start,
                        &Some(end),
                        fmt,
                    )?,
                    fmt,
                },
            },
        ))
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Static(keys) => keys.len(),
            Self::Days { grid, .. } => grid.len(),
            Self::Interval {
                secs, start, end, ..
            } => interval_window_count(*secs, start, *end),
            Self::Listed { keys, .. } => keys.len(),
        }
    }

    /// The key format of a time grid.
    pub(crate) fn fmt(&self) -> Option<&'a str> {
        match self {
            Self::Static(_) => None,
            Self::Days { fmt, .. } | Self::Interval { fmt, .. } | Self::Listed { fmt, .. } => {
                Some(*fmt)
            }
        }
    }

    /// At most `limit` keys from the place `offset`.
    pub(crate) fn window(&self, offset: usize, limit: usize) -> Vec<String> {
        match self {
            Self::Static(keys) => keys.iter().skip(offset).take(limit).cloned().collect(),
            Self::Days { grid, fmt } => (offset..grid.len().min(offset.saturating_add(limit)))
                .map(|index| grid.key(index, fmt))
                .collect(),
            Self::Interval {
                secs,
                start,
                end,
                fmt,
            } => interval_window(*secs, start, *end, fmt, offset, limit),
            Self::Listed { keys, .. } => keys.iter().skip(offset).take(limit).cloned().collect(),
        }
    }

    /// The key at the place `index`.
    pub(crate) fn get(&self, index: usize) -> Option<String> {
        self.window(index, 1).pop()
    }

    /// The place of `key`.
    pub(crate) fn place(&self, key: &str) -> Option<usize> {
        match self {
            Self::Static(keys) => keys.get_index_of(key),
            Self::Days { grid, fmt } => parse_key_datetime(key, fmt)
                .ok()
                .and_then(|time| grid.place(time))
                .filter(|place| grid.key(*place, fmt) == key),
            Self::Interval {
                secs,
                start,
                end,
                fmt,
            } => interval_index(*secs, start, *end, fmt, key),
            Self::Listed { keys, .. } => keys.iter().position(|listed| listed == key),
        }
    }

    /// `limit` keys from the match number `offset` of the keys that hold
    /// `text`, each with its place, and the number of all matches.
    pub(crate) fn find(&self, text: &str, offset: usize, limit: usize) -> PyResult<Found> {
        // The search of a grid as days and times reads no key.
        let by_days = match self {
            Self::Days { grid, fmt } => grid.find(fmt, text, offset, limit),
            Self::Interval {
                secs,
                start,
                end,
                fmt,
            } => day_grid(&None, &Some(*secs), start, *end)?
                .and_then(|grid| grid.find(fmt, text, offset, limit)),
            Self::Static(_) | Self::Listed { .. } => None,
        };
        if let Some(found) = by_days {
            return Ok(found);
        }

        let (mut total, mut page) = (0usize, Vec::new());
        let mut place = 0usize;
        let mut read = |key: &str| {
            if key.contains(text) {
                if total >= offset && page.len() < limit {
                    page.push((place, key.to_string()));
                }
                total += 1;
            }
            place += 1;
        };

        match self {
            Self::Static(keys) => keys.iter().for_each(|key| read(key)),
            Self::Listed { keys, .. } => keys.iter().for_each(|key| read(key)),
            Self::Days { grid, fmt } => {
                (0..grid.len()).for_each(|index| read(&grid.key(index, fmt)))
            }
            Self::Interval {
                secs,
                start,
                end,
                fmt,
            } => for_each_interval_tick(*secs, start, *end, |tick| {
                read(&tick.strftime(fmt).to_string());
                true
            }),
        }
        Ok((page, total))
    }
}

impl Keys for KeysByPlace<'_> {
    fn len(&self) -> usize {
        KeysByPlace::len(self)
    }

    fn key(&self, index: usize) -> String {
        self.get(index).unwrap_or_default()
    }

    fn cuts(&self, key: &str) -> Vec<usize> {
        cuts(key, &self.fmt().map(time_cuts).unwrap_or_default())
    }
}
