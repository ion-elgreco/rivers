//! A change of an asset's own partitions definition: whether its keys
//! change, and which partitions of the new definition its stored partitions
//! fill.

use std::collections::{BTreeMap, HashMap, HashSet};

use jiff::{SignedDuration, civil};

use super::partitions_fingerprint::{PartitionsFingerprint, describe_partitions};
use crate::storage::PartitionKey;
use crate::timegrid::{TimeGrid, cron_ticks, parse_cron};
use crate::util::parse_key_datetime;

/// Whether the stored partitions of an asset, made under `before`, name the
/// same partitions under `after`. A change of static keys keeps the keys, as
/// does a time window that only moves its range.
pub fn same_keys(
    before: Option<&PartitionsFingerprint>,
    after: Option<&PartitionsFingerprint>,
) -> bool {
    match (before, after) {
        (None, None) => true,
        (Some(before), Some(after)) => same_dimension_keys(before, after),
        _ => false,
    }
}

fn same_dimension_keys(before: &PartitionsFingerprint, after: &PartitionsFingerprint) -> bool {
    use PartitionsFingerprint::*;

    match (before, after) {
        (Static { .. }, Static { .. }) => true,
        (TimeWindow(before), TimeWindow(after)) => before.aligns_with(after),
        (Dynamic { name: before }, Dynamic { name: after }) => before == after,
        (Multi { dimensions: before }, Multi { dimensions: after }) => {
            before.len() == after.len()
                && before
                    .iter()
                    .zip(after)
                    .all(|((b_name, b), (a_name, a))| b_name == a_name && same_dimension_keys(b, a))
        }
        _ => false,
    }
}

/// `"before: daily from 2024-01-01; now: hourly from 2024-01-01-00:00"`.
pub fn describe_change(
    before: Option<&PartitionsFingerprint>,
    after: Option<&PartitionsFingerprint>,
) -> String {
    format!(
        "before: {}; now: {}",
        describe_partitions(before),
        describe_partitions(after)
    )
}

/// The partitions of the new definition that the stored partitions fill.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fill {
    /// The asset has no partitions after the change: it is filled as a whole.
    Whole,
    /// These partitions of the new definition, in key order. None when no
    /// stored partition covers one.
    Keys(Vec<PartitionKey>),
}

impl Fill {
    /// How many partitions to fill; 1 for [`Fill::Whole`].
    pub fn count(&self) -> u64 {
        match self {
            Self::Whole => 1,
            Self::Keys(keys) => keys.len() as u64,
        }
    }
}

/// The partitions of `after` that `materialized`, the stored partitions made
/// under `before`, fill. A partition of a time window is filled when the
/// stored partitions cover all of its window. `static_keys` gives the keys
/// of a static dimension of `after` by its name: a stored value that it
/// lacks fills nothing. `Err` with the reason when rivers cannot compute the
/// partitions: the migration then names them.
pub fn partitions_to_fill(
    before: Option<&PartitionsFingerprint>,
    after: Option<&PartitionsFingerprint>,
    materialized: &[PartitionKey],
    static_keys: &HashMap<String, HashSet<String>>,
) -> Result<Fill, String> {
    use PartitionsFingerprint::*;

    let cannot = || {
        format!(
            "rivers cannot find the partitions to fill when the partitions change ({}); give \
             them with partitions=",
            describe_change(before, after)
        )
    };
    match (before, after) {
        (Some(_), None) => Ok(if materialized.is_empty() {
            Fill::Keys(Vec::new())
        } else {
            Fill::Whole
        }),
        (Some(TimeWindow(before)), Some(TimeWindow(after))) => {
            let mut starts = Vec::new();
            for key in materialized {
                for member in key.members() {
                    let PartitionKey::Single { keys } = &member else {
                        return Err(not_a_key(&member, before));
                    };
                    for value in keys {
                        starts.push(key_start(value, before)?);
                    }
                }
            }
            let keys = filled_windows(before, after, starts)?
                .into_iter()
                .map(|start| PartitionKey::Single {
                    keys: vec![format_key(start, after)],
                })
                .collect();
            Ok(Fill::Keys(keys))
        }
        (Some(Multi { dimensions: before }), Some(Multi { dimensions: after })) => {
            fill_multi(before, after, materialized, static_keys)?
                .map(Fill::Keys)
                .ok_or_else(cannot)
        }
        _ => Err(cannot()),
    }
}

/// [`partitions_to_fill`] of multi-partitions: `None` unless the dimensions
/// keep their names and exactly one of them, a time window, changes its
/// keys.
fn fill_multi(
    before: &[(String, PartitionsFingerprint)],
    after: &[(String, PartitionsFingerprint)],
    materialized: &[PartitionKey],
    static_keys: &HashMap<String, HashSet<String>>,
) -> Result<Option<Vec<PartitionKey>>, String> {
    if before.len() != after.len() || before.iter().zip(after).any(|(b, a)| b.0 != a.0) {
        return Ok(None);
    }
    let changed: Vec<usize> = (0..after.len())
        .filter(|&i| !same_dimension_keys(&before[i].1, &after[i].1))
        .collect();
    let &[i] = changed.as_slice() else {
        return Ok(None);
    };
    let (
        PartitionsFingerprint::TimeWindow(time_before),
        PartitionsFingerprint::TimeWindow(time_after),
    ) = (&before[i].1, &after[i].1)
    else {
        return Ok(None);
    };
    let time_dim = after[i].0.as_str();

    // The windows of the time dimension under each value of the others.
    let mut groups: BTreeMap<Vec<(String, String)>, Vec<civil::DateTime>> = BTreeMap::new();
    for key in materialized {
        'members: for member in key.members() {
            let PartitionKey::Multi { dims } = &member else {
                return Err(not_a_key(&member, time_before));
            };
            let mut others = Vec::with_capacity(dims.len());
            let mut start = None;
            for (name, values) in dims {
                let [value] = values.as_slice() else {
                    return Err(not_a_key(&member, time_before));
                };
                if name == time_dim {
                    start = Some(key_start(value, time_before)?);
                } else if static_keys
                    .get(name)
                    .is_some_and(|keys| !keys.contains(value))
                {
                    continue 'members;
                } else {
                    others.push((name.clone(), value.clone()));
                }
            }
            let Some(start) = start else {
                return Err(not_a_key(&member, time_before));
            };
            others.sort();
            groups.entry(others).or_default().push(start);
        }
    }

    let mut keys = Vec::new();
    for (others, starts) in groups {
        for start in filled_windows(time_before, time_after, starts)? {
            let mut dims: Vec<(String, Vec<String>)> = others
                .iter()
                .map(|(name, value)| (name.clone(), vec![value.clone()]))
                .collect();
            dims.push((time_dim.to_string(), vec![format_key(start, time_after)]));
            dims.sort();
            keys.push(PartitionKey::Multi { dims });
        }
    }
    Ok(Some(keys))
}

/// The starts of the windows of `after` that the windows of `before` at
/// `starts` cover together, in order.
fn filled_windows(
    before: &TimeGrid,
    after: &TimeGrid,
    mut starts: Vec<civil::DateTime>,
) -> Result<Vec<civil::DateTime>, String> {
    let before_ticks = Ticks::of(before)?;
    let after_ticks = Ticks::of(after)?;

    starts.sort_unstable();
    starts.dedup();
    let mut covered: Vec<(civil::DateTime, civil::DateTime)> = Vec::new();
    for start in starts {
        let Some(end) = before_ticks.after(start) else {
            continue;
        };
        match covered.last_mut() {
            Some(last) if start <= last.1 => last.1 = last.1.max(end),
            _ => covered.push((start, end)),
        }
    }

    let range_end = after.end_bound();
    let mut filled = Vec::new();
    for (from, to) in covered {
        let mut tick = after_ticks.at_or_after(from.max(after.start));
        while let Some(start) = tick.filter(|&start| start < to && start < range_end) {
            let Some(end) = after_ticks.after(start) else {
                break;
            };
            if end <= to {
                filled.push(start);
            }
            tick = Some(end);
        }
    }
    Ok(filled)
}

/// The ticks of a time grid, ready to walk.
enum Ticks {
    Every {
        start: civil::DateTime,
        step_ns: i64,
    },
    Cron(croner::Cron),
}

impl Ticks {
    fn of(grid: &TimeGrid) -> Result<Self, String> {
        if let Some(seconds) = grid.interval_seconds {
            let step_ns = (seconds * 1_000_000_000.0) as i64;
            if step_ns <= 0 {
                return Err("a time window interval must be positive".to_string());
            }
            return Ok(Self::Every {
                start: grid.start,
                step_ns,
            });
        }
        match &grid.cron_schedule {
            Some(expr) => parse_cron(expr).map(Self::Cron).map_err(|e| e.to_string()),
            None => Err("a time window needs a cron schedule or an interval".to_string()),
        }
    }

    /// The first tick at or after `t`.
    fn at_or_after(&self, t: civil::DateTime) -> Option<civil::DateTime> {
        match self {
            Self::Every { start, step_ns } => {
                if t <= *start {
                    return Some(*start);
                }
                let offset = t.duration_since(*start).as_nanos();
                let step = i128::from(*step_ns);
                let ns = i64::try_from((offset + step - 1).div_euclid(step) * step).ok()?;
                start.checked_add(SignedDuration::from_nanos(ns)).ok()
            }
            Self::Cron(cron) => cron_ticks(cron, t).next(),
        }
    }

    /// The tick after the tick `t`.
    fn after(&self, t: civil::DateTime) -> Option<civil::DateTime> {
        match self {
            Self::Every { step_ns, .. } => t.checked_add(SignedDuration::from_nanos(*step_ns)).ok(),
            Self::Cron(cron) => cron_ticks(cron, t).find(|&next| next > t),
        }
    }
}

fn key_start(value: &str, grid: &TimeGrid) -> Result<civil::DateTime, String> {
    parse_key_datetime(value, &grid.fmt).map_err(|e| {
        format!("the stored partition '{value}' is not a key of the partitions before: {e}")
    })
}

fn format_key(start: civil::DateTime, grid: &TimeGrid) -> String {
    start.strftime(&grid.fmt).to_string()
}

fn not_a_key(key: &PartitionKey, grid: &TimeGrid) -> String {
    format!(
        "the stored partition '{}' is not a key of the partitions before ({})",
        key.to_display(),
        describe_partitions(Some(&PartitionsFingerprint::TimeWindow(grid.clone())))
    )
}

#[cfg(test)]
mod tests {
    use jiff::civil::date;

    use super::*;

    fn daily() -> TimeGrid {
        TimeGrid {
            cron_schedule: Some("0 0 * * *".into()),
            interval_seconds: None,
            start: date(2024, 1, 1).at(0, 0, 0, 0),
            end: Some(date(2025, 1, 1).at(0, 0, 0, 0)),
            fmt: "%Y-%m-%d".into(),
        }
    }

    fn hourly() -> TimeGrid {
        TimeGrid {
            cron_schedule: Some("0 * * * *".into()),
            fmt: "%Y-%m-%d-%H:%M".into(),
            ..daily()
        }
    }

    fn every_6h(start_hour: i8) -> TimeGrid {
        TimeGrid {
            cron_schedule: None,
            interval_seconds: Some(21_600.0),
            start: date(2024, 1, 1).at(start_hour, 0, 0, 0),
            end: Some(date(2025, 1, 1).at(0, 0, 0, 0)),
            fmt: "%Y-%m-%dT%H:%M".into(),
        }
    }

    fn time(grid: TimeGrid) -> PartitionsFingerprint {
        PartitionsFingerprint::TimeWindow(grid)
    }

    fn statics(keys: &[&str]) -> PartitionsFingerprint {
        PartitionsFingerprint::static_keys(keys.iter().copied())
    }

    fn single(value: &str) -> PartitionKey {
        PartitionKey::Single {
            keys: vec![value.to_string()],
        }
    }

    fn multi(date: &str, region: &str) -> PartitionKey {
        PartitionKey::Multi {
            dims: vec![
                ("date".to_string(), vec![date.to_string()]),
                ("region".to_string(), vec![region.to_string()]),
            ],
        }
    }

    fn by_date_and_region(date: TimeGrid, regions: &[&str]) -> PartitionsFingerprint {
        PartitionsFingerprint::multi([
            ("date".to_string(), time(date)),
            ("region".to_string(), statics(regions)),
        ])
    }

    fn fill(
        before: &PartitionsFingerprint,
        after: &PartitionsFingerprint,
        materialized: &[PartitionKey],
    ) -> Fill {
        partitions_to_fill(Some(before), Some(after), materialized, &HashMap::new()).unwrap()
    }

    fn values(fill: &Fill) -> Vec<String> {
        let Fill::Keys(keys) = fill else {
            panic!("expected keys, got {fill:?}");
        };
        keys.iter().map(PartitionKey::to_display).collect()
    }

    #[test]
    fn keys_stay_the_same_when_only_the_range_or_the_static_keys_change() {
        let later_start = TimeGrid {
            start: date(2024, 6, 1).at(0, 0, 0, 0),
            end: None,
            ..daily()
        };
        let same = [
            (None, None),
            (Some(statics(&["a", "b"])), Some(statics(&["a", "c", "d"]))),
            (Some(time(daily())), Some(time(later_start))),
            (Some(time(every_6h(0))), Some(time(every_6h(0)))),
            (
                Some(PartitionsFingerprint::Dynamic {
                    name: "files".into(),
                }),
                Some(PartitionsFingerprint::Dynamic {
                    name: "files".into(),
                }),
            ),
            (
                Some(by_date_and_region(daily(), &["eu", "us"])),
                Some(by_date_and_region(daily(), &["eu"])),
            ),
        ];
        for (before, after) in same {
            assert!(
                same_keys(before.as_ref(), after.as_ref()),
                "{before:?} -> {after:?}"
            );
        }
    }

    #[test]
    fn keys_change_with_another_grid_format_kind_or_dimension() {
        let other_format = TimeGrid {
            fmt: "%Y%m%d".into(),
            ..daily()
        };
        let changed = [
            (None, Some(time(daily()))),
            (Some(time(daily())), None),
            (Some(time(daily())), Some(time(hourly()))),
            (Some(time(daily())), Some(time(other_format))),
            // An interval start moved by part of the interval.
            (Some(time(every_6h(0))), Some(time(every_6h(3)))),
            (Some(statics(&["a"])), Some(time(daily()))),
            (
                Some(PartitionsFingerprint::Dynamic {
                    name: "files".into(),
                }),
                Some(PartitionsFingerprint::Dynamic {
                    name: "uploads".into(),
                }),
            ),
            (
                Some(by_date_and_region(daily(), &["eu"])),
                Some(by_date_and_region(hourly(), &["eu"])),
            ),
            (
                Some(by_date_and_region(daily(), &["eu"])),
                Some(PartitionsFingerprint::multi([(
                    "date".to_string(),
                    time(daily()),
                )])),
            ),
        ];
        for (before, after) in changed {
            assert!(
                !same_keys(before.as_ref(), after.as_ref()),
                "{before:?} -> {after:?}"
            );
        }
    }

    #[test]
    fn daily_to_hourly_fills_each_hour_of_a_stored_day() {
        let stored = [
            single("2024-01-02"),
            single("2024-01-01"),
            single("2024-01-04"),
        ];

        let filled = values(&fill(&time(daily()), &time(hourly()), &stored));

        assert_eq!(filled.len(), 72);
        assert_eq!(filled[0], "2024-01-01-00:00");
        assert_eq!(filled[23], "2024-01-01-23:00");
        assert_eq!(filled[24], "2024-01-02-00:00");
        assert_eq!(filled[48], "2024-01-04-00:00");
        assert_eq!(filled[71], "2024-01-04-23:00");
    }

    #[test]
    fn hourly_to_daily_fills_only_days_with_every_hour_stored() {
        let mut stored: Vec<PartitionKey> = (0..24)
            .map(|h| single(&format!("2024-01-01-{h:02}:00")))
            .collect();
        stored.extend((0..23).map(|h| single(&format!("2024-01-02-{h:02}:00"))));

        assert_eq!(
            values(&fill(&time(hourly()), &time(daily()), &stored)),
            ["2024-01-01"]
        );
    }

    #[test]
    fn a_day_has_24_hours_also_when_the_clocks_change() {
        // Keys are wall-clock times without a zone: 2024-03-31, when Europe
        // moves its clocks forward, has 24 hourly keys like any other day.
        let filled = values(&fill(
            &time(daily()),
            &time(hourly()),
            &[single("2024-03-31")],
        ));

        assert_eq!(filled.len(), 24);
        assert_eq!(filled[2], "2024-03-31-02:00");
    }

    #[test]
    fn a_start_moved_by_part_of_an_interval_fills_the_windows_that_stored_ones_cover() {
        let stored = [single("2024-01-01T00:00"), single("2024-01-01T06:00")];

        // [03:00, 09:00) lies in [00:00, 12:00); [09:00, 15:00) does not.
        assert_eq!(
            values(&fill(&time(every_6h(0)), &time(every_6h(3)), &stored)),
            ["2024-01-01T03:00"]
        );
    }

    #[test]
    fn multi_partitions_fill_the_changed_time_dimension_under_each_other_value() {
        let stored = [
            multi("2024-01-01", "eu"),
            multi("2024-01-02", "us"),
            multi("2024-01-01", "ap"),
        ];
        let before = by_date_and_region(daily(), &["ap", "eu", "us"]);
        let after = by_date_and_region(hourly(), &["eu", "us"]);
        let static_keys = HashMap::from([(
            "region".to_string(),
            HashSet::from(["eu".to_string(), "us".to_string()]),
        )]);

        let filled =
            partitions_to_fill(Some(&before), Some(&after), &stored, &static_keys).unwrap();

        // `ap` is no key of the new definition.
        let filled = values(&filled);
        assert_eq!(filled.len(), 48);
        assert_eq!(filled[0], multi("2024-01-01-00:00", "eu").to_display());
        assert_eq!(filled[24], multi("2024-01-02-00:00", "us").to_display());
    }

    #[test]
    fn partitioned_to_unpartitioned_fills_the_whole_asset_when_a_partition_is_stored() {
        let before = time(daily());

        assert_eq!(
            partitions_to_fill(
                Some(&before),
                None,
                &[single("2024-01-01")],
                &HashMap::new()
            ),
            Ok(Fill::Whole)
        );
        assert_eq!(
            partitions_to_fill(Some(&before), None, &[], &HashMap::new()),
            Ok(Fill::Keys(Vec::new()))
        );
    }

    #[test]
    fn other_changes_need_the_partitions_from_the_migration() {
        let cases = [
            (Some(statics(&["a"])), Some(time(daily()))),
            (None, Some(time(daily()))),
            (
                Some(PartitionsFingerprint::Dynamic {
                    name: "files".into(),
                }),
                Some(time(daily())),
            ),
            (
                Some(by_date_and_region(daily(), &["eu"])),
                Some(PartitionsFingerprint::multi([
                    ("day".to_string(), time(hourly())),
                    ("region".to_string(), statics(&["eu"])),
                ])),
            ),
        ];
        for (before, after) in cases {
            let error = partitions_to_fill(before.as_ref(), after.as_ref(), &[], &HashMap::new())
                .unwrap_err();
            assert!(error.contains("give them with partitions="), "got: {error}");
        }
    }

    #[test]
    fn a_stored_key_of_another_format_is_an_error() {
        let error = partitions_to_fill(
            Some(&time(daily())),
            Some(&time(hourly())),
            &[single("01/02/2024")],
            &HashMap::new(),
        )
        .unwrap_err();

        assert!(error.contains("'01/02/2024' is not a key"), "got: {error}");
    }

    /// `cargo test --release -p rivers-core --lib -- --ignored million`.
    #[test]
    #[ignore = "timing: run on a release build"]
    fn a_million_hours_from_daily_partitions() {
        let long = TimeGrid {
            start: date(1900, 1, 1).at(0, 0, 0, 0),
            end: Some(date(2100, 1, 1).at(0, 0, 0, 0)),
            ..daily()
        };
        let hours = TimeGrid {
            cron_schedule: Some("0 * * * *".into()),
            fmt: "%Y-%m-%d-%H:%M".into(),
            ..long.clone()
        };
        let mut day = date(1980, 1, 1);
        let stored: Vec<PartitionKey> = (0..41_667)
            .map(|_| {
                let key = single(&day.strftime("%Y-%m-%d").to_string());
                day = day.tomorrow().unwrap();
                key
            })
            .collect();

        let started = std::time::Instant::now();
        let filled = fill(&time(long), &time(hours), &stored);
        let elapsed = started.elapsed();

        assert_eq!(filled.count(), 1_000_008);
        eprintln!("1,000,008 hours to fill in {elapsed:?}");
    }
}
