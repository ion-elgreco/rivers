//! The keys of the planned runs of a backfill, with rows of time windows
//! written as spans.

use crate::storage::{KeyItem, PartitionKey, items_with_spans};
use crate::timegrid::TimeGrid;

/// The time grid of a time dimension of the keys of a backfill. The name is
/// `None` for the one dimension of a single-dimension key.
pub type DimensionGrid = (Option<String>, TimeGrid);

/// `run_keys` with each row of three or more consecutive time windows written
/// as a span. Each key names the same partitions as before, in the same order.
pub fn with_spans(run_keys: Vec<PartitionKey>, grids: &[DimensionGrid]) -> Vec<PartitionKey> {
    if grids.is_empty() {
        return run_keys;
    }
    let mut writer = SpanWriter {
        grids,
        last: vec![None; grids.len()],
    };
    run_keys.into_iter().map(|key| writer.write(key)).collect()
}

struct SpanWriter<'a> {
    grids: &'a [DimensionGrid],
    /// The values that each grid got last, with their items. The runs of one
    /// backfill mostly share the values of a time dimension.
    last: Vec<Option<(Vec<String>, Option<Vec<KeyItem>>)>>,
}

impl SpanWriter<'_> {
    fn write(&mut self, key: PartitionKey) -> PartitionKey {
        match key {
            PartitionKey::Single { keys } => match self.items(None, &keys) {
                Some(items) => PartitionKey::SingleItems { items },
                None => PartitionKey::Single { keys },
            },
            PartitionKey::Multi { dims } => {
                let written: Vec<Option<Vec<KeyItem>>> = dims
                    .iter()
                    .map(|(name, values)| self.items(Some(name), values))
                    .collect();
                if written.iter().all(Option::is_none) {
                    return PartitionKey::Multi { dims };
                }

                let dims = dims
                    .into_iter()
                    .zip(written)
                    .map(|((name, values), items)| {
                        let items = items
                            .unwrap_or_else(|| values.into_iter().map(KeyItem::Value).collect());
                        (name, items)
                    })
                    .collect();
                PartitionKey::MultiItems { dims }
            }
            PartitionKey::Set { keys } => PartitionKey::Set {
                keys: keys.into_iter().map(|key| self.write(key)).collect(),
            },
            key @ (PartitionKey::SingleItems { .. } | PartitionKey::MultiItems { .. }) => key,
        }
    }

    /// The values of `dimension` as items; `None` when they hold no span.
    fn items(&mut self, dimension: Option<&str>, values: &[String]) -> Option<Vec<KeyItem>> {
        // Shorter than any span: most keys of a run per partition.
        if values.len() < 3 {
            return None;
        }
        let place = self
            .grids
            .iter()
            .position(|(name, _)| name.as_deref() == dimension)?;

        if let Some((seen, items)) = &self.last[place]
            && seen == values
        {
            return items.clone();
        }
        let items = items_with_spans(values, &self.grids[place].1);
        self.last[place] = Some((values.to_vec(), items.clone()));
        items
    }
}

#[cfg(test)]
mod tests {
    use jiff::civil::date;

    use super::*;
    use crate::execution::backfill::plan_runs;
    use crate::storage::BackfillStrategy;

    const HOUR_FMT: &str = "%Y-%m-%d-%H:%M";

    fn hourly(cron: bool) -> TimeGrid {
        TimeGrid {
            cron_schedule: cron.then(|| "0 * * * *".to_string()),
            interval_seconds: (!cron).then_some(3600.0),
            start: date(2024, 1, 1).at(0, 0, 0, 0),
            end: None,
            fmt: HOUR_FMT.into(),
        }
    }

    fn hours(grid: &TimeGrid, days: i8) -> Vec<String> {
        grid.keys_in_range(
            date(2024, 1, 1).at(0, 0, 0, 0),
            date(2024, 1, days).at(23, 0, 0, 0),
        )
        .unwrap()
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| v.to_string()).collect()
    }

    fn per_zone() -> BackfillStrategy {
        BackfillStrategy::PerDimension {
            multi_run: vec!["zone".into()],
            single_run: vec!["hour".into()],
        }
    }

    #[test]
    fn a_run_per_zone_holds_its_hours_as_one_span() {
        for grid in [hourly(true), hourly(false)] {
            let requested = [PartitionKey::Multi {
                dims: vec![
                    ("hour".into(), hours(&grid, 2)),
                    ("zone".into(), strings(&["132", "7"])),
                ],
            }];
            let plan = plan_runs(&per_zone(), &requested);
            let grids = [(Some("hour".to_string()), grid)];

            let runs = with_spans(plan.clone(), &grids);

            assert_eq!(runs.len(), 2);
            assert_eq!(
                runs[0].to_display(),
                "hour=2024-01-01-00:00..2024-01-02-23:00|zone=132"
            );
            assert_eq!(
                runs[1].to_display(),
                "hour=2024-01-01-00:00..2024-01-02-23:00|zone=7"
            );
            for (run, planned) in runs.iter().zip(&plan) {
                assert!(matches!(run, PartitionKey::MultiItems { .. }));
                assert_eq!(run.expanded().as_ref(), planned);
                assert_eq!(run.member_count(), 48);
            }
        }
    }

    #[test]
    fn a_single_dimension_run_holds_its_windows_as_a_span() {
        for grid in [hourly(true), hourly(false)] {
            let requested = [PartitionKey::Single {
                keys: hours(&grid, 1),
            }];
            let plan = plan_runs(&BackfillStrategy::SingleRun, &requested);

            let runs = with_spans(plan.clone(), &[(None, grid)]);

            assert_eq!(runs.len(), 1);
            assert_eq!(runs[0].to_display(), "2024-01-01-00:00..2024-01-01-23:00");
            assert_eq!(runs[0].expanded().as_ref(), &plan[0]);
        }
    }

    #[test]
    fn hours_that_are_not_in_a_row_stay_in_the_same_run() {
        let grid = hourly(true);
        let mut kept = hours(&grid, 1);
        kept.remove(12);
        kept.truncate(20);
        let requested = [PartitionKey::Multi {
            dims: vec![("hour".into(), kept), ("zone".into(), strings(&["132"]))],
        }];
        let plan = plan_runs(&per_zone(), &requested);

        let runs = with_spans(plan.clone(), &[(Some("hour".to_string()), grid)]);

        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs[0].to_display(),
            "hour=2024-01-01-00:00..2024-01-01-11:00,2024-01-01-13:00..2024-01-01-20:00|zone=132"
        );
        assert_eq!(runs[0].expanded().as_ref(), &plan[0]);
    }

    #[test]
    fn a_key_with_no_row_of_three_windows_is_left_as_it_is() {
        let grid = hourly(true);
        let grids = [(Some("hour".to_string()), grid.clone())];
        let requested = [PartitionKey::Multi {
            dims: vec![
                ("hour".into(), hours(&grid, 1)),
                ("zone".into(), strings(&["132", "7"])),
            ],
        }];

        // One run per partition: one hour each.
        let per_partition = plan_runs(&BackfillStrategy::MultiRun, &requested);
        assert_eq!(with_spans(per_partition.clone(), &grids), per_partition);

        // One run per hour: the zones have no grid.
        let per_hour = plan_runs(
            &BackfillStrategy::PerDimension {
                multi_run: vec!["hour".into()],
                single_run: vec!["zone".into()],
            },
            &requested,
        );
        assert_eq!(with_spans(per_hour.clone(), &grids), per_hour);

        // No grid at all, and a grid for a dimension that the keys do not have.
        let per_zone = plan_runs(&per_zone(), &requested);
        assert_eq!(with_spans(per_zone.clone(), &[]), per_zone);
        assert_eq!(
            with_spans(per_zone.clone(), &[(None, grid.clone())]),
            per_zone
        );
        assert_eq!(
            with_spans(per_zone.clone(), &[(Some("day".to_string()), grid)]),
            per_zone
        );
    }

    #[test]
    fn the_keys_of_a_set_get_their_spans() {
        let grid = hourly(true);
        // Two zones with different hours: no one key holds both.
        let requested = [
            PartitionKey::Multi {
                dims: vec![
                    ("hour".into(), hours(&grid, 1)),
                    ("zone".into(), strings(&["132"])),
                ],
            },
            PartitionKey::Multi {
                dims: vec![
                    ("hour".into(), hours(&grid, 2)[30..40].to_vec()),
                    ("zone".into(), strings(&["7"])),
                ],
            },
        ];
        let plan = plan_runs(&BackfillStrategy::SingleRun, &requested);
        assert!(matches!(&plan[..], [PartitionKey::Set { .. }]));

        let runs = with_spans(plan.clone(), &[(Some("hour".to_string()), grid)]);

        assert_eq!(
            runs[0].to_display(),
            "hour=2024-01-01-00:00..2024-01-01-23:00|zone=132, \
             hour=2024-01-02-06:00..2024-01-02-15:00|zone=7"
        );
        assert_eq!(runs[0].expanded().as_ref(), &plan[0]);
        assert_eq!(runs[0].member_count(), 34);
    }

    #[test]
    fn half_a_year_of_hours_for_265_zones_is_265_small_keys() {
        let grid = hourly(true);
        let all_hours = grid
            .keys_in_range(
                date(2024, 1, 1).at(0, 0, 0, 0),
                date(2024, 6, 30).at(23, 0, 0, 0),
            )
            .unwrap();
        assert_eq!(all_hours.len(), 4368);
        let zones: Vec<String> = (1..=265).map(|zone| zone.to_string()).collect();
        let requested = [PartitionKey::Multi {
            dims: vec![("hour".into(), all_hours), ("zone".into(), zones)],
        }];
        let plan = plan_runs(&per_zone(), &requested);

        let runs = with_spans(plan.clone(), &[(Some("hour".to_string()), grid)]);

        assert_eq!(runs.len(), 265);
        for (run, planned) in runs.iter().zip(&plan) {
            assert_eq!(run.member_count(), 4368);
            let size = serde_json::to_string(run).unwrap().len();
            assert!(size < 300, "{size} bytes");
            assert!(planned.member_count() == 4368);
        }
        assert_eq!(runs[0].expanded().as_ref(), &plan[0]);
        assert_eq!(runs[264].expanded().as_ref(), &plan[264]);
    }
}
