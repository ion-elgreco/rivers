//! Run-status sensor cursor.
//!
//! A run's terminal status and its `end_time` are written in one update, so a
//! run-status sensor reads runs by `end_time`. Writers stamp `end_time` with
//! their own clock and the row can commit after that time, so each read starts
//! [`OVERLAP_NS`] back and skips the run ids it already handled.
use std::collections::HashSet;

use crate::storage::RunRecord;

pub const OVERLAP_NS: i64 = 60 * 1_000_000_000;
pub const RUNS_PER_TICK: usize = 5;

/// Persisted on the tick. Runs that ended before `floor` are never read: it
/// starts at the first tick and trails the clock by [`OVERLAP_NS`] on idle
/// ticks. `seen` holds the handled runs within [`OVERLAP_NS`] of the newest one.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RunStatusCursor {
    floor: i64,
    seen: Vec<(String, i64)>,
}

impl RunStatusCursor {
    /// `None` for a missing cursor or one a plain sensor wrote.
    pub fn parse(raw: Option<&str>) -> Option<Self> {
        raw.and_then(|s| serde_json::from_str(s).ok())
    }

    pub fn init(now: i64) -> Self {
        Self {
            floor: now,
            seen: Vec::new(),
        }
    }

    /// Lower `end_time` bound of the next read.
    pub fn since(&self) -> i64 {
        match self.seen.iter().map(|(_, end)| *end).max() {
            Some(newest) => (newest - OVERLAP_NS).max(self.floor),
            None => self.floor,
        }
    }

    /// Every seen run lies in the read window, so this many rows in end-time
    /// order hold the next [`RUNS_PER_TICK`] unseen runs.
    pub fn read_limit(&self) -> usize {
        self.seen.len() + RUNS_PER_TICK
    }

    /// The first [`RUNS_PER_TICK`] rows not handled yet; `rows` are in end-time order.
    pub fn select_new(&self, rows: Vec<RunRecord>) -> Vec<RunRecord> {
        let seen: HashSet<&str> = self.seen.iter().map(|(id, _)| id.as_str()).collect();
        rows.into_iter()
            .filter(|r| !seen.contains(r.run_id.as_str()))
            .take(RUNS_PER_TICK)
            .collect()
    }

    pub fn advance(mut self, handled: &[RunRecord]) -> Self {
        for run in handled {
            let end = run.end_time.unwrap_or(self.floor);
            self.seen.push((run.run_id.clone(), end));
        }
        self.prune()
    }

    /// No new runs: anything that ended before `now - OVERLAP_NS` has committed
    /// by now, so later reads can start there.
    pub fn idle(mut self, now: i64) -> Self {
        self.floor = self.floor.max(now - OVERLAP_NS);
        self.prune()
    }

    fn prune(mut self) -> Self {
        let since = self.since();
        self.seen.retain(|(_, end)| *end >= since);
        self
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("cursor serializes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, LaunchedBy, RunStatus};

    const SEC: i64 = 1_000_000_000;

    fn run(id: &str, end_time: i64) -> RunRecord {
        RunRecord {
            run_id: id.to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: None,
            status: RunStatus::Failure,
            start_time: 0,
            end_time: Some(end_time),
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        }
    }

    fn ids(runs: &[RunRecord]) -> Vec<&str> {
        runs.iter().map(|r| r.run_id.as_str()).collect()
    }

    #[test]
    fn init_reads_from_start() {
        let cursor = RunStatusCursor::init(500 * SEC);
        assert_eq!(cursor.since(), 500 * SEC);
        assert_eq!(cursor.read_limit(), RUNS_PER_TICK);
    }

    #[test]
    fn select_new_skips_seen_and_caps() {
        let cursor = RunStatusCursor {
            floor: 0,
            seen: vec![("a".into(), 90 * SEC), ("b".into(), 100 * SEC)],
        };
        let rows: Vec<RunRecord> = ["a", "b", "c", "d", "e", "f", "g", "h"]
            .iter()
            .enumerate()
            .map(|(i, id)| run(id, (90 + i as i64) * SEC))
            .collect();
        assert_eq!(
            ids(&cursor.select_new(rows)),
            ["c", "d", "e", "f", "g"],
            "seen runs are skipped and at most RUNS_PER_TICK are taken, in end order"
        );
    }

    #[test]
    fn late_run_inside_overlap_is_read_once() {
        let cursor = RunStatusCursor::init(0).advance(&[run("a", 100 * SEC)]);
        assert_eq!(cursor.since(), 40 * SEC);
        // `b` committed late with an end time before `a`'s.
        let new = cursor.select_new(vec![run("b", 95 * SEC), run("a", 100 * SEC)]);
        assert_eq!(ids(&new), ["b"]);
        let cursor = cursor.advance(&new);
        assert_eq!(
            cursor.since(),
            40 * SEC,
            "a late run does not move the window back"
        );
        assert!(
            cursor
                .select_new(vec![run("b", 95 * SEC), run("a", 100 * SEC)])
                .is_empty()
        );
    }

    #[test]
    fn since_never_goes_before_start() {
        let cursor = RunStatusCursor::init(1_000 * SEC).advance(&[run("a", 1_010 * SEC)]);
        assert_eq!(cursor.since(), 1_000 * SEC);
    }

    #[test]
    fn advance_prunes_runs_older_than_the_overlap() {
        let cursor = RunStatusCursor::init(0)
            .advance(&[run("old", 10 * SEC), run("tie", 100 * SEC)])
            .advance(&[run("new", 100 * SEC)]);
        assert_eq!(
            cursor.seen,
            vec![
                ("tie".to_string(), 100 * SEC),
                ("new".to_string(), 100 * SEC)
            ]
        );
    }

    #[test]
    fn idle_ticks_keep_the_window_one_overlap_wide() {
        let cursor = RunStatusCursor::init(0).advance(&[run("a", 100 * SEC)]);
        let cursor = cursor.idle(130 * SEC);
        assert_eq!(cursor.since(), 70 * SEC);
        assert_eq!(cursor.seen, vec![("a".to_string(), 100 * SEC)]);
        let cursor = cursor.idle(200 * SEC);
        assert_eq!(cursor.since(), 140 * SEC);
        assert!(cursor.seen.is_empty());
        assert_eq!(
            RunStatusCursor::init(500 * SEC).idle(10 * SEC).since(),
            500 * SEC,
            "the floor never moves back"
        );
    }

    #[test]
    fn parse_rejects_foreign_cursors() {
        assert_eq!(RunStatusCursor::parse(None), None);
        assert_eq!(RunStatusCursor::parse(Some("cursor_42")), None);
        let cursor = RunStatusCursor::init(7).advance(&[run("a", 9)]);
        assert_eq!(
            RunStatusCursor::parse(Some(&cursor.to_json())),
            Some(cursor)
        );
    }
}
