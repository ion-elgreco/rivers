//! The partitions of a backfill with what happened to each: counted by the
//! rows of an outline, or one by one.
//!
//! The requested keys and the outcome of each run that ended are read once.
//! A count works on the requested keys, which hold whole dimensions, and
//! does not write out each partition.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use futures_util::{StreamExt, TryStreamExt};

use crate::execution::backfill::RunNumbers;
use crate::storage::{
    BackfillRecord, BackfillRunOutcome, EndedBackfillRun, PartitionKey, StorageBackend,
};

/// The reads of the failed partitions of runs that are in flight at once.
const READS_AT_ONCE: usize = 8;

/// What happened to one partition of a backfill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillPartitionState {
    Pending,
    Completed,
    Failed,
    Canceled,
}

impl From<BackfillRunOutcome> for BackfillPartitionState {
    fn from(outcome: BackfillRunOutcome) -> Self {
        match outcome {
            BackfillRunOutcome::Completed => Self::Completed,
            BackfillRunOutcome::Failed => Self::Failed,
            BackfillRunOutcome::Canceled => Self::Canceled,
        }
    }
}

/// What happened to one partition, and the run that held it when that run
/// ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionOutcome {
    pub state: BackfillPartitionState,
    pub run_id: Option<String>,
}

impl PartitionOutcome {
    fn pending() -> Self {
        Self {
            state: BackfillPartitionState::Pending,
            run_id: None,
        }
    }
}

/// The partitions of a backfill, or of one part of it, by what happened to
/// them. The partitions in no other state are pending.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BackfillCounts {
    pub total: u64,
    pub completed: u64,
    pub failed: u64,
    pub canceled: u64,
}

impl BackfillCounts {
    pub fn pending(&self) -> u64 {
        self.total
            .saturating_sub(self.completed + self.failed + self.canceled)
    }

    fn add(&mut self, state: BackfillPartitionState, partitions: u64) {
        match state {
            BackfillPartitionState::Completed => self.completed += partitions,
            BackfillPartitionState::Failed => self.failed += partitions,
            BackfillPartitionState::Canceled => self.canceled += partitions,
            BackfillPartitionState::Pending => {}
        }
    }
}

/// The partitions of one row of an outline: the value of each dimension
/// above the row, and the keys of `dimension` from `first` to `last`, by
/// the order of their text. `first` and `last` both empty: every key.
/// `dimension` is empty for keys of one dimension. The default is every
/// partition.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KeyRange {
    pub fixed: Vec<(String, String)>,
    pub dimension: String,
    pub first: String,
    pub last: String,
}

impl KeyRange {
    fn bounded(&self) -> bool {
        !(self.first.is_empty() && self.last.is_empty())
    }

    fn holds_key(&self, key: &str) -> bool {
        key >= self.first.as_str() && (self.last.is_empty() || key <= self.last.as_str())
    }

    fn fixed_value(&self, name: &str) -> Option<&str> {
        self.fixed
            .iter()
            .find(|(fixed, _)| fixed == name)
            .map(|(_, value)| value.as_str())
    }

    /// Whether the range holds the partition with `values`, one value per
    /// dimension.
    fn holds(&self, values: &[(&str, &str)]) -> bool {
        let has = |name: &str| values.iter().any(|(of, _)| *of == name);
        if self.fixed.iter().any(|(name, _)| !has(name))
            || (self.bounded() && !has(&self.dimension))
        {
            return false;
        }

        values
            .iter()
            .all(|(name, value)| match self.fixed_value(name) {
                Some(fixed) => fixed == *value,
                None if self.bounded() && *name == self.dimension => self.holds_key(value),
                None => true,
            })
    }
}

/// One requested key: the values of each of its dimensions. Its partitions
/// are every combination of them, in the order of `PartitionKey::member_at`.
struct Block {
    /// Each dimension by name, the empty name for keys of one dimension,
    /// with its values in the requested order.
    dims: Vec<(String, Vec<String>)>,
    /// The places of the values of each dimension, in the order of the
    /// values.
    sorted: Vec<Vec<u32>>,
    /// The place of the first partition of the block among the requested
    /// partitions.
    first: usize,
    count: usize,
}

impl Block {
    fn new(dims: Vec<(String, Vec<String>)>, first: usize) -> Self {
        let sorted = dims
            .iter()
            .map(|(_, values)| {
                let mut places: Vec<u32> = (0..values.len() as u32).collect();
                places.sort_unstable_by_key(|&at| &values[at as usize]);
                places
            })
            .collect();
        let count = dims.iter().fold(1usize, |count, (_, values)| {
            count.saturating_mul(values.len())
        });
        Self {
            dims,
            sorted,
            first,
            count,
        }
    }

    fn has(&self, name: &str) -> bool {
        self.dims.iter().any(|(of, _)| of == name)
    }

    /// The values of dimension `name`, when no value repeats.
    fn distinct_values(&self, name: &str) -> Option<&[String]> {
        let at = self.dims.iter().position(|(of, _)| of == name)?;
        let values = &self.dims[at].1;
        self.sorted[at]
            .windows(2)
            .all(|pair| values[pair[0] as usize] != values[pair[1] as usize])
            .then_some(values)
    }

    /// The partitions of the block in `range` whose values on the
    /// dimensions of `pinned` are those values.
    fn count_in(&self, range: &KeyRange, pinned: &[(String, String)]) -> u64 {
        if range.fixed.iter().any(|(name, _)| !self.has(name))
            || (range.bounded() && !self.has(&range.dimension))
            || pinned.iter().any(|(name, _)| !self.has(name))
        {
            return 0;
        }

        let mut count = 1u64;
        for ((name, block_values), sorted) in self.dims.iter().zip(&self.sorted) {
            let pin = pinned
                .iter()
                .find(|(of, _)| of == name)
                .map(|(_, value)| value.as_str());
            let in_dimension = range.bounded() && *name == range.dimension;
            let values = match (pin, range.fixed_value(name)) {
                (Some(pin), Some(fixed)) if pin != fixed => 0,
                (Some(value), _) | (None, Some(value)) => {
                    if in_dimension && !range.holds_key(value) {
                        0
                    } else {
                        equal_range(block_values, sorted, value).len()
                    }
                }
                (None, None) if in_dimension => {
                    key_range(block_values, sorted, &range.first, &range.last).len()
                }
                (None, None) => sorted.len(),
            };
            count = count.saturating_mul(values as u64);
            if count == 0 {
                break;
            }
        }
        count
    }

    /// The values of the partition at `offset` in the block.
    fn member_at(&self, offset: usize) -> Vec<(&str, &str)> {
        let mut rest = offset;
        let mut member: Vec<(&str, &str)> = Vec::with_capacity(self.dims.len());
        for (name, values) in self.dims.iter().rev() {
            member.push((name.as_str(), values[rest % values.len()].as_str()));
            rest /= values.len();
        }
        member.reverse();
        member
    }

    /// Whether the block holds the partition with `values`.
    fn holds(&self, values: &[(&str, &str)]) -> bool {
        values.len() == self.dims.len()
            && self.dims.iter().zip(&self.sorted).zip(values).all(
                |(((name, block_values), sorted), (of, value))| {
                    name == of && !equal_range(block_values, sorted, value).is_empty()
                },
            )
    }

    /// The place of the partition with `values` in the block.
    fn offset_of(&self, values: &[(&str, &str)]) -> Option<usize> {
        if values.len() != self.dims.len() {
            return None;
        }
        let mut offset = 0usize;
        for ((name, block_values), (of, value)) in self.dims.iter().zip(values) {
            if name != of {
                return None;
            }
            let at = block_values.iter().position(|v| v == value)?;
            offset = offset * block_values.len() + at;
        }
        Some(offset)
    }
}

/// The places in `sorted` of the values equal to `value`. `sorted` holds
/// the places of `values` in the order of the values.
fn equal_range(values: &[String], sorted: &[u32], value: &str) -> std::ops::Range<usize> {
    let at = |place: &u32| values[*place as usize].as_str();
    let start = sorted.partition_point(|place| at(place) < value);
    let end = sorted.partition_point(|place| at(place) <= value);
    start..end
}

/// The places in `sorted` of the values from `first` to `last`. An empty
/// `last` has no end.
fn key_range(values: &[String], sorted: &[u32], first: &str, last: &str) -> std::ops::Range<usize> {
    let at = |place: &u32| values[*place as usize].as_str();
    let start = sorted.partition_point(|place| at(place) < first);
    let end = match last {
        "" => sorted.len(),
        last => sorted.partition_point(|place| at(place) <= last),
    };
    start..end.max(start)
}

/// The value of each dimension of a partition that holds one value per
/// dimension, by the name of the dimension. `None` for a batched key.
fn values_of(partition: &PartitionKey) -> Option<Vec<(&str, &str)>> {
    match partition {
        PartitionKey::Single { keys } if keys.len() == 1 => Some(vec![("", keys[0].as_str())]),
        PartitionKey::Multi { dims } => {
            let mut values: Vec<(&str, &str)> = dims
                .iter()
                .map(|(name, values)| match values.as_slice() {
                    [value] => Some((name.as_str(), value.as_str())),
                    _ => None,
                })
                .collect::<Option<_>>()?;
            values.sort_by(|a, b| a.0.cmp(b.0));
            Some(values)
        }
        _ => None,
    }
}

/// The requested keys as blocks, in the requested order.
fn blocks_of(requested: &[PartitionKey]) -> Vec<Block> {
    fn push(key: &PartitionKey, blocks: &mut Vec<Block>, first: &mut usize) {
        let expanded = key.expanded();
        if let PartitionKey::Set { keys } = &*expanded {
            for key in keys {
                push(key, blocks, first);
            }
            return;
        }

        let dims = match expanded.into_owned() {
            PartitionKey::Single { keys } => vec![(String::new(), keys)],
            PartitionKey::Multi { mut dims } => {
                dims.sort_by(|a, b| a.0.cmp(&b.0));
                dims
            }
            PartitionKey::Set { .. }
            | PartitionKey::SingleItems { .. }
            | PartitionKey::MultiItems { .. } => {
                unreachable!("a set is pushed above, and an expanded key has no items")
            }
        };
        let block = Block::new(dims, *first);
        *first += block.count;
        blocks.push(block);
    }

    let mut blocks = Vec::new();
    let mut first = 0;
    for key in requested {
        push(key, &mut blocks, &mut first);
    }
    blocks
}

/// The state of every partition of a run that ended, before the partitions
/// that failed alone in it.
fn state_of_run(run: &EndedBackfillRun) -> BackfillPartitionState {
    if run.outcome == BackfillRunOutcome::Completed && run.failed_members >= run.member_count {
        BackfillPartitionState::Failed
    } else {
        run.outcome.into()
    }
}

/// What happened to each partition of a backfill, read once for the reads of
/// one request.
pub struct BackfillStates {
    requested: Vec<PartitionKey>,
    blocks: Vec<Block>,
    numbers: RunNumbers,
    ended: HashMap<usize, EndedBackfillRun>,
    /// The partitions that failed alone in each run that succeeded.
    failed_alone: HashMap<usize, HashSet<PartitionKey>>,
    /// A backfill that ended before outcomes were kept per run: the outcome
    /// of each partition that has one.
    outcomes: Option<HashMap<PartitionKey, BackfillRunOutcome>>,
}

impl BackfillStates {
    pub async fn read<S: StorageBackend>(storage: &S, backfill: &BackfillRecord) -> Result<Self> {
        let id = backfill.backfill_id.as_str();
        let requested = storage.get_backfill_keys(id).await?;
        let blocks = blocks_of(&requested);
        let numbers = RunNumbers::new(&backfill.strategy, &requested);

        if backfill.planned_runs == 0 {
            let members: Vec<PartitionKey> =
                requested.iter().flat_map(PartitionKey::members).collect();
            let outcomes = storage
                .get_backfill_partition_outcomes(id, &members)
                .await?
                .into_iter()
                .collect();
            return Ok(Self {
                requested,
                blocks,
                numbers,
                ended: HashMap::new(),
                failed_alone: HashMap::new(),
                outcomes: Some(outcomes),
            });
        }

        let last = usize::try_from(backfill.planned_runs - 1).unwrap_or(usize::MAX);
        let ended: HashMap<usize, EndedBackfillRun> = storage
            .get_ended_backfill_runs(id, 0, last)
            .await?
            .into_iter()
            .map(|run| (run.run_number, run))
            .collect();

        let partly_failed: Vec<(usize, String)> = ended
            .values()
            .filter(|run| {
                run.outcome == BackfillRunOutcome::Completed
                    && run.failed_members > 0
                    && run.failed_members < run.member_count
            })
            .filter_map(|run| Some((run.run_number, run.run_id.clone()?)))
            .collect();
        let failed_alone: HashMap<usize, HashSet<PartitionKey>> =
            futures_util::stream::iter(partly_failed)
                .map(|(number, run_id)| async move {
                    let failed = storage.get_run_failed_partitions(&run_id).await?;
                    anyhow::Ok((number, failed.into_iter().collect()))
                })
                .buffer_unordered(READS_AT_ONCE)
                .try_collect()
                .await?;

        Ok(Self {
            requested,
            blocks,
            numbers,
            ended,
            failed_alone,
            outcomes: None,
        })
    }

    /// The distinct values of `dimension` among the requested keys, in the
    /// requested order. The empty name gives the keys of one dimension.
    pub fn keys_of(&self, dimension: &str) -> Vec<&str> {
        let mut holding = self.blocks.iter().filter(|block| block.has(dimension));
        if let (Some(block), None) = (holding.next(), holding.next())
            && let Some(values) = block.distinct_values(dimension)
        {
            return values.iter().map(String::as_str).collect();
        }

        let values = || {
            self.blocks
                .iter()
                .filter_map(|block| block.dims.iter().find(|(name, _)| name == dimension))
                .flat_map(|(_, values)| values.iter().map(String::as_str))
        };
        let mut unseen: HashSet<&str> = values().collect();
        let mut keys = Vec::with_capacity(unseen.len());
        keys.extend(values().filter(|value| unseen.remove(value)));
        keys
    }

    /// The partitions of each of `ranges` by what happened to them.
    pub fn counts_in(&self, ranges: &[KeyRange]) -> Vec<BackfillCounts> {
        let mut counts: Vec<BackfillCounts> = ranges
            .iter()
            .map(|range| BackfillCounts {
                total: self
                    .blocks
                    .iter()
                    .map(|block| block.count_in(range, &[]))
                    .sum(),
                ..BackfillCounts::default()
            })
            .collect();
        let add_to_each = |counts: &mut [BackfillCounts], values: &[(&str, &str)], state| {
            for (range, counts) in ranges.iter().zip(counts.iter_mut()) {
                if range.holds(values) {
                    counts.add(state, 1);
                }
            }
        };

        if let Some(outcomes) = &self.outcomes {
            for (partition, outcome) in outcomes {
                if let Some(values) = values_of(partition) {
                    add_to_each(&mut counts, &values, (*outcome).into());
                }
            }
            return counts;
        }

        match &self.numbers {
            RunNumbers::OneRun => {
                if let Some(run) = self.ended.get(&0) {
                    for counts in &mut counts {
                        counts.add(state_of_run(run), counts.total);
                    }
                }
            }
            RunNumbers::RunPerPartition => {
                for run in self.ended.values() {
                    if let Some(values) = self.member_at(run.run_number) {
                        add_to_each(&mut counts, &values, state_of_run(run));
                    }
                }
            }
            RunNumbers::RunPerGroup { multi_run, groups } => {
                for run in self.ended.values() {
                    let Some(group) = groups.get(run.run_number) else {
                        continue;
                    };
                    for (range, counts) in ranges.iter().zip(&mut counts) {
                        let partitions = self
                            .blocks
                            .iter()
                            .filter(|block| holds_group(block, multi_run, group))
                            .map(|block| block.count_in(range, group))
                            .sum();
                        counts.add(state_of_run(run), partitions);
                    }
                }
            }
        }

        for failed in self.failed_alone.values() {
            for partition in failed {
                let Some(values) = values_of(partition) else {
                    continue;
                };
                if !self.holds(&values) {
                    continue;
                }
                for (range, counts) in ranges.iter().zip(&mut counts) {
                    if range.holds(&values) {
                        counts.completed = counts.completed.saturating_sub(1);
                        counts.failed += 1;
                    }
                }
            }
        }
        counts
    }

    /// What happened to each of `partitions`. `None` for a partition that
    /// the backfill does not hold.
    pub fn outcomes_of(&self, partitions: &[PartitionKey]) -> Vec<Option<PartitionOutcome>> {
        partitions
            .iter()
            .map(|partition| {
                let values = values_of(partition)?;
                // Only a run per partition takes its number from the place.
                let place = match self.numbers {
                    RunNumbers::RunPerPartition => self.place_of(&values)?,
                    _ => self.holds(&values).then_some(0)?,
                };
                Some(self.outcome_at(place, partition))
            })
            .collect()
    }

    /// The partitions from `offset`, at most `limit`, in the order of the
    /// requested keys, each with what happened to it.
    pub fn page(&self, offset: u64, limit: u64) -> Vec<(PartitionKey, PartitionOutcome)> {
        partitions_at(&self.requested, offset, limit)
            .into_iter()
            .map(|(place, partition)| {
                let outcome = self.outcome_at(place, &partition);
                (partition, outcome)
            })
            .collect()
    }

    /// The number of the requested partitions.
    pub fn total(&self) -> u64 {
        self.blocks.iter().map(|block| block.count as u64).sum()
    }

    fn outcome_at(&self, place: usize, partition: &PartitionKey) -> PartitionOutcome {
        if let Some(outcomes) = &self.outcomes {
            return PartitionOutcome {
                state: outcomes
                    .get(partition)
                    .map_or(BackfillPartitionState::Pending, |outcome| (*outcome).into()),
                run_id: None,
            };
        }

        let Some(run) = self
            .numbers
            .of(place, partition)
            .and_then(|number| self.ended.get(&number))
        else {
            return PartitionOutcome::pending();
        };
        let failed_alone = run.outcome == BackfillRunOutcome::Completed
            && self
                .failed_alone
                .get(&run.run_number)
                .is_some_and(|failed| failed.contains(partition));
        PartitionOutcome {
            state: match failed_alone {
                true => BackfillPartitionState::Failed,
                false => state_of_run(run),
            },
            run_id: run.run_id.clone(),
        }
    }

    fn holds(&self, values: &[(&str, &str)]) -> bool {
        self.blocks.iter().any(|block| block.holds(values))
    }

    /// The place among the requested partitions of the partition with
    /// `values`.
    fn place_of(&self, values: &[(&str, &str)]) -> Option<usize> {
        self.blocks
            .iter()
            .find_map(|block| Some(block.first + block.offset_of(values)?))
    }

    /// The values of the requested partition at `place`.
    fn member_at(&self, place: usize) -> Option<Vec<(&str, &str)>> {
        let at = self.blocks.partition_point(|block| block.first <= place);
        let block = self.blocks.get(at.checked_sub(1)?)?;
        (place < block.first + block.count).then(|| block.member_at(place - block.first))
    }
}

/// Whether the run of `group` holds partitions of `block`: the block has the
/// dimensions of `multi_run` that the group names, and no other.
fn holds_group(block: &Block, multi_run: &[String], group: &[(String, String)]) -> bool {
    let grouped = block
        .dims
        .iter()
        .filter(|(name, _)| multi_run.contains(name))
        .count();
    grouped == group.len()
}

/// The partitions that `requested` names from `offset`, at most `limit`, each
/// with its place among all of them.
fn partitions_at(
    requested: &[PartitionKey],
    offset: u64,
    limit: u64,
) -> Vec<(usize, PartitionKey)> {
    let mut page: Vec<(usize, PartitionKey)> = Vec::new();
    let mut first_of_key = 0u64;

    for key in requested {
        let end_of_key = first_of_key.saturating_add(key.member_count() as u64);
        let mut place = offset.max(first_of_key);
        while place < end_of_key && (page.len() as u64) < limit {
            if let Some(partition) = key.member_at((place - first_of_key) as usize) {
                page.push((place as usize, partition));
            }
            place += 1;
        }
        if page.len() as u64 >= limit {
            break;
        }
        first_of_key = end_of_key;
    }
    page
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        BackfillFailurePolicy, BackfillStatus, BackfillStrategy, EventRecord, EventType,
        LaunchedBy, RunRecord, RunStatus, UnstartedBackfillRun, default_code_location_id,
    };

    use BackfillPartitionState::{Canceled, Completed, Failed, Pending};

    fn single(values: &[&str]) -> PartitionKey {
        PartitionKey::Single {
            keys: values.iter().map(|v| v.to_string()).collect(),
        }
    }

    fn multi(dims: &[(&str, &[&str])]) -> PartitionKey {
        PartitionKey::Multi {
            dims: dims
                .iter()
                .map(|(name, values)| {
                    (
                        name.to_string(),
                        values.iter().map(|v| v.to_string()).collect(),
                    )
                })
                .collect(),
        }
    }

    /// Store backfill `bf` for `requested` and start it with its plan.
    async fn started_backfill(
        storage: &SurrealStorage,
        strategy: BackfillStrategy,
        requested: &[PartitionKey],
    ) -> Vec<PartitionKey> {
        let record = BackfillRecord {
            backfill_id: "bf".to_string(),
            code_location_id: default_code_location_id(),
            status: BackfillStatus::Requested,
            strategy: strategy.clone(),
            failure_policy: BackfillFailurePolicy::Continue,
            asset_selection: vec!["a".to_string()],
            job_name: None,
            num_partitions: 0,
            planned_runs: 0,
            run_ids: vec![],
            completed_partitions: 0,
            failed_partitions: 0,
            canceled_partitions: 0,
            max_concurrency: 1,
            tags: vec![],
            create_time: 1,
            end_time: None,
            error: None,
            launched_by: LaunchedBy::default(),
            action: None,
            config: None,
        };
        storage.create_backfill(&record, requested).await.unwrap();

        let plan = crate::execution::backfill::plan_runs(&strategy, requested);
        assert!(storage.start_backfill("bf", plan.len()).await.unwrap());
        plan
    }

    /// Link run `run_number` of the plan and end it with `status`.
    async fn ended_run(
        storage: &SurrealStorage,
        plan: &[PartitionKey],
        run_number: usize,
        status: RunStatus,
        failed_alone: &[PartitionKey],
    ) {
        let run_id = format!("run-{run_number}");
        let run_key = &plan[run_number];
        assert!(
            storage
                .link_backfill_run("bf", &run_id, run_number, run_key.member_count())
                .await
                .unwrap()
        );
        storage
            .create_run(&RunRecord {
                run_id: run_id.clone(),
                code_location_id: default_code_location_id(),
                job_name: None,
                status: RunStatus::Started,
                start_time: 1,
                end_time: None,
                execution_start_time: None,
                tags: vec![],
                node_names: vec!["a".to_string()],
                priority: 0,
                partition_key: Some(run_key.clone()),
                block_reason: None,
                launched_by: LaunchedBy::Backfill {
                    backfill_id: "bf".to_string(),
                },
                action: None,
                config: None,
            })
            .await
            .unwrap();
        for partition in failed_alone {
            storage
                .store_event(&EventRecord {
                    code_location_id: default_code_location_id(),
                    event_type: EventType::StepFailure,
                    asset_key: Some("a".to_string()),
                    run_id: run_id.clone(),
                    partition_key: Some(partition.clone()),
                    timestamp: 2,
                    metadata: vec![],
                    input_data_versions: vec![],
                })
                .await
                .unwrap();
        }
        storage
            .update_run_status(&run_id, status, Some(3))
            .await
            .unwrap();
    }

    async fn read_states(storage: &SurrealStorage) -> BackfillStates {
        let backfill = storage.get_backfill("bf").await.unwrap().unwrap();
        BackfillStates::read(storage, &backfill).await.unwrap()
    }

    async fn page(
        storage: &SurrealStorage,
        offset: u64,
        limit: u64,
    ) -> Vec<(String, BackfillPartitionState)> {
        read_states(storage)
            .await
            .page(offset, limit)
            .into_iter()
            .map(|(partition, outcome)| (partition.to_display(), outcome.state))
            .collect()
    }

    fn range(fixed: &[(&str, &str)], dimension: &str, first: &str, last: &str) -> KeyRange {
        KeyRange {
            fixed: fixed
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
            dimension: dimension.to_string(),
            first: first.to_string(),
            last: last.to_string(),
        }
    }

    /// Counts of `total` partitions: completed, failed, canceled.
    fn counted(total: u64, completed: u64, failed: u64, canceled: u64) -> BackfillCounts {
        BackfillCounts {
            total,
            completed,
            failed,
            canceled,
        }
    }

    #[tokio::test]
    async fn each_partition_takes_the_outcome_of_its_run() {
        let storage = SurrealStorage::new_memory().await.unwrap();
        let requested = [multi(&[
            ("zone", &["z1", "z2", "z3"]),
            ("hour", &["h1", "h2"]),
        ])];
        let strategy = BackfillStrategy::PerDimension {
            multi_run: vec!["zone".to_string()],
            single_run: vec!["hour".to_string()],
        };
        let plan = started_backfill(&storage, strategy, &requested).await;
        assert_eq!(plan.len(), 3, "one run per zone");

        // z1 succeeded with one partition failed alone, z2 failed, z3 is open.
        let failed = multi(&[("zone", &["z1"]), ("hour", &["h2"])]);
        ended_run(&storage, &plan, 0, RunStatus::Success, &[failed]).await;
        ended_run(&storage, &plan, 1, RunStatus::Failure, &[]).await;

        let cell = |hour: &str, zone: &str, state| (format!("hour={hour}|zone={zone}"), state);
        assert_eq!(
            page(&storage, 0, 10).await,
            vec![
                cell("h1", "z1", Completed),
                cell("h1", "z2", Failed),
                cell("h1", "z3", Pending),
                cell("h2", "z1", Failed),
                cell("h2", "z2", Failed),
                cell("h2", "z3", Pending),
            ]
        );
        assert_eq!(
            page(&storage, 4, 1).await,
            vec![cell("h2", "z2", Failed)],
            "a page reads its own partitions"
        );
        assert!(page(&storage, 6, 10).await.is_empty());

        // The run of z3 is canceled before it starts.
        storage
            .settle_unstarted_backfill_runs(
                "bf",
                &[UnstartedBackfillRun {
                    run_number: 2,
                    member_count: 2,
                    outcome: BackfillRunOutcome::Canceled,
                }],
            )
            .await
            .unwrap();
        assert_eq!(
            page(&storage, 5, 10).await,
            vec![cell("h2", "z3", Canceled)]
        );
    }

    #[tokio::test]
    async fn a_run_per_partition_reads_the_runs_of_the_page() {
        let storage = SurrealStorage::new_memory().await.unwrap();
        let requested = [single(&["a", "b"]), single(&["c", "d"])];
        let plan = started_backfill(&storage, BackfillStrategy::MultiRun, &requested).await;
        assert_eq!(plan.len(), 4);

        ended_run(&storage, &plan, 0, RunStatus::Success, &[]).await;
        // The one partition of this run is marked failed; the run succeeds.
        ended_run(&storage, &plan, 2, RunStatus::Success, &[single(&["c"])]).await;
        ended_run(&storage, &plan, 3, RunStatus::Canceled, &[]).await;

        let cell = |key: &str, state| (key.to_string(), state);
        assert_eq!(
            page(&storage, 0, 10).await,
            vec![
                cell("a", Completed),
                cell("b", Pending),
                cell("c", Failed),
                cell("d", Canceled),
            ]
        );
        assert_eq!(
            page(&storage, 2, 2).await,
            vec![cell("c", Failed), cell("d", Canceled)]
        );
    }

    #[tokio::test]
    async fn one_run_gives_its_outcome_to_each_partition() {
        let storage = SurrealStorage::new_memory().await.unwrap();
        let requested = [single(&["a", "b", "c"])];
        let plan = started_backfill(&storage, BackfillStrategy::SingleRun, &requested).await;
        assert_eq!(plan.len(), 1);

        ended_run(&storage, &plan, 0, RunStatus::Success, &[single(&["b"])]).await;

        let cell = |key: &str, state| (key.to_string(), state);
        assert_eq!(
            page(&storage, 0, 10).await,
            vec![
                cell("a", Completed),
                cell("b", Failed),
                cell("c", Completed)
            ]
        );
    }

    #[tokio::test]
    async fn a_run_per_zone_counts_the_rows_of_an_outline() {
        let storage = SurrealStorage::new_memory().await.unwrap();
        let requested = [multi(&[
            ("hour", &["h1", "h2", "h3", "h4"]),
            ("zone", &["z1", "z2", "z3"]),
        ])];
        let strategy = BackfillStrategy::PerDimension {
            multi_run: vec!["zone".to_string()],
            single_run: vec!["hour".to_string()],
        };
        let plan = started_backfill(&storage, strategy, &requested).await;

        // z1 succeeded with h2 failed alone, z2 failed, z3 is open.
        let failed = multi(&[("zone", &["z1"]), ("hour", &["h2"])]);
        ended_run(&storage, &plan, 0, RunStatus::Success, &[failed]).await;
        ended_run(&storage, &plan, 1, RunStatus::Failure, &[]).await;

        let states = read_states(&storage).await;
        assert_eq!(states.total(), 12);
        assert_eq!(
            states.counts_in(&[
                KeyRange::default(),
                range(&[], "hour", "h1", "h2"),
                range(&[], "hour", "h3", "h3"),
                range(&[("hour", "h2")], "zone", "", ""),
                range(&[("hour", "h2")], "zone", "z2", "z3"),
                range(&[("zone", "z1")], "hour", "h2", "h9"),
                range(&[], "hour", "h5", "h9"),
                range(&[("tier", "pro")], "", "", ""),
            ]),
            vec![
                counted(12, 3, 5, 0),
                counted(6, 1, 3, 0),
                counted(3, 1, 1, 0),
                counted(3, 0, 2, 0),
                counted(2, 0, 1, 0),
                counted(3, 2, 1, 0),
                counted(0, 0, 0, 0),
                counted(0, 0, 0, 0),
            ]
        );

        // The run of z3 is canceled before it starts.
        storage
            .settle_unstarted_backfill_runs(
                "bf",
                &[UnstartedBackfillRun {
                    run_number: 2,
                    member_count: 4,
                    outcome: BackfillRunOutcome::Canceled,
                }],
            )
            .await
            .unwrap();
        let states = read_states(&storage).await;
        assert_eq!(
            states.counts_in(&[KeyRange::default(), range(&[], "zone", "z3", "z3")]),
            vec![counted(12, 3, 5, 4), counted(4, 0, 0, 4)]
        );
    }

    #[tokio::test]
    async fn a_run_per_partition_counts_each_run_that_ended() {
        let storage = SurrealStorage::new_memory().await.unwrap();
        // Keys out of the order of their text, in two requested keys.
        let requested = [single(&["d", "b"]), single(&["a", "c"])];
        let plan = started_backfill(&storage, BackfillStrategy::MultiRun, &requested).await;

        ended_run(&storage, &plan, 0, RunStatus::Success, &[]).await;
        ended_run(&storage, &plan, 1, RunStatus::Failure, &[]).await;
        ended_run(&storage, &plan, 3, RunStatus::Canceled, &[]).await;

        let states = read_states(&storage).await;
        assert_eq!(states.keys_of(""), ["d", "b", "a", "c"]);
        assert_eq!(
            states.counts_in(&[
                KeyRange::default(),
                range(&[], "", "a", "b"),
                range(&[], "", "c", "d"),
                range(&[], "", "d", "d"),
            ]),
            vec![
                counted(4, 1, 1, 1),
                counted(2, 0, 1, 0),
                counted(2, 1, 0, 1),
                counted(1, 1, 0, 0),
            ]
        );
    }

    #[tokio::test]
    async fn the_keys_of_a_dimension_are_its_distinct_values_in_the_requested_order() {
        let storage = SurrealStorage::new_memory().await.unwrap();
        let requested = [
            single(&["d", "b", "d", "a"]),
            single(&["b", "e"]),
            multi(&[("date", &["2", "1"]), ("region", &["eu", "us", "eu"])]),
        ];
        started_backfill(&storage, BackfillStrategy::MultiRun, &requested).await;

        let states = read_states(&storage).await;
        assert_eq!(states.keys_of(""), ["d", "b", "a", "e"]);
        assert_eq!(states.keys_of("date"), ["2", "1"]);
        assert_eq!(states.keys_of("region"), ["eu", "us"]);
        assert!(states.keys_of("zone").is_empty());
    }

    #[tokio::test]
    async fn one_run_counts_every_partition_with_the_ones_that_failed_alone() {
        let storage = SurrealStorage::new_memory().await.unwrap();
        let requested = [single(&["a", "b", "c"])];
        let plan = started_backfill(&storage, BackfillStrategy::SingleRun, &requested).await;

        let states = read_states(&storage).await;
        assert_eq!(
            states.counts_in(&[KeyRange::default()]),
            vec![counted(3, 0, 0, 0)],
            "nothing ended: every partition is pending"
        );

        ended_run(&storage, &plan, 0, RunStatus::Success, &[single(&["b"])]).await;

        let states = read_states(&storage).await;
        assert_eq!(
            states.counts_in(&[KeyRange::default(), range(&[], "", "b", "c")]),
            vec![counted(3, 2, 1, 0), counted(2, 1, 1, 0)]
        );
    }

    #[tokio::test]
    async fn each_partition_names_its_run_and_a_key_outside_the_backfill_has_none() {
        let storage = SurrealStorage::new_memory().await.unwrap();
        let requested = [multi(&[("hour", &["h1", "h2"]), ("zone", &["z1", "z2"])])];
        let strategy = BackfillStrategy::PerDimension {
            multi_run: vec!["zone".to_string()],
            single_run: vec!["hour".to_string()],
        };
        let plan = started_backfill(&storage, strategy, &requested).await;
        let failed = multi(&[("zone", &["z1"]), ("hour", &["h2"])]);
        ended_run(&storage, &plan, 0, RunStatus::Success, &[failed]).await;

        let cell = |hour: &str, zone: &str| multi(&[("zone", &[zone]), ("hour", &[hour])]);
        let outcome = |state, run: Option<&str>| {
            Some(PartitionOutcome {
                state,
                run_id: run.map(String::from),
            })
        };
        assert_eq!(
            read_states(&storage).await.outcomes_of(&[
                cell("h1", "z1"),
                cell("h2", "z1"),
                cell("h2", "z2"),
                cell("h3", "z1"),
                multi(&[("hour", &["h1", "h2"]), ("zone", &["z1"])]),
            ]),
            vec![
                outcome(Completed, Some("run-0")),
                outcome(Failed, Some("run-0")),
                outcome(Pending, None),
                None,
                None,
            ]
        );
    }

    #[test]
    fn a_page_starts_inside_a_key_and_goes_on_into_the_next() {
        let requested = [single(&["a", "b", "c"]), single(&[]), single(&["d", "e"])];
        let places = |offset, limit| -> Vec<(usize, String)> {
            partitions_at(&requested, offset, limit)
                .into_iter()
                .map(|(place, partition)| (place, partition.to_display()))
                .collect()
        };

        assert_eq!(
            places(2, 2),
            vec![(2, "c".to_string()), (3, "d".to_string())]
        );
        assert_eq!(places(4, 10), vec![(4, "e".to_string())]);
        assert!(places(5, 10).is_empty());
        assert!(places(0, 0).is_empty());
    }
}
