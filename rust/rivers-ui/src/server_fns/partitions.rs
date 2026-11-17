//! Server functions for the partition outline of an asset or a backfill.
//!
//! The code location splits the keys of a dimension into rows. For an asset,
//! the store counts the partitions of each row; no read grows with the
//! number of partitions of the asset. For a backfill, the counts come from
//! its requested keys and the outcome of each of its runs.

use leptos::prelude::*;
use leptos::server_fn::codec::Json;

use crate::types::{OutlineLevel, OutlinePlace, PartitionOverview};

/// The counts, the strip and the top rows of the partition outline.
#[server]
pub async fn get_partition_overview(
    loc_ns: String,
    loc_name: String,
    asset_key: String,
) -> Result<PartitionOverview, ServerFnError> {
    read::of_asset(&loc_ns, &loc_name, &asset_key, async |outline| {
        outline.overview().await
    })
    .await
}

/// The rows below `place`: its next split. With `query`, or for keys that do
/// not ascend, one page of the keys from `offset`.
#[server(input = Json)]
pub async fn get_partition_rows(
    loc_ns: String,
    loc_name: String,
    asset_key: String,
    place: OutlinePlace,
    query: String,
    offset: u64,
) -> Result<OutlineLevel, ServerFnError> {
    read::of_asset(&loc_ns, &loc_name, &asset_key, async |outline| {
        outline.rows(&place, &query, offset).await
    })
    .await
}

/// The counts, the strip and the top rows of the partition outline of a
/// backfill.
#[server]
pub async fn get_backfill_partition_overview(
    backfill_id: String,
) -> Result<PartitionOverview, ServerFnError> {
    read::of_backfill(&backfill_id, async |outline| outline.overview().await).await
}

/// The rows below `place` in the partition outline of a backfill, as
/// [`get_partition_rows`] gives them for an asset.
#[server(input = Json)]
pub async fn get_backfill_partition_rows(
    backfill_id: String,
    place: OutlinePlace,
    query: String,
    offset: u64,
) -> Result<OutlineLevel, ServerFnError> {
    read::of_backfill(&backfill_id, async |outline| {
        outline.rows(&place, &query, offset).await
    })
    .await
}

#[cfg(feature = "ssr")]
mod read {
    use anyhow::{Context, Result, anyhow};
    use leptos::prelude::{ServerFnError, expect_context};
    use rivers_api::rivers::code_location_service_client::CodeLocationServiceClient;
    use rivers_api::rivers::{
        GetPartitionGroupsRequest, GetPartitionGroupsResponse, GetPartitionKeysRequest,
        GetPartitionKeysResponse, PartitionDimension,
    };
    use rivers_core::execution::asset_partitions::{
        FailedPartitions, PartitionRow as StoredRow, PartitionState, count_states, count_states_in,
        partition_rows_in, partition_rows_of,
    };
    use rivers_core::execution::backfill_partitions::{
        BackfillCounts, BackfillPartitionState, BackfillStates, KeyRange, PartitionOutcome,
    };
    use rivers_core::execution::partition_outline::{self as keys, KeyGroup};
    use rivers_core::storage::surrealdb_backend::SurrealStorage;
    use rivers_core::storage::{
        CodeLocationContext, PartitionBounds, PartitionKey, ScopedStorage, StorageBackend,
    };
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock};
    use tokio::sync::OnceCell;
    use tonic::transport::Channel;

    use crate::helpers::format_with_commas;
    use crate::types::{
        OutlineDimension, OutlineLevel, OutlinePlace, OutlineRow, OutlineRows, PartitionCounts,
        PartitionOverview, PartitionRow, PartitionRowState, PartitionShape, PartitionStrip,
        StripBar,
    };

    /// The most bars of the strip.
    const BARS_MOST: u32 = 400;
    /// A dimension with at most this many keys has a top row for each key.
    const KEYS_MOST: u64 = 120;
    /// The most top rows of a dimension with more keys.
    const ROWS_MOST: u32 = 31;
    /// The keys of one page, for keys that are read in pages.
    const PAGE: u64 = crate::types::OUTLINE_PAGE;
    /// The most marks along the strip.
    const MARKS_MOST: usize = 12;

    /// The keys of one dimension as the rows of one level.
    #[derive(Debug, Clone, Default)]
    pub(super) struct Split {
        pub groups: Vec<KeyGroup>,
        /// Each group is one key.
        pub of_keys: bool,
        /// The unit of time of one group. Empty when not known.
        pub unit: String,
        /// The unit of time of one key. Empty when not known.
        pub key_unit: String,
        /// The keys are windows of time.
        pub time: bool,
        /// The places where a key is cut into the starts that the keys of a
        /// group share. Empty: a key is cut where a run of digits ends.
        pub cuts: Vec<usize>,
    }

    impl Split {
        fn of_keys(keys: Vec<String>) -> Self {
            Self {
                groups: keys
                    .into_iter()
                    .map(|key| KeyGroup {
                        label: key.clone(),
                        first: key.clone(),
                        last: key,
                        keys: 1,
                    })
                    .collect(),
                of_keys: true,
                ..Self::default()
            }
        }

        fn of_reply(reply: GetPartitionGroupsResponse) -> Self {
            Self {
                groups: reply
                    .groups
                    .into_iter()
                    .map(|group| KeyGroup {
                        label: group.label,
                        first: group.first_key,
                        last: group.last_key,
                        keys: group.keys as usize,
                    })
                    .collect(),
                of_keys: reply.of_keys,
                unit: reply.unit,
                key_unit: reply.key_unit,
                time: reply.time,
                cuts: reply.cuts.into_iter().map(|cut| cut as usize).collect(),
            }
        }
    }

    /// The partitions with a key of the first dimension of `place` from
    /// `first` to `last`.
    pub(super) fn bounds_of(place: &OutlinePlace, first: &str, last: &str) -> PartitionBounds {
        match place.dimensions.first() {
            Some(dimension) if !dimension.name.is_empty() => {
                PartitionBounds::of_dimension(&place.fixed, &dimension.name, first, last)
            }
            _ => PartitionBounds::of_keys(first, last),
        }
    }

    /// The partition with `key` for the first dimension of `place`, which is
    /// the last dimension of the asset.
    pub(super) fn partition_of(place: &OutlinePlace, key: &str) -> PartitionKey {
        match place.dimensions.first() {
            Some(dimension) if !dimension.name.is_empty() => {
                let mut dims: Vec<(String, Vec<String>)> = place
                    .fixed
                    .iter()
                    .map(|(name, value)| (name.clone(), vec![value.clone()]))
                    .collect();
                dims.push((dimension.name.clone(), vec![key.to_string()]));
                dims.sort_by(|a, b| a.0.cmp(&b.0));
                PartitionKey::Multi { dims }
            }
            _ => PartitionKey::Single {
                keys: vec![key.to_string()],
            },
        }
    }

    /// The partitions of one key of the first dimension of `place`.
    fn partitions_per_key(place: &OutlinePlace) -> u64 {
        place
            .dimensions
            .iter()
            .skip(1)
            .fold(1u64, |product, dimension| {
                product.saturating_mul(dimension.keys)
            })
    }

    /// The head of the column of the rows of `place`.
    fn heading_of(place: &OutlinePlace, split: &Split) -> String {
        match place.dimensions.first() {
            Some(dimension) if !dimension.name.is_empty() => dimension.name.clone(),
            _ if !split.key_unit.is_empty() => split.key_unit.clone(),
            _ => "partition".to_string(),
        }
    }

    fn counted(count: u64, one: &str) -> String {
        let many = if count == 1 { "" } else { "s" };
        format!("{} {one}{many}", format_with_commas(count))
    }

    /// The rows of `split` that open to more rows, with `counts` the
    /// partitions of each group by state.
    pub(super) fn group_rows(
        place: &OutlinePlace,
        split: &Split,
        counts: Vec<PartitionCounts>,
    ) -> Vec<OutlineRow> {
        let Some((dimension, below)) = place.dimensions.split_first() else {
            return Vec::new();
        };

        split
            .groups
            .iter()
            .zip(counts)
            .map(|(group, counts)| {
                // One key opens to the keys of the next dimension. A group
                // of keys opens to its next split.
                let (holds, place) = match (split.of_keys, below.first()) {
                    (true, Some(next)) => {
                        let mut fixed = place.fixed.clone();
                        fixed.push((dimension.name.clone(), group.first.clone()));
                        (
                            format!("{} × {}", format_with_commas(next.keys), next.name),
                            OutlinePlace {
                                fixed,
                                dimensions: below.to_vec(),
                                ..OutlinePlace::default()
                            },
                        )
                    }
                    _ => {
                        let unit = match split.key_unit.as_str() {
                            "" => "key",
                            unit => unit,
                        };
                        (
                            counted(group.keys as u64, unit),
                            OutlinePlace {
                                first_key: group.first.clone(),
                                last_key: group.last.clone(),
                                ..place.clone()
                            },
                        )
                    }
                };

                OutlineRow {
                    label: group.label.clone(),
                    holds,
                    counts,
                    place,
                }
            })
            .collect()
    }

    /// What one bar of the strip is, and how many partitions it holds when
    /// every bar holds as many.
    pub(super) fn strip_note(place: &OutlinePlace, split: &Split, bars: &[StripBar]) -> String {
        let name = place
            .dimensions
            .first()
            .map(|dimension| dimension.name.as_str())
            .unwrap_or_default();
        let bar = match (split.of_keys, split.key_unit.as_str(), split.unit.as_str()) {
            (true, "", _) if name.is_empty() => "partition",
            (true, "", _) => name,
            (true, key_unit, _) => key_unit,
            (false, _, "") => "group of keys",
            (false, _, unit) => unit,
        };

        let size = bars.first().map(|bar| bar.counts.total).unwrap_or_default();
        let same_size = bars.iter().all(|bar| bar.counts.total == size);
        if same_size && size > 1 {
            format!(
                "one bar per {bar} · {} partitions each",
                format_with_commas(size)
            )
        } else {
            format!("one bar per {bar}")
        }
    }

    fn row_of(key: String, stored: StoredRow) -> PartitionRow {
        PartitionRow {
            key,
            state: match stored.state {
                PartitionState::Materialized => PartitionRowState::Materialized,
                PartitionState::Failed => PartitionRowState::Failed,
                PartitionState::Missing => PartitionRowState::Missing,
            },
            materialized_at: stored.materialized_at,
            run_id: stored.run_id,
            failed_at: stored.failed_at,
        }
    }

    fn backfill_row(key: String, outcome: PartitionOutcome) -> PartitionRow {
        PartitionRow {
            key,
            state: match outcome.state {
                BackfillPartitionState::Completed => PartitionRowState::Done,
                BackfillPartitionState::Failed => PartitionRowState::Failed,
                BackfillPartitionState::Canceled => PartitionRowState::Canceled,
                BackfillPartitionState::Pending => PartitionRowState::Pending,
            },
            materialized_at: None,
            run_id: outcome.run_id,
            failed_at: None,
        }
    }

    fn counts_of(counts: BackfillCounts) -> PartitionCounts {
        PartitionCounts::of_backfill(
            counts.total,
            counts.completed,
            counts.failed,
            counts.canceled,
        )
    }

    /// The counts of one partition in `state`.
    fn counts_of_state(state: PartitionRowState) -> PartitionCounts {
        use PartitionRowState::*;
        PartitionCounts {
            total: 1,
            done: matches!(state, Materialized | Done) as u64,
            failed: (state == Failed) as u64,
            canceled: (state == Canceled) as u64,
        }
    }

    /// Where the rows read the state of each partition.
    pub(super) enum States<'a> {
        /// The stored state of an asset, with its failed partitions in the
        /// place of the read.
        Asset(&'a FailedPartitions),
        /// What happened to each partition in a backfill.
        Backfill(&'a BackfillStates),
    }

    /// The keys of one dimension of a backfill.
    struct BackfillKeys {
        /// In the order they were asked for. The keys of a dynamic set are
        /// sorted: they are read in ascending order only.
        in_order: Vec<String>,
        /// A sorted copy of `in_order`, made on the first read when
        /// `in_order` does not ascend.
        sorted: OnceLock<Option<Vec<String>>>,
    }

    impl BackfillKeys {
        fn sorted(&self) -> &[String] {
            let copy = self.sorted.get_or_init(|| {
                (!self.in_order.is_sorted()).then(|| {
                    let mut sorted = self.in_order.clone();
                    sorted.sort_unstable();
                    sorted
                })
            });
            copy.as_deref().unwrap_or(&self.in_order)
        }
    }

    /// The backfill of an outline: what happened to its partitions, and the
    /// keys of its dimensions, which take the place of the keys of the asset.
    pub(super) struct Backfill {
        states: BackfillStates,
        keys: Mutex<HashMap<String, Arc<BackfillKeys>>>,
    }

    impl Backfill {
        pub(super) fn new(states: BackfillStates) -> Self {
            Self {
                states,
                keys: Mutex::default(),
            }
        }

        fn keys(&self, dimension: &str, dynamic_name: &str) -> Arc<BackfillKeys> {
            let mut keys = self.keys.lock().expect("no read panics with the lock held");
            if let Some(found) = keys.get(dimension) {
                return Arc::clone(found);
            }

            let mut in_order = self.states.keys_of(dimension);
            if !dynamic_name.is_empty() {
                in_order.sort_unstable();
            }
            let found = Arc::new(BackfillKeys {
                in_order,
                sorted: OnceLock::new(),
            });
            keys.insert(dimension.to_string(), Arc::clone(&found));
            found
        }

        /// One page of the partitions in the order they were asked for, for a
        /// backfill whose code location gives no keys.
        fn page(&self, offset: u64) -> OutlineLevel {
            let page = self.states.page(offset, PAGE);
            let read = offset.saturating_add(page.len() as u64);
            OutlineLevel {
                heading: "partition".to_string(),
                more: self.states.total().saturating_sub(read),
                rows: OutlineRows::Partitions(
                    page.into_iter()
                        .map(|(partition, outcome)| backfill_row(partition.to_display(), outcome))
                        .collect(),
                ),
            }
        }
    }

    /// The code location as the outline asks it for keys.
    pub(super) trait KeySource {
        async fn groups(
            &self,
            request: GetPartitionGroupsRequest,
        ) -> Result<GetPartitionGroupsResponse>;

        async fn keys(&self, request: GetPartitionKeysRequest) -> Result<GetPartitionKeysResponse>;
    }

    impl KeySource for CodeLocationServiceClient<Channel> {
        async fn groups(
            &self,
            request: GetPartitionGroupsRequest,
        ) -> Result<GetPartitionGroupsResponse> {
            let reply = self.clone().get_partition_groups(request).await;
            Ok(reply
                .map_err(|status| anyhow!("{}", status.message()))?
                .into_inner())
        }

        async fn keys(&self, request: GetPartitionKeysRequest) -> Result<GetPartitionKeysResponse> {
            let reply = self.clone().get_partition_keys(request).await;
            Ok(reply
                .map_err(|status| anyhow!("{}", status.message()))?
                .into_inner())
        }
    }

    /// The reads of the partition outline of one asset or backfill, for one
    /// request.
    pub(super) struct Outline<'a, S, K> {
        storage: &'a ScopedStorage<'a, S>,
        /// `None`: the code location does not answer.
        source: Option<K>,
        /// The asset whose partition definition the code location splits.
        asset_key: &'a str,
        /// The name and the sorted keys of the dynamic set of the request.
        dynamic: OnceCell<(String, Vec<String>)>,
        /// `Some`: the outline of a backfill of the asset.
        backfill: Option<Backfill>,
    }

    /// The result of `read` on the outline of `asset_key`.
    pub(super) async fn of_asset<T>(
        loc_ns: &str,
        loc_name: &str,
        asset_key: &str,
        read: impl AsyncFnOnce(
            &Outline<'_, SurrealStorage, CodeLocationServiceClient<Channel>>,
        ) -> Result<T>,
    ) -> Result<T, ServerFnError> {
        let ctx = crate::server_fns::resolve_identity(loc_ns, loc_name).await?;
        let state = expect_context::<crate::state::AppState>();
        let storage = state.storage.for_code_location(&ctx);

        let outline = Outline {
            storage: &storage,
            source: state
                .connect_to(loc_ns, loc_name)
                .await
                .ok()
                .map(|(_, client)| client),
            asset_key,
            dynamic: OnceCell::new(),
            backfill: None,
        };
        read(&outline)
            .await
            .map_err(|e| ServerFnError::new(e.to_string()))
    }

    /// The result of `read` on the outline of backfill `backfill_id`. The
    /// code location of the backfill splits the keys of its first asset.
    pub(super) async fn of_backfill<T>(
        backfill_id: &str,
        read: impl AsyncFnOnce(
            &Outline<'_, SurrealStorage, CodeLocationServiceClient<Channel>>,
        ) -> Result<T>,
    ) -> Result<T, ServerFnError> {
        let state = expect_context::<crate::state::AppState>();
        let found = async {
            let record = state
                .storage
                .get_backfill(backfill_id)
                .await?
                .context("backfill not found")?;
            let states = BackfillStates::read(&*state.storage, &record).await?;
            anyhow::Ok((record, states))
        };
        let (record, states) = found.await.map_err(|e| ServerFnError::new(e.to_string()))?;

        let ctx = CodeLocationContext::new(record.code_location_id.clone());
        let storage = state.storage.for_code_location(&ctx);
        let entries = state.registry.list().await.unwrap_or_default();
        let entry = entries
            .iter()
            .find(|entry| entry.identity == record.code_location_id);
        let source = match entry {
            Some(entry) => state
                .connect_to(&entry.namespace, &entry.name)
                .await
                .ok()
                .map(|(_, client)| client),
            None => None,
        };

        let outline = Outline {
            storage: &storage,
            source,
            asset_key: record
                .asset_selection
                .first()
                .map(String::as_str)
                .unwrap_or_default(),
            dynamic: OnceCell::new(),
            backfill: Some(Backfill::new(states)),
        };
        read(&outline)
            .await
            .map_err(|e| ServerFnError::new(e.to_string()))
    }

    impl<S: StorageBackend, K: KeySource> Outline<'_, S, K> {
        pub async fn overview(&self) -> Result<PartitionOverview> {
            match &self.backfill {
                Some(backfill) => self.backfill_overview(backfill).await,
                None => self.asset_overview().await,
            }
        }

        async fn asset_overview(&self) -> Result<PartitionOverview> {
            let failed = FailedPartitions::read(self.storage, self.asset_key, None).await?;
            let states = count_states(self.storage, self.asset_key, &failed).await?;

            let Some(reply) = self.first_split().await else {
                let top = self.stored_level(0, Some(&failed)).await?;
                return Ok(PartitionOverview {
                    counts: PartitionCounts::of(
                        states.materialized + states.failed,
                        states.materialized,
                        states.failed,
                    ),
                    shape: PartitionShape::Keys,
                    strip: None,
                    top,
                    root: OutlinePlace::default(),
                    from_definition: false,
                });
            };

            let root = OutlinePlace {
                dimensions: self.dimensions(&reply.dimensions).await?,
                ..OutlinePlace::default()
            };
            let leading = root
                .dimensions
                .first()
                .context("the asset has no partition dimension")?;
            let total = root.dimensions.iter().fold(1u64, |product, dimension| {
                product.saturating_mul(dimension.keys)
            });
            let counts = PartitionCounts::of(total, states.materialized, states.failed);

            let bars = if leading.dynamic_name.is_empty() {
                reply.ascending.then(|| Split::of_reply(reply))
            } else {
                self.split(&root, BARS_MOST).await?
            };
            self.overview_of(root, counts, bars, &States::Asset(&failed))
                .await
        }

        /// The overview of a backfill: the dimensions of the asset with the
        /// keys that the backfill holds, from its first key to its last.
        async fn backfill_overview(&self, backfill: &Backfill) -> Result<PartitionOverview> {
            let counts = backfill
                .states
                .counts_in(&[KeyRange::default()])
                .pop()
                .map(counts_of)
                .unwrap_or_default();

            let Some(reply) = self.first_split().await else {
                return Ok(PartitionOverview {
                    counts,
                    shape: PartitionShape::Keys,
                    strip: None,
                    top: backfill.page(0),
                    root: OutlinePlace::default(),
                    from_definition: false,
                });
            };

            let mut root = OutlinePlace {
                dimensions: reply
                    .dimensions
                    .iter()
                    .map(|dimension| OutlineDimension {
                        name: dimension.name.clone(),
                        keys: backfill
                            .keys(&dimension.name, &dimension.dynamic_name)
                            .in_order
                            .len() as u64,
                        dynamic_name: dimension.dynamic_name.clone(),
                    })
                    .collect(),
                ..OutlinePlace::default()
            };
            let leading = root
                .dimensions
                .first()
                .context("the asset has no partition dimension")?;
            if leading.dynamic_name.is_empty() && reply.ascending {
                let keys = backfill.keys(&leading.name, &leading.dynamic_name);
                let sorted = keys.sorted();
                if let (Some(first), Some(last)) = (sorted.first(), sorted.last()) {
                    root.first_key = first.clone();
                    root.last_key = last.clone();
                }
            }

            let bars = self.split(&root, BARS_MOST).await?;
            self.overview_of(root, counts, bars, &States::Backfill(&backfill.states))
                .await
        }

        /// The overview of the partitions of `root` with `counts`, and
        /// `bars` the split of the strip. `None`: the keys do not ascend.
        async fn overview_of(
            &self,
            root: OutlinePlace,
            counts: PartitionCounts,
            bars: Option<Split>,
            states: &States<'_>,
        ) -> Result<PartitionOverview> {
            let leading = root
                .dimensions
                .first()
                .context("the asset has no partition dimension")?;

            let (strip, top, shape_of) = match bars {
                Some(bars) => {
                    // The split of the strip is the split of the top rows
                    // when it is no finer than the top rows can be.
                    let most = if leading.keys <= KEYS_MOST {
                        KEYS_MOST as u32
                    } else {
                        ROWS_MOST
                    };
                    let finer;
                    let top = if bars.groups.len() <= most as usize {
                        &bars
                    } else {
                        finer = self
                            .split(&root, most)
                            .await?
                            .context("the keys of the dimension do not ascend")?;
                        &finer
                    };

                    let (strip, level) = futures_util::try_join!(
                        self.strip(&root, &bars, true, states),
                        self.level(&root, top, true, 0, states),
                    )?;
                    (Some(strip), level, bars)
                }
                None => {
                    let (page, keys_total) = self.keys_page(&root, "", 0, BARS_MOST as u64).await?;
                    let shown = page.len().min(PAGE as usize);
                    let more = keys_total.saturating_sub(shown as u64);
                    let top = Split::of_keys(page[..shown].to_vec());

                    let level = self.level(&root, &top, false, more, states).await?;
                    let strip = match keys_total <= BARS_MOST as u64 {
                        true => {
                            let bars = Split::of_keys(page);
                            Some(self.strip(&root, &bars, false, states).await?)
                        }
                        false => None,
                    };
                    (strip, level, Split::default())
                }
            };

            let shape = match (root.dimensions.len(), shape_of.groups.as_slice()) {
                (1, [first, .., last]) if shape_of.time => PartitionShape::Windows {
                    unit: shape_of.key_unit.clone(),
                    first: first.first.clone(),
                    last: last.last.clone(),
                },
                (1, _) => PartitionShape::Keys,
                _ => PartitionShape::Dimensions(
                    root.dimensions
                        .iter()
                        .map(|dimension| (dimension.name.clone(), dimension.keys))
                        .collect(),
                ),
            };

            Ok(PartitionOverview {
                counts,
                shape,
                strip,
                top,
                root,
                from_definition: true,
            })
        }

        pub async fn rows(
            &self,
            place: &OutlinePlace,
            query: &str,
            offset: u64,
        ) -> Result<OutlineLevel> {
            if place.dimensions.is_empty() {
                return match &self.backfill {
                    Some(backfill) => Ok(backfill.page(offset)),
                    None => self.stored_level(offset, None).await,
                };
            }

            let next = match (query.is_empty(), offset) {
                (true, 0) => self.split(place, 0).await?,
                _ => None,
            };
            let (split, in_order, more) = match next {
                Some(split) => (split, true, 0),
                None => {
                    let (page, keys_total) = self.keys_page(place, query, offset, PAGE).await?;
                    let read = offset.saturating_add(page.len() as u64);
                    (Split::of_keys(page), false, keys_total.saturating_sub(read))
                }
            };

            if let Some(backfill) = &self.backfill {
                let states = States::Backfill(&backfill.states);
                return self.level(place, &split, in_order, more, &states).await;
            }

            // The failed partitions that the rows can show, and no others.
            let bounds: Vec<PartitionBounds> = match (in_order, split.groups.as_slice()) {
                (true, [first, .., last]) => vec![bounds_of(place, &first.first, &last.last)],
                (_, groups) => groups
                    .iter()
                    .map(|group| bounds_of(place, &group.first, &group.last))
                    .collect(),
            };
            let failed =
                FailedPartitions::read(self.storage, self.asset_key, Some(&bounds)).await?;

            self.level(place, &split, in_order, more, &States::Asset(&failed))
                .await
        }

        /// The split of the strip for the first dimension of the asset, with
        /// the dimensions of the asset. `None`: the code location does not
        /// answer, or the asset has no partitions there.
        async fn first_split(&self) -> Option<GetPartitionGroupsResponse> {
            let request = GetPartitionGroupsRequest {
                asset_key: self.asset_key.to_string(),
                max_groups: BARS_MOST,
                ..Default::default()
            };
            let reply = self.source.as_ref()?.groups(request).await.ok()?;
            (!reply.dimensions.is_empty()).then_some(reply)
        }

        /// The dimensions of the asset. The number of the keys of a dynamic
        /// set is in storage.
        async fn dimensions(
            &self,
            of_asset: &[PartitionDimension],
        ) -> Result<Vec<OutlineDimension>> {
            let mut dimensions = Vec::with_capacity(of_asset.len());
            for dimension in of_asset {
                let keys = match dimension.dynamic_name.as_str() {
                    "" => dimension.keys,
                    name => self.storage.count_dynamic_partitions(name).await?,
                };
                dimensions.push(OutlineDimension {
                    name: dimension.name.clone(),
                    keys,
                    dynamic_name: dimension.dynamic_name.clone(),
                });
            }
            Ok(dimensions)
        }

        /// The keys of the dynamic set `name`, in ascending order. One
        /// request reads the keys of one set: the dimension of its place.
        async fn dynamic_keys(&self, name: &str) -> Result<&[String]> {
            let (of_set, keys) = self
                .dynamic
                .get_or_try_init(|| async {
                    let mut keys = self.storage.get_dynamic_partitions(name).await?;
                    keys.sort();
                    anyhow::Ok((name.to_string(), keys))
                })
                .await?;
            anyhow::ensure!(of_set == name, "a request reads one dynamic set");
            Ok(keys)
        }

        fn source(&self) -> Result<&K> {
            self.source
                .as_ref()
                .context("the code location does not answer")
        }

        /// The keys of `place` as rows: the finest split with at most `most`
        /// groups, or with 0 the next split. `None`: the keys do not ascend,
        /// and are read in pages.
        async fn split(&self, place: &OutlinePlace, most: u32) -> Result<Option<Split>> {
            let dimension = place
                .dimensions
                .first()
                .context("the place has no dimension")?;

            let backfill_keys = self
                .backfill
                .as_ref()
                .map(|b| b.keys(&dimension.name, &dimension.dynamic_name));
            if !dimension.dynamic_name.is_empty() {
                let all = match &backfill_keys {
                    Some(keys) => keys.sorted(),
                    None => self.dynamic_keys(&dimension.dynamic_name).await?,
                };
                let places = keys::between(all, &place.first_key, &place.last_key);
                let split = match most {
                    0 => keys::children(all, places),
                    most => keys::split(all, places, most as usize),
                };
                return Ok(Some(Split {
                    groups: split.groups,
                    of_keys: split.of_keys,
                    ..Split::default()
                }));
            }

            let request = GetPartitionGroupsRequest {
                asset_key: self.asset_key.to_string(),
                dimension: dimension.name.clone(),
                first_key: place.first_key.clone(),
                last_key: place.last_key.clone(),
                max_groups: most,
            };
            let reply = self.source()?.groups(request).await?;
            if !reply.ascending {
                return Ok(None);
            }

            // A group of a backfill holds the keys of the backfill only.
            let mut split = Split::of_reply(reply);
            if let Some(keys) = backfill_keys {
                let sorted = keys.sorted();
                split.groups.retain_mut(|group| {
                    group.keys = keys::between(sorted, &group.first, &group.last).len();
                    group.keys > 0
                });
            }
            Ok(Some(split))
        }

        /// `limit` keys of the first dimension of `place` from `offset`, in
        /// the order of the definition, and the number of all of them. With
        /// `query`: the keys that hold that text. The keys of a backfill are
        /// in the order they were asked for.
        async fn keys_page(
            &self,
            place: &OutlinePlace,
            query: &str,
            offset: u64,
            limit: u64,
        ) -> Result<(Vec<String>, u64)> {
            let dimension = place
                .dimensions
                .first()
                .context("the place has no dimension")?;

            let backfill_keys = self
                .backfill
                .as_ref()
                .map(|b| b.keys(&dimension.name, &dimension.dynamic_name));
            let all = match (&backfill_keys, dimension.dynamic_name.is_empty()) {
                (Some(keys), true) => Some(keys.in_order.as_slice()),
                (Some(keys), false) => Some(keys.sorted()),
                (None, false) => Some(self.dynamic_keys(&dimension.dynamic_name).await?),
                (None, true) => None,
            };
            if let Some(all) = all {
                let matches = || all.iter().filter(|key| key.contains(query));
                let page = matches()
                    .skip(usize::try_from(offset).unwrap_or(usize::MAX))
                    .take(limit as usize)
                    .cloned()
                    .collect();
                return Ok((page, matches().count() as u64));
            }

            let request = GetPartitionKeysRequest {
                asset_key: self.asset_key.to_string(),
                offset,
                limit,
                query: query.to_string(),
                dimension: dimension.name.clone(),
            };
            let reply = self.source()?.keys(request).await?;
            Ok((reply.keys, reply.total))
        }

        /// The partitions of each group of `split` by state.
        async fn counts(
            &self,
            place: &OutlinePlace,
            split: &Split,
            states: &States<'_>,
        ) -> Result<Vec<PartitionCounts>> {
            let failed = match states {
                States::Asset(failed) => failed,
                States::Backfill(backfill) => {
                    let dimension = place
                        .dimensions
                        .first()
                        .map(|dimension| dimension.name.clone())
                        .unwrap_or_default();
                    let ranges: Vec<KeyRange> = split
                        .groups
                        .iter()
                        .map(|group| KeyRange {
                            fixed: place.fixed.clone(),
                            dimension: dimension.clone(),
                            first: group.first.clone(),
                            last: group.last.clone(),
                        })
                        .collect();
                    return Ok(backfill
                        .counts_in(&ranges)
                        .into_iter()
                        .map(counts_of)
                        .collect());
                }
            };

            let per_key = partitions_per_key(place);
            let bounds: Vec<PartitionBounds> = split
                .groups
                .iter()
                .map(|group| bounds_of(place, &group.first, &group.last))
                .collect();
            let states = count_states_in(self.storage, self.asset_key, failed, &bounds).await?;

            Ok(split
                .groups
                .iter()
                .zip(states)
                .map(|(group, states)| {
                    PartitionCounts::of(
                        (group.keys as u64).saturating_mul(per_key),
                        states.materialized,
                        states.failed,
                    )
                })
                .collect())
        }

        /// The partitions of `split`, the keys of the last dimension. With
        /// `in_order`, the keys follow each other: one read gives their rows.
        /// `None` for a partition that a backfill does not hold.
        async fn partitions(
            &self,
            place: &OutlinePlace,
            split: &Split,
            in_order: bool,
            states: &States<'_>,
        ) -> Result<Vec<Option<PartitionRow>>> {
            let partitions: Vec<PartitionKey> = split
                .groups
                .iter()
                .map(|group| partition_of(place, &group.first))
                .collect();

            let failed = match states {
                States::Asset(failed) => failed,
                States::Backfill(backfill) => {
                    return Ok(split
                        .groups
                        .iter()
                        .zip(backfill.outcomes_of(&partitions))
                        .map(|(group, outcome)| Some(backfill_row(group.first.clone(), outcome?)))
                        .collect());
                }
            };

            let stored = match (in_order, split.groups.as_slice()) {
                (true, [first, .., last]) => {
                    let bounds = bounds_of(place, &first.first, &last.last);
                    partition_rows_in(self.storage, self.asset_key, failed, &bounds, &partitions)
                        .await?
                }
                _ => partition_rows_of(self.storage, self.asset_key, failed, &partitions).await?,
            };

            Ok(split
                .groups
                .iter()
                .zip(stored)
                .map(|(group, stored)| Some(row_of(group.first.clone(), stored)))
                .collect())
        }

        /// The rows of `split`, a split of the keys of `place`. With
        /// `in_order`, the groups follow each other in ascending order. A
        /// backfill shows no row without a partition of its own.
        async fn level(
            &self,
            place: &OutlinePlace,
            split: &Split,
            in_order: bool,
            more: u64,
            states: &States<'_>,
        ) -> Result<OutlineLevel> {
            let heading = heading_of(place, split);
            let newest_first = in_order && split.time;

            let rows = if split.of_keys && place.dimensions.len() == 1 {
                let mut rows: Vec<PartitionRow> = self
                    .partitions(place, split, in_order, states)
                    .await?
                    .into_iter()
                    .flatten()
                    .collect();
                if newest_first {
                    rows.reverse();
                }
                OutlineRows::Partitions(rows)
            } else {
                let counts = self.counts(place, split, states).await?;
                let mut rows = group_rows(place, split, counts);
                if matches!(states, States::Backfill(_)) {
                    rows.retain(|row| row.counts.total > 0);
                }
                if newest_first {
                    rows.reverse();
                }
                OutlineRows::Groups(rows)
            };

            Ok(OutlineLevel {
                heading,
                rows,
                more,
            })
        }

        /// The strip along `split`, a split of every key of `place`. With
        /// `in_order`, the groups follow each other in ascending order.
        async fn strip(
            &self,
            place: &OutlinePlace,
            split: &Split,
            in_order: bool,
            states: &States<'_>,
        ) -> Result<PartitionStrip> {
            let counts = if split.of_keys && place.dimensions.len() == 1 {
                self.partitions(place, split, in_order, states)
                    .await?
                    .into_iter()
                    .map(|row| {
                        row.map_or_else(PartitionCounts::default, |row| counts_of_state(row.state))
                    })
                    .collect()
            } else {
                self.counts(place, split, states).await?
            };

            let bars: Vec<StripBar> = split
                .groups
                .iter()
                .zip(counts)
                .map(|(group, counts)| StripBar {
                    label: group.label.clone(),
                    counts,
                })
                .collect();

            Ok(PartitionStrip {
                heading: heading_of(place, split),
                note: strip_note(place, split, &bars),
                marks: keys::marks(&split.groups, &split.cuts, MARKS_MOST)
                    .into_iter()
                    .map(|(at, text)| (at as u32, text))
                    .collect(),
                bars,
            })
        }

        /// One page of the materialized partitions, for an asset whose code
        /// location gives no keys. Without `failed`, the failed partitions
        /// of the page are read.
        async fn stored_level(
            &self,
            offset: u64,
            failed: Option<&FailedPartitions>,
        ) -> Result<OutlineLevel> {
            let partitions: Vec<PartitionKey> = self
                .storage
                .get_materialized_partitions_page(self.asset_key, offset, PAGE)
                .await?
                .iter()
                .flat_map(PartitionKey::members)
                .collect();

            let of_page;
            let failed = match failed {
                Some(failed) => failed,
                None => {
                    let bounds: Vec<PartitionBounds> = partitions
                        .iter()
                        .map(|partition| PartitionBounds {
                            lower: partition.clone(),
                            upper: partition.clone(),
                        })
                        .collect();
                    of_page =
                        FailedPartitions::read(self.storage, self.asset_key, Some(&bounds)).await?;
                    &of_page
                }
            };
            let stored =
                partition_rows_of(self.storage, self.asset_key, failed, &partitions).await?;
            let with_row = self
                .storage
                .count_materialized_partitions(self.asset_key)
                .await?;
            let read = offset.saturating_add(partitions.len() as u64);

            Ok(OutlineLevel {
                heading: "partition".to_string(),
                more: with_row.saturating_sub(read),
                rows: OutlineRows::Partitions(
                    partitions
                        .iter()
                        .zip(stored)
                        .map(|(partition, stored)| row_of(partition.to_display(), stored))
                        .collect(),
                ),
            })
        }
    }

    #[cfg(test)]
    mod tests {
        use std::borrow::Cow;

        use rivers_api::rivers::PartitionKeyGroup;
        use rivers_core::execution::backfill::plan_runs;
        use rivers_core::storage::{
            AssetRecord, BackfillFailurePolicy, BackfillRecord, BackfillStatus, BackfillStrategy,
            CodeLocationContext, DEFAULT_CODE_LOCATION_ID, EventRecord, EventType, LaunchedBy,
            RunRecord, RunStatus,
        };

        use super::*;

        fn dimension(name: &str, keys: u64) -> OutlineDimension {
            OutlineDimension {
                name: name.to_string(),
                keys,
                dynamic_name: String::new(),
            }
        }

        /// Every hour of an asset with 1,440 hours, each with 40 zones.
        fn hours() -> OutlinePlace {
            OutlinePlace {
                dimensions: vec![dimension("hour", 1440), dimension("zone", 40)],
                ..OutlinePlace::default()
            }
        }

        /// The zones of one hour of that asset.
        fn zones_of(hour: &str) -> OutlinePlace {
            OutlinePlace {
                fixed: vec![("hour".to_string(), hour.to_string())],
                dimensions: vec![dimension("zone", 40)],
                ..OutlinePlace::default()
            }
        }

        /// Every key of an asset with one dimension.
        fn days() -> OutlinePlace {
            OutlinePlace {
                dimensions: vec![dimension("", 90)],
                ..OutlinePlace::default()
            }
        }

        fn cell(hour: &str, zone: &str) -> PartitionKey {
            PartitionKey::Multi {
                dims: vec![
                    ("hour".to_string(), vec![hour.to_string()]),
                    ("zone".to_string(), vec![zone.to_string()]),
                ],
            }
        }

        fn single(key: &str) -> PartitionKey {
            PartitionKey::Single {
                keys: vec![key.to_string()],
            }
        }

        fn group(label: &str, first: &str, last: &str, keys: usize) -> KeyGroup {
            KeyGroup {
                label: label.to_string(),
                first: first.to_string(),
                last: last.to_string(),
                keys,
            }
        }

        fn months() -> Split {
            Split {
                groups: vec![
                    group("2024-01", "2024-01-01T00:00", "2024-01-31T23:00", 744),
                    group("2024-02", "2024-02-01T00:00", "2024-02-29T23:00", 696),
                ],
                unit: "month".to_string(),
                key_unit: "hour".to_string(),
                time: true,
                ..Split::default()
            }
        }

        fn bars(totals: &[u64]) -> Vec<StripBar> {
            totals
                .iter()
                .map(|total| StripBar {
                    label: String::new(),
                    counts: PartitionCounts::of(*total, 0, 0),
                })
                .collect()
        }

        /// One dimension of an asset as a code location holds it.
        struct Dimension {
            name: &'static str,
            keys: Vec<String>,
            /// The unit of time of one key. Empty: the keys are not windows.
            key_unit: &'static str,
            /// The dynamic set that holds the keys. Empty: `keys` are these.
            dynamic: &'static str,
            /// The ends of the fields of the key format. Empty: a key is cut
            /// where a run of digits ends.
            cuts: Vec<usize>,
        }

        impl keys::Keys for Dimension {
            fn len(&self) -> usize {
                self.keys.len()
            }

            fn key(&self, index: usize) -> Cow<'_, str> {
                Cow::Borrowed(&self.keys[index])
            }

            fn cuts(&self, key: &str) -> Vec<usize> {
                keys::cuts(key, &self.cuts)
            }
        }

        fn named(name: &'static str, keys: &[&str]) -> Dimension {
            Dimension {
                name,
                keys: keys.iter().map(|key| key.to_string()).collect(),
                key_unit: "",
                dynamic: "",
                cuts: vec![],
            }
        }

        /// The hours of January and February 2024.
        fn hours_of_two_months() -> Dimension {
            let days = (1..=31)
                .map(|day| (1, day))
                .chain((1..=29).map(|day| (2, day)));
            Dimension {
                name: "hour",
                keys: days
                    .flat_map(|(month, day)| {
                        (0..24).map(move |hour| format!("2024-{month:02}-{day:02}T{hour:02}:00"))
                    })
                    .collect(),
                key_unit: "hour",
                dynamic: "",
                cuts: vec![],
            }
        }

        /// The hours of January and February 2024 with the key format
        /// `%Y%m%d%H`.
        fn hours_with_no_separator() -> Dimension {
            let hours = hours_of_two_months();
            Dimension {
                name: "",
                keys: hours
                    .keys
                    .iter()
                    .map(|key| key[..13].replace(['-', 'T'], ""))
                    .collect(),
                cuts: vec![4, 6, 8, 10],
                ..hours
            }
        }

        /// A code location that answers for one asset.
        struct Definition(Vec<Dimension>);

        impl Definition {
            fn dimension(&self, name: &str) -> Result<&Dimension> {
                let mut dimensions: Vec<&Dimension> = self.0.iter().collect();
                dimensions.sort_by_key(|dimension| dimension.name);
                match name {
                    "" => dimensions.first().copied(),
                    name => dimensions
                        .into_iter()
                        .find(|dimension| dimension.name == name),
                }
                .context("no such dimension")
            }
        }

        impl KeySource for Definition {
            async fn groups(
                &self,
                request: GetPartitionGroupsRequest,
            ) -> Result<GetPartitionGroupsResponse> {
                let dimension = self.dimension(&request.dimension)?;
                let mut dimensions: Vec<PartitionDimension> = self
                    .0
                    .iter()
                    .map(|dimension| PartitionDimension {
                        name: dimension.name.to_string(),
                        keys: dimension.keys.len() as u64,
                        dynamic_name: dimension.dynamic.to_string(),
                    })
                    .collect();
                dimensions.sort_by(|a, b| a.name.cmp(&b.name));
                let reply = GetPartitionGroupsResponse {
                    dimensions,
                    ..Default::default()
                };

                let all = &dimension.keys;
                if !dimension.dynamic.is_empty() {
                    return Ok(reply);
                }
                if !keys::ascending(all) {
                    return Ok(GetPartitionGroupsResponse {
                        total_keys: all.len() as u64,
                        ..reply
                    });
                }

                let places = keys::between(dimension, &request.first_key, &request.last_key);
                let split = match request.max_groups {
                    0 => keys::children(dimension, places),
                    most => keys::split(dimension, places, most as usize),
                };
                Ok(GetPartitionGroupsResponse {
                    groups: split
                        .groups
                        .into_iter()
                        .map(|group| PartitionKeyGroup {
                            label: group.label,
                            first_key: group.first,
                            last_key: group.last,
                            keys: group.keys as u64,
                        })
                        .collect(),
                    ascending: true,
                    of_keys: split.of_keys,
                    key_unit: dimension.key_unit.to_string(),
                    total_keys: all.len() as u64,
                    time: !dimension.key_unit.is_empty(),
                    cuts: dimension.cuts.iter().map(|cut| *cut as u32).collect(),
                    ..reply
                })
            }

            async fn keys(
                &self,
                request: GetPartitionKeysRequest,
            ) -> Result<GetPartitionKeysResponse> {
                let found: Vec<&String> = self
                    .dimension(&request.dimension)?
                    .keys
                    .iter()
                    .filter(|key| key.contains(&request.query))
                    .collect();
                Ok(GetPartitionKeysResponse {
                    total: found.len() as u64,
                    keys: found
                        .into_iter()
                        .skip(request.offset as usize)
                        .take(request.limit as usize)
                        .cloned()
                        .collect(),
                    indices: Vec::new(),
                })
            }
        }

        fn event(event_type: EventType, partition: PartitionKey, at: i64) -> EventRecord {
            EventRecord {
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type,
                asset_key: Some("asset".to_string()),
                run_id: format!("run-{at}"),
                partition_key: Some(partition),
                timestamp: at,
                metadata: vec![],
                input_data_versions: vec![],
            }
        }

        fn built(partition: PartitionKey, at: i64) -> EventRecord {
            event(
                EventType::Materialization { data_version: None },
                partition,
                at,
            )
        }

        fn failure(partition: PartitionKey, at: i64) -> EventRecord {
            event(EventType::StepFailure, partition, at)
        }

        /// A store with the asset `asset` and `events` of it.
        async fn store_with(events: &[EventRecord]) -> SurrealStorage {
            let storage = SurrealStorage::new_memory().await.unwrap();
            let asset = AssetRecord {
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                asset_key: "asset".to_string(),
                tags: vec![],
                kinds: vec![],
                asset_group: None,
                code_version: None,
                last_event_id: None,
                last_run_id: None,
                last_timestamp: None,
                last_data_version: None,
                last_materialization_code_version: None,
                last_input_data_versions: vec![],
                pool: vec![],
            };
            storage
                .for_code_location(&CodeLocationContext::default_for_tests())
                .register_assets(vec![asset])
                .await
                .unwrap();
            storage.store_events(events).await.unwrap();
            storage
        }

        fn outline<'a>(
            storage: &'a ScopedStorage<'a, SurrealStorage>,
            source: Option<Definition>,
        ) -> Outline<'a, SurrealStorage, Definition> {
            Outline {
                storage,
                source,
                asset_key: "asset",
                dynamic: OnceCell::new(),
                backfill: None,
            }
        }

        fn groups(level: &OutlineLevel) -> &[OutlineRow] {
            match &level.rows {
                OutlineRows::Groups(rows) => rows,
                OutlineRows::Partitions(_) => panic!("the level lists partitions: {level:?}"),
            }
        }

        /// The key and the state of each partition of `level`.
        fn states(level: &OutlineLevel) -> Vec<(&str, PartitionRowState)> {
            match &level.rows {
                OutlineRows::Partitions(rows) => rows
                    .iter()
                    .map(|row| (row.key.as_str(), row.state))
                    .collect(),
                OutlineRows::Groups(_) => panic!("the level lists groups: {level:?}"),
            }
        }

        /// The label of each bar with its partitions: all, done, failed.
        fn bars_of(overview: &PartitionOverview) -> Vec<(&str, u64, u64, u64)> {
            overview
                .strip
                .iter()
                .flat_map(|strip| &strip.bars)
                .map(|bar| {
                    let PartitionCounts {
                        total,
                        done,
                        failed,
                        ..
                    } = bar.counts;
                    (bar.label.as_str(), total, done, failed)
                })
                .collect()
        }

        use PartitionRowState::{Failed, Materialized, Missing};

        #[tokio::test]
        async fn an_asset_with_two_dimensions_opens_from_months_down_to_its_partitions() {
            let hour = "2024-02-10T05:00";
            let storage = store_with(&[
                built(cell("2024-01-20T12:00", "a"), 100),
                built(cell(hour, "a"), 100),
                built(cell(hour, "b"), 100),
                failure(cell(hour, "b"), 200),
                failure(cell(hour, "c"), 200),
            ])
            .await;
            let ctx = CodeLocationContext::default_for_tests();
            let scoped = storage.for_code_location(&ctx);
            let definition =
                Definition(vec![named("zone", &["a", "b", "c"]), hours_of_two_months()]);
            let outline = outline(&scoped, Some(definition));

            let overview = outline.overview().await.unwrap();

            // `b` was materialized and failed later: it is failed only.
            assert_eq!(overview.counts, PartitionCounts::of(4320, 2, 2));
            assert_eq!(
                overview.shape,
                PartitionShape::Dimensions(vec![
                    ("hour".to_string(), 1440),
                    ("zone".to_string(), 3)
                ])
            );
            assert!(overview.from_definition);

            // One bar per day, the oldest first.
            let bars = bars_of(&overview);
            assert_eq!(bars.len(), 60);
            assert_eq!(bars[0], ("2024-01-01", 72, 0, 0));
            assert_eq!(bars[19], ("2024-01-20", 72, 1, 0));
            assert_eq!(bars[40], ("2024-02-10", 72, 1, 2));

            // One row per month, the newest first.
            let months = groups(&overview.top);
            assert_eq!(overview.top.heading, "hour");
            assert_eq!(
                months
                    .iter()
                    .map(|row| (row.label.as_str(), row.holds.as_str(), row.counts))
                    .collect::<Vec<_>>(),
                vec![
                    ("2024-02", "696 hours", PartitionCounts::of(2088, 1, 2)),
                    ("2024-01", "744 hours", PartitionCounts::of(2232, 1, 0)),
                ]
            );

            let days = outline.rows(&months[0].place, "", 0).await.unwrap();
            let days = groups(&days);
            assert_eq!(days.len(), 29);
            assert_eq!(days[0].label, "2024-02-29");
            assert_eq!(days[19].label, "2024-02-10");
            assert_eq!(days[19].counts, PartitionCounts::of(72, 1, 2));

            let hours = outline.rows(&days[19].place, "", 0).await.unwrap();
            let hours = groups(&hours);
            assert_eq!(hours.len(), 24);
            assert_eq!(hours[0].label, "2024-02-10T23:00");
            assert_eq!(hours[18].label, hour);
            assert_eq!(hours[18].holds, "3 × zone");
            assert_eq!(hours[18].counts, PartitionCounts::of(3, 1, 2));

            let zones = outline.rows(&hours[18].place, "", 0).await.unwrap();
            assert_eq!(zones.heading, "zone");
            assert_eq!(
                states(&zones),
                vec![("a", Materialized), ("b", Failed), ("c", Failed)]
            );
            assert_eq!(zones.more, 0);
        }

        #[tokio::test]
        async fn keys_with_no_separator_have_rows_and_marks_at_the_fields_of_their_format() {
            let storage = store_with(&[built(single("2024021005"), 100)]).await;
            let ctx = CodeLocationContext::default_for_tests();
            let scoped = storage.for_code_location(&ctx);
            let outline = outline(&scoped, Some(Definition(vec![hours_with_no_separator()])));

            let overview = outline.overview().await.unwrap();

            // One bar per day, with a mark where the month changes.
            let strip = overview.strip.as_ref().unwrap();
            assert_eq!(strip.bars.len(), 60);
            assert_eq!(strip.bars[40].label, "20240210");
            assert_eq!(
                strip.marks,
                vec![(0, "202401".to_string()), (31, "202402".to_string())]
            );

            let months = groups(&overview.top);
            assert_eq!(
                months
                    .iter()
                    .map(|row| (row.label.as_str(), row.counts))
                    .collect::<Vec<_>>(),
                vec![
                    ("202402", PartitionCounts::of(696, 1, 0)),
                    ("202401", PartitionCounts::of(744, 0, 0)),
                ]
            );
        }

        #[tokio::test]
        async fn the_find_field_lists_the_keys_that_hold_the_text() {
            let storage = store_with(&[built(cell("2024-02-10T05:00", "a"), 100)]).await;
            let ctx = CodeLocationContext::default_for_tests();
            let scoped = storage.for_code_location(&ctx);
            let definition =
                Definition(vec![named("zone", &["a", "b", "c"]), hours_of_two_months()]);
            let outline = outline(&scoped, Some(definition));
            let root = outline.overview().await.unwrap().root;

            let found = outline.rows(&root, "02-10T0", 0).await.unwrap();

            let found = groups(&found);
            assert_eq!(found.len(), 10);
            assert_eq!(found[5].label, "2024-02-10T05:00");
            assert_eq!(found[5].counts, PartitionCounts::of(3, 1, 0));
            assert_eq!(found[4].counts, PartitionCounts::of(3, 0, 0));
        }

        #[tokio::test]
        async fn keys_in_free_order_keep_their_order_and_their_state() {
            let storage = store_with(&[
                built(single("small"), 100),
                built(single("large"), 100),
                failure(single("medium"), 200),
            ])
            .await;
            let ctx = CodeLocationContext::default_for_tests();
            let scoped = storage.for_code_location(&ctx);
            let definition = Definition(vec![named("", &["small", "medium", "large", "xl"])]);
            let outline = outline(&scoped, Some(definition));

            let overview = outline.overview().await.unwrap();

            assert_eq!(overview.counts, PartitionCounts::of(4, 2, 1));
            assert_eq!(overview.shape, PartitionShape::Keys);
            assert_eq!(
                states(&overview.top),
                vec![
                    ("small", Materialized),
                    ("medium", Failed),
                    ("large", Materialized),
                    ("xl", Missing),
                ]
            );
            // The bars show the same states as the rows.
            assert_eq!(
                bars_of(&overview),
                vec![
                    ("small", 1, 1, 0),
                    ("medium", 1, 0, 1),
                    ("large", 1, 1, 0),
                    ("xl", 1, 0, 0),
                ]
            );
        }

        #[tokio::test]
        async fn many_keys_in_free_order_are_read_in_pages() {
            // 150 keys from `k149` down to `k000`.
            let keys: Vec<String> = (0..150).rev().map(|at| format!("k{at:03}")).collect();
            let storage =
                store_with(&[built(single("k149"), 100), built(single("k010"), 100)]).await;
            let ctx = CodeLocationContext::default_for_tests();
            let scoped = storage.for_code_location(&ctx);
            let definition = Definition(vec![Dimension {
                name: "",
                keys,
                key_unit: "",
                dynamic: "",
                cuts: vec![],
            }]);
            let outline = outline(&scoped, Some(definition));

            let overview = outline.overview().await.unwrap();
            let rest = outline.rows(&overview.root, "", 100).await.unwrap();
            let found = outline.rows(&overview.root, "k01", 0).await.unwrap();

            assert_eq!(overview.top.rows.len(), 100);
            assert_eq!(overview.top.more, 50);
            assert_eq!(states(&overview.top)[0], ("k149", Materialized));
            assert_eq!(bars_of(&overview).len(), 150);
            assert_eq!(bars_of(&overview)[139], ("k010", 1, 1, 0));

            assert_eq!(rest.rows.len(), 50);
            assert_eq!(rest.more, 0);
            assert_eq!(states(&rest)[39], ("k010", Materialized));
            assert_eq!(states(&rest)[49], ("k000", Missing));

            assert_eq!(found.rows.len(), 10);
            assert_eq!(states(&found)[0], ("k019", Missing));
            assert_eq!(states(&found)[9], ("k010", Materialized));
        }

        #[tokio::test]
        async fn the_keys_of_a_dynamic_set_are_read_from_the_store() {
            let storage = store_with(&[built(single("acme"), 100)]).await;
            let ctx = CodeLocationContext::default_for_tests();
            let scoped = storage.for_code_location(&ctx);
            let customers = ["globex", "acme", "initech"].map(str::to_string);
            scoped
                .add_dynamic_partitions("customers", &customers)
                .await
                .unwrap();
            let definition = Definition(vec![Dimension {
                name: "",
                keys: vec![],
                key_unit: "",
                dynamic: "customers",
                cuts: vec![],
            }]);
            let outline = outline(&scoped, Some(definition));

            let overview = outline.overview().await.unwrap();
            let found = outline.rows(&overview.root, "ex", 0).await.unwrap();

            assert_eq!(overview.counts, PartitionCounts::of(3, 1, 0));
            assert_eq!(
                states(&overview.top),
                vec![
                    ("acme", Materialized),
                    ("globex", Missing),
                    ("initech", Missing)
                ]
            );
            assert_eq!(bars_of(&overview).len(), 3);
            assert_eq!(states(&found), vec![("globex", Missing)]);
        }

        #[tokio::test]
        async fn the_materialized_partitions_are_listed_when_the_code_location_does_not_answer() {
            let storage = store_with(&[
                built(single("p1"), 100),
                built(single("p2"), 100),
                failure(single("p2"), 200),
                failure(single("p3"), 200),
            ])
            .await;
            let ctx = CodeLocationContext::default_for_tests();
            let scoped = storage.for_code_location(&ctx);
            let outline = outline(&scoped, None);

            let overview = outline.overview().await.unwrap();
            let page = outline.rows(&overview.root, "", 0).await.unwrap();

            assert!(!overview.from_definition);
            assert_eq!(overview.strip, None);
            assert_eq!(overview.counts, PartitionCounts::of(3, 1, 2));
            assert_eq!(
                states(&overview.top),
                vec![("p1", Materialized), ("p2", Failed)]
            );
            assert_eq!(overview.top.more, 0);
            assert_eq!(page, overview.top);
        }

        #[test]
        fn bounds_of_a_run_of_keys_hold_the_partitions_of_each_key() {
            let day = bounds_of(&hours(), "2024-02-10T00:00", "2024-02-10T23:00");

            assert!(day.contains(&cell("2024-02-10T00:00", "001")));
            assert!(day.contains(&cell("2024-02-10T23:00", "040")));
            assert!(!day.contains(&cell("2024-02-09T23:00", "040")));
            assert!(!day.contains(&cell("2024-02-11T00:00", "001")));
        }

        #[test]
        fn bounds_below_a_key_hold_the_keys_of_the_next_dimension() {
            let zones = bounds_of(&zones_of("2024-02-10T05:00"), "010", "019");

            assert!(zones.contains(&cell("2024-02-10T05:00", "010")));
            assert!(zones.contains(&cell("2024-02-10T05:00", "019")));
            assert!(!zones.contains(&cell("2024-02-10T05:00", "020")));
            assert!(!zones.contains(&cell("2024-02-10T06:00", "010")));
        }

        #[test]
        fn bounds_of_an_asset_with_one_dimension() {
            let january = bounds_of(&days(), "2024-01-01", "2024-01-31");

            assert!(january.contains(&single("2024-01-31")));
            assert!(!january.contains(&single("2024-02-01")));
        }

        #[test]
        fn partition_of_the_last_dimension_holds_the_keys_above() {
            let partition = partition_of(&zones_of("2024-02-10T05:00"), "013");

            assert_eq!(partition, cell("2024-02-10T05:00", "013"));
        }

        #[test]
        fn partition_of_an_asset_with_one_dimension_keeps_the_key_as_text() {
            // The key reads like a key with a dimension. It is not one.
            assert_eq!(partition_of(&days(), "region=eu"), single("region=eu"));
        }

        #[test]
        fn a_group_of_keys_opens_to_its_next_split() {
            let counts = vec![
                PartitionCounts::of(29_760, 29_760, 0),
                PartitionCounts::of(27_840, 100, 1),
            ];

            let rows = group_rows(&hours(), &months(), counts.clone());

            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].label, "2024-01");
            assert_eq!(rows[0].holds, "744 hours");
            assert_eq!(rows[1].counts, counts[1]);
            assert_eq!(
                rows[1].place,
                OutlinePlace {
                    first_key: "2024-02-01T00:00".to_string(),
                    last_key: "2024-02-29T23:00".to_string(),
                    ..hours()
                }
            );
        }

        #[test]
        fn a_key_opens_to_the_keys_of_the_next_dimension() {
            let hour = "2024-02-10T05:00";
            let split = Split {
                of_keys: true,
                key_unit: "hour".to_string(),
                time: true,
                groups: vec![group(hour, hour, hour, 1)],
                ..Split::default()
            };

            let rows = group_rows(&hours(), &split, vec![PartitionCounts::of(40, 39, 1)]);

            assert_eq!(rows[0].holds, "40 × zone");
            assert_eq!(rows[0].place, zones_of(hour));
        }

        #[test]
        fn a_row_names_what_it_holds() {
            let of = |keys: usize, key_unit: &str| {
                let split = Split {
                    groups: vec![group("g", "a", "b", keys)],
                    key_unit: key_unit.to_string(),
                    ..Split::default()
                };
                group_rows(&days(), &split, vec![PartitionCounts::default()])[0]
                    .holds
                    .clone()
            };

            assert_eq!(of(1, "day"), "1 day");
            assert_eq!(of(31, "day"), "31 days");
            assert_eq!(of(1500, ""), "1,500 keys");
        }

        #[test]
        fn one_key_counts_the_partitions_of_the_dimensions_below() {
            assert_eq!(partitions_per_key(&hours()), 40);
            assert_eq!(partitions_per_key(&days()), 1);
        }

        #[test]
        fn the_note_of_the_strip_names_one_bar() {
            let split = |of_keys: bool, unit: &str, key_unit: &str| Split {
                of_keys,
                unit: unit.to_string(),
                key_unit: key_unit.to_string(),
                ..Split::default()
            };

            // Every bar holds as many partitions: the note gives the number.
            assert_eq!(
                strip_note(&hours(), &split(false, "day", "hour"), &bars(&[960, 960])),
                "one bar per day · 960 partitions each"
            );
            assert_eq!(
                strip_note(
                    &hours(),
                    &split(false, "month", "hour"),
                    &bars(&[29_760, 27_840])
                ),
                "one bar per month"
            );
            assert_eq!(
                strip_note(&days(), &split(true, "", "day"), &bars(&[1, 1])),
                "one bar per day"
            );
            assert_eq!(
                strip_note(&days(), &split(true, "", ""), &bars(&[1, 1])),
                "one bar per partition"
            );
            assert_eq!(
                strip_note(&zones_of("h"), &split(true, "", ""), &bars(&[1])),
                "one bar per zone"
            );
            assert_eq!(
                strip_note(&days(), &split(false, "", ""), &bars(&[5, 4])),
                "one bar per group of keys"
            );
        }

        /// Store backfill `bf` of `asset` for `requested` and start it.
        /// Returns the key of each run of its plan.
        async fn started_backfill(
            storage: &SurrealStorage,
            strategy: BackfillStrategy,
            requested: &[PartitionKey],
        ) -> Vec<PartitionKey> {
            let record = BackfillRecord {
                backfill_id: "bf".to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                status: BackfillStatus::Requested,
                strategy: strategy.clone(),
                failure_policy: BackfillFailurePolicy::Continue,
                asset_selection: vec!["asset".to_string()],
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

            let plan = plan_runs(&strategy, requested);
            assert!(storage.start_backfill("bf", plan.len()).await.unwrap());
            plan
        }

        /// Run `run-<n>` of the plan ends with `status`, with a step failure
        /// for each of `failed`.
        async fn ended_run(
            storage: &SurrealStorage,
            plan: &[PartitionKey],
            run_number: usize,
            status: RunStatus,
            failed: &[PartitionKey],
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
                    code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                    job_name: None,
                    status: RunStatus::Started,
                    start_time: 1,
                    end_time: None,
                    execution_start_time: None,
                    tags: vec![],
                    node_names: vec!["asset".to_string()],
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
            for partition in failed {
                let mut failure = failure(partition.clone(), 2);
                failure.run_id = run_id.clone();
                storage.store_event(&failure).await.unwrap();
            }
            storage
                .update_run_status(&run_id, status, Some(3))
                .await
                .unwrap();
        }

        async fn backfill_outline<'a>(
            storage: &'a ScopedStorage<'a, SurrealStorage>,
            base: &SurrealStorage,
            source: Option<Definition>,
        ) -> Outline<'a, SurrealStorage, Definition> {
            let record = base.get_backfill("bf").await.unwrap().unwrap();
            let states = BackfillStates::read(base, &record).await.unwrap();
            Outline {
                backfill: Some(Backfill::new(states)),
                ..outline(storage, source)
            }
        }

        /// The key, the state and the run of each partition of `level`.
        fn runs(level: &OutlineLevel) -> Vec<(&str, PartitionRowState, Option<&str>)> {
            match &level.rows {
                OutlineRows::Partitions(rows) => rows
                    .iter()
                    .map(|row| (row.key.as_str(), row.state, row.run_id.as_deref()))
                    .collect(),
                OutlineRows::Groups(_) => panic!("the level lists groups: {level:?}"),
            }
        }

        #[tokio::test]
        async fn a_backfill_opens_from_months_down_to_the_partitions_it_holds() {
            let storage = store_with(&[]).await;
            let hours: Vec<String> = hours_of_two_months()
                .keys
                .into_iter()
                .filter(|hour| hour.as_str() >= "2024-01-20")
                .collect();
            let requested = [PartitionKey::Multi {
                dims: vec![
                    ("hour".to_string(), hours),
                    ("zone".to_string(), vec!["a".to_string(), "b".to_string()]),
                ],
            }];
            let strategy = BackfillStrategy::PerDimension {
                multi_run: vec!["zone".to_string()],
                single_run: vec!["hour".to_string()],
            };
            let plan = started_backfill(&storage, strategy, &requested).await;
            // Zone a succeeds with one hour failed alone; zone b fails.
            let hour = "2024-02-10T05:00";
            ended_run(&storage, &plan, 0, RunStatus::Success, &[cell(hour, "a")]).await;
            ended_run(&storage, &plan, 1, RunStatus::Failure, &[]).await;

            let ctx = CodeLocationContext::default_for_tests();
            let scoped = storage.for_code_location(&ctx);
            let definition =
                Definition(vec![named("zone", &["a", "b", "c"]), hours_of_two_months()]);
            let outline = backfill_outline(&scoped, &storage, Some(definition)).await;

            let overview = outline.overview().await.unwrap();

            assert_eq!(
                overview.counts,
                PartitionCounts::of_backfill(1968, 983, 985, 0)
            );
            assert_eq!(
                overview.shape,
                PartitionShape::Dimensions(vec![
                    ("hour".to_string(), 984),
                    ("zone".to_string(), 2)
                ])
            );
            assert_eq!(
                (
                    overview.root.first_key.as_str(),
                    overview.root.last_key.as_str()
                ),
                ("2024-01-20T00:00", "2024-02-29T23:00"),
                "the outline starts and ends with the keys of the backfill"
            );

            // One bar per day of the backfill.
            let bars = bars_of(&overview);
            assert_eq!(bars.len(), 41);
            assert_eq!(bars[0], ("2024-01-20", 48, 24, 24));
            assert_eq!(bars[21], ("2024-02-10", 48, 23, 25));

            let months = groups(&overview.top);
            assert_eq!(
                months
                    .iter()
                    .map(|row| (row.label.as_str(), row.holds.as_str(), row.counts))
                    .collect::<Vec<_>>(),
                vec![
                    (
                        "2024-02",
                        "696 hours",
                        PartitionCounts::of_backfill(1392, 695, 697, 0)
                    ),
                    (
                        "2024-01",
                        "288 hours",
                        PartitionCounts::of_backfill(576, 288, 288, 0)
                    ),
                ]
            );

            let days = outline.rows(&months[0].place, "", 0).await.unwrap();
            let days = groups(&days);
            assert_eq!(days.len(), 29);
            assert_eq!(days[19].label, "2024-02-10");
            assert_eq!(days[19].counts, PartitionCounts::of_backfill(48, 23, 25, 0));

            let hours = outline.rows(&days[19].place, "", 0).await.unwrap();
            let hours = groups(&hours);
            assert_eq!(hours.len(), 24);
            assert_eq!(hours[18].label, hour);
            assert_eq!(hours[18].holds, "2 × zone");
            assert_eq!(hours[18].counts, PartitionCounts::of_backfill(2, 0, 2, 0));

            // Zone c is not in the backfill.
            let zones = outline.rows(&hours[18].place, "", 0).await.unwrap();
            assert_eq!(
                runs(&zones),
                vec![
                    ("a", PartitionRowState::Failed, Some("run-0")),
                    ("b", PartitionRowState::Failed, Some("run-1")),
                ]
            );

            let found = outline.rows(&overview.root, "02-10T0", 0).await.unwrap();
            let found = groups(&found);
            assert_eq!(found.len(), 10);
            assert_eq!(found[5].label, hour);
            assert_eq!(found[5].counts, PartitionCounts::of_backfill(2, 0, 2, 0));
            assert_eq!(found[4].counts, PartitionCounts::of_backfill(2, 1, 1, 0));
        }

        #[tokio::test]
        async fn a_backfill_of_keys_in_free_order_lists_them_in_the_order_asked() {
            let storage = store_with(&[]).await;
            let requested = [single("large"), single("small")];
            let plan = started_backfill(&storage, BackfillStrategy::MultiRun, &requested).await;
            ended_run(&storage, &plan, 0, RunStatus::Success, &[]).await;
            ended_run(&storage, &plan, 1, RunStatus::Failure, &[]).await;

            let ctx = CodeLocationContext::default_for_tests();
            let scoped = storage.for_code_location(&ctx);
            let definition = Definition(vec![named("", &["small", "medium", "large", "xl"])]);
            let outline = backfill_outline(&scoped, &storage, Some(definition)).await;

            let overview = outline.overview().await.unwrap();

            assert_eq!(overview.counts, PartitionCounts::of_backfill(2, 1, 1, 0));
            assert_eq!(overview.shape, PartitionShape::Keys);
            assert_eq!(
                runs(&overview.top),
                vec![
                    ("large", PartitionRowState::Done, Some("run-0")),
                    ("small", PartitionRowState::Failed, Some("run-1")),
                ]
            );
            assert_eq!(
                bars_of(&overview),
                vec![("large", 1, 1, 0), ("small", 1, 0, 1)]
            );
        }

        #[tokio::test]
        async fn a_backfill_of_windows_asked_newest_first_spans_its_oldest_to_its_newest() {
            let storage = store_with(&[]).await;
            let requested = [single("2024-02-10T05:00"), single("2024-01-20T12:00")];
            let plan = started_backfill(&storage, BackfillStrategy::MultiRun, &requested).await;
            ended_run(&storage, &plan, 0, RunStatus::Success, &[]).await;

            let ctx = CodeLocationContext::default_for_tests();
            let scoped = storage.for_code_location(&ctx);
            let hours = Dimension {
                name: "",
                ..hours_of_two_months()
            };
            let outline = backfill_outline(&scoped, &storage, Some(Definition(vec![hours]))).await;

            let overview = outline.overview().await.unwrap();

            assert_eq!(
                (
                    overview.root.first_key.as_str(),
                    overview.root.last_key.as_str()
                ),
                ("2024-01-20T12:00", "2024-02-10T05:00")
            );
            assert_eq!(
                bars_of(&overview),
                vec![("2024-01-20", 1, 0, 0), ("2024-02-10", 1, 1, 0)]
            );
        }

        #[tokio::test]
        async fn a_backfill_of_a_dynamic_set_lists_its_keys_in_ascending_order() {
            let storage = store_with(&[]).await;
            let requested = [PartitionKey::Single {
                keys: ["globex", "acme", "initech"].map(str::to_string).to_vec(),
            }];
            let plan = started_backfill(&storage, BackfillStrategy::MultiRun, &requested).await;
            ended_run(&storage, &plan, 0, RunStatus::Success, &[]).await;

            let ctx = CodeLocationContext::default_for_tests();
            let scoped = storage.for_code_location(&ctx);
            let customers = Dimension {
                name: "",
                keys: vec![],
                key_unit: "",
                dynamic: "customers",
                cuts: vec![],
            };
            let outline =
                backfill_outline(&scoped, &storage, Some(Definition(vec![customers]))).await;

            let overview = outline.overview().await.unwrap();
            let found = outline.rows(&overview.root, "ex", 0).await.unwrap();

            assert_eq!(overview.counts, PartitionCounts::of_backfill(3, 1, 0, 0));
            assert_eq!(
                runs(&overview.top),
                vec![
                    ("acme", PartitionRowState::Pending, None),
                    ("globex", PartitionRowState::Done, Some("run-0")),
                    ("initech", PartitionRowState::Pending, None),
                ]
            );
            assert_eq!(
                runs(&found),
                vec![("globex", PartitionRowState::Done, Some("run-0"))]
            );
        }

        #[tokio::test]
        async fn a_backfill_whose_code_location_does_not_answer_lists_its_partitions() {
            let storage = store_with(&[]).await;
            let requested = [PartitionKey::Single {
                keys: vec!["b".to_string(), "a".to_string(), "c".to_string()],
            }];
            let plan = started_backfill(&storage, BackfillStrategy::MultiRun, &requested).await;
            ended_run(&storage, &plan, 0, RunStatus::Success, &[]).await;
            ended_run(&storage, &plan, 2, RunStatus::Canceled, &[]).await;

            let ctx = CodeLocationContext::default_for_tests();
            let scoped = storage.for_code_location(&ctx);
            let outline = backfill_outline(&scoped, &storage, None).await;

            let overview = outline.overview().await.unwrap();

            assert!(!overview.from_definition);
            assert_eq!(overview.counts, PartitionCounts::of_backfill(3, 1, 0, 1));
            assert_eq!(overview.top.heading, "partition");
            assert_eq!(
                runs(&overview.top),
                vec![
                    ("b", PartitionRowState::Done, Some("run-0")),
                    ("a", PartitionRowState::Pending, None),
                    ("c", PartitionRowState::Canceled, Some("run-2")),
                ]
            );

            let rest = outline.rows(&overview.root, "", 1).await.unwrap();
            assert_eq!(runs(&rest).len(), 2);
            assert_eq!(rest.more, 0);
        }

        #[test]
        fn the_head_of_the_rows_is_the_dimension_or_the_unit_of_a_key() {
            assert_eq!(heading_of(&hours(), &months()), "hour");
            assert_eq!(heading_of(&days(), &months()), "hour");
            assert_eq!(heading_of(&days(), &Split::default()), "partition");
        }
    }
}
