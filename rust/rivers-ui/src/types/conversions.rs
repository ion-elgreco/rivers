use super::*;

impl AssetRecord {
    /// Build a UI DTO from a core record plus a staleness value computed
    /// for it via `staleness::compute_staleness`. Staleness is not on the
    /// core record itself — it depends on the rest of the graph and is
    /// computed once per request, then merged into each row.
    pub fn from_core_with_staleness(
        r: rivers_core::storage::AssetRecord,
        stale_status: StaleStatus,
    ) -> Self {
        Self {
            asset_key: r.asset_key,
            tags: r.tags,
            kinds: r.kinds,
            asset_group: r.asset_group,
            code_version: r.code_version,
            last_event_id: r.last_event_id,
            last_run_id: r.last_run_id,
            last_timestamp: r.last_timestamp,
            last_data_version: r.last_data_version,
            pool: r.pool,
            stale_status,
        }
    }
}

impl From<rivers_core::storage::StaleStatus> for StaleStatus {
    fn from(s: rivers_core::storage::StaleStatus) -> Self {
        match s {
            rivers_core::storage::StaleStatus::UpToDate => Self::UpToDate,
            rivers_core::storage::StaleStatus::Stale => Self::Stale,
            rivers_core::storage::StaleStatus::Missing => Self::Missing,
        }
    }
}

impl From<rivers_core::storage::RunStatus> for RunStatus {
    fn from(s: rivers_core::storage::RunStatus) -> Self {
        match s {
            rivers_core::storage::RunStatus::Queued => Self::Queued,
            rivers_core::storage::RunStatus::NotStarted => Self::NotStarted,
            rivers_core::storage::RunStatus::Started => Self::Started,
            rivers_core::storage::RunStatus::Success => Self::Success,
            rivers_core::storage::RunStatus::Failure => Self::Failure,
            rivers_core::storage::RunStatus::Canceled => Self::Canceled,
        }
    }
}

impl From<rivers_core::storage::UserRef> for UserRef {
    fn from(u: rivers_core::storage::UserRef) -> Self {
        Self {
            subject: u.subject,
            email: u.email,
            name: u.name,
        }
    }
}

impl From<rivers_core::storage::LaunchedBy> for LaunchedBy {
    fn from(l: rivers_core::storage::LaunchedBy) -> Self {
        match l {
            rivers_core::storage::LaunchedBy::Manual { user } => Self::Manual {
                user: user.map(Into::into),
            },
            rivers_core::storage::LaunchedBy::Schedule { name } => Self::Schedule { name },
            rivers_core::storage::LaunchedBy::Sensor { name } => Self::Sensor { name },
            rivers_core::storage::LaunchedBy::Backfill { backfill_id } => {
                Self::Backfill { backfill_id }
            }
            rivers_core::storage::LaunchedBy::Condition => Self::Condition,
        }
    }
}

impl From<rivers_core::storage::RunRecord> for RunRecord {
    fn from(r: rivers_core::storage::RunRecord) -> Self {
        let partition_key = r.partition_key.as_ref().map(|pk| {
            const PREVIEW_N: usize = 3;
            PartitionPreview {
                preview: pk
                    .members_preview(PREVIEW_N)
                    .into_iter()
                    .map(partition_key_to_display)
                    .collect(),
                total: pk.member_count(),
            }
        });
        Self {
            run_id: r.run_id,
            job_name: r.job_name,
            status: r.status.into(),
            start_time: r.start_time,
            end_time: r.end_time,
            tags: r.tags,
            node_names: r.node_names,
            priority: r.priority,
            partition_key,
            block_reason: r.block_reason,
            launched_by: r.launched_by.into(),
            code_location_id: r.code_location_id,
            action: r.action,
            config: r.config,
        }
    }
}

impl From<rivers_core::storage::RunsSummary> for RunsSummary {
    fn from(s: rivers_core::storage::RunsSummary) -> Self {
        Self {
            total: s.total,
            in_progress: s.in_progress,
            queued: s.queued,
            failure: s.failure,
            success: s.success,
            last_24h: s.last_24h,
        }
    }
}

impl From<rivers_core::storage::RunsPage> for RunsPage {
    fn from(p: rivers_core::storage::RunsPage) -> Self {
        Self {
            rows: p.rows.into_iter().map(Into::into).collect(),
            total: p.total,
        }
    }
}

impl From<RunStatus> for rivers_core::storage::RunStatus {
    fn from(s: RunStatus) -> Self {
        match s {
            RunStatus::Queued => Self::Queued,
            RunStatus::NotStarted => Self::NotStarted,
            RunStatus::Started => Self::Started,
            RunStatus::Success => Self::Success,
            RunStatus::Failure => Self::Failure,
            RunStatus::Canceled => Self::Canceled,
        }
    }
}

impl From<RunFilter> for rivers_core::storage::RunFilter {
    fn from(f: RunFilter) -> Self {
        Self {
            status: f.status.map(Into::into),
            job_name: f.job_name.filter(|s| !s.is_empty()),
            job_substring: f.job_substring.filter(|s| !s.is_empty()),
            asset_substring: f.asset_substring.filter(|s| !s.is_empty()),
            partition_substring: f.partition_substring.filter(|s| !s.is_empty()),
            action: f.action.into_core(),
        }
    }
}

impl From<rivers_core::storage::EventType> for EventType {
    fn from(e: rivers_core::storage::EventType) -> Self {
        match e {
            rivers_core::storage::EventType::Materialization { .. } => Self::Materialization,
            rivers_core::storage::EventType::Observation { .. } => Self::Observation,
            rivers_core::storage::EventType::StepStart => Self::StepStart,
            rivers_core::storage::EventType::StepSuccess => Self::StepSuccess,
            rivers_core::storage::EventType::StepFailure => Self::StepFailure,
            rivers_core::storage::EventType::StepRetry => Self::StepRetry,
            rivers_core::storage::EventType::RunQueued => Self::RunQueued,
            rivers_core::storage::EventType::RunDequeued => Self::RunDequeued,
            rivers_core::storage::EventType::RunLaunchFailed => Self::RunLaunchFailed,
            rivers_core::storage::EventType::StepSlotClaimed => Self::StepSlotClaimed,
            rivers_core::storage::EventType::StepSlotWaiting => Self::StepSlotWaiting,
            rivers_core::storage::EventType::StepSlotRenewed => Self::StepSlotRenewed,
            rivers_core::storage::EventType::StepSlotReleased => Self::StepSlotReleased,
            rivers_core::storage::EventType::ActionCompleted => Self::ActionCompleted,
            rivers_core::storage::EventType::Deletion => Self::Deletion,
        }
    }
}

impl From<rivers_core::storage::StoredEvent> for StoredEvent {
    fn from(e: rivers_core::storage::StoredEvent) -> Self {
        let data_version = e.event_type.data_version().map(|s| s.to_string());
        Self {
            id: format!("{:?}", e.id),
            event_type: e.event_type.into(),
            asset_key: e.asset_key,
            run_id: e.run_id,
            partition_key: e.partition_key.map(partition_key_to_display),
            timestamp: e.timestamp,
            metadata: e
                .metadata
                .into_iter()
                .map(|(k, v)| (k, MetadataDisplay::from_stored(&v)))
                .collect(),
            data_version,
        }
    }
}

impl From<rivers_core::storage::StoredLog> for RunLog {
    fn from(l: rivers_core::storage::StoredLog) -> Self {
        Self {
            id: format!("{:?}", l.id),
            run_id: l.run_id,
            step_key: l.step_key,
            timestamp: l.timestamp,
            stdout: l.stdout,
            stderr: l.stderr,
            logs: l.logs,
            traceback: l
                .traceback
                .as_deref()
                .and_then(|json| serde_json::from_str(json).ok()),
        }
    }
}

impl From<rivers_core::assets::graph::GraphTopology> for GraphTopology {
    fn from(g: rivers_core::assets::graph::GraphTopology) -> Self {
        Self {
            nodes: g
                .nodes
                .into_iter()
                .map(|n| TopologyNode {
                    name: n.name,
                    kind: n.kind.as_str().to_string(),
                    group: n.group,
                    parent_graph: n.parent_graph,
                })
                .collect(),
            edges: g.edges,
        }
    }
}

impl From<rivers_core::condition::NodeStatus> for NodeStatus {
    fn from(s: rivers_core::condition::NodeStatus) -> Self {
        match s {
            rivers_core::condition::NodeStatus::True => Self::True,
            rivers_core::condition::NodeStatus::False => Self::False,
            rivers_core::condition::NodeStatus::Skipped => Self::Skipped,
        }
    }
}

impl From<rivers_core::condition::EvalNodeResult> for EvalNodeResult {
    fn from(t: rivers_core::condition::EvalNodeResult) -> Self {
        Self {
            node_idx: t.node_idx,
            label: t.label,
            node_type: t.node_type,
            status: t.status.into(),
            children: t.children.into_iter().map(Into::into).collect(),
            num_partitions: t.num_partitions,
        }
    }
}

impl ConditionTickRecord {
    pub fn from_stored(t: rivers_core::storage::StoredConditionTick) -> Self {
        Self {
            // Must match the format used by record_id_str() in the daemon,
            // since that's what's stored in condition_evals.tick_id.
            id: format!("{}:{:?}", t.id.table.as_str(), t.id.key),
            timestamp: t.timestamp,
            total_evaluated: t.total_evaluated,
            total_fired: t.total_fired,
            eval_duration_us: t.eval_duration_us,
            run_ids: t.run_ids,
            backfill_ids: t.backfill_ids,
        }
    }
}

impl ConditionEvalRecord {
    pub fn from_stored(e: rivers_core::storage::StoredConditionEval) -> Self {
        let tree: rivers_core::condition::EvalNodeResult = serde_json::from_slice(&e.tree_json)
            .unwrap_or_else(|err| {
                tracing::warn!(
                    target: "rivers::ui",
                    asset_key = %e.asset_key,
                    error = %err,
                    "failed to deserialize condition eval tree"
                );
                rivers_core::condition::EvalNodeResult {
                    node_idx: 0,
                    label: "parse error".into(),
                    node_type: "Leaf".into(),
                    status: rivers_core::condition::NodeStatus::False,
                    children: vec![],
                    num_partitions: None,
                }
            });
        Self {
            id: format!("{:?}", e.id),
            asset_key: e.asset_key,
            tick_id: e.tick_id,
            timestamp: e.timestamp,
            fired: e.fired,
            eval_duration_us: e.eval_duration_us,
            run_ids: e.run_ids,
            backfill_ids: Vec::new(),
            tree: tree.into(),
            selected_partitions: e.selection_json.and_then(|json| {
                serde_json::from_slice::<rivers_core::condition::PartitionSelection>(&json)
                    .ok()
                    .and_then(|sel| match sel {
                        rivers_core::condition::PartitionSelection::Keys(keys) => Some(
                            keys.into_iter()
                                .map(|pk| partition_key_to_display(pk))
                                .collect(),
                        ),
                        rivers_core::condition::PartitionSelection::All => None,
                        rivers_core::condition::PartitionSelection::Empty => Some(vec![]),
                    })
            }),
        }
    }
}

impl From<rivers_core::storage::PoolInfo> for PoolInfo {
    fn from(p: rivers_core::storage::PoolInfo) -> Self {
        Self {
            pool_key: p.pool_key,
            slot_limit: p.slot_limit,
            lease_duration_secs: p.lease_duration_secs,
            claimed_count: p.claimed_count,
            pending_count: p.pending_count,
        }
    }
}

impl From<rivers_core::storage::SlotHolder> for SlotHolder {
    fn from(h: rivers_core::storage::SlotHolder) -> Self {
        Self {
            run_id: h.run_id,
            step_key: h.step_key,
            slots_consumed: h.slots_consumed,
            claimed_at: h.claimed_at,
            lease_expires_at: h.lease_expires_at,
        }
    }
}

impl From<rivers_core::storage::BackfillRecord> for BackfillInfo {
    fn from(b: rivers_core::storage::BackfillRecord) -> Self {
        let (strategy, strategy_code) = match &b.strategy {
            rivers_core::storage::BackfillStrategy::MultiRun => (
                "one run per partition".to_string(),
                "multi_run()".to_string(),
            ),
            rivers_core::storage::BackfillStrategy::SingleRun => {
                ("one run".to_string(), "single_run()".to_string())
            }
            rivers_core::storage::BackfillStrategy::PerDimension {
                multi_run,
                single_run,
            } => (
                if multi_run.is_empty() {
                    "one run".to_string()
                } else {
                    format!("one run per {}", multi_run.join(" × "))
                },
                format!("per_dimension(multi_run={multi_run:?}, single_run={single_run:?})"),
            ),
        };
        Self {
            backfill_id: b.backfill_id,
            status: format!("{:?}", b.status),
            strategy,
            strategy_code,
            job_name: b.job_name,
            asset_selection: b.asset_selection,
            total_partitions: b
                .partition_keys
                .iter()
                .map(|pk| pk.member_count())
                .sum::<usize>() as u32,
            completed_partitions: b.completed_partitions.len() as u32,
            failed_partitions: b.failed_partitions.len() as u32,
            canceled_partitions: b.canceled_partitions.len() as u32,
            max_concurrency: b.max_concurrency as u32,
            run_ids: b.run_ids,
            tags: b.tags,
            create_time: b.create_time,
            end_time: b.end_time,
            error: b.error,
            code_location_id: b.code_location_id,
            launched_by: b.launched_by.into(),
            action: b.action,
        }
    }
}

impl From<rivers_core::storage::BackfillsPage> for BackfillsPage {
    fn from(p: rivers_core::storage::BackfillsPage) -> Self {
        Self {
            rows: p.rows.into_iter().map(Into::into).collect(),
            total: p.total,
        }
    }
}

impl From<rivers_api::rivers::CodeLocationEntry> for CodeLocationEntry {
    fn from(e: rivers_api::rivers::CodeLocationEntry) -> Self {
        Self {
            namespace: e.namespace,
            name: e.name,
            grpc_endpoint: e.grpc_endpoint,
            image: e.image,
            module: e.module,
            phase: e.phase,
            observed_generation: e.observed_generation,
            identity: e.identity,
        }
    }
}

impl From<rivers_core::storage::BackfillsSummary> for BackfillsSummary {
    fn from(s: rivers_core::storage::BackfillsSummary) -> Self {
        Self {
            total: s.total,
            in_progress: s.in_progress,
            completed_success: s.completed_success,
            completed_failed: s.completed_failed,
            canceled: s.canceled,
        }
    }
}

impl From<BackfillFilter> for rivers_core::storage::BackfillFilter {
    fn from(f: BackfillFilter) -> Self {
        use rivers_core::storage::BackfillStatus;
        Self {
            status: f.status.and_then(|s| match s.as_str() {
                "Requested" => Some(BackfillStatus::Requested),
                "InProgress" => Some(BackfillStatus::InProgress),
                "CompletedSuccess" => Some(BackfillStatus::CompletedSuccess),
                "CompletedFailed" => Some(BackfillStatus::CompletedFailed),
                "Canceled" => Some(BackfillStatus::Canceled),
                _ => None,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The UI's filter inputs send `Some("")` when a search box is empty.
    /// The storage layer expects `None` for "no filter" — if the empty
    /// string leaked through, `string::contains(x, "")` is always true
    /// in SurrealQL so nothing would be filtered out (wrong), or worse,
    /// some backends would ERROR on an empty substring. The `From`
    /// impl is where that coercion happens — regression-proof it.
    #[test]
    fn run_filter_coerces_empty_strings_to_none() {
        let ui = RunFilter {
            status: None,
            job_name: Some(String::new()),
            job_substring: Some(String::new()),
            asset_substring: Some(String::new()),
            partition_substring: Some(String::new()),
            action: VerbFilter::Any,
        };
        let core: rivers_core::storage::RunFilter = ui.into();
        assert!(core.status.is_none());
        assert!(core.job_name.is_none());
        assert!(core.job_substring.is_none());
        assert!(core.asset_substring.is_none());
        assert!(core.partition_substring.is_none());
    }

    /// `get_runs_page` is `#[server(input = Json)]`, so every filter state
    /// has to survive a JSON round-trip. A nested `Option` did not: both
    /// `None` and `Some(None)` encode as `null`, so "materialize only"
    /// silently degraded to "no filter" on the way to the server.
    #[test]
    fn verb_filter_survives_json_and_maps_to_core() {
        for (filter, expected) in [
            (VerbFilter::Any, None),
            (VerbFilter::MaterializeOnly, Some(None)),
            (
                VerbFilter::Verb("purge".into()),
                Some(Some("purge".to_string())),
            ),
        ] {
            let json = serde_json::to_string(&RunFilter {
                action: filter.clone(),
                ..Default::default()
            })
            .expect("filter serializes");
            let back: RunFilter = serde_json::from_str(&json).expect("filter deserializes");
            assert_eq!(back.action, filter, "round-trip changed the verb filter");
            let core: rivers_core::storage::RunFilter = back.into();
            assert_eq!(core.action, expected, "core encoding for {filter:?}");
        }
    }

    /// A destructive backfill has to stay distinguishable from a rebuild:
    /// the detail page's "Re-run" threads the record's verb, so dropping it
    /// in the DTO makes a delete sweep look like a materialize and repeat.
    #[test]
    fn backfill_info_keeps_the_verb() {
        let core = rivers_core::storage::BackfillRecord {
            backfill_id: "bf1".into(),
            code_location_id: rivers_core::storage::default_code_location_id(),
            status: rivers_core::storage::BackfillStatus::InProgress,
            strategy: rivers_core::storage::BackfillStrategy::MultiRun,
            failure_policy: rivers_core::storage::BackfillFailurePolicy::Continue,
            asset_selection: vec!["orders".into()],
            job_name: None,
            partition_keys: vec![rivers_core::storage::PartitionKey::Single {
                keys: vec!["p1".into()],
            }],
            run_ids: vec![],
            completed_partitions: vec![],
            failed_partitions: vec![],
            canceled_partitions: vec![],
            max_concurrency: 4,
            tags: vec![],
            create_time: 0,
            end_time: None,
            error: None,
            launched_by: rivers_core::storage::LaunchedBy::Manual { user: None },
            action: Some("purge".into()),
            config: None,
        };
        let ui: BackfillInfo = core.into();
        assert_eq!(ui.action.as_deref(), Some("purge"));
    }

    /// Run-list partition labels must use the same canonical `dim=v|dim=v`
    /// encoding as the picker/heatmap — a second hand-rolled format here
    /// renders the same partition differently across pages.
    #[test]
    fn run_record_partition_key_uses_canonical_display() {
        let core = rivers_core::storage::RunRecord {
            run_id: "r1".into(),
            code_location_id: rivers_core::storage::default_code_location_id(),
            job_name: None,
            status: rivers_core::storage::RunStatus::Success,
            start_time: 0,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: Some(rivers_core::storage::PartitionKey::Multi {
                dims: vec![
                    ("region".into(), vec!["us".into()]),
                    ("date".into(), vec!["2024-01-01".into()]),
                ],
            }),
            block_reason: None,
            launched_by: rivers_core::storage::LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        };
        let ui: RunRecord = core.into();
        let preview = ui
            .partition_key
            .expect("partitioned run keeps its partition preview");
        assert_eq!(preview.preview, vec!["date=2024-01-01|region=us"]);
        assert_eq!(preview.total, 1);
        assert_eq!(preview.label(), "date=2024-01-01|region=us");
    }

    /// Event partition labels must use the same canonical `dim=v|dim=v`
    /// encoding as everywhere else — `{:?}` leaks the Rust enum shape
    /// (`Multi { dims: [...] }`) into the UI.
    #[test]
    fn stored_event_partition_key_uses_canonical_display() {
        let core = rivers_core::storage::StoredEvent {
            id: rivers_core::surrealdb::types::RecordId::new("events", "e1"),
            event_type: rivers_core::storage::EventType::Materialization { data_version: None },
            asset_key: Some("orders".into()),
            run_id: "r1".into(),
            partition_key: Some(rivers_core::storage::PartitionKey::Multi {
                dims: vec![
                    ("region".into(), vec!["us".into()]),
                    ("date".into(), vec!["2024-01-01".into()]),
                ],
            }),
            timestamp: 0,
            metadata: vec![],
            code_version: None,
            input_data_versions: vec![],
        };
        let ui: StoredEvent = core.into();
        assert_eq!(
            ui.partition_key.as_deref(),
            Some("date=2024-01-01|region=us")
        );
    }

    /// The UI reads the traceback JSON the executor writes.
    #[test]
    fn run_log_reads_the_stored_traceback() {
        use rivers_core::execution::traceback as core;
        let json = core::Traceback {
            exceptions: vec![core::ExceptionInfo {
                exc_type: "KeyError".into(),
                module: None,
                value: "'b'".into(),
                chain: Some(core::ChainLink::Cause),
                frames: vec![core::Frame {
                    filename: "assets.py".into(),
                    abs_path: "/app/assets.py".into(),
                    function: "sales".into(),
                    lineno: Some(42),
                    colno: Some(12),
                    end_lineno: Some(42),
                    end_colno: Some(28),
                    pre_context: vec!["    values = {}".into()],
                    context_line: Some("    return values['b']".into()),
                    post_context: vec![],
                    in_app: true,
                    repeated: 2,
                }],
                group: vec![],
                group_omitted: 0,
            }],
            text: "Traceback (most recent call last):".into(),
        }
        .to_json();
        let stored = rivers_core::storage::StoredLog {
            id: rivers_core::surrealdb::types::RecordId::new("run_logs", "l1"),
            code_location_id: "default".into(),
            run_id: "r1".into(),
            step_key: "sales".into(),
            timestamp: 7,
            stdout: None,
            stderr: None,
            logs: None,
            traceback: Some(json),
        };
        let ui: RunLog = stored.into();
        let tb = ui.traceback.expect("the traceback parses");
        assert_eq!(tb.text, "Traceback (most recent call last):");
        let exc = &tb.exceptions[0];
        assert_eq!(
            (exc.exc_type.as_str(), exc.value.as_str(), exc.chain),
            ("KeyError", "'b'", Some(ChainLink::Cause))
        );
        let f = &exc.frames[0];
        assert_eq!(
            (
                f.lineno,
                f.colno,
                f.end_lineno,
                f.end_colno,
                f.repeated,
                f.in_app
            ),
            (Some(42), Some(12), Some(42), Some(28), 2, true)
        );
        assert_eq!(f.pre_context, vec!["    values = {}".to_string()]);
        assert!(f.post_context.is_empty());
    }

    /// Non-empty filter values round-trip unchanged.
    #[test]
    fn run_filter_preserves_non_empty_strings() {
        let ui = RunFilter {
            status: Some(RunStatus::Failure),
            job_name: Some("daily_ingest".into()),
            job_substring: Some("daily".into()),
            asset_substring: Some("orders".into()),
            partition_substring: Some("2024-01".into()),
            action: VerbFilter::Any,
        };
        let core: rivers_core::storage::RunFilter = ui.into();
        assert_eq!(core.status, Some(rivers_core::storage::RunStatus::Failure));
        assert_eq!(core.job_name.as_deref(), Some("daily_ingest"));
        assert_eq!(core.job_substring.as_deref(), Some("daily"));
        assert_eq!(core.asset_substring.as_deref(), Some("orders"));
        assert_eq!(core.partition_substring.as_deref(), Some("2024-01"));
    }

    /// Backfill filter: status string → enum mapping round-trips the
    /// five valid variants and drops anything unknown (instead of
    /// passing garbage through to the DB).
    #[test]
    fn backfill_filter_status_mapping() {
        use rivers_core::storage::BackfillStatus;
        let cases = [
            ("Requested", Some(BackfillStatus::Requested)),
            ("InProgress", Some(BackfillStatus::InProgress)),
            ("CompletedSuccess", Some(BackfillStatus::CompletedSuccess)),
            ("CompletedFailed", Some(BackfillStatus::CompletedFailed)),
            ("Canceled", Some(BackfillStatus::Canceled)),
            ("Garbage", None),
            ("", None),
        ];
        for (input, expected) in cases {
            let ui = BackfillFilter {
                status: Some(input.to_string()),
            };
            let core: rivers_core::storage::BackfillFilter = ui.into();
            assert_eq!(core.status, expected, "for input {input:?}");
        }
        // `None` stays `None`.
        let ui = BackfillFilter { status: None };
        let core: rivers_core::storage::BackfillFilter = ui.into();
        assert!(core.status.is_none());
    }
}
