//! The condition tree type system.
//!
//! [`ConditionNode`] is a recursive enum representing automation conditions:
//! leaf predicates (Missing, CronTickPassed, CodeVersionChanged, ...),
//! dependency aggregates (AnyDepsMatch, AllDepsMatch, ...), boolean combinators
//! (And, Or, Not), and stateful operators (NewlyTrue, Since, SinceLastHandled).
//! Includes preset constructors like `eager()`, `on_cron()`, and `on_missing()`.

use serde::{Deserialize, Serialize};

pub fn format_tag_label(
    name: &str,
    tag_keys: &[String],
    tag_values: &[(String, String)],
) -> String {
    let mut parts: Vec<String> = tag_keys.to_vec();
    parts.extend(tag_values.iter().map(|(k, v)| format!("{}={}", k, v)));
    format!("{}({})", name, parts.join(", "))
}

/// A condition tree node describing when an asset should be auto-materialized.
///
/// Mirrors the Python-side `ConditionNode` in `python/src/automation/condition.rs`.
/// Serde derives allow persisting evaluation state to KV store.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ConditionNode {
    Missing,
    InProgress,
    ExecutionFailed,
    NewlyUpdated,
    NewlyRequested,
    CodeVersionChanged,
    CronTickPassed {
        cron_schedule: String,
        timezone: Option<String>,
    },
    InLatestTimeWindow {
        lookback_delta: Option<f64>,
    },
    /// True on the first evaluation tick after daemon startup or condition tree change.
    /// Use this as a composable primitive for first-tick behavior instead of baking
    /// `is_initial` checks into stateful operators like `NewlyTrue`.
    InitialEvaluation,
    /// True when the asset's `last_data_version` changed since the previous tick.
    /// Tracks the data version in `AssetConditionState.last_data_version` across ticks.
    DataVersionChanged,
    /// True when the asset is part of an active backfill (Requested or InProgress status).
    /// Distinct from `InProgress` which tracks regular run-level in-progress state.
    /// Users who want both can compose: `InProgress | BackfillInProgress`.
    BackfillInProgress,
    /// True when the latest run that materialized this asset/partition had matching tags.
    /// Supports two matching modes: `tag_keys` checks key presence (any value),
    /// `tag_values` checks exact key-value pairs. Both are combined with AND.
    LastExecutedWithTags {
        tag_keys: Vec<String>,
        tag_values: Vec<(String, String)>,
    },
    /// True if the latest run that materialized this asset also included the
    /// root target (the top-level asset being evaluated) in its `node_names`.
    /// Always false when `target_key == root_key` (self-referential guard).
    /// Designed for use inside `any_deps_match()` to suppress cascading when
    /// a joint run already covered both the dep and the downstream.
    LastRunIncludesTarget,
    /// True if the target asset's condition has already fired (will be requested
    /// for materialization) earlier in this evaluation tick.
    /// Requires topological evaluation order (deps before downstreams).
    /// Used in `any_deps_updated()` to trigger downstream before dep run completes,
    /// and in `any_deps_missing()` to avoid blocking when the missing dep is about
    /// to be materialized.
    WillBeRequested,

    AnyDepsMatch {
        condition: Box<ConditionNode>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
    },
    AllDepsMatch {
        condition: Box<ConditionNode>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
    },

    And(Vec<ConditionNode>),
    Or(Vec<ConditionNode>),
    Not(Box<ConditionNode>),

    /// Rising-edge detector: true only on the tick where `child` transitions false → true.
    /// Stores the child's raw value in `sub_results` so the next tick can detect the transition.
    NewlyTrue(Box<ConditionNode>),

    /// Latch (SR flip-flop): once `trigger` fires, stays true until `reset` fires.
    /// Reset takes priority — if both fire on the same tick, the result is false.
    ///
    /// Use case: fire-once-then-wait semantics. Without `Since`, conditions that stay
    /// true across ticks (e.g. code version mismatch) would spam materializations every tick.
    /// With `Since`, the condition fires once, then turns off when the reset acknowledges it:
    ///
    /// - `code_version_changed().since(newly_requested())` — "code changed and we haven't
    ///   requested materialization yet"
    /// - `any_deps_updated().since(newly_requested() | newly_updated())` — "a dep updated
    ///   and we haven't handled it yet" (this is what `since_last_handled` expands to)
    Since {
        trigger: Box<ConditionNode>,
        reset: Box<ConditionNode>,
    },

    /// Shorthand for debounce: true while `child` is true and hasn't been handled
    /// (i.e. `last_handled_timestamp < last_tick_timestamp`). Used in presets like
    /// `eager()` and `on_cron()` to prevent re-firing on the tick immediately after handling.
    SinceLastHandled(Box<ConditionNode>),
    /// True if any of this asset's new materializations (this tick) came from runs with matching tags.
    /// Designed for composition inside `any_deps_match()` to filter dep updates by run tags.
    HasRunWithTags {
        tag_keys: Vec<String>,
        tag_values: Vec<(String, String)>,
    },
    /// True if all of this asset's new materializations (this tick) came from runs with matching tags.
    /// Vacuously true if there were no new materializations this tick.
    AllRunsHaveTags {
        tag_keys: Vec<String>,
        tag_values: Vec<(String, String)>,
    },
    /// Evaluate a condition on specific named assets. True if the condition
    /// is true for any of the listed assets.
    AssetMatches {
        keys: Vec<String>,
        condition: Box<ConditionNode>,
    },
}

impl ConditionNode {
    /// Eager materialization preset.
    ///
    /// ```text
    /// And([
    ///     SinceLastHandled(Or([
    ///         NewlyTrue(Missing),      // = newly_missing()
    ///         any_deps_updated(),
    ///     ])),
    ///     Not(AnyDepsMissing),
    ///     Not(AnyDepsInProgress),
    ///     Not(in_flight),             // = Not(InProgress | BackfillInProgress)
    ///     Not(ExecutionFailed),
    /// ])
    /// ```
    ///
    /// `NewlyTrue` is a pure rising-edge detector (fires when child transitions
    /// false→true). On the first tick with no previous state, `previous=false`
    /// so `NewlyTrue(Missing)` fires if the asset is missing — no `is_initial`
    /// hack needed. Users can compose `InitialEvaluation` explicitly for custom
    /// first-tick behavior.
    ///
    /// `Not(in_flight)` excludes partitions already being materialized by a run
    /// *or* an active backfill, and is the *only* dispatch gate (`apply_results`
    /// dispatches whatever fires). The backfill arm matters: a backfill registers
    /// its per-partition runs lazily, so a not-yet-started partition is still
    /// owned by it and must not be re-fired (root floor `None`) as a duplicate.
    ///
    /// `Not(ExecutionFailed)` excludes failed partitions/assets, so they aren't
    /// auto-retried every tick until re-run.
    ///
    pub fn eager() -> Self {
        (ConditionNode::Missing.newly_true() | ConditionNode::any_deps_updated())
            .since_last_handled()
            & !ConditionNode::any_deps_missing()
            & !ConditionNode::any_deps_in_progress()
            & !ConditionNode::in_flight()
            & !ConditionNode::ExecutionFailed
    }

    /// Composite: true if any dep was updated (and the run didn't already
    /// include the target) OR will be requested this tick. Enables same-tick
    /// cascading: downstream fires when its dep's condition fires, without
    /// waiting for the dep to materialize first.
    pub fn any_deps_updated() -> Self {
        ConditionNode::AnyDepsMatch {
            condition: Box::new(
                (ConditionNode::NewlyUpdated & !ConditionNode::LastRunIncludesTarget)
                    | ConditionNode::WillBeRequested,
            ),
            label: Some("any_deps_updated".into()),
        }
    }

    /// Composite: true if any dep is missing AND won't be requested this tick.
    /// A missing dep that is about to be materialized (WillBeRequested) does
    /// not block the downstream.
    pub fn any_deps_missing() -> Self {
        ConditionNode::AnyDepsMatch {
            condition: Box::new(ConditionNode::Missing & !ConditionNode::WillBeRequested),
            label: Some("any_deps_missing".into()),
        }
    }

    /// Composite: true if any dep is in progress. Equivalent to `any_deps_match(InProgress)`.
    pub fn any_deps_in_progress() -> Self {
        ConditionNode::AnyDepsMatch {
            condition: Box::new(ConditionNode::InProgress),
            label: Some("any_deps_in_progress".into()),
        }
    }

    /// Composite: being materialized by *anything* — a run (`InProgress`) or an
    /// active backfill (`BackfillInProgress`). Presets negate this as their sole
    /// re-dispatch guard; the two leaves stay separate for composing either alone.
    pub fn in_flight() -> Self {
        ConditionNode::InProgress | ConditionNode::BackfillInProgress
    }

    /// Composite: true if all deps have been updated since the last cron tick.
    ///
    /// ```text
    /// AllDepsMatch(
    ///     Since(trigger=NewlyUpdated, reset=CronTickPassed) | WillBeRequested
    /// )
    /// ```
    ///
    /// Each dep's SR latch resets on the cron tick and goes true when the dep
    /// updates. `WillBeRequested` allows same-tick cascading.
    pub fn all_deps_updated_since_cron(cron_schedule: String, timezone: Option<String>) -> Self {
        ConditionNode::AllDepsMatch {
            condition: Box::new(
                ConditionNode::NewlyUpdated.since(ConditionNode::CronTickPassed {
                    cron_schedule,
                    timezone,
                }) | ConditionNode::WillBeRequested,
            ),
            label: Some("all_deps_updated_since_cron".into()),
        }
    }

    /// Cron-based automation preset.
    ///
    /// ```text
    /// And([
    ///     SinceLastHandled(CronTickPassed),
    ///     all_deps_updated_since_cron(schedule, tz),
    ///     Not(in_flight),
    /// ])
    /// ```
    ///
    /// Waits for a cron tick, then fires once all deps have been updated since
    /// that tick. Users can compose with `InLatestTimeWindow` or `.without()`
    /// to customize partition filtering.
    ///
    /// `Not(in_flight)` keeps a cron tick from overlapping a materialization
    /// still in flight (cron interval shorter than the run)
    pub fn on_cron(cron_schedule: String, timezone: Option<String>) -> Self {
        ConditionNode::CronTickPassed {
            cron_schedule: cron_schedule.clone(),
            timezone: timezone.clone(),
        }
        .since_last_handled()
            & ConditionNode::all_deps_updated_since_cron(cron_schedule, timezone)
            & !ConditionNode::in_flight()
    }

    /// On-missing preset. Fires when an asset becomes missing and has no
    /// missing deps, skipping partitions in a failed state (so a deliberate
    /// `mark_partition_failed` isn't re-requested). Users can compose with
    /// `InLatestTimeWindow` to restrict to recent partitions.
    pub fn on_missing() -> Self {
        ConditionNode::Missing.newly_true().since_last_handled()
            & !ConditionNode::any_deps_missing()
            & !ConditionNode::ExecutionFailed
    }

    /// Returns true if this condition tree contains any time-based nodes
    /// (e.g. `CronTickPassed`) that need evaluation even when storage hasn't changed.
    pub fn has_time_based_conditions(&self) -> bool {
        match self {
            ConditionNode::CronTickPassed { .. } => true,
            ConditionNode::And(children) | ConditionNode::Or(children) => {
                children.iter().any(|c| c.has_time_based_conditions())
            }
            ConditionNode::Not(child)
            | ConditionNode::NewlyTrue(child)
            | ConditionNode::SinceLastHandled(child) => child.has_time_based_conditions(),
            ConditionNode::Since { trigger, reset } => {
                trigger.has_time_based_conditions() || reset.has_time_based_conditions()
            }
            ConditionNode::AnyDepsMatch { condition, .. }
            | ConditionNode::AllDepsMatch { condition, .. }
            | ConditionNode::AssetMatches { condition, .. } => {
                condition.has_time_based_conditions()
            }
            _ => false,
        }
    }

    /// True if the tree contains a stateful operator (`Since`/`NewlyTrue`).
    /// Dep-aggregates must not short-circuit over one, or a skipped dep loses
    /// its latch.
    pub fn has_stateful_nodes(&self) -> bool {
        match self {
            ConditionNode::Since { .. } | ConditionNode::NewlyTrue(_) => true,
            ConditionNode::And(children) | ConditionNode::Or(children) => {
                children.iter().any(|c| c.has_stateful_nodes())
            }
            ConditionNode::Not(child)
            | ConditionNode::SinceLastHandled(child)
            | ConditionNode::AnyDepsMatch {
                condition: child, ..
            }
            | ConditionNode::AllDepsMatch {
                condition: child, ..
            }
            | ConditionNode::AssetMatches {
                condition: child, ..
            } => child.has_stateful_nodes(),
            _ => false,
        }
    }

    /// Returns true if this condition tree contains `HasRunWithTags` or `AllRunsHaveTags`.
    pub fn uses_tick_tags(&self) -> bool {
        match self {
            ConditionNode::HasRunWithTags { .. } | ConditionNode::AllRunsHaveTags { .. } => true,
            ConditionNode::And(children) | ConditionNode::Or(children) => {
                children.iter().any(|c| c.uses_tick_tags())
            }
            ConditionNode::Not(child)
            | ConditionNode::NewlyTrue(child)
            | ConditionNode::SinceLastHandled(child) => child.uses_tick_tags(),
            ConditionNode::Since { trigger, reset } => {
                trigger.uses_tick_tags() || reset.uses_tick_tags()
            }
            ConditionNode::AnyDepsMatch { condition, .. }
            | ConditionNode::AllDepsMatch { condition, .. }
            | ConditionNode::AssetMatches { condition, .. } => condition.uses_tick_tags(),
            _ => false,
        }
    }

    /// True if any upstream dep matches the given condition.
    pub fn any_deps_match(condition: ConditionNode) -> Self {
        ConditionNode::AnyDepsMatch {
            condition: Box::new(condition),
            label: None,
        }
    }

    /// True if all upstream deps match the given condition.
    pub fn all_deps_match(condition: ConditionNode) -> Self {
        ConditionNode::AllDepsMatch {
            condition: Box::new(condition),
            label: None,
        }
    }

    /// `self.since(reset)` — true while `self` has fired and `reset` has not.
    pub fn since(self, reset: ConditionNode) -> Self {
        ConditionNode::Since {
            trigger: Box::new(self),
            reset: Box::new(reset),
        }
    }

    /// `self.newly_true()` — true only on the tick where `self` transitions false→true.
    pub fn newly_true(self) -> Self {
        ConditionNode::NewlyTrue(Box::new(self))
    }

    /// `self.since_last_handled()` — true while `self` is true and hasn't been handled.
    pub fn since_last_handled(self) -> Self {
        ConditionNode::SinceLastHandled(Box::new(self))
    }

    /// Recursively replace nodes. Matches by label string.
    pub fn replace_by_label(&self, old_label: &str, replacement: &ConditionNode) -> ConditionNode {
        self.replace_inner(&|node| node.node_label() == old_label, replacement)
    }

    /// Recursively replace nodes. Matches by structural equality.
    pub fn replace_by_node(
        &self,
        old: &ConditionNode,
        replacement: &ConditionNode,
    ) -> ConditionNode {
        self.replace_inner(&|node| node == old, replacement)
    }

    fn replace_inner(
        &self,
        matches: &dyn Fn(&ConditionNode) -> bool,
        replacement: &ConditionNode,
    ) -> ConditionNode {
        if matches(self) {
            return replacement.clone();
        }
        match self {
            ConditionNode::And(children) => ConditionNode::And(
                children
                    .iter()
                    .map(|c| c.replace_inner(matches, replacement))
                    .collect(),
            ),
            ConditionNode::Or(children) => ConditionNode::Or(
                children
                    .iter()
                    .map(|c| c.replace_inner(matches, replacement))
                    .collect(),
            ),
            ConditionNode::Not(child) => {
                ConditionNode::Not(Box::new(child.replace_inner(matches, replacement)))
            }
            ConditionNode::NewlyTrue(child) => {
                ConditionNode::NewlyTrue(Box::new(child.replace_inner(matches, replacement)))
            }
            ConditionNode::SinceLastHandled(child) => {
                ConditionNode::SinceLastHandled(Box::new(child.replace_inner(matches, replacement)))
            }
            ConditionNode::Since { trigger, reset } => ConditionNode::Since {
                trigger: Box::new(trigger.replace_inner(matches, replacement)),
                reset: Box::new(reset.replace_inner(matches, replacement)),
            },
            ConditionNode::AnyDepsMatch { condition, label } => ConditionNode::AnyDepsMatch {
                condition: Box::new(condition.replace_inner(matches, replacement)),
                label: label.clone(),
            },
            ConditionNode::AllDepsMatch { condition, label } => ConditionNode::AllDepsMatch {
                condition: Box::new(condition.replace_inner(matches, replacement)),
                label: label.clone(),
            },
            ConditionNode::AssetMatches { keys, condition } => ConditionNode::AssetMatches {
                keys: keys.clone(),
                condition: Box::new(condition.replace_inner(matches, replacement)),
            },
            other => other.clone(),
        }
    }

    pub fn without_matching(&self, pred: &dyn Fn(&ConditionNode) -> bool) -> ConditionNode {
        match self {
            ConditionNode::And(children) => {
                ConditionNode::And(children.iter().filter(|&c| !pred(c)).cloned().collect())
            }
            other => other.clone(),
        }
    }

    /// Returns true if this node or any descendant has the given label.
    pub fn contains_label(&self, label: &str) -> bool {
        if self.node_label() == label {
            return true;
        }
        match self {
            ConditionNode::And(children) | ConditionNode::Or(children) => {
                children.iter().any(|c| c.contains_label(label))
            }
            ConditionNode::Not(child)
            | ConditionNode::NewlyTrue(child)
            | ConditionNode::SinceLastHandled(child) => child.contains_label(label),
            ConditionNode::Since { trigger, reset } => {
                trigger.contains_label(label) || reset.contains_label(label)
            }
            ConditionNode::AnyDepsMatch { condition, .. }
            | ConditionNode::AllDepsMatch { condition, .. }
            | ConditionNode::AssetMatches { condition, .. } => condition.contains_label(label),
            _ => false,
        }
    }

    /// Evaluate a condition on specific named assets (true if any match).
    pub fn asset_matches(keys: Vec<String>, condition: ConditionNode) -> Self {
        ConditionNode::AssetMatches {
            keys,
            condition: Box::new(condition),
        }
    }

    /// Short display label for a single node (non-recursive).
    pub fn node_label(&self) -> String {
        match self {
            ConditionNode::Missing => "missing".into(),
            ConditionNode::InProgress => "in_progress".into(),
            ConditionNode::ExecutionFailed => "execution_failed".into(),
            ConditionNode::NewlyUpdated => "newly_updated".into(),
            ConditionNode::NewlyRequested => "newly_requested".into(),
            ConditionNode::CodeVersionChanged => "code_version_changed".into(),
            ConditionNode::CronTickPassed {
                cron_schedule,
                timezone,
            } => match timezone {
                // Include the tz: it's load-bearing (changes fire times), so two
                // crons differing only by zone must not collapse to one label.
                Some(tz) => format!("cron_tick_passed('{}', tz='{}')", cron_schedule, tz),
                None => format!("cron_tick_passed('{}')", cron_schedule),
            },
            ConditionNode::InLatestTimeWindow { lookback_delta } => match lookback_delta {
                Some(d) => format!("in_latest_time_window({}s)", d),
                None => "in_latest_time_window".into(),
            },
            ConditionNode::InitialEvaluation => "initial_evaluation".into(),
            ConditionNode::DataVersionChanged => "data_version_changed".into(),
            ConditionNode::BackfillInProgress => "backfill_in_progress".into(),
            ConditionNode::LastExecutedWithTags {
                tag_keys,
                tag_values,
            } => format_tag_label("last_executed_with_tags", tag_keys, tag_values),
            ConditionNode::LastRunIncludesTarget => "last_run_includes_target".into(),
            ConditionNode::WillBeRequested => "will_be_requested".into(),
            ConditionNode::HasRunWithTags {
                tag_keys,
                tag_values,
            } => format_tag_label("has_run_with_tags", tag_keys, tag_values),
            ConditionNode::AllRunsHaveTags {
                tag_keys,
                tag_values,
            } => format_tag_label("all_runs_have_tags", tag_keys, tag_values),
            // Unlabeled dep-aggregates fold the inner condition's fingerprint in
            // so two siblings differing only by inner condition don't collapse
            // to one label (replace_by_label/contains_label would otherwise hit
            // the wrong subtree — same reasoning as the cron tz above).
            ConditionNode::AnyDepsMatch { condition, label } => match label {
                Some(l) => l.clone(),
                None => format!("any_deps_match({})", condition.fingerprint_hex()),
            },
            ConditionNode::AllDepsMatch { condition, label } => match label {
                Some(l) => l.clone(),
                None => format!("all_deps_match({})", condition.fingerprint_hex()),
            },
            ConditionNode::AssetMatches { keys, condition } => {
                let keys_label = if keys.len() == 1 {
                    format!("'{}'", keys[0])
                } else {
                    let joined: Vec<_> = keys.iter().map(|k| format!("'{}'", k)).collect();
                    format!("[{}]", joined.join(", "))
                };
                format!("asset_matches({}, {})", keys_label, condition.fingerprint_hex())
            }
            ConditionNode::And(_) => "All of".into(),
            ConditionNode::Or(_) => "Any of".into(),
            ConditionNode::Not(_) => "Not".into(),
            ConditionNode::NewlyTrue(_) => "newly_true".into(),
            ConditionNode::Since { .. } => "since".into(),
            ConditionNode::SinceLastHandled(_) => "since_last_handled".into(),
        }
    }

    /// Deterministic fingerprint of this condition tree.
    ///
    /// Serializes the tree to canonical JSON and hashes with fixed-key SipHash
    /// for cross-process stability. Used to detect when the condition tree has
    /// changed across daemon restarts.
    pub fn fingerprint(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = siphasher::sip::SipHasher::new_with_keys(0, 0);
        let json = serde_json::to_string(self).unwrap_or_default();
        json.hash(&mut hasher);
        hasher.finish()
    }

    /// Hex-encoded fingerprint string for storage.
    pub fn fingerprint_hex(&self) -> String {
        format!("{:016x}", self.fingerprint())
    }

    /// Find the first `InLatestTimeWindow` node in the tree and return its `lookback_delta`.
    /// Returns `None` if no such node exists, `Some(None)` if it exists without a lookback.
    pub fn find_lookback_delta(&self) -> Option<Option<f64>> {
        match self {
            ConditionNode::InLatestTimeWindow { lookback_delta } => Some(*lookback_delta),
            ConditionNode::And(children) | ConditionNode::Or(children) => {
                children.iter().find_map(|c| c.find_lookback_delta())
            }
            ConditionNode::Not(child)
            | ConditionNode::NewlyTrue(child)
            | ConditionNode::SinceLastHandled(child) => child.find_lookback_delta(),
            ConditionNode::Since { trigger, reset } => trigger
                .find_lookback_delta()
                .or_else(|| reset.find_lookback_delta()),
            ConditionNode::AnyDepsMatch { condition, .. }
            | ConditionNode::AllDepsMatch { condition, .. }
            | ConditionNode::AssetMatches { condition, .. } => condition.find_lookback_delta(),
            _ => None,
        }
    }

    /// Type tag for UI rendering.
    pub fn node_type_str(&self) -> &'static str {
        match self {
            ConditionNode::And(_) => "And",
            ConditionNode::Or(_) => "Or",
            ConditionNode::Not(_) => "Not",
            ConditionNode::NewlyTrue(_) => "NewlyTrue",
            ConditionNode::Since { .. } => "Since",
            ConditionNode::SinceLastHandled(_) => "SinceLastHandled",
            ConditionNode::AnyDepsMatch { .. } => "AnyDepsMatch",
            ConditionNode::AllDepsMatch { .. } => "AllDepsMatch",
            ConditionNode::AssetMatches { .. } => "AssetMatches",
            _ => "Leaf",
        }
    }
}

impl std::ops::Not for ConditionNode {
    type Output = Self;

    fn not(self) -> Self {
        ConditionNode::Not(Box::new(self))
    }
}

impl std::ops::BitAnd for ConditionNode {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        let mut children = match self {
            ConditionNode::And(c) => c,
            other => vec![other],
        };
        match rhs {
            ConditionNode::And(c) => children.extend(c),
            other => children.push(other),
        }
        ConditionNode::And(children)
    }
}

impl std::ops::BitOr for ConditionNode {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        let mut children = match self {
            ConditionNode::Or(c) => c,
            other => vec![other],
        };
        match rhs {
            ConditionNode::Or(c) => children.extend(c),
            other => children.push(other),
        }
        ConditionNode::Or(children)
    }
}
