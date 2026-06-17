//! Python bindings for automation conditions (AutomationCondition class).
use pyo3::prelude::*;

pub use rivers_core::condition::ConditionNode;

/// Reject an invalid cron schedule at construction so the error surfaces when
/// the condition is defined, not when the daemon first evaluates it.
fn validate_cron_schedule(schedule: &str) -> PyResult<()> {
    rivers_core::condition::validate_cron(schedule).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid cron schedule {schedule:?}: {e}"))
    })
}

/// Human-readable description of a condition node (for Python display).
pub(crate) fn description(node: &ConditionNode) -> String {
    match node {
        ConditionNode::Missing => "missing".to_string(),
        ConditionNode::InProgress => "in_progress".to_string(),
        ConditionNode::ExecutionFailed => "execution_failed".to_string(),
        ConditionNode::NewlyUpdated => "newly_updated".to_string(),
        ConditionNode::NewlyRequested => "newly_requested".to_string(),
        ConditionNode::CodeVersionChanged => "code_version_changed".to_string(),
        ConditionNode::CronTickPassed {
            cron_schedule,
            timezone,
        } => {
            if let Some(tz) = timezone {
                format!("cron_tick_passed('{}', tz='{}')", cron_schedule, tz)
            } else {
                format!("cron_tick_passed('{}')", cron_schedule)
            }
        }
        ConditionNode::InLatestTimeWindow { lookback_delta } => {
            if let Some(d) = lookback_delta {
                format!("in_latest_time_window(lookback={})", d)
            } else {
                "in_latest_time_window".to_string()
            }
        }
        ConditionNode::InitialEvaluation => "initial_evaluation".to_string(),
        ConditionNode::DataVersionChanged => "data_version_changed".to_string(),
        ConditionNode::BackfillInProgress => "backfill_in_progress".to_string(),
        ConditionNode::LastExecutedWithTags {
            tag_keys,
            tag_values,
        } => rivers_core::condition::node::format_tag_label(
            "last_executed_with_tags",
            tag_keys,
            tag_values,
        ),
        ConditionNode::LastRunIncludesTarget => "last_run_includes_target".to_string(),
        ConditionNode::WillBeRequested => "will_be_requested".to_string(),
        ConditionNode::HasRunWithTags {
            tag_keys,
            tag_values,
        } => rivers_core::condition::node::format_tag_label(
            "has_run_with_tags",
            tag_keys,
            tag_values,
        ),
        ConditionNode::AllRunsHaveTags {
            tag_keys,
            tag_values,
        } => rivers_core::condition::node::format_tag_label(
            "all_runs_have_tags",
            tag_keys,
            tag_values,
        ),
        ConditionNode::AnyDepsMatch { condition, label } => match label {
            Some(l) => l.clone(),
            None => format!("any_deps_match({})", description(condition)),
        },
        ConditionNode::AllDepsMatch { condition, label } => match label {
            Some(l) => l.clone(),
            None => format!("all_deps_match({})", description(condition)),
        },
        ConditionNode::AssetMatches { keys, condition } => {
            if keys.len() == 1 {
                format!("asset_matches('{}', {})", keys[0], description(condition))
            } else {
                let joined: Vec<_> = keys.iter().map(|k| format!("'{}'", k)).collect();
                format!(
                    "asset_matches([{}], {})",
                    joined.join(", "),
                    description(condition)
                )
            }
        }
        ConditionNode::And(children) => {
            let parts: Vec<String> = children.iter().map(description).collect();
            format!("({})", parts.join(" & "))
        }
        ConditionNode::Or(children) => {
            let parts: Vec<String> = children.iter().map(description).collect();
            format!("({})", parts.join(" | "))
        }
        ConditionNode::Not(child) => format!("~{}", description(child)),
        ConditionNode::NewlyTrue(child) => {
            format!("{}.newly_true()", description(child))
        }
        ConditionNode::Since { trigger, reset } => {
            format!("{}.since({})", description(trigger), description(reset))
        }
        ConditionNode::SinceLastHandled(child) => {
            format!("{}.since_last_handled()", description(child))
        }
    }
}

/// Extract `str | list[str]` into `Vec<String>`.
fn extract_keys(ob: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(s) = ob.extract::<String>() {
        Ok(vec![s])
    } else {
        ob.extract::<Vec<String>>()
    }
}

/// Python-facing automation condition. Supports `&`, `|`, `~` operators for composition.
#[pyclass(
    name = "AutomationCondition",
    frozen,
    from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyAutomationCondition {
    pub(crate) node: ConditionNode,
    pub(crate) label: Option<String>,
}

impl PyAutomationCondition {
    fn new_node(node: ConditionNode) -> Self {
        let label = Some(description(&node));
        Self { node, label }
    }

    fn new_junction(node: ConditionNode) -> Self {
        Self { node, label: None }
    }
}

#[pymethods]
impl PyAutomationCondition {
    /// Eager materialization: run when deps update or asset becomes missing;
    /// excludes failed partitions/assets, so they aren't auto-retried until re-run.
    #[staticmethod]
    fn eager() -> Self {
        Self {
            node: ConditionNode::eager(),
            label: Some("eager".to_string()),
        }
    }

    /// Cron-based automation: run on cron ticks.
    #[staticmethod]
    #[pyo3(signature = (cron_schedule, timezone=None))]
    fn on_cron(cron_schedule: String, timezone: Option<String>) -> PyResult<Self> {
        validate_cron_schedule(&cron_schedule)?;
        let label = if let Some(ref tz) = timezone {
            format!("on_cron('{}', tz='{}')", cron_schedule, tz)
        } else {
            format!("on_cron('{}')", cron_schedule)
        };
        Ok(Self {
            node: ConditionNode::on_cron(cron_schedule, timezone),
            label: Some(label),
        })
    }

    /// Only run when asset becomes missing; skips failed partitions.
    #[staticmethod]
    fn on_missing() -> Self {
        Self {
            node: ConditionNode::on_missing(),
            label: Some("on_missing".to_string()),
        }
    }

    /// True when the asset/partition has never been materialized.
    #[staticmethod]
    fn missing() -> Self {
        Self::new_node(ConditionNode::Missing)
    }

    /// True when the asset is part of an in-progress run.
    #[staticmethod]
    fn in_progress() -> Self {
        Self::new_node(ConditionNode::InProgress)
    }

    /// True when the latest execution failed.
    #[staticmethod]
    fn execution_failed() -> Self {
        Self::new_node(ConditionNode::ExecutionFailed)
    }

    /// True when the asset was updated since the previous tick.
    #[staticmethod]
    fn newly_updated() -> Self {
        Self::new_node(ConditionNode::NewlyUpdated)
    }

    /// True when the asset was requested on the previous tick.
    #[staticmethod]
    fn newly_requested() -> Self {
        Self::new_node(ConditionNode::NewlyRequested)
    }

    /// True when the asset's code version changed since the last tick.
    #[staticmethod]
    fn code_version_changed() -> Self {
        Self::new_node(ConditionNode::CodeVersionChanged)
    }

    /// True when a cron tick just passed.
    #[staticmethod]
    #[pyo3(signature = (cron_schedule, timezone=None))]
    fn cron_tick_passed(cron_schedule: String, timezone: Option<String>) -> PyResult<Self> {
        validate_cron_schedule(&cron_schedule)?;
        Ok(Self::new_node(ConditionNode::CronTickPassed {
            cron_schedule,
            timezone,
        }))
    }

    /// True when the partition is in the latest time window.
    #[staticmethod]
    #[pyo3(signature = (lookback_delta=None))]
    fn in_latest_time_window(lookback_delta: Option<f64>) -> PyResult<Self> {
        if let Some(delta) = lookback_delta
            && (!delta.is_finite() || delta <= 0.0)
        {
            // NaN/negative silently select no windows; inf overflows the
            // cutoff arithmetic.
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "lookback_delta must be a positive number of seconds, got {delta}"
            )));
        }
        Ok(Self::new_node(ConditionNode::InLatestTimeWindow {
            lookback_delta,
        }))
    }

    /// True on the first evaluation tick after daemon startup or condition tree change.
    #[staticmethod]
    fn initial_evaluation() -> Self {
        Self::new_node(ConditionNode::InitialEvaluation)
    }

    /// True when the asset's data version changed since the previous tick.
    #[staticmethod]
    fn data_version_changed() -> Self {
        Self::new_node(ConditionNode::DataVersionChanged)
    }

    /// True when the asset is part of an active backfill (Requested or InProgress).
    #[staticmethod]
    fn backfill_in_progress() -> Self {
        Self::new_node(ConditionNode::BackfillInProgress)
    }

    /// True while being materialized by anything — a run (`in_progress`) or an
    /// active backfill (`backfill_in_progress`). Negate it
    /// (`~AutomationCondition.in_flight()`) in custom conditions to avoid
    /// re-dispatching running work; the presets already do.
    #[staticmethod]
    fn in_flight() -> Self {
        Self {
            node: ConditionNode::in_flight(),
            label: Some("in_flight".to_string()),
        }
    }

    /// True when the latest run that materialized this asset/partition had matching tags.
    /// `tag_keys` checks key presence (any value), `tag_values` checks exact key-value pairs.
    #[staticmethod]
    #[pyo3(signature = (*, tag_keys=None, tag_values=None))]
    fn last_executed_with_tags(
        tag_keys: Option<Vec<String>>,
        tag_values: Option<Vec<(String, String)>>,
    ) -> Self {
        Self::new_node(ConditionNode::LastExecutedWithTags {
            tag_keys: tag_keys.unwrap_or_default(),
            tag_values: tag_values.unwrap_or_default(),
        })
    }

    /// True if this asset's latest run also included the target (root) asset.
    #[staticmethod]
    fn last_run_includes_target() -> Self {
        Self::new_node(ConditionNode::LastRunIncludesTarget)
    }

    /// True if this asset's condition has already fired earlier in this tick.
    #[staticmethod]
    fn will_be_requested() -> Self {
        Self::new_node(ConditionNode::WillBeRequested)
    }

    /// True if any of this asset's new materializations (this tick) came from runs with matching tags.
    #[staticmethod]
    #[pyo3(signature = (*, tag_keys=None, tag_values=None))]
    fn has_run_with_tags(
        tag_keys: Option<Vec<String>>,
        tag_values: Option<Vec<(String, String)>>,
    ) -> Self {
        Self::new_node(ConditionNode::HasRunWithTags {
            tag_keys: tag_keys.unwrap_or_default(),
            tag_values: tag_values.unwrap_or_default(),
        })
    }

    /// True if all of this asset's new materializations (this tick) came from runs with matching tags.
    #[staticmethod]
    #[pyo3(signature = (*, tag_keys=None, tag_values=None))]
    fn all_runs_have_tags(
        tag_keys: Option<Vec<String>>,
        tag_values: Option<Vec<(String, String)>>,
    ) -> Self {
        Self::new_node(ConditionNode::AllRunsHaveTags {
            tag_keys: tag_keys.unwrap_or_default(),
            tag_values: tag_values.unwrap_or_default(),
        })
    }

    /// True when any dependency is missing.
    #[staticmethod]
    fn any_deps_missing() -> Self {
        Self::new_node(ConditionNode::any_deps_missing())
    }

    /// True when any dependency is in progress.
    #[staticmethod]
    fn any_deps_in_progress() -> Self {
        Self::new_node(ConditionNode::any_deps_in_progress())
    }

    /// True when any dependency has been updated and the update wasn't from a
    /// joint run that already included the target asset.
    #[staticmethod]
    fn any_deps_updated() -> Self {
        Self::new_node(ConditionNode::any_deps_updated())
    }

    /// True when any dependency matches the given condition.
    #[staticmethod]
    fn any_deps_match(condition: PyAutomationCondition) -> Self {
        Self::new_node(ConditionNode::any_deps_match(condition.node))
    }

    /// True when all dependencies match the given condition.
    #[staticmethod]
    fn all_deps_match(condition: PyAutomationCondition) -> Self {
        Self::new_node(ConditionNode::all_deps_match(condition.node))
    }

    /// True when all deps have been updated since the last tick of the given cron schedule.
    #[staticmethod]
    #[pyo3(signature = (cron_schedule, timezone=None))]
    fn all_deps_updated_since_cron(
        cron_schedule: String,
        timezone: Option<String>,
    ) -> PyResult<Self> {
        validate_cron_schedule(&cron_schedule)?;
        Ok(Self::new_node(ConditionNode::all_deps_updated_since_cron(
            cron_schedule,
            timezone,
        )))
    }

    /// Evaluate this condition on specific named assets (true if any match).
    /// Accepts a single asset key or a list of keys.
    fn on_selected(&self, keys: &Bound<'_, PyAny>) -> PyResult<Self> {
        let keys = extract_keys(keys)?;
        Ok(Self::new_node(ConditionNode::asset_matches(
            keys,
            self.node.clone(),
        )))
    }

    /// Transition detection: true only on the tick where condition transitions to true.
    fn newly_true(&self) -> Self {
        Self::new_junction(self.node.clone().newly_true())
    }

    /// State-tracking: true if this condition has been true since reset_condition last became true.
    fn since(&self, reset_condition: PyAutomationCondition) -> Self {
        Self::new_junction(self.node.clone().since(reset_condition.node))
    }

    /// Shorthand for `.since(newly_requested() | newly_updated() | initial_evaluation())`.
    fn since_last_handled(&self) -> Self {
        Self::new_junction(self.node.clone().since_last_handled())
    }

    /// Recursively replace sub-conditions matching `old` with `new`.
    /// `old` can be a label string (matches by label) or an AutomationCondition
    /// (matches by structural equality).
    fn replace(&self, old: &Bound<'_, PyAny>, new: PyAutomationCondition) -> PyResult<Self> {
        let node = if let Ok(s) = old.extract::<String>() {
            self.node.replace_by_label(&s, &new.node)
        } else {
            let cond = old.extract::<PyAutomationCondition>()?;
            self.node.replace_by_node(&cond.node, &new.node)
        };
        Ok(Self {
            node,
            label: self.label.clone(),
        })
    }

    /// Remove a child from an And condition by label or condition.
    fn without(&self, condition: &Bound<'_, PyAny>) -> PyResult<Self> {
        let label = if let Ok(s) = condition.extract::<String>() {
            s
        } else {
            let cond = condition.extract::<PyAutomationCondition>()?;
            // Match the key `ConditionNode::without` actually compares against:
            // the node's non-recursive effective label (unwrapping one Not). The
            // wrapper `label` is the recursive `description()` (set by new_node),
            // which never equals an And child's effective label.
            cond.node.effective_label()
        };
        Ok(Self {
            node: self.node.without(&label),
            label: self.label.clone(),
        })
    }

    /// Attach a label for debugging and UI display.
    fn with_label(&self, label: String) -> Self {
        Self {
            node: self.node.clone(),
            label: Some(label),
        }
    }

    /// Get the label, if any.
    #[getter]
    fn label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    /// Human-readable description of this condition.
    #[getter]
    fn get_description(&self) -> String {
        description(&self.node)
    }

    /// Get the child conditions (for composite conditions).
    #[getter]
    fn children(&self) -> Vec<PyAutomationCondition> {
        match &self.node {
            ConditionNode::And(children) | ConditionNode::Or(children) => {
                children.iter().map(|c| Self::new_node(c.clone())).collect()
            }
            ConditionNode::Not(child) | ConditionNode::NewlyTrue(child) => {
                vec![Self::new_node(child.as_ref().clone())]
            }
            ConditionNode::Since { trigger, reset } => {
                vec![
                    Self::new_node(trigger.as_ref().clone()),
                    Self::new_node(reset.as_ref().clone()),
                ]
            }
            ConditionNode::SinceLastHandled(child) => {
                vec![Self::new_node(child.as_ref().clone())]
            }
            ConditionNode::AnyDepsMatch { condition, .. }
            | ConditionNode::AllDepsMatch { condition, .. }
            | ConditionNode::AssetMatches { condition, .. } => {
                vec![Self::new_node(condition.as_ref().clone())]
            }
            _ => vec![],
        }
    }

    fn __and__(&self, other: &PyAutomationCondition) -> Self {
        // Flatten nested ANDs
        let mut children = Vec::new();
        match &self.node {
            ConditionNode::And(c) if self.label.is_none() => children.extend(c.clone()),
            _ => children.push(self.node.clone()),
        }
        match &other.node {
            ConditionNode::And(c) if other.label.is_none() => children.extend(c.clone()),
            _ => children.push(other.node.clone()),
        }
        Self::new_junction(ConditionNode::And(children))
    }

    fn __or__(&self, other: &PyAutomationCondition) -> Self {
        // Flatten nested ORs
        let mut children = Vec::new();
        match &self.node {
            ConditionNode::Or(c) if self.label.is_none() => children.extend(c.clone()),
            _ => children.push(self.node.clone()),
        }
        match &other.node {
            ConditionNode::Or(c) if other.label.is_none() => children.extend(c.clone()),
            _ => children.push(other.node.clone()),
        }
        Self::new_junction(ConditionNode::Or(children))
    }

    fn __invert__(&self) -> Self {
        Self::new_junction(ConditionNode::Not(Box::new(self.node.clone())))
    }

    fn __repr__(&self) -> String {
        let name = self.label.as_deref().unwrap_or("");
        if name.is_empty() {
            format!("AutomationCondition({})", description(&self.node))
        } else {
            format!("AutomationCondition({})", name)
        }
    }
}
