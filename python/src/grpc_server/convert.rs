use std::collections::HashMap;

use pyo3::prelude::*;
use rivers_api::rivers::*;
use tonic::Status;

use crate::automation::schedule::TickRequest;
use crate::config::run_config::{ConfigIssue, LocPart, config_schema_json, validate_run_config};
use crate::partitions::backfill_strategy::PyBackfillStrategy;
use crate::partitions::key_range::{
    DimensionSelection, PartitionKeyRangeInner, PyPartitionKeyRange,
};
use crate::partitions::{PartitionsDefinition, PyPartitionKey};
use crate::repository::ResolvedState;

pub(super) fn proto_config_error(issue: ConfigIssue) -> ConfigError {
    ConfigError {
        path: issue.path,
        loc: issue
            .loc
            .into_iter()
            .map(|part| ConfigLoc {
                part: Some(match part {
                    LocPart::Key(key) => config_loc::Part::Key(key),
                    LocPart::Index(index) => config_loc::Part::Index(index),
                }),
            })
            .collect(),
        message: issue.message,
        kind: issue.kind,
    }
}

/// The config a launch request carries, checked against the assets it will
/// run. Unset or blank means no overrides.
#[allow(clippy::result_large_err)]
pub(super) fn request_config(
    config: Option<String>,
    selection: &[String],
) -> Result<Option<String>, Status> {
    match config.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(json) => validate_run_config(json, |name| selection.iter().any(|s| s == name))
            .map_err(|e| Status::invalid_argument(e.to_string())),
        None => Ok(None),
    }
}

/// The JSON schema of `func`'s config class, or `None`. A schema that cannot
/// be built is logged; it must not hide the asset from the UI.
pub(super) fn config_schema_or_log(
    py: Python<'_>,
    func: &Py<PyAny>,
    label: &str,
) -> Option<String> {
    match config_schema_json(py, func) {
        Ok(schema) => schema,
        Err(e) => {
            tracing::warn!(
                target: "rivers::grpc",
                asset = %label,
                error = %e,
                "config schema unavailable"
            );
            None
        }
    }
}

/// Manual provenance from an optional proto `UserRef`.
pub(super) fn manual_launch(
    user: Option<rivers_api::rivers::UserRef>,
) -> rivers_core::storage::LaunchedBy {
    rivers_core::storage::LaunchedBy::Manual {
        user: user.map(|u| rivers_core::storage::UserRef {
            subject: u.subject,
            email: u.email,
            name: u.name,
        }),
    }
}

/// Flatten a `LaunchedBy` into the proto response's `(kind, name, user)` triple.
pub(super) fn launched_by_to_proto(
    lb: rivers_core::storage::LaunchedBy,
) -> (String, Option<String>, Option<rivers_api::rivers::UserRef>) {
    use rivers_core::storage::LaunchedBy;
    match lb {
        LaunchedBy::Manual { user } => (
            "manual".to_string(),
            None,
            user.map(|u| rivers_api::rivers::UserRef {
                subject: u.subject,
                email: u.email,
                name: u.name,
            }),
        ),
        LaunchedBy::Schedule { name } => ("schedule".to_string(), Some(name), None),
        LaunchedBy::Sensor { name } => ("sensor".to_string(), Some(name), None),
        LaunchedBy::Backfill { backfill_id } => ("backfill".to_string(), Some(backfill_id), None),
        LaunchedBy::Condition => ("condition".to_string(), None, None),
    }
}

pub(super) fn empty_to_none<T>(v: Vec<T>) -> Option<Vec<T>> {
    if v.is_empty() { None } else { Some(v) }
}

pub(super) fn proto_tags_to_pairs(tags: Vec<Tag>) -> Option<Vec<(String, String)>> {
    if tags.is_empty() {
        None
    } else {
        Some(tags.into_iter().map(|t| (t.key, t.value)).collect())
    }
}

pub(super) fn tags_to_proto(tags: Option<&HashMap<String, String>>) -> Vec<Tag> {
    tags.map(|t| {
        t.iter()
            .map(|(k, v)| Tag {
                key: k.clone(),
                value: v.clone(),
            })
            .collect()
    })
    .unwrap_or_default()
}

pub(super) fn collect_run_ids(py: Python<'_>, run_requests: &[TickRequest]) -> Vec<String> {
    run_requests
        .iter()
        .filter_map(|tr| {
            tr.as_run().map(|r| {
                r.borrow(py)
                    .run_key
                    .clone()
                    .unwrap_or_else(|| "pending".to_string())
            })
        })
        .collect()
}

/// Resolve the `PartitionsDefinition` a partition-keys request targets: the
/// asset's own def, or — when `dimension` is non-empty — that dimension's
/// sub-definition of a Multi. Shared by `get_partition_keys` /
/// `get_partition_key_index` so the lookup + error strings live in one place.
pub(super) fn resolve_partition_target<'a>(
    state: &'a ResolvedState,
    asset_key: &str,
    dimension: &str,
) -> Result<&'a PartitionsDefinition, String> {
    let node = state
        .node_map
        .get(asset_key)
        .ok_or_else(|| format!("asset '{asset_key}' not found"))?;
    let pd = node
        .partitions_def()
        .ok_or_else(|| format!("asset '{asset_key}' is not partitioned"))?;
    if dimension.is_empty() {
        Ok(pd)
    } else {
        pd.dimension_def(dimension)
            .ok_or_else(|| format!("asset '{asset_key}' has no partition dimension '{dimension}'"))
    }
}

/// For Multi, surface per-dimension keys so the UI can render one selector per
/// dimension instead of the cartesian-product enumeration. Static and TimeWindow
/// populate the flat `keys` list. Dynamic returns Err from `get_partition_keys`
/// — both lists stay empty and the UI hides the picker.
pub(super) fn node_partition_def_info(pd: &PartitionsDefinition) -> PartitionDefInfo {
    // Max keys shipped inline. Beyond this the UI pages via the windowed API,
    // so the payload stays bounded no matter how many partitions exist.
    const KEYS_WINDOW: usize = 1000;
    let window = |d: &PartitionsDefinition| {
        d.get_partition_keys_window(0, KEYS_WINDOW)
            .ok()
            .map(|pks| {
                pks.into_iter()
                    .map(py_partition_key_display)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    };
    let total = pd.partition_count() as u64;
    let (kind, keys, dimensions, keys_truncated) = match pd {
        // Static and TimeWindow are both single-dim: a windowed `keys` list,
        // truncated when shorter than the total.
        PartitionsDefinition::Static { .. } | PartitionsDefinition::TimeWindow { .. } => {
            let k = window(pd);
            let trunc = (k.len() as u64) < total;
            (pd.variant_name(), k, vec![], trunc)
        }
        PartitionsDefinition::Multi { dimensions } => {
            // Each dimension's keys are windowed independently; flag truncation
            // if any dimension was capped (the flat `keys` list is empty here).
            let mut trunc = false;
            let dims = dimensions
                .iter()
                .map(|(name, dim_def)| {
                    let k = window(dim_def);
                    let dim_total = dim_def.partition_count();
                    let dim_trunc = k.len() < dim_total;
                    if dim_trunc {
                        trunc = true;
                    }
                    PartitionDimensionInfo {
                        name: name.clone(),
                        keys: k,
                        total_count: dim_total as u64,
                        keys_truncated: dim_trunc,
                    }
                })
                .collect();
            ("Multi", vec![], dims, trunc)
        }
        PartitionsDefinition::Dynamic { .. } => ("Dynamic", vec![], vec![], false),
    };
    // Dynamic keys are storage-managed; ship the namespace so the UI can source
    // the real count + keys from storage (def-level `total_count` is 0 here).
    let dynamic_name = match pd {
        PartitionsDefinition::Dynamic { name } => name.clone(),
        _ => String::new(),
    };
    PartitionDefInfo {
        kind: kind.to_string(),
        keys,
        dimensions,
        total_count: total,
        keys_truncated,
        dynamic_name,
    }
}

/// Render a `PyPartitionKey` for display in the UI — delegates to the
/// canonical `PartitionKey::to_display` encoding (`dim=val|dim=val`).
pub(super) fn py_partition_key_display(pk: PyPartitionKey) -> String {
    rivers_core::storage::PartitionKey::from(&pk).to_display()
}

/// gRPC is the system boundary: malformed partition keys are rejected with
/// `InvalidArgument` instead of being silently dropped or coerced. `Single`
/// values are sorted to match the in-process constructors, so the same
/// logical key compares equal regardless of which side built it.
pub(super) fn proto_partition_key_to_py(pk: ProtoPartitionKey) -> Result<PyPartitionKey, Status> {
    let kind = pk
        .kind
        .ok_or_else(|| Status::invalid_argument("partition_key has no kind set"))?;
    match kind {
        proto_partition_key::Kind::Single(s) => {
            if s.keys.is_empty() {
                return Err(Status::invalid_argument(
                    "partition_key.single.keys must not be empty",
                ));
            }
            let mut keys = s.keys;
            keys.sort();
            Ok(PyPartitionKey::Single { key: keys })
        }
        proto_partition_key::Kind::Multi(m) => {
            if m.dimensions.is_empty() {
                return Err(Status::invalid_argument(
                    "multi partition_key has no dimensions",
                ));
            }
            let mut keys: HashMap<String, Vec<String>> = HashMap::with_capacity(m.dimensions.len());
            for d in m.dimensions {
                if d.keys.is_empty() {
                    return Err(Status::invalid_argument(format!(
                        "partition_key dimension '{}' has no keys",
                        d.name
                    )));
                }
                let mut vals = d.keys;
                vals.sort();
                if keys.insert(d.name.clone(), vals).is_some() {
                    return Err(Status::invalid_argument(format!(
                        "duplicate partition_key dimension '{}'",
                        d.name
                    )));
                }
            }
            Ok(PyPartitionKey::Multi { keys })
        }
    }
}

pub(super) fn proto_partition_range_to_py(
    r: PartitionRange,
) -> Result<PyPartitionKeyRange, Status> {
    let kind = r
        .kind
        .ok_or_else(|| Status::invalid_argument("partition_range has no kind set"))?;
    match kind {
        partition_range::Kind::Single(s) => Ok(PyPartitionKeyRange {
            inner: PartitionKeyRangeInner::Single {
                from_key: s.from_key,
                to_key: s.to_key,
            },
        }),
        partition_range::Kind::Multi(m) => {
            let mut dims = HashMap::new();
            for d in m.dimensions {
                let selection = d.selection.ok_or_else(|| {
                    Status::invalid_argument(format!(
                        "partition_range dimension '{}' has no selection set",
                        d.name
                    ))
                })?;
                let sel = match selection {
                    dimension_range::Selection::Range(r) => DimensionSelection::Range {
                        from_key: r.from_key,
                        to_key: r.to_key,
                    },
                    dimension_range::Selection::Keys(k) => {
                        DimensionSelection::Keys(k.keys.into_iter().collect())
                    }
                };
                if dims.insert(d.name.clone(), sel).is_some() {
                    return Err(Status::invalid_argument(format!(
                        "duplicate partition_range dimension '{}'",
                        d.name
                    )));
                }
            }
            Ok(PyPartitionKeyRange {
                inner: PartitionKeyRangeInner::Multi { dimensions: dims },
            })
        }
    }
}

/// Unknown shorthands are rejected — silently mapping a typo to `MultiRun`
/// would fan a one-run backfill out into one run per partition.
pub(super) fn proto_strategy_to_py(s: BackfillStrategyProto) -> Result<PyBackfillStrategy, Status> {
    let kind = s
        .kind
        .ok_or_else(|| Status::invalid_argument("backfill strategy has no kind set"))?;
    match kind {
        backfill_strategy_proto::Kind::Shorthand(s) => match s.as_str() {
            "single_run" => Ok(PyBackfillStrategy::SingleRun {}),
            "multi_run" => Ok(PyBackfillStrategy::MultiRun {}),
            other => Err(Status::invalid_argument(format!(
                "unknown backfill strategy '{other}' (expected 'single_run' or 'multi_run')"
            ))),
        },
        backfill_strategy_proto::Kind::PerDimension(pd) => Ok(PyBackfillStrategy::PerDimension {
            multi_run_dims: pd.multi_run_dimensions,
            single_run_dims: pd.single_run_dimensions,
        }),
    }
}
