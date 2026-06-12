//! PartitionMapping — defines how partition keys map between connected assets.
//!
//! `PartitionMapping` enum with 7 variants (Identity, AllPartitions, LastPartition, etc.).
//! `map_key()` transforms downstream partition keys to upstream keys for dependency loading.
//! `PartitionMappingDict` accepts `str` or `AssetDef` keys from Python for per-dep overrides.
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Deref;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::errors::PartitionValidationError;

use super::PyPartitionKey;
use super::definition::{
    PartitionsDefinition, cron_grid_contains, for_each_cron_tick, for_each_interval_tick,
};
use super::key_range::PyPartitionKeyRange;

/// Identifies which side of a dependency edge an error refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Downstream,
    Upstream,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Downstream => "downstream",
            Self::Upstream => "upstream",
        })
    }
}

/// A structural problem with a Multi or MultiToSingle dimension mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DimensionErrorKind {
    BothSidesMulti,
    NeitherSideMulti {
        downstream: String,
        upstream: String,
    },
    /// `Multi`: an upstream dimension referenced by the mapping doesn't exist on the upstream def.
    MultiUpstreamDimMissing {
        dim: String,
    },
    /// `Multi`: a downstream dimension targeted by the mapping doesn't exist on the downstream def.
    MultiDownstreamDimMissing {
        dim: String,
    },
    /// `MultiToSingle`: the named dimension doesn't exist on the Multi side.
    MultiToSingleDimMissing {
        dim: String,
        side: Side,
        available: Vec<String>,
    },
    MissingMapping {
        dim: String,
    },
    TargetedTwice {
        dim: String,
    },
    NotCovered {
        dim: String,
    },
}

impl std::fmt::Display for DimensionErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BothSidesMulti => write!(
                f,
                "MultiToSingle mapping requires exactly one side to be Multi, but both are Multi"
            ),
            Self::NeitherSideMulti {
                downstream,
                upstream,
            } => write!(
                f,
                "MultiToSingle mapping requires one side to be Multi, but downstream is {downstream} and upstream is {upstream}"
            ),
            Self::MultiUpstreamDimMissing { dim } => write!(
                f,
                "Multi mapping references upstream dimension '{dim}' which does not exist"
            ),
            Self::MultiDownstreamDimMissing { dim } => write!(
                f,
                "Multi mapping targets downstream dimension '{dim}' which does not exist"
            ),
            Self::MultiToSingleDimMissing {
                dim,
                side,
                available,
            } => write!(
                f,
                "MultiToSingle dimension '{dim}' does not exist in the {side} Multi partitions (available: {})",
                available.join(", ")
            ),
            Self::MissingMapping { dim } => {
                write!(f, "Multi mapping is missing upstream dimension '{dim}'")
            }
            Self::TargetedTwice { dim } => write!(
                f,
                "Multi mapping targets downstream dimension '{dim}' more than once"
            ),
            Self::NotCovered { dim } => {
                write!(
                    f,
                    "Multi mapping does not cover downstream dimension '{dim}'"
                )
            }
        }
    }
}

/// Why a `PartitionMapping` is not valid for a given dependency edge.
///
/// Carries enough structured detail to test specific failure modes without
/// substring-matching prose. Callers add asset-name context when wrapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MappingValidationError {
    /// Mapping requires both definitions to be the same partition type.
    DefinitionTypeMismatch {
        mapping: &'static str,
        downstream: String,
        upstream: String,
    },
    /// Mapping requires a specific partition type on one side.
    RequiredDefinitionType {
        mapping: &'static str,
        side: Side,
        expected: &'static str,
        found: String,
    },
    /// A partition key referenced by the mapping is not valid for the named side.
    KeyNotInDefinition {
        key: String,
        side: Side,
        mapping: &'static str,
    },
    /// The mapping variant is not allowed for this (down-partitioned, up-partitioned) shape.
    IncompatibleMappingForShape {
        mapping: &'static str,
        downstream_partitioned: bool,
        upstream_partitioned: bool,
    },
    /// Unpartitioned downstream depending on partitioned upstream requires an explicit mapping.
    ExplicitMappingRequired,
    /// A non-Identity mapping was supplied but neither side is partitioned.
    MappingOnUnpartitionedPair { mapping: &'static str },
    /// Subset mapping requires upstream keys to be a subset of downstream (Static-Static case).
    UpstreamKeysNotSubset { extras: Vec<String> },
    /// Structural problem with a Multi or MultiToSingle dimension mapping.
    Dimension(DimensionErrorKind),
    /// Recursive wrapper: an inner per-dimension mapping inside Multi/MultiToSingle failed.
    InDimension {
        dim: String,
        source: Box<MappingValidationError>,
    },
    /// An underlying partition definition operation failed (e.g. enumerating keys).
    DefinitionError(String),
}

impl std::fmt::Display for MappingValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DefinitionTypeMismatch {
                mapping,
                downstream,
                upstream,
            } => write!(
                f,
                "{mapping} mapping requires same partition type, but downstream is {downstream} and upstream is {upstream}"
            ),
            Self::RequiredDefinitionType {
                mapping,
                side,
                expected,
                found,
            } => write!(
                f,
                "{mapping} mapping requires {expected} partitions on {side}, but found {found}"
            ),
            // Per-(mapping, side) prose preserved from the original validator.
            Self::KeyNotInDefinition { key, side, mapping } => match (*mapping, *side) {
                ("Static", Side::Downstream) => write!(
                    f,
                    "Static partition mapping key '{key}' is not a valid partition key for this asset"
                ),
                ("Static", Side::Upstream) => write!(
                    f,
                    "Static mapping target '{key}' is not a valid partition key for upstream"
                ),
                ("SpecificPartitions", Side::Upstream) => write!(
                    f,
                    "SpecificPartitions key '{key}' is not a valid partition key for upstream"
                ),
                ("ForKeys", Side::Downstream) => write!(
                    f,
                    "ForKeys key {key} is not a valid downstream partition key"
                ),
                _ => write!(
                    f,
                    "{mapping} mapping references key '{key}' which is not a valid partition key for {side}"
                ),
            },
            // Per-(mapping, shape) prose preserved from the original validator.
            Self::IncompatibleMappingForShape {
                mapping,
                downstream_partitioned,
                upstream_partitioned,
            } => match (*downstream_partitioned, *upstream_partitioned, *mapping) {
                (true, true, "SpecificPartitions") => write!(
                    f,
                    "SpecificPartitions mapping is only valid when downstream is unpartitioned. \
                     Use Static, Identity, or AllPartitions for partitioned-to-partitioned dependencies"
                ),
                (true, true, "ForKeys") => write!(
                    f,
                    "ForKeys mapping is only valid when upstream is unpartitioned"
                ),
                (true, false, _) => write!(
                    f,
                    "only AllPartitions, ForKeys, or no mapping is valid when upstream has no partitions"
                ),
                (false, true, _) => write!(
                    f,
                    "only AllPartitions or SpecificPartitions mapping is valid"
                ),
                _ => write!(
                    f,
                    "{mapping} mapping is not valid for this dependency shape"
                ),
            },
            Self::ExplicitMappingRequired => write!(
                f,
                "a partition_mapping (e.g. AllPartitions or SpecificPartitions) is required"
            ),
            Self::MappingOnUnpartitionedPair { .. } => write!(
                f,
                "partition_mapping specified but neither asset has partitions"
            ),
            Self::UpstreamKeysNotSubset { extras } => write!(
                f,
                "Subset mapping requires upstream keys to be a subset of downstream, but upstream has extra keys: {extras:?}"
            ),
            Self::Dimension(d) => write!(f, "{d}"),
            Self::InDimension { dim, source } => write!(f, "in dimension '{dim}': {source}"),
            Self::DefinitionError(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for MappingValidationError {}

/// Resolution of an upstream load decision at materialize time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpstreamKeyResolution {
    /// Load upstream with this partition key (None = upstream is unpartitioned).
    Load(Option<PyPartitionKey>),
    /// Skip loading — parameter receives Python None.
    Skip,
}

/// Why a `PartitionMapping` could not produce an `UpstreamKeyResolution`.
///
/// Distinct from `MappingValidationError` because these are runtime invariants
/// (materialize time), not edge validity (resolve time).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MappingResolutionError {
    /// Mapping requires a downstream partition key but none was supplied.
    MissingDownstreamKey { mapping: &'static str },
    /// Mapping requires upstream to be partitioned but it isn't.
    UpstreamNotPartitioned { mapping: &'static str },
    /// Per-variant key transformation failed (e.g. Multi missing a dimension at runtime).
    KeyMappingFailed(String),
    /// An underlying partition definition operation failed.
    DefinitionError(String),
}

impl std::fmt::Display for MappingResolutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingDownstreamKey { mapping } => {
                write!(f, "{mapping} mapping requires a downstream partition key")
            }
            Self::UpstreamNotPartitioned { mapping } => {
                write!(f, "{mapping} mapping requires upstream to be partitioned")
            }
            Self::KeyMappingFailed(s) => write!(f, "{s}"),
            Self::DefinitionError(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for MappingResolutionError {}

/// Single-dim partition keys as a string set, for set-based validation.
fn single_dim_key_set(
    def: &PartitionsDefinition,
) -> Result<HashSet<String>, MappingValidationError> {
    def.get_partition_keys()
        .map(|keys| {
            keys.into_iter()
                .filter_map(|pk| match pk {
                    PyPartitionKey::Single { key } => key.into_iter().next(),
                    _ => None,
                })
                .collect()
        })
        .map_err(|e| MappingValidationError::DefinitionError(e.to_string()))
}

/// Selector for matching partition keys: either an exact key or a range.
/// Used by `ForKeys` to specify which downstream partition keys map to the upstream.
#[derive(Clone, Debug, PartialEq)]
pub enum PartitionKeySelector {
    Key(PyPartitionKey),
    Range(PyPartitionKeyRange),
}

impl PartitionKeySelector {
    pub fn matches(&self, key: &PyPartitionKey, def: Option<&PartitionsDefinition>) -> bool {
        match self {
            Self::Key(k) => k == key,
            Self::Range(range) => range.contains(key, def),
        }
    }
}

impl<'py> FromPyObject<'py, '_> for PartitionKeySelector {
    type Error = PyErr;

    fn extract(ob: pyo3::Borrowed<'py, '_, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(key) = ob.extract::<PyPartitionKey>() {
            Ok(Self::Key(key))
        } else if let Ok(range) = ob.extract::<PyPartitionKeyRange>() {
            Ok(Self::Range(range))
        } else {
            Err(PyTypeError::new_err(
                "Expected PartitionKey or PartitionKeyRange for PartitionKeySelector",
            ))
        }
    }
}

impl<'py> pyo3::IntoPyObject<'py> for PartitionKeySelector {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Self::Key(k) => Ok(k.into_pyobject(py)?.into_any()),
            Self::Range(r) => Ok(r.into_pyobject(py)?.into_any()),
        }
    }
}

impl<'py> pyo3::IntoPyObject<'py> for &PartitionKeySelector {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.clone().into_pyobject(py)
    }
}

/// Newtype around `Box<PartitionMapping>` with manual `FromPyObject`/`IntoPyObject`
/// so it can be used as a field in a PyO3 `from_py_object` enum variant.
#[derive(Clone, Debug)]
pub struct BoxedMapping(pub Box<PartitionMapping>);

impl PartialEq for BoxedMapping {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<'py> FromPyObject<'py, '_> for BoxedMapping {
    type Error = PyErr;

    fn extract(ob: pyo3::Borrowed<'py, '_, PyAny>) -> Result<Self, Self::Error> {
        let m = ob.extract::<PartitionMapping>()?;
        Ok(BoxedMapping(Box::new(m)))
    }
}

impl<'py> pyo3::IntoPyObject<'py> for BoxedMapping {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok((*self.0).into_pyobject(py)?.into_any())
    }
}

impl<'py> pyo3::IntoPyObject<'py> for &BoxedMapping {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok((*self.0).clone().into_pyobject(py)?.into_any())
    }
}

#[pyclass(
    name = "PartitionMapping",
    frozen,
    from_py_object,
    module = "rivers._core"
)]
#[derive(Clone, Debug, PartialEq)]
pub enum PartitionMapping {
    Identity {},
    AllPartitions {},
    Static {
        mapping: HashMap<String, String>,
    },
    TimeWindow {
        offset: i64,
    },
    /// Maps dimensions between two MultiPartitionsDefinitions.
    /// Keys = upstream dimension names, values = (downstream dimension name, per-dimension mapping).
    Multi {
        dimension_mappings: HashMap<String, (String, PartitionMapping)>,
    },
    /// Maps a single dimension of a MultiPartitionsDefinition to a single-dimension PartitionsDefinition.
    MultiToSingle {
        dimension_name: String,
        partition_mapping: BoxedMapping,
    },
    /// Maps all downstream partitions to a specific set of upstream partition keys.
    SpecificPartitions {
        partition_keys: Vec<String>,
    },
    /// Maps an unpartitioned upstream to specific downstream partition keys.
    /// When the downstream partition key matches a selector, the upstream is loaded;
    /// otherwise the parameter receives `None`.
    ForKeys {
        selectors: Vec<PartitionKeySelector>,
    },
    /// Subset mapping for partitioned-to-partitioned edges where upstream has a
    /// subset of downstream's partition keys. When the downstream key doesn't
    /// exist in the upstream, the parameter receives `None`.
    Subset {},
}

impl PartitionMapping {
    /// For Multi variant, get a reference to the dimension mappings.
    pub fn dimension_mappings(&self) -> Option<&HashMap<String, (String, PartitionMapping)>> {
        match self {
            Self::Multi { dimension_mappings } => Some(dimension_mappings),
            _ => None,
        }
    }

    /// Resolve the upstream load decision at materialize time.
    ///
    /// Owns the variant-specific Skip semantics (`ForKeys`, `Subset`) and the
    /// `SpecificPartitions` short-circuit. For variants that simply transform
    /// the partition key, delegates to the private `map_key` helper.
    ///
    /// `downstream_key` is `None` when the downstream asset is unpartitioned;
    /// in that case all "passthrough" mappings load the upstream unpartitioned.
    pub fn resolve_upstream_key(
        &self,
        downstream_key: Option<&PyPartitionKey>,
        downstream_def: Option<&PartitionsDefinition>,
        upstream_def: Option<&PartitionsDefinition>,
    ) -> Result<UpstreamKeyResolution, MappingResolutionError> {
        match self {
            Self::SpecificPartitions { partition_keys } => {
                Ok(UpstreamKeyResolution::Load(Some(PyPartitionKey::Single {
                    key: partition_keys.clone(),
                })))
            }
            Self::ForKeys { selectors } => {
                let key = downstream_key
                    .ok_or(MappingResolutionError::MissingDownstreamKey { mapping: "ForKeys" })?;
                let matched = selectors.iter().any(|s| s.matches(key, downstream_def));
                if matched {
                    Ok(UpstreamKeyResolution::Load(None))
                } else {
                    Ok(UpstreamKeyResolution::Skip)
                }
            }
            Self::Subset {} => {
                let key = downstream_key
                    .ok_or(MappingResolutionError::MissingDownstreamKey { mapping: "Subset" })?;
                let up = upstream_def
                    .ok_or(MappingResolutionError::UpstreamNotPartitioned { mapping: "Subset" })?;
                let valid = up
                    .validate_partition_key(key)
                    .map_err(|e| MappingResolutionError::DefinitionError(e.to_string()))?;
                if valid {
                    Ok(UpstreamKeyResolution::Load(Some(key.clone())))
                } else {
                    Ok(UpstreamKeyResolution::Skip)
                }
            }
            // Identity / AllPartitions / Static / TimeWindow / Multi / MultiToSingle
            _ => match downstream_key {
                None => Ok(UpstreamKeyResolution::Load(None)),
                Some(key) => self
                    .map_key(key, upstream_def)
                    .map(|k| UpstreamKeyResolution::Load(Some(k)))
                    .map_err(MappingResolutionError::KeyMappingFailed),
            },
        }
    }

    /// Transform a downstream partition key into an upstream key. Private — only
    /// reachable for variants that don't carry Skip semantics; callers go through
    /// `resolve_upstream_key`. Errors for `ForKeys`/`Subset` (use `resolve_upstream_key`).
    ///
    /// `upstream_def` is needed for `MultiToSingle` (Single→Multi) to enumerate
    /// unmapped dimensions.
    fn map_key(
        &self,
        downstream_key: &PyPartitionKey,
        upstream_def: Option<&PartitionsDefinition>,
    ) -> Result<PyPartitionKey, String> {
        match self {
            Self::Identity {} => Ok(downstream_key.clone()),

            Self::AllPartitions {} => {
                // AllPartitions means the downstream depends on ALL upstream partitions.
                // We pass through the key as-is (the IO handler handles fan-in).
                Ok(downstream_key.clone())
            }

            Self::Static { mapping } => {
                match downstream_key {
                    PyPartitionKey::Single { key } => {
                        let downstream_str = key.first().ok_or("Empty partition key")?;
                        match mapping.get(downstream_str) {
                            Some(upstream_str) => Ok(PyPartitionKey::Single {
                                key: vec![upstream_str.clone()],
                            }),
                            // Unmapped keys use identity
                            None => Ok(downstream_key.clone()),
                        }
                    }
                    _ => Err("Static mapping only works with Single partition keys".to_string()),
                }
            }

            Self::TimeWindow { offset } => {
                if *offset == 0 {
                    return Ok(downstream_key.clone());
                }
                let PyPartitionKey::Single { key } = downstream_key else {
                    return Err(
                        "TimeWindow mapping only works with Single partition keys".to_string()
                    );
                };
                let k = key.first().ok_or("Empty partition key")?;
                let def =
                    upstream_def.ok_or("TimeWindow mapping requires a partitioned upstream")?;
                let shifted = def.shift_time_key(k, *offset).map_err(|e| e.to_string())?;
                let shifted_key = PyPartitionKey::Single {
                    key: vec![shifted.clone()],
                };
                // A cross-cadence mapping can land between upstream windows —
                // loading a partition that doesn't exist must fail loudly.
                if !def
                    .validate_partition_key(&shifted_key)
                    .map_err(|e| e.to_string())?
                {
                    return Err(format!(
                        "time_window(offset={offset}) maps '{k}' to '{shifted}', \
                         which is not a partition of the upstream definition"
                    ));
                }
                Ok(shifted_key)
            }

            Self::Multi { dimension_mappings } => match downstream_key {
                PyPartitionKey::Multi {
                    keys: downstream_keys,
                } => {
                    let mut upstream_keys = HashMap::new();
                    for (upstream_dim, (downstream_dim, per_dim_mapping)) in dimension_mappings {
                        let dim_values = downstream_keys
                                .get(downstream_dim.as_str())
                                .ok_or_else(|| {
                                    format!(
                                        "Multi mapping expects downstream dimension '{}' but key has: {:?}",
                                        downstream_dim,
                                        downstream_keys.keys().collect::<Vec<_>>()
                                    )
                                })?;
                        let single_key = PyPartitionKey::Single {
                            key: dim_values.clone(),
                        };
                        // Thread the upstream dimension's own def so nested
                        // mappings that need it (TimeWindow offset) can shift.
                        let dim_def = upstream_def.and_then(|d| match d {
                            PartitionsDefinition::Multi { dimensions } => dimensions
                                .iter()
                                .find(|(n, _)| n == upstream_dim)
                                .map(|(_, dd)| dd),
                            _ => None,
                        });
                        let mapped = per_dim_mapping.map_key(&single_key, dim_def)?;
                        match mapped {
                            PyPartitionKey::Single { key } => {
                                upstream_keys.insert(upstream_dim.clone(), key);
                            }
                            _ => {
                                return Err(
                                    "Per-dimension mapping produced a Multi key".to_string()
                                );
                            }
                        }
                    }
                    Ok(PyPartitionKey::Multi {
                        keys: upstream_keys,
                    })
                }
                _ => Err("Multi mapping requires a Multi partition key".to_string()),
            },

            Self::SpecificPartitions { partition_keys } => Ok(PyPartitionKey::Single {
                key: partition_keys.clone(),
            }),

            Self::ForKeys { .. } => Err(
                "ForKeys mapping uses skip semantics and cannot be resolved via map_key()"
                    .to_string(),
            ),

            Self::Subset {} => Err(
                "Subset mapping uses skip semantics and cannot be resolved via map_key()"
                    .to_string(),
            ),

            Self::MultiToSingle {
                dimension_name,
                partition_mapping,
            } => {
                // One side is Multi, the other is Single.
                // We extract/inject the named dimension.
                match downstream_key {
                    // Downstream is Single → upstream is Multi: construct a full Multi key.
                    // The named dimension gets the (mapped) single key value.
                    // Unmapped dimensions get ALL their partition keys (fan-in).
                    PyPartitionKey::Single { key } => {
                        // Get the upstream Multi definition to enumerate unmapped dimensions
                        let up_def = upstream_def.ok_or(
                            "MultiToSingle (Single→Multi) requires upstream PartitionsDefinition",
                        )?;
                        let dimensions =
                            match up_def {
                                PartitionsDefinition::Multi { dimensions } => dimensions,
                                _ => return Err(
                                    "MultiToSingle upstream must be a Multi PartitionsDefinition"
                                        .to_string(),
                                ),
                            };

                        let single_key = PyPartitionKey::Single { key: key.clone() };
                        // The inner mapping maps within the named dimension —
                        // give it that dimension's def (TimeWindow offset).
                        let named_dim_def = dimensions
                            .iter()
                            .find(|(n, _)| n == dimension_name)
                            .map(|(_, dd)| dd);
                        let mapped_single =
                            partition_mapping.0.map_key(&single_key, named_dim_def)?;
                        let mapped_values = match &mapped_single {
                            PyPartitionKey::Single { key } => key.clone(),
                            _ => return Err("Inner mapping produced a Multi key".to_string()),
                        };

                        let mut multi_keys = HashMap::new();
                        for (dim_name, dim_def) in dimensions {
                            if dim_name == dimension_name {
                                // This is the mapped dimension — use the transformed key
                                multi_keys.insert(dim_name.clone(), mapped_values.clone());
                            } else {
                                let key_strings =
                                    dim_def.enumerate_single_dim_keys().map_err(|e| {
                                        format!(
                                            "Failed to get partition keys for dimension '{}': {}",
                                            dim_name, e
                                        )
                                    })?;
                                multi_keys.insert(dim_name.clone(), key_strings);
                            }
                        }
                        Ok(PyPartitionKey::Multi { keys: multi_keys })
                    }
                    // Downstream is Multi → upstream is Single: extract the named dimension
                    PyPartitionKey::Multi { keys } => {
                        let dim_values = keys.get(dimension_name.as_str()).ok_or_else(|| {
                            format!(
                                "MultiToSingle expects dimension '{}' but key has: {:?}",
                                dimension_name,
                                keys.keys().collect::<Vec<_>>()
                            )
                        })?;
                        let single_key = PyPartitionKey::Single {
                            key: dim_values.clone(),
                        };
                        // Upstream is Single here, so its whole def belongs to
                        // the inner mapping (TimeWindow offset shifts in it).
                        partition_mapping.0.map_key(&single_key, upstream_def)
                    }
                    PyPartitionKey::Set { .. } => {
                        Err("MultiToSingle mapping does not support batched Set keys".to_string())
                    }
                }
            }
        }
    }
}

/// Require the downstream grid to be a subgrid of the upstream one (same
/// fmt; identical cron, or interval multiple with aligned starts). Applies
/// to any mapping that resolves downstream keys on the upstream grid —
/// `time_window` shifts them, `Identity` loads them verbatim. Ranges
/// (start/end) may differ; range edges are a per-key concern. No-op unless
/// both definitions are TimeWindow.
fn validate_time_window_grid_compat(
    mapping: &str,
    down: &PartitionsDefinition,
    up: &PartitionsDefinition,
) -> Result<(), MappingValidationError> {
    let (
        PartitionsDefinition::TimeWindow {
            cron_schedule: down_cron,
            interval_seconds: down_interval,
            start: down_start,
            fmt: down_fmt,
            ..
        },
        PartitionsDefinition::TimeWindow {
            cron_schedule: up_cron,
            interval_seconds: up_interval,
            start: up_start,
            fmt: up_fmt,
            ..
        },
    ) = (down, up)
    else {
        return Ok(());
    };
    if down_fmt != up_fmt {
        return Err(MappingValidationError::DefinitionError(format!(
            "{mapping} mapping requires matching key formats: \
             downstream fmt '{down_fmt}' != upstream fmt '{up_fmt}'"
        )));
    }
    match (down_cron, down_interval, up_cron, up_interval) {
        (None, Some(di), None, Some(ui)) => {
            let down_ns = (di * 1_000_000_000.0) as i64;
            let up_ns = (ui * 1_000_000_000.0) as i64;
            if up_ns <= 0 || down_ns <= 0 || down_ns % up_ns != 0 {
                return Err(MappingValidationError::DefinitionError(format!(
                    "{mapping} mapping requires the downstream grid to be a \
                     subgrid of the upstream grid: downstream interval {di}s \
                     is not a multiple of upstream interval {ui}s"
                )));
            }
            let aligned = (*down_start - *up_start)
                .num_nanoseconds()
                .is_some_and(|ns| ns.rem_euclid(up_ns) == 0);
            if !aligned {
                return Err(MappingValidationError::DefinitionError(format!(
                    "{mapping} mapping requires the downstream grid to be a \
                     subgrid of the upstream grid: downstream start {down_start} \
                     is not aligned to the upstream grid (start {up_start}, \
                     interval {ui}s)"
                )));
            }
        }
        _ => {
            // At least one side is a cron grid. Equivalent schedules can be
            // spelled differently (5- vs 6-field, nicknames) and a cron grid
            // can coincide with an interval grid, so prove the subgrid
            // relation on the ticks themselves: every downstream window
            // start over a bounded probe must fall on the upstream grid.
            // The budget matches validate_time_window_fmt's walk — calendar
            // gates (day-of-week, month ranges) diverge far past the first
            // ticks: an hourly grid meets its first Saturday at tick 121.
            const PROBE_TICKS: usize = 1024;
            let horizon = down_start
                .checked_add_signed(chrono::Duration::days(1461))
                .unwrap_or(chrono::NaiveDateTime::MAX);
            let mut count = 0usize;
            let mut offgrid: Option<chrono::NaiveDateTime> = None;
            let mut walk_err: Option<String> = None;
            let mut visit = |t: chrono::NaiveDateTime| -> bool {
                count += 1;
                let on_upstream = match (up_cron, up_interval) {
                    (Some(expr), _) => match cron_grid_contains(expr, t) {
                        Ok(hit) => hit,
                        Err(e) => {
                            walk_err = Some(e.to_string());
                            return false;
                        }
                    },
                    (None, Some(ui)) => {
                        let up_ns = (ui * 1_000_000_000.0) as i64;
                        up_ns > 0
                            && (t - *up_start)
                                .num_nanoseconds()
                                .is_some_and(|ns| ns.rem_euclid(up_ns) == 0)
                    }
                    (None, None) => false,
                };
                if !on_upstream {
                    offgrid = Some(t);
                    return false;
                }
                count < PROBE_TICKS
            };
            if let Some(dc) = down_cron {
                for_each_cron_tick(dc, down_start, horizon, &mut visit)
                    .map_err(|e| MappingValidationError::DefinitionError(e.to_string()))?;
            } else if let Some(di) = down_interval {
                for_each_interval_tick(*di, down_start, horizon, &mut visit);
            }
            if let Some(msg) = walk_err {
                return Err(MappingValidationError::DefinitionError(msg));
            }
            if let Some(t) = offgrid {
                let upstream_grid = match (up_cron, up_interval) {
                    (Some(expr), _) => format!("cron '{expr}'"),
                    (None, Some(ui)) => format!("every {ui}s from {up_start}"),
                    (None, None) => "with no schedule".to_string(),
                };
                return Err(MappingValidationError::DefinitionError(format!(
                    "{mapping} mapping requires the downstream grid to be a \
                     subgrid of the upstream grid: downstream window start {t} \
                     (key '{}') is not on the upstream grid ({upstream_grid}), \
                     so that key would never exist upstream",
                    t.format(down_fmt)
                )));
            }
        }
    }
    Ok(())
}

impl PartitionMapping {
    /// Variant name as a string (e.g. "Identity", "Multi"). Used in error messages.
    pub fn variant_name(&self) -> &'static str {
        match self {
            Self::Identity {} => "Identity",
            Self::AllPartitions {} => "AllPartitions",
            Self::Static { .. } => "Static",
            Self::TimeWindow { .. } => "TimeWindow",
            Self::Multi { .. } => "Multi",
            Self::MultiToSingle { .. } => "MultiToSingle",
            Self::SpecificPartitions { .. } => "SpecificPartitions",
            Self::ForKeys { .. } => "ForKeys",
            Self::Subset {} => "Subset",
        }
    }

    /// Validate a dependency edge. Owns the full decision matrix:
    /// whether the mapping is allowed for the (downstream_partitioned, upstream_partitioned)
    /// shape, and whether it is internally valid against the partition definitions.
    ///
    /// `mapping=None` means no explicit mapping was supplied; semantics are
    /// shape-dependent (treated as Identity when both sides are partitioned;
    /// rejected when only the upstream is partitioned; otherwise allowed).
    pub fn validate_edge(
        mapping: Option<&Self>,
        downstream: Option<&PartitionsDefinition>,
        upstream: Option<&PartitionsDefinition>,
    ) -> Result<(), MappingValidationError> {
        match (downstream, upstream) {
            (Some(down), Some(up)) => match mapping {
                None => Self::Identity {}.validate_partitioned_pair(down, up),
                Some(m) => m.validate_partitioned_pair(down, up),
            },
            (Some(down), None) => match mapping {
                None => Ok(()),
                Some(m) => m.validate_partitioned_to_unpartitioned(down),
            },
            (None, Some(up)) => match mapping {
                None => Err(MappingValidationError::ExplicitMappingRequired),
                Some(m) => m.validate_unpartitioned_to_partitioned(up),
            },
            (None, None) => match mapping {
                None | Some(Self::Identity {}) => Ok(()),
                Some(m) => Err(MappingValidationError::MappingOnUnpartitionedPair {
                    mapping: m.variant_name(),
                }),
            },
        }
    }

    fn validate_partitioned_pair(
        &self,
        down: &PartitionsDefinition,
        up: &PartitionsDefinition,
    ) -> Result<(), MappingValidationError> {
        match self {
            Self::Identity {} => {
                if !down.same_variant(up) {
                    return Err(MappingValidationError::DefinitionTypeMismatch {
                        mapping: "Identity",
                        downstream: down.variant_name().to_string(),
                        upstream: up.variant_name().to_string(),
                    });
                }
                // Identity loads each downstream key verbatim from upstream,
                // so every downstream key must exist there — same subgrid
                // requirement as a zero-offset time_window mapping.
                validate_time_window_grid_compat("Identity", down, up)?;
                if let (
                    PartitionsDefinition::Dynamic { name: down_name },
                    PartitionsDefinition::Dynamic { name: up_name },
                ) = (down, up)
                {
                    if down_name != up_name {
                        return Err(MappingValidationError::DefinitionError(format!(
                            "Identity mapping requires matching dynamic namespaces: \
                             downstream '{down_name}' != upstream '{up_name}'"
                        )));
                    }
                }
                if let (
                    PartitionsDefinition::Static { keys: down_keys },
                    PartitionsDefinition::Static { keys: up_keys },
                ) = (down, up)
                {
                    let mut missing: Vec<&str> = down_keys
                        .iter()
                        .filter(|k| !up_keys.contains(k.as_str()))
                        .map(String::as_str)
                        .collect();
                    if !missing.is_empty() {
                        missing.sort_unstable();
                        return Err(MappingValidationError::DefinitionError(format!(
                            "Identity mapping requires every downstream key to exist \
                             upstream; missing upstream: {}",
                            missing.join(", ")
                        )));
                    }
                }
                if let (
                    PartitionsDefinition::Multi {
                        dimensions: down_dims,
                    },
                    PartitionsDefinition::Multi {
                        dimensions: up_dims,
                    },
                ) = (down, up)
                {
                    let down_names: BTreeSet<&str> =
                        down_dims.iter().map(|(n, _)| n.as_str()).collect();
                    let up_names: BTreeSet<&str> =
                        up_dims.iter().map(|(n, _)| n.as_str()).collect();
                    if down_names != up_names {
                        let join = |names: &BTreeSet<&str>| {
                            names.iter().copied().collect::<Vec<_>>().join(", ")
                        };
                        return Err(MappingValidationError::DefinitionError(format!(
                            "Identity mapping requires matching Multi dimensions: \
                             downstream [{}] != upstream [{}]",
                            join(&down_names),
                            join(&up_names)
                        )));
                    }
                    let up_map: HashMap<&str, &PartitionsDefinition> =
                        up_dims.iter().map(|(n, d)| (n.as_str(), d)).collect();
                    for (name, down_sub) in down_dims {
                        Self::Identity {}
                            .validate_partitioned_pair(down_sub, up_map[name.as_str()])
                            .map_err(|e| MappingValidationError::InDimension {
                                dim: name.clone(),
                                source: Box::new(e),
                            })?;
                    }
                }
                Ok(())
            }
            Self::TimeWindow { .. } => {
                if !matches!(down, PartitionsDefinition::TimeWindow { .. }) {
                    return Err(MappingValidationError::RequiredDefinitionType {
                        mapping: "TimeWindow",
                        side: Side::Downstream,
                        expected: "TimeWindow",
                        found: down.variant_name().to_string(),
                    });
                }
                if !matches!(up, PartitionsDefinition::TimeWindow { .. }) {
                    return Err(MappingValidationError::RequiredDefinitionType {
                        mapping: "TimeWindow",
                        side: Side::Upstream,
                        expected: "TimeWindow",
                        found: up.variant_name().to_string(),
                    });
                }
                validate_time_window_grid_compat("time_window", down, up)
            }
            Self::Static { mapping: key_map } => {
                let down_keys = single_dim_key_set(down)?;
                for src in key_map.keys() {
                    if !down_keys.contains(src) {
                        return Err(MappingValidationError::KeyNotInDefinition {
                            key: src.clone(),
                            side: Side::Downstream,
                            mapping: "Static",
                        });
                    }
                }
                let up_keys = single_dim_key_set(up)?;
                for tgt in key_map.values() {
                    if !up_keys.contains(tgt) {
                        return Err(MappingValidationError::KeyNotInDefinition {
                            key: tgt.clone(),
                            side: Side::Upstream,
                            mapping: "Static",
                        });
                    }
                }
                Ok(())
            }
            Self::AllPartitions {} => Ok(()),
            Self::SpecificPartitions { .. } => {
                Err(MappingValidationError::IncompatibleMappingForShape {
                    mapping: "SpecificPartitions",
                    downstream_partitioned: true,
                    upstream_partitioned: true,
                })
            }
            Self::ForKeys { .. } => Err(MappingValidationError::IncompatibleMappingForShape {
                mapping: "ForKeys",
                downstream_partitioned: true,
                upstream_partitioned: true,
            }),
            Self::Subset {} => {
                if !down.same_variant(up) {
                    return Err(MappingValidationError::DefinitionTypeMismatch {
                        mapping: "Subset",
                        downstream: down.variant_name().to_string(),
                        upstream: up.variant_name().to_string(),
                    });
                }
                if let (PartitionsDefinition::Static { .. }, PartitionsDefinition::Static { .. }) =
                    (down, up)
                {
                    let down_keys = single_dim_key_set(down)?;
                    let up_keys = single_dim_key_set(up)?;
                    if !up_keys.is_subset(&down_keys) {
                        let mut extras: Vec<String> =
                            up_keys.difference(&down_keys).cloned().collect();
                        extras.sort();
                        return Err(MappingValidationError::UpstreamKeysNotSubset { extras });
                    }
                }
                // For TimeWindow / Dynamic: subset relationship is checked at runtime.
                Ok(())
            }
            Self::MultiToSingle {
                dimension_name,
                partition_mapping,
            } => {
                let (multi_dims, single_def, multi_side) = match (down, up) {
                    (PartitionsDefinition::Multi { .. }, PartitionsDefinition::Multi { .. }) => {
                        return Err(MappingValidationError::Dimension(
                            DimensionErrorKind::BothSidesMulti,
                        ));
                    }
                    (PartitionsDefinition::Multi { dimensions }, _) => {
                        (dimensions, up, Side::Downstream)
                    }
                    (_, PartitionsDefinition::Multi { dimensions }) => {
                        (dimensions, down, Side::Upstream)
                    }
                    _ => {
                        return Err(MappingValidationError::Dimension(
                            DimensionErrorKind::NeitherSideMulti {
                                downstream: down.variant_name().to_string(),
                                upstream: up.variant_name().to_string(),
                            },
                        ));
                    }
                };

                let dim_def = multi_dims
                    .iter()
                    .find(|(name, _)| name == dimension_name)
                    .map(|(_, def)| def)
                    .ok_or_else(|| {
                        MappingValidationError::Dimension(
                            DimensionErrorKind::MultiToSingleDimMissing {
                                dim: dimension_name.clone(),
                                side: multi_side,
                                available: multi_dims.iter().map(|(n, _)| n.clone()).collect(),
                            },
                        )
                    })?;

                // The inner mapping's orientation follows the edge: with a
                // Multi downstream it maps the named dim's key -> the single
                // upstream; with a Multi upstream it maps the downstream
                // single key -> the named dim.
                let (inner_down, inner_up) = match multi_side {
                    Side::Downstream => (dim_def, single_def),
                    Side::Upstream => (single_def, dim_def),
                };
                partition_mapping
                    .0
                    .validate_partitioned_pair(inner_down, inner_up)
                    .map_err(|e| MappingValidationError::InDimension {
                        dim: dimension_name.clone(),
                        source: Box::new(e),
                    })
            }
            Self::Multi { dimension_mappings } => {
                let down_dims = match down {
                    PartitionsDefinition::Multi { dimensions } => dimensions,
                    _ => {
                        return Err(MappingValidationError::RequiredDefinitionType {
                            mapping: "Multi",
                            side: Side::Downstream,
                            expected: "Multi",
                            found: down.variant_name().to_string(),
                        });
                    }
                };
                let up_dims = match up {
                    PartitionsDefinition::Multi { dimensions } => dimensions,
                    _ => {
                        return Err(MappingValidationError::RequiredDefinitionType {
                            mapping: "Multi",
                            side: Side::Upstream,
                            expected: "Multi",
                            found: up.variant_name().to_string(),
                        });
                    }
                };

                let down_dim_map: HashMap<&str, &PartitionsDefinition> =
                    down_dims.iter().map(|(n, d)| (n.as_str(), d)).collect();
                let up_dim_map: HashMap<&str, &PartitionsDefinition> =
                    up_dims.iter().map(|(n, d)| (n.as_str(), d)).collect();

                for (up_dim_name, _) in up_dims {
                    if !dimension_mappings.contains_key(up_dim_name) {
                        return Err(MappingValidationError::Dimension(
                            DimensionErrorKind::MissingMapping {
                                dim: up_dim_name.clone(),
                            },
                        ));
                    }
                }

                let mut targeted: HashSet<&str> = HashSet::new();
                for (up_dim, (down_dim, per_dim)) in dimension_mappings {
                    let up_sub = up_dim_map.get(up_dim.as_str()).ok_or_else(|| {
                        MappingValidationError::Dimension(
                            DimensionErrorKind::MultiUpstreamDimMissing {
                                dim: up_dim.clone(),
                            },
                        )
                    })?;
                    let down_sub = down_dim_map.get(down_dim.as_str()).ok_or_else(|| {
                        MappingValidationError::Dimension(
                            DimensionErrorKind::MultiDownstreamDimMissing {
                                dim: down_dim.clone(),
                            },
                        )
                    })?;

                    if !targeted.insert(down_dim.as_str()) {
                        return Err(MappingValidationError::Dimension(
                            DimensionErrorKind::TargetedTwice {
                                dim: down_dim.clone(),
                            },
                        ));
                    }

                    per_dim
                        .validate_partitioned_pair(down_sub, up_sub)
                        .map_err(|e| MappingValidationError::InDimension {
                            dim: down_dim.clone(),
                            source: Box::new(e),
                        })?;
                }

                for (down_dim_name, _) in down_dims {
                    if !targeted.contains(down_dim_name.as_str()) {
                        return Err(MappingValidationError::Dimension(
                            DimensionErrorKind::NotCovered {
                                dim: down_dim_name.clone(),
                            },
                        ));
                    }
                }
                Ok(())
            }
        }
    }

    fn validate_partitioned_to_unpartitioned(
        &self,
        down: &PartitionsDefinition,
    ) -> Result<(), MappingValidationError> {
        match self {
            Self::AllPartitions {} => Ok(()),
            Self::ForKeys { selectors } => {
                for s in selectors {
                    match s {
                        PartitionKeySelector::Key(k) => {
                            let valid = down.validate_partition_key(k).map_err(|e| {
                                MappingValidationError::DefinitionError(e.to_string())
                            })?;
                            if !valid {
                                return Err(MappingValidationError::KeyNotInDefinition {
                                    key: format!("{k:?}"),
                                    side: Side::Downstream,
                                    mapping: "ForKeys",
                                });
                            }
                        }
                        // An unknown or inverted range endpoint would silently
                        // match nothing (every downstream key Skips its dep) —
                        // surface it here instead.
                        PartitionKeySelector::Range(range) => {
                            range
                                .validate_against(down)
                                .map_err(MappingValidationError::DefinitionError)?;
                        }
                    }
                }
                Ok(())
            }
            other => Err(MappingValidationError::IncompatibleMappingForShape {
                mapping: other.variant_name(),
                downstream_partitioned: true,
                upstream_partitioned: false,
            }),
        }
    }

    fn validate_unpartitioned_to_partitioned(
        &self,
        up: &PartitionsDefinition,
    ) -> Result<(), MappingValidationError> {
        match self {
            Self::AllPartitions {} => Ok(()),
            Self::SpecificPartitions { partition_keys } => {
                let up_keys = single_dim_key_set(up)?;
                for pk in partition_keys {
                    if !up_keys.contains(pk) {
                        return Err(MappingValidationError::KeyNotInDefinition {
                            key: pk.clone(),
                            side: Side::Upstream,
                            mapping: "SpecificPartitions",
                        });
                    }
                }
                Ok(())
            }
            other => Err(MappingValidationError::IncompatibleMappingForShape {
                mapping: other.variant_name(),
                downstream_partitioned: false,
                upstream_partitioned: true,
            }),
        }
    }
}

#[pymethods]
impl PartitionMapping {
    #[staticmethod]
    fn identity() -> Self {
        Self::Identity {}
    }

    #[staticmethod]
    fn all_partitions() -> Self {
        Self::AllPartitions {}
    }

    #[staticmethod]
    fn static_(mapping: HashMap<String, String>) -> Self {
        Self::Static { mapping }
    }

    #[staticmethod]
    fn time_window(offset: i64) -> Self {
        Self::TimeWindow { offset }
    }

    /// Create a SpecificPartitions mapping that maps all downstream partitions to
    /// a specific set of upstream partition keys.
    #[staticmethod]
    fn specific_partitions(mut partition_keys: Vec<String>) -> PyResult<Self> {
        if partition_keys.is_empty() {
            return Err(PartitionValidationError::new_err(
                "SpecificPartitions must have at least one partition key",
            ));
        }
        partition_keys.sort();
        Ok(Self::SpecificPartitions { partition_keys })
    }

    /// Create a Multi mapping that maps dimensions between MultiPartitionsDefinitions.
    ///
    /// Each key is an upstream dimension name. Each value is either:
    /// - A `PartitionMapping` (shorthand: maps to same-named downstream dimension)
    /// - A `(str, PartitionMapping)` tuple (maps to a differently-named downstream dimension)
    #[staticmethod]
    fn multi(dimension_mappings: &Bound<'_, PyDict>) -> PyResult<Self> {
        let mut map = HashMap::with_capacity(dimension_mappings.len());
        for (key, value) in dimension_mappings.iter() {
            let upstream_dim: String = key.extract()?;

            let (downstream_dim, mapping) = if let Ok(tuple) =
                value.extract::<(String, PartitionMapping)>()
            {
                tuple
            } else if let Ok(mapping) = value.extract::<PartitionMapping>() {
                (upstream_dim.clone(), mapping)
            } else {
                return Err(PyTypeError::new_err(
                    "Multi mapping values must be PartitionMapping or (str, PartitionMapping) tuple",
                ));
            };

            let rejected = match &mapping {
                PartitionMapping::Multi { .. } => Some("Multi"),
                PartitionMapping::ForKeys { .. } => Some("ForKeys"),
                PartitionMapping::Subset {} => Some("Subset"),
                _ => None,
            };
            if let Some(name) = rejected {
                return Err(PartitionValidationError::new_err(format!(
                    "Nested {name} mappings are not allowed inside Multi",
                )));
            }
            map.insert(upstream_dim, (downstream_dim, mapping));
        }
        if map.is_empty() {
            return Err(PartitionValidationError::new_err(
                "Multi mapping must have at least one dimension",
            ));
        }
        Ok(Self::Multi {
            dimension_mappings: map,
        })
    }

    /// Create a MultiToSingle mapping that maps one dimension of a MultiPartitionsDefinition
    /// to a single-dimension PartitionsDefinition (or vice versa).
    ///
    /// `partition_mapping` controls how the selected dimension maps to the single-dimension side.
    /// Defaults to Identity if not provided.
    #[staticmethod]
    #[pyo3(signature = (dimension_name, partition_mapping=None))]
    fn multi_to_single(
        dimension_name: String,
        partition_mapping: Option<PartitionMapping>,
    ) -> PyResult<Self> {
        if let Some(ref m) = partition_mapping {
            let rejected = match m {
                PartitionMapping::Multi { .. } => Some("Multi"),
                PartitionMapping::MultiToSingle { .. } => Some("MultiToSingle"),
                PartitionMapping::ForKeys { .. } => Some("ForKeys"),
                PartitionMapping::Subset {} => Some("Subset"),
                _ => None,
            };
            if let Some(name) = rejected {
                return Err(PartitionValidationError::new_err(format!(
                    "MultiToSingle partition_mapping cannot be {name}",
                )));
            }
        }
        Ok(Self::MultiToSingle {
            dimension_name,
            partition_mapping: BoxedMapping(Box::new(
                partition_mapping.unwrap_or(PartitionMapping::Identity {}),
            )),
        })
    }

    /// Create a ForKeys mapping that maps an unpartitioned upstream to specific
    /// downstream partition keys. The upstream is loaded only when the downstream
    /// key matches one of the selectors; otherwise the parameter receives `None`.
    #[staticmethod]
    fn for_keys(selectors: Vec<PartitionKeySelector>) -> PyResult<Self> {
        if selectors.is_empty() {
            return Err(PartitionValidationError::new_err(
                "for_keys must have at least one key or range",
            ));
        }
        Ok(Self::ForKeys { selectors })
    }

    /// Create a Subset mapping for partitioned-to-partitioned edges where upstream
    /// has a subset of downstream's partition keys.
    #[staticmethod]
    fn subset() -> Self {
        Self::Subset {}
    }

    fn __repr__(&self) -> String {
        match self {
            Self::Identity {} => "PartitionMapping.identity()".to_string(),
            Self::AllPartitions {} => "PartitionMapping.all_partitions()".to_string(),
            Self::Static { mapping } => {
                format!("PartitionMapping.static_({mapping:?})")
            }
            Self::TimeWindow { offset } => {
                format!("PartitionMapping.time_window(offset={offset})")
            }
            Self::SpecificPartitions { partition_keys } => {
                format!("PartitionMapping.specific_partitions({partition_keys:?})")
            }
            Self::Multi { dimension_mappings } => {
                let entries: Vec<String> = dimension_mappings
                    .iter()
                    .map(|(up_dim, (down_dim, mapping))| {
                        if up_dim == down_dim {
                            format!("{up_dim:?}: {}", mapping.__repr__())
                        } else {
                            format!("{up_dim:?}: ({down_dim:?}, {})", mapping.__repr__())
                        }
                    })
                    .collect();
                format!("PartitionMapping.multi({{{}}})", entries.join(", "))
            }
            Self::MultiToSingle {
                dimension_name,
                partition_mapping,
            } => {
                format!(
                    "PartitionMapping.multi_to_single(dimension_name={dimension_name:?}, partition_mapping={})",
                    partition_mapping.0.__repr__()
                )
            }
            Self::ForKeys { selectors } => {
                format!("PartitionMapping.for_keys({selectors:?})")
            }
            Self::Subset {} => "PartitionMapping.subset()".to_string(),
        }
    }

    fn __eq__(&self, other: &Self) -> bool {
        self == other
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let reconstruct = py
            .import("rivers._core")?
            .getattr("_reconstruct_partition_mapping")?
            .unbind();
        let data = PyDict::new(py);
        match self {
            Self::Identity {} => {
                data.set_item("variant", "Identity")?;
            }
            Self::AllPartitions {} => {
                data.set_item("variant", "AllPartitions")?;
            }
            Self::Static { mapping } => {
                data.set_item("variant", "Static")?;
                data.set_item("mapping", mapping.clone())?;
            }
            Self::TimeWindow { offset } => {
                data.set_item("variant", "TimeWindow")?;
                data.set_item("offset", *offset)?;
            }
            Self::Multi { dimension_mappings } => {
                data.set_item("variant", "Multi")?;
                let dims = PyDict::new(py);
                for (up_dim, (down_dim, mapping)) in dimension_mappings {
                    let mapping_py = Py::new(py, mapping.clone())?;
                    if up_dim == down_dim {
                        dims.set_item(up_dim, mapping_py)?;
                    } else {
                        let tuple = PyTuple::new(
                            py,
                            [
                                down_dim.clone().into_pyobject(py)?.into_any(),
                                mapping_py.into_any().into_bound(py).into_any(),
                            ],
                        )?;
                        dims.set_item(up_dim, tuple)?;
                    }
                }
                data.set_item("dimension_mappings", dims)?;
            }
            Self::MultiToSingle {
                dimension_name,
                partition_mapping,
            } => {
                data.set_item("variant", "MultiToSingle")?;
                data.set_item("dimension_name", dimension_name.clone())?;
                let inner = Py::new(py, (*partition_mapping.0).clone())?;
                data.set_item("partition_mapping", inner)?;
            }
            Self::SpecificPartitions { partition_keys } => {
                data.set_item("variant", "SpecificPartitions")?;
                data.set_item("partition_keys", partition_keys.clone())?;
            }
            Self::ForKeys { selectors } => {
                data.set_item("variant", "ForKeys")?;
                let keys_list = PyList::empty(py);
                for s in selectors {
                    keys_list.append(s.into_pyobject(py)?)?;
                }
                data.set_item("selectors", keys_list)?;
            }
            Self::Subset {} => {
                data.set_item("variant", "Subset")?;
            }
        }
        let args = PyTuple::new(py, [data.into_any()])?;
        Ok((reconstruct, args.unbind().into_any()))
    }
}

/// A newtype for `HashMap<String, PartitionMapping>` that accepts keys as
/// `str` or `AssetDef` (extracting `.name`) from Python.
#[derive(Clone, Debug)]
pub struct PartitionMappingDict(pub HashMap<String, PartitionMapping>);

impl Deref for PartitionMappingDict {
    type Target = HashMap<String, PartitionMapping>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'py> pyo3::IntoPyObject<'py> for PartitionMappingDict {
    type Target = PyDict;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.0.into_pyobject(py)
    }
}

impl<'py> pyo3::IntoPyObject<'py> for &PartitionMappingDict {
    type Target = PyDict;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.0.clone().into_pyobject(py)
    }
}

impl FromPyObject<'_, '_> for PartitionMappingDict {
    type Error = PyErr;

    #[allow(deprecated)] // downcast is correct for Borrowed; cast() has different semantics
    fn extract(ob: pyo3::Borrowed<'_, '_, PyAny>) -> Result<Self, Self::Error> {
        let dict = ob.downcast::<PyDict>()?;
        let mut map = HashMap::with_capacity(dict.len());
        for (key, value) in dict.iter() {
            let key_str = if let Ok(s) = key.extract::<String>() {
                s
            } else if let Ok(name_attr) = key.getattr("name") {
                name_attr.extract::<String>().map_err(|_| {
                    PyTypeError::new_err(
                        "partition_mapping keys must be str or AssetDef (object with str 'name' attribute)",
                    )
                })?
            } else {
                return Err(PyTypeError::new_err(
                    "partition_mapping keys must be str or AssetDef",
                ));
            };
            let mapping = value.extract::<PartitionMapping>()?;
            map.insert(key_str, mapping);
        }
        Ok(PartitionMappingDict(map))
    }
}
