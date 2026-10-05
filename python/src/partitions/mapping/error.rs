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

/// Why a `PartitionMapping` could not produce an `UpstreamKeyResolution`.
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
