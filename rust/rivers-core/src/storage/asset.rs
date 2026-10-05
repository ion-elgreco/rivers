use surrealdb::types::{Error as SurrealError, Kind, SurrealValue, Value};

use super::default_code_location_id;

// ── Staleness types ──

/// Whether an asset needs re-materialization.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub enum StaleStatus {
    UpToDate,
    Stale,
    #[default]
    Missing,
}

impl SurrealValue for StaleStatus {
    fn kind_of() -> Kind {
        String::kind_of()
    }

    fn into_value(self) -> Value {
        let s = match self {
            Self::UpToDate => "UpToDate",
            Self::Stale => "Stale",
            Self::Missing => "Missing",
        };
        s.to_string().into_value()
    }

    fn from_value(value: Value) -> std::result::Result<Self, SurrealError> {
        let s = String::from_value(value)?;
        match s.as_str() {
            "UpToDate" => Ok(Self::UpToDate),
            "Stale" => Ok(Self::Stale),
            "Missing" => Ok(Self::Missing),
            _ => Err(SurrealError::internal(format!("unknown StaleStatus: {s}"))),
        }
    }
}

/// Category of staleness cause.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum StaleCauseCategory {
    /// Asset's code version changed since last materialization.
    Code,
    /// Upstream dependency has newer data.
    Data {
        /// The upstream asset that caused staleness.
        dependency: String,
    },
}

impl StaleCauseCategory {
    /// Return the dependency name if this is a Data cause.
    pub fn dependency(&self) -> Option<&str> {
        match self {
            Self::Data { dependency } => Some(dependency),
            Self::Code => None,
        }
    }
}

impl SurrealValue for StaleCauseCategory {
    fn kind_of() -> Kind {
        String::kind_of()
    }

    fn into_value(self) -> Value {
        let s = match self {
            Self::Code => "Code".to_string(),
            Self::Data { dependency } => format!("Data:{}", dependency),
        };
        s.into_value()
    }

    fn from_value(value: Value) -> std::result::Result<Self, SurrealError> {
        let s = String::from_value(value)?;
        if s == "Code" {
            Ok(Self::Code)
        } else if let Some(dep) = s.strip_prefix("Data:") {
            Ok(Self::Data {
                dependency: dep.to_string(),
            })
        } else {
            Err(SurrealError::internal(format!(
                "unknown StaleCauseCategory: {s}"
            )))
        }
    }
}

/// A single reason why an asset is stale.
#[derive(Debug, Clone, PartialEq, SurrealValue, serde::Serialize, serde::Deserialize)]
pub struct StaleCause {
    pub asset_key: String,
    pub category: StaleCauseCategory,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, SurrealValue, serde::Serialize, serde::Deserialize)]
pub struct AssetRecord {
    /// Owning code location; uniqueness is per-CL via composite index on `(code_location_id, asset_key)`.
    #[serde(default = "default_code_location_id")]
    pub code_location_id: String,
    pub asset_key: String,
    pub tags: Vec<String>,
    pub kinds: Vec<String>,
    pub asset_group: Option<String>,
    pub code_version: Option<String>,
    pub last_event_id: Option<String>,
    pub last_run_id: Option<String>,
    pub last_timestamp: Option<i64>,
    pub last_data_version: Option<String>,
    /// Code version used when this asset was last materialized.
    #[serde(default)]
    pub last_materialization_code_version: Option<String>,
    /// Input data versions consumed during last materialization: (upstream_key, data_version).
    #[serde(default)]
    pub last_input_data_versions: Vec<(String, String)>,
    /// Pool membership: (pool_key, slots_consumed) pairs. Empty = no pool constraint.
    #[serde(default)]
    pub pool: Vec<(String, u32)>,
}
