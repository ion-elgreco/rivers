use super::{PartitionKey, default_code_location_id};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum EventType {
    Materialization {
        data_version: Option<String>,
    },
    Observation {
        data_version: Option<String>,
    },
    StepStart,
    StepSuccess,
    StepFailure,
    /// A failed attempt is about to be retried. Metadata carries the attempt
    /// number, classified reason, and next delay/resources.
    StepRetry,
    // ── Concurrency observability events ──
    /// Run entered the queue (Queued status).
    RunQueued,
    /// Coordinator dequeued a run (Queued → NotStarted).
    RunDequeued,
    /// A dequeued run never launched (backend launch error or no executor
    /// appeared within the start timeout). Metadata carries the error.
    RunLaunchFailed,
    /// Step successfully claimed pool slots.
    StepSlotClaimed,
    /// Step waiting for pool slots (claim returned Pending).
    StepSlotWaiting,
    /// Background lease renewal succeeded.
    StepSlotRenewed,
    /// Step released pool slots.
    StepSlotReleased,
    /// An asset action ran to completion without changing materialization
    /// state. The action name rides in the event metadata (`action` key) and
    /// on the run's `RunRecord::action`.
    ActionCompleted,
    /// An `Unmaterialize` action cleared the asset's (or partition's)
    /// materialization state. The action name rides in the event metadata.
    Deletion,
}

impl EventType {
    /// Returns the data version if this is a Materialization or Observation event.
    pub fn data_version(&self) -> Option<&str> {
        match self {
            Self::Materialization { data_version } | Self::Observation { data_version } => {
                data_version.as_deref()
            }
            _ => None,
        }
    }

    /// Returns the type name as a string (for DB serialization).
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Materialization { .. } => "Materialization",
            Self::Observation { .. } => "Observation",
            Self::StepStart => "StepStart",
            Self::StepSuccess => "StepSuccess",
            Self::StepFailure => "StepFailure",
            Self::StepRetry => "StepRetry",
            Self::RunQueued => "RunQueued",
            Self::RunDequeued => "RunDequeued",
            Self::RunLaunchFailed => "RunLaunchFailed",
            Self::StepSlotClaimed => "StepSlotClaimed",
            Self::StepSlotWaiting => "StepSlotWaiting",
            Self::StepSlotRenewed => "StepSlotRenewed",
            Self::StepSlotReleased => "StepSlotReleased",
            Self::ActionCompleted => "ActionCompleted",
            Self::Deletion => "Deletion",
        }
    }

    /// Reconstruct an EventType from a type name string and optional data_version.
    pub fn from_type_name(
        name: &str,
        data_version: Option<String>,
    ) -> std::result::Result<Self, String> {
        match name {
            "Materialization" => Ok(Self::Materialization { data_version }),
            "Observation" => Ok(Self::Observation { data_version }),
            "StepStart" => Ok(Self::StepStart),
            "StepSuccess" => Ok(Self::StepSuccess),
            "StepFailure" => Ok(Self::StepFailure),
            "StepRetry" => Ok(Self::StepRetry),
            "RunQueued" => Ok(Self::RunQueued),
            "RunDequeued" => Ok(Self::RunDequeued),
            "RunLaunchFailed" => Ok(Self::RunLaunchFailed),
            "StepSlotClaimed" => Ok(Self::StepSlotClaimed),
            "StepSlotWaiting" => Ok(Self::StepSlotWaiting),
            "StepSlotRenewed" => Ok(Self::StepSlotRenewed),
            "StepSlotReleased" => Ok(Self::StepSlotReleased),
            "ActionCompleted" => Ok(Self::ActionCompleted),
            "Deletion" => Ok(Self::Deletion),
            _ => Err(format!("unknown EventType: {name}")),
        }
    }

    pub fn is_materialization(&self) -> bool {
        matches!(self, Self::Materialization { .. })
    }

    pub fn is_observation(&self) -> bool {
        matches!(self, Self::Observation { .. })
    }

    pub fn is_deletion(&self) -> bool {
        matches!(self, Self::Deletion)
    }

    /// Sort priority within the same timestamp.
    pub fn sort_order(&self) -> i64 {
        match self {
            Self::StepStart => 0,
            Self::Observation { .. } => 2,
            Self::Materialization { .. } | Self::ActionCompleted | Self::Deletion => 3,
            Self::StepSuccess | Self::StepFailure | Self::StepRetry => 4,
            Self::RunQueued | Self::RunDequeued | Self::RunLaunchFailed => 5,
            Self::StepSlotClaimed
            | Self::StepSlotWaiting
            | Self::StepSlotRenewed
            | Self::StepSlotReleased => 6,
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EventRecord {
    /// Owning code location.
    #[serde(default = "default_code_location_id")]
    pub code_location_id: String,
    pub event_type: EventType,
    pub asset_key: Option<String>,
    pub run_id: String,
    pub partition_key: Option<PartitionKey>,
    pub timestamp: i64,
    pub metadata: Vec<(String, String)>,
    /// Upstream input data versions consumed during this materialization.
    #[serde(default)]
    pub input_data_versions: Vec<(String, String)>,
}

/// One asset's step reaching a terminal state inside one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepOutcome {
    pub asset_key: String,
    pub run_id: String,
    /// `true` for `StepSuccess`, `false` for `StepFailure`.
    pub succeeded: bool,
}

/// Captured output of one step execution — one row per step in `run_logs`.
/// Streams the step didn't produce stay `None`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LogRecord {
    #[serde(default = "default_code_location_id")]
    pub code_location_id: String,
    pub run_id: String,
    pub step_key: String,
    pub timestamp: i64,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub logs: Option<String>,
    /// JSON [`crate::execution::traceback::Traceback`] of a failed attempt,
    /// in a row of its own stamped with the attempt's `StepFailure` /
    /// `StepRetry` event time.
    #[serde(default)]
    pub traceback: Option<String>,
}

impl LogRecord {
    pub fn is_empty(&self) -> bool {
        self.stdout.is_none()
            && self.stderr.is_none()
            && self.logs.is_none()
            && self.traceback.is_none()
    }
}

/// Stored step log with its database-assigned ID.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredLog {
    pub id: surrealdb::types::RecordId,
    #[serde(default = "default_code_location_id")]
    pub code_location_id: String,
    pub run_id: String,
    pub step_key: String,
    pub timestamp: i64,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub logs: Option<String>,
    #[serde(default)]
    pub traceback: Option<String>,
}

/// Stored event with its database-assigned ID.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredEvent {
    pub id: surrealdb::types::RecordId,
    pub event_type: EventType,
    pub asset_key: Option<String>,
    pub run_id: String,
    pub partition_key: Option<PartitionKey>,
    pub timestamp: i64,
    pub metadata: Vec<(String, String)>,
    /// Code version of the asset at materialization time (set by storage layer).
    #[serde(default)]
    pub code_version: Option<String>,
    /// Upstream input data versions at materialization time (set by storage layer).
    #[serde(default)]
    pub input_data_versions: Vec<(String, String)>,
}

#[cfg(test)]
mod tests {
    use super::EventType;

    #[test]
    fn all_variants_round_trip_by_type_name() {
        let variants = [
            EventType::Materialization {
                data_version: Some("v1".into()),
            },
            EventType::Observation { data_version: None },
            EventType::StepStart,
            EventType::StepSuccess,
            EventType::StepFailure,
            EventType::StepRetry,
            EventType::RunQueued,
            EventType::RunDequeued,
            EventType::StepSlotClaimed,
            EventType::StepSlotWaiting,
            EventType::StepSlotRenewed,
            EventType::StepSlotReleased,
            EventType::ActionCompleted,
            EventType::Deletion,
        ];
        for ev in variants {
            let name = ev.type_name();
            let dv = ev.data_version().map(String::from);
            let back = EventType::from_type_name(name, dv).unwrap();
            assert_eq!(back, ev, "round-trip failed for {name}");
        }
    }

    #[test]
    fn step_retry_names_and_sorts() {
        assert_eq!(EventType::StepRetry.type_name(), "StepRetry");
        assert_eq!(
            EventType::from_type_name("StepRetry", None).unwrap(),
            EventType::StepRetry
        );
        // sorts alongside the other step-terminal events within a tick
        assert_eq!(
            EventType::StepRetry.sort_order(),
            EventType::StepFailure.sort_order()
        );
    }

    #[test]
    fn unknown_type_name_errors() {
        assert!(EventType::from_type_name("Nope", None).is_err());
    }
}
