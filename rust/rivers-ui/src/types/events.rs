use serde::{Deserialize, Serialize};

use super::{MetadataDisplay, Page};

/// One page of an asset's events plus the total number matching the type filter.
pub type EventsPage = Page<StoredEvent>;

/// Discriminator for [`StoredEvent`]. Mirrors
/// `rivers_core::storage::EventType` (without payload variants — the UI
/// reads payload fields as separate columns).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventType {
    Materialization,
    Observation,
    StepStart,
    StepSuccess,
    StepFailure,
    StepRetry,
    RunQueued,
    RunDequeued,
    RunLaunchFailed,
    StepSlotClaimed,
    StepSlotWaiting,
    StepSlotRenewed,
    StepSlotReleased,
    ActionCompleted,
    Deletion,
}

impl EventType {
    /// Log-style label shown in event tables and tooltips.
    pub fn label(&self) -> &'static str {
        match self {
            EventType::StepStart => "STEP_START",
            EventType::StepSuccess => "STEP_SUCCESS",
            EventType::StepFailure => "STEP_FAILURE",
            EventType::StepRetry => "STEP_RETRY",
            EventType::Materialization => "MATERIALIZATION",
            EventType::Observation => "OBSERVATION",
            EventType::RunQueued => "RUN_QUEUED",
            EventType::RunDequeued => "RUN_DEQUEUED",
            EventType::RunLaunchFailed => "RUN_LAUNCH_FAILED",
            EventType::StepSlotClaimed => "SLOT_CLAIMED",
            EventType::StepSlotWaiting => "SLOT_WAITING",
            EventType::StepSlotRenewed => "SLOT_RENEWED",
            EventType::StepSlotReleased => "SLOT_RELEASED",
            EventType::ActionCompleted => "ACTION_COMPLETED",
            EventType::Deletion => "DELETION",
        }
    }
}

/// One row from the events table — drives the run-detail / asset-detail
/// timelines. Mirrors `rivers_core::storage::EventRecord`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredEvent {
    pub id: String,
    pub event_type: EventType,
    pub asset_key: Option<String>,
    pub run_id: String,
    pub partition_key: Option<String>,
    pub timestamp: i64,
    pub metadata: Vec<(String, MetadataDisplay)>,
    pub data_version: Option<String>,
}

/// One step's captured output from the `run_logs` table — drives the
/// run-detail log tabs. Mirrors `rivers_core::storage::StoredLog`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunLog {
    pub id: String,
    pub run_id: String,
    pub step_key: String,
    pub timestamp: i64,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub logs: Option<String>,
    pub traceback: Option<Traceback>,
}

/// The traceback stored for `step`'s attempt that ended with the
/// `StepFailure` or `StepRetry` event at `at`: its row carries that time.
pub fn traceback_for<'a>(logs: &'a [RunLog], step: &str, at: i64) -> Option<&'a Traceback> {
    logs.iter()
        .filter(|l| l.step_key == step && l.timestamp == at)
        .find_map(|l| l.traceback.as_ref())
}

/// A failed attempt's Python traceback. Mirrors
/// `rivers_core::execution::traceback::Traceback`, whose JSON it reads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Traceback {
    /// The exception chain, oldest first. The last entry failed the step.
    pub exceptions: Vec<ExceptionInfo>,
    /// The traceback as Python prints it.
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExceptionInfo {
    #[serde(rename = "type")]
    pub exc_type: String,
    #[serde(default)]
    pub module: Option<String>,
    pub value: String,
    /// How this exception is linked to the entry before it in the chain.
    #[serde(default)]
    pub chain: Option<ChainLink>,
    pub frames: Vec<TracebackFrame>,
    #[serde(default)]
    pub group: Vec<Vec<ExceptionInfo>>,
    #[serde(default)]
    pub group_omitted: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChainLink {
    /// Raised `from` the previous exception.
    Cause,
    /// Raised while handling the previous exception.
    Context,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TracebackFrame {
    pub filename: String,
    pub abs_path: String,
    pub function: String,
    #[serde(default)]
    pub lineno: Option<u32>,
    /// First column of the running expression, from 1.
    #[serde(default)]
    pub colno: Option<u32>,
    #[serde(default)]
    pub end_lineno: Option<u32>,
    /// Last column of the running expression on `end_lineno`, inclusive.
    #[serde(default)]
    pub end_colno: Option<u32>,
    #[serde(default)]
    pub pre_context: Vec<String>,
    #[serde(default)]
    pub context_line: Option<String>,
    #[serde(default)]
    pub post_context: Vec<String>,
    pub in_app: bool,
    /// More calls of this same frame right after it, left out.
    #[serde(default)]
    pub repeated: u32,
}

#[cfg(test)]
mod traceback_tests {
    use super::*;

    fn log(step: &str, at: i64, text: Option<&str>) -> RunLog {
        RunLog {
            id: format!("{step}-{at}"),
            run_id: "r1".into(),
            step_key: step.into(),
            timestamp: at,
            stdout: text.is_none().then(|| "out".to_string()),
            stderr: None,
            logs: None,
            traceback: text.map(|t| Traceback {
                exceptions: vec![],
                text: t.into(),
            }),
        }
    }

    #[test]
    fn traceback_for_pairs_a_row_with_its_event_by_step_and_time() {
        let logs = vec![
            log("a", 10, None),
            log("a", 10, Some("first attempt")),
            log("a", 20, Some("second attempt")),
            log("b", 20, Some("other step")),
        ];
        let text = |step, at| traceback_for(&logs, step, at).map(|t| t.text.as_str());
        assert_eq!(text("a", 10), Some("first attempt"));
        assert_eq!(text("a", 20), Some("second attempt"));
        assert_eq!(text("b", 20), Some("other step"));
        // An attempt that stored none never shows another attempt's traceback.
        assert_eq!(text("a", 15), None);
        assert_eq!(text("c", 20), None);
    }
}
