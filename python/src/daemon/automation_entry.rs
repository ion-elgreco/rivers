use std::time::Duration;

use chrono::Utc;
use croner::Cron;
use rivers_core::storage::LaunchedBy;

use super::schedule::ScheduleInfo;
use super::sensors::SensorInfo;
use super::types::{AutomationKind, EvalOutcome, EvalParams, ResolvedEvalMode};
use crate::executor::ops::now_ts;

pub(crate) enum AutomationEntry {
    Schedule {
        info: ScheduleInfo,
        cron: Box<Cron>,
        next_occurrence: Option<chrono::DateTime<Utc>>,
    },
    Sensor {
        info: SensorInfo,
        cursor: Option<String>,
        last_tick_time: Option<f64>,
        last_eval: Option<chrono::DateTime<Utc>>,
        in_flight: bool,
    },
}

impl AutomationEntry {
    pub(crate) fn name(&self) -> &str {
        match self {
            AutomationEntry::Schedule { info, .. } => &info.name,
            AutomationEntry::Sensor { info, .. } => &info.name,
        }
    }

    pub(crate) fn eval_mode(&self) -> &ResolvedEvalMode {
        match self {
            AutomationEntry::Schedule { info, .. } => &info.eval_mode,
            AutomationEntry::Sensor { info, .. } => &info.eval_mode,
        }
    }

    pub(crate) fn automation_type_str(&self) -> &'static str {
        match self {
            AutomationEntry::Schedule { .. } => "Schedule",
            AutomationEntry::Sensor { .. } => "Sensor",
        }
    }

    /// The `LaunchedBy` origin to stamp on runs created by this automation's tick.
    pub(crate) fn launched_by(&self) -> LaunchedBy {
        match self {
            AutomationEntry::Schedule { info, .. } => LaunchedBy::Schedule {
                name: info.name.clone(),
            },
            AutomationEntry::Sensor { info, .. } => LaunchedBy::Sensor {
                name: info.name.clone(),
            },
        }
    }

    #[allow(dead_code)]
    pub(crate) fn in_flight(&self) -> bool {
        matches!(
            self,
            AutomationEntry::Sensor {
                in_flight: true,
                ..
            }
        )
    }

    pub(crate) fn is_due(&self, now: chrono::DateTime<Utc>) -> bool {
        match self {
            AutomationEntry::Schedule {
                next_occurrence, ..
            } => next_occurrence.map(|next| now >= next).unwrap_or(false),
            AutomationEntry::Sensor {
                info,
                last_eval,
                in_flight,
                ..
            } => {
                if *in_flight {
                    return false;
                }
                let interval_secs = info.minimum_interval.as_secs() as i64;
                last_eval
                    .map(|last| (now - last).num_seconds() >= interval_secs)
                    .unwrap_or(true)
            }
        }
    }

    pub(crate) fn cursor(&self) -> Option<&str> {
        match self {
            AutomationEntry::Schedule { .. } => None,
            AutomationEntry::Sensor { cursor, .. } => cursor.as_deref(),
        }
    }

    pub(crate) fn next_due_in(&self, now: chrono::DateTime<Utc>) -> Option<Duration> {
        match self {
            AutomationEntry::Schedule {
                next_occurrence, ..
            } => next_occurrence
                .filter(|next| *next > now)
                .and_then(|next| (next - now).to_std().ok()),
            AutomationEntry::Sensor {
                info,
                last_eval,
                in_flight,
                ..
            } => {
                if *in_flight {
                    return None;
                }
                let interval = chrono::Duration::seconds(info.minimum_interval.as_secs() as i64);
                last_eval
                    .map(|last| last + interval)
                    .filter(|next| *next > now)
                    .and_then(|next| (next - now).to_std().ok())
            }
        }
    }

    /// Called at dispatch time: sets last_eval and marks sensor as in-flight.
    pub(crate) fn mark_dispatched(&mut self, now: chrono::DateTime<Utc>) {
        match self {
            AutomationEntry::Sensor {
                last_eval,
                in_flight,
                ..
            } => {
                *last_eval = Some(now);
                *in_flight = true;
            }
            AutomationEntry::Schedule {
                info,
                cron,
                next_occurrence,
            } => {
                *next_occurrence = rivers_core::condition::next_cron_occurrence_utc(
                    cron,
                    now,
                    info.timezone.as_deref(),
                );
            }
        }
    }

    /// Called when eval result is processed: clears in-flight and updates cursor/last_tick_time.
    pub(crate) fn complete_eval(&mut self, outcome: &EvalOutcome) {
        if let AutomationEntry::Sensor {
            cursor,
            last_tick_time,
            in_flight,
            ..
        } = self
        {
            *in_flight = false;
            if let Some(new_cursor) = outcome.cursor_or(None) {
                *cursor = Some(new_cursor);
            }
            if matches!(outcome, EvalOutcome::RunRequests { .. }) {
                *last_tick_time = Some(now_ts() as f64);
            }
        }
    }

    /// Called when eval fails: clears in-flight so the sensor can be retried.
    pub(crate) fn complete_eval_on_error(&mut self) {
        if let AutomationEntry::Sensor { in_flight, .. } = self {
            *in_flight = false;
        }
    }

    pub(crate) fn to_eval_params(&self) -> EvalParams {
        let launched_by = self.launched_by();
        match self {
            AutomationEntry::Schedule {
                info,
                next_occurrence,
                ..
            } => EvalParams {
                name: info.name.clone(),
                eval_mode: info.eval_mode.clone(),
                timeout: info.eval_timeout,
                kind: AutomationKind::Schedule {
                    exec_time: next_occurrence.map(|t| t.to_rfc3339()).unwrap_or_default(),
                },
                eval_fn: info.eval_fn.clone(),
                default_job_name: Some(info.job_name.clone()),
                default_asset_selection: None,
                launched_by,
                tags: info.tags.clone(),
                precomputed: info.precomputed.clone(),
            },
            AutomationEntry::Sensor {
                info,
                cursor,
                last_tick_time,
                ..
            } => EvalParams {
                name: info.name.clone(),
                eval_mode: info.eval_mode.clone(),
                timeout: info.eval_timeout,
                kind: AutomationKind::Sensor {
                    cursor: cursor.clone(),
                    last_tick_time: *last_tick_time,
                },
                eval_fn: info.eval_fn.clone(),
                default_job_name: info.job_name.clone(),
                default_asset_selection: info.asset_selection.clone(),
                launched_by,
                tags: info.tags.clone(),
                precomputed: info.precomputed.clone(),
            },
        }
    }
}
