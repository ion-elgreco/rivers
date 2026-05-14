//! Automation primitives: schedules, sensors, and automation conditions.
pub mod condition;
pub mod schedule;
pub mod sensor;

pub use condition::PyAutomationCondition;
pub(crate) use schedule::parse_schedule_result;
pub use schedule::{
    PyBackfillRequest, PyRunRequest, PyScheduleDefinition, PyScheduleStatus, PyScheduleTickResult,
    PySkipReason, TickRequest, register_schedule_module,
};
pub(crate) use sensor::parse_sensor_result;
pub use sensor::{PySensorDefinition, PySensorStatus, PySensorTickResult, register_sensor_module};

use pyo3::prelude::*;

/// Evaluation mode for sensor/schedule eval functions.
///
/// - `Auto`: Inferred at daemon start — async functions run in-process,
///   sync functions run in a loky subprocess.
/// - `InProcess`: Always run in-process (async via pyo3-async-runtimes,
///   sync via thread + GIL).
/// - `Subprocess`: Always run in a loky subprocess.
#[pyclass(
    name = "EvalMode",
    frozen,
    eq,
    eq_int,
    from_py_object,
    module = "rivers._core"
)]
#[derive(Clone, PartialEq, Debug)]
pub enum PyEvalMode {
    Auto = 0,
    InProcess = 1,
    Subprocess = 2,
}

/// Register the `rivers._core.automation` submodule (AutomationCondition, EvalMode).
pub fn register_automation_module(parent_module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_submodule!(parent_module, "automation", [
        PyAutomationCondition as "AutomationCondition",
        PyEvalMode as "EvalMode",
    ])
}
