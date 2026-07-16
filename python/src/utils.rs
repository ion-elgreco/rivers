//! Utility helpers shared across the crate.
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

pub const CORE_MODULE: &str = "rivers._core";

/// `module.qualname` of a Python type. The `retry_on` allow-list and MRO
/// failure classification match on these strings, so both must format them
/// through this helper. (Not `PyType::fully_qualified_name` — that omits the
/// `builtins.` prefix.)
pub(crate) fn qualified_type_name(ty: &Bound<'_, PyAny>) -> PyResult<String> {
    let module: String = ty.getattr("__module__")?.extract()?;
    let qualname: String = ty.getattr("__qualname__")?.extract()?;
    Ok(format!("{module}.{qualname}"))
}

/// Parse a human-readable duration string (e.g. `"30s"`, `"5m"`, `"1h30m"`)
/// into a `std::time::Duration`. Returns a PyO3 `ValueError` with a descriptive
/// message on parse failure.
pub fn parse_duration(field_name: &str, value: &str) -> PyResult<Duration> {
    humantime::parse_duration(value).map_err(|e| {
        PyValueError::new_err(format!(
            "invalid duration for '{field_name}': \"{value}\" — {e}. \
             Examples: \"30s\", \"5m\", \"1h30m\", \"250ms\""
        ))
    })
}

pub fn parse_duration_secs_u32(field_name: &str, value: &str) -> PyResult<u32> {
    let secs = parse_duration(field_name, value)?.as_secs();
    u32::try_from(secs).map_err(|_| {
        PyValueError::new_err(format!(
            "duration for '{field_name}' is too large: {secs}s exceeds u32::MAX"
        ))
    })
}

/// Register a PyO3 submodule with classes re-exported on the parent module.
///
/// Usage:
/// ```ignore
/// register_submodule!(parent, "partitions", [
///     PyPartitionKey as "PartitionKey",
///     PartitionsDefinition as "PartitionsDefinition",
/// ]);
/// ```
macro_rules! register_submodule {
    // All classes re-exported to parent — forwards to the two-list arm with no child-only types.
    ($parent:expr, $mod_name:expr, [$($type:ty as $py_name:expr),* $(,)?]) => {
        register_submodule!($parent, $mod_name, [$($type as $py_name),*], [])
    };
    ($parent:expr, $mod_name:expr, [$($type:ty as $py_name:expr),* $(,)?], [$($child_type:ty),* $(,)?]) => {{
        let py = $parent.py();
        let child = pyo3::types::PyModule::new(py, $mod_name)?;
        $(
            child.add_class::<$type>()?;
        )*
        $(
            child.add_class::<$child_type>()?;
        )*
        $parent.add_submodule(&child)?;
        $(
            $parent.setattr($py_name, child.getattr($py_name)?)?;
        )*
        py.import("sys")?
            .getattr("modules")?
            .set_item(format!("{}.{}", $crate::utils::CORE_MODULE, $mod_name), child)
    }};
}
