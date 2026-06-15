//! Custom Python exception types for rivers.
use pyo3::create_exception;
use pyo3::prelude::*;

create_exception!(rivers, AssetDefinitionError, pyo3::exceptions::PyException);
create_exception!(rivers, AssetNotFoundError, pyo3::exceptions::PyException);
create_exception!(
    rivers,
    AssetOutputValidationError,
    pyo3::exceptions::PyException
);
create_exception!(rivers, ConfigurationError, pyo3::exceptions::PyException);
create_exception!(rivers, ExecutionError, pyo3::exceptions::PyException);
create_exception!(rivers, GraphValidationError, pyo3::exceptions::PyException);
create_exception!(rivers, InvalidMetadataError, pyo3::exceptions::PyException);
create_exception!(rivers, NodeNotFoundError, pyo3::exceptions::PyException);
create_exception!(
    rivers,
    PartitionDefinitionError,
    pyo3::exceptions::PyException
);
create_exception!(
    rivers,
    PartitionValidationError,
    pyo3::exceptions::PyException
);
create_exception!(rivers, ResultDefinitionError, pyo3::exceptions::PyException);
create_exception!(
    rivers,
    ScheduleDefinitionError,
    pyo3::exceptions::PyException
);
create_exception!(rivers, SensorDefinitionError, pyo3::exceptions::PyException);
create_exception!(rivers, StorageError, pyo3::exceptions::PyException);
// Subclass of StorageError so `except StorageError` still catches it; the
// `rivers dev` prompt catches this specific type to offer `rivers db migrate`.
create_exception!(rivers, SchemaMigrationNeededError, StorageError);
create_exception!(rivers, TaskDefinitionError, pyo3::exceptions::PyException);

pub fn register_exceptions(parent_module: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = parent_module.py();
    let child = pyo3::types::PyModule::new(py, "exceptions")?;

    child.add(
        "AssetDefinitionError",
        py.get_type::<AssetDefinitionError>(),
    )?;
    child.add("AssetNotFoundError", py.get_type::<AssetNotFoundError>())?;
    child.add(
        "AssetOutputValidationError",
        py.get_type::<AssetOutputValidationError>(),
    )?;
    child.add("ConfigurationError", py.get_type::<ConfigurationError>())?;
    child.add("ExecutionError", py.get_type::<ExecutionError>())?;
    child.add(
        "GraphValidationError",
        py.get_type::<GraphValidationError>(),
    )?;
    child.add(
        "InvalidMetadataError",
        py.get_type::<InvalidMetadataError>(),
    )?;
    child.add("NodeNotFoundError", py.get_type::<NodeNotFoundError>())?;
    child.add(
        "PartitionDefinitionError",
        py.get_type::<PartitionDefinitionError>(),
    )?;
    child.add(
        "PartitionValidationError",
        py.get_type::<PartitionValidationError>(),
    )?;
    child.add(
        "ResultDefinitionError",
        py.get_type::<ResultDefinitionError>(),
    )?;
    child.add(
        "ScheduleDefinitionError",
        py.get_type::<ScheduleDefinitionError>(),
    )?;
    child.add(
        "SensorDefinitionError",
        py.get_type::<SensorDefinitionError>(),
    )?;
    child.add("StorageError", py.get_type::<StorageError>())?;
    child.add(
        "SchemaMigrationNeededError",
        py.get_type::<SchemaMigrationNeededError>(),
    )?;
    child.add("TaskDefinitionError", py.get_type::<TaskDefinitionError>())?;

    parent_module.add_submodule(&child)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item(format!("{}.exceptions", crate::utils::CORE_MODULE), child)?;

    Ok(())
}
