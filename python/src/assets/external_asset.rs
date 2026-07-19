//! External asset — assets not materialized by rivers but observed for data freshness.
use std::collections::HashMap;

use pyo3::prelude::*;

use super::decorator::{Kinds, PyAsset};
use super::io_handler::IOHandler;
use crate::automation::PyAutomationCondition;
use crate::partitions::PartitionsDefRef;
use crate::partitions::backfill_strategy::PyBackfillStrategy;

pub struct ExternalAsset {
    pub name: Option<String>,
    pub tags: Option<Vec<String>>,
    pub kinds: Kinds,
    pub group: Option<String>,
    pub io_handler: IOHandler,
    pub metadata: Option<HashMap<String, String>>,
    pub partitions_def: Option<PartitionsDefRef>,
    pub observe_fn: Option<Py<PyAny>>,
    pub is_async_observe: bool,
    pub automation_condition: Option<PyAutomationCondition>,
    pub backfill_strategy: Option<PyBackfillStrategy>,
}

/// Python-exposed marker subclass created via `Asset.external(...)`.
#[pyclass(name = "ExternalAsset", extends=PyAsset, module = "rivers._core")]
pub struct PyExternalAsset;
