//! BackfillStrategy — controls how partition dimensions map to runs during backfills.
use pyo3::prelude::*;

use crate::errors::PartitionDefinitionError;
use rivers_core::storage::BackfillStrategy;

#[pyclass(
    name = "BackfillStrategy",
    frozen,
    from_py_object,
    module = "rivers._core"
)]
#[derive(Clone, Debug, PartialEq)]
pub enum PyBackfillStrategy {
    /// One run per partition key (default).
    MultiRun {},
    /// One run covers all partition keys.
    SingleRun {},
    /// Per-dimension control: multi_run dimensions are iterated,
    /// single_run dimensions are batched within each run.
    PerDimension {
        multi_run_dims: Vec<String>,
        single_run_dims: Vec<String>,
    },
}

impl PyBackfillStrategy {
    pub fn to_core(&self) -> BackfillStrategy {
        match self {
            Self::MultiRun {} => BackfillStrategy::MultiRun,
            Self::SingleRun {} => BackfillStrategy::SingleRun,
            Self::PerDimension {
                multi_run_dims,
                single_run_dims,
            } => BackfillStrategy::PerDimension {
                multi_run: multi_run_dims.clone(),
                single_run: single_run_dims.clone(),
            },
        }
    }

    pub fn from_core(s: &BackfillStrategy) -> Self {
        match s {
            BackfillStrategy::MultiRun => Self::MultiRun {},
            BackfillStrategy::SingleRun => Self::SingleRun {},
            BackfillStrategy::PerDimension {
                multi_run,
                single_run,
            } => Self::PerDimension {
                multi_run_dims: multi_run.clone(),
                single_run_dims: single_run.clone(),
            },
        }
    }
}

#[pymethods]
impl PyBackfillStrategy {
    /// One run per partition key (default).
    #[staticmethod]
    fn multi_run() -> Self {
        Self::MultiRun {}
    }

    /// One run covers all partition keys.
    #[staticmethod]
    fn single_run() -> Self {
        Self::SingleRun {}
    }

    /// Per-dimension control: multi_run dimensions are iterated across runs,
    /// single_run dimensions are batched within each run.
    #[staticmethod]
    fn per_dimension(multi_run: Vec<String>, single_run: Vec<String>) -> PyResult<Self> {
        if multi_run.is_empty() {
            return Err(PartitionDefinitionError::new_err(
                "multi_run must contain at least one dimension",
            ));
        }
        if single_run.is_empty() {
            return Err(PartitionDefinitionError::new_err(
                "single_run must contain at least one dimension",
            ));
        }
        for dim in &multi_run {
            if single_run.contains(dim) {
                return Err(PartitionDefinitionError::new_err(format!(
                    "dimension '{dim}' cannot be in both multi_run and single_run"
                )));
            }
        }
        Ok(Self::PerDimension {
            multi_run_dims: multi_run,
            single_run_dims: single_run,
        })
    }

    fn __repr__(&self) -> String {
        match self {
            Self::MultiRun {} => "BackfillStrategy.multi_run()".to_string(),
            Self::SingleRun {} => "BackfillStrategy.single_run()".to_string(),
            Self::PerDimension {
                multi_run_dims,
                single_run_dims,
            } => format!(
                "BackfillStrategy.per_dimension(multi_run={multi_run_dims:?}, single_run={single_run_dims:?})"
            ),
        }
    }

    fn __eq__(&self, other: &Self) -> bool {
        self == other
    }
}
