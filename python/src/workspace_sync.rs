use pyo3::prelude::*;

/// Exit code of the workspace sync (see `rivers_k8s::workspace_sync`).
#[pyfunction]
pub fn workspace_sync(py: Python<'_>) -> i32 {
    py.detach(rivers_k8s::workspace_sync::run)
}
