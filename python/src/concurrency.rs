use std::collections::BTreeMap;
use std::time::Duration;

use pyo3::prelude::*;
use rivers_core::concurrency::{RunQueueConfig, TagConcurrencyLimit};

use crate::errors::ConfigurationError;

#[pyclass(name = "RunQueueConfig", frozen, module = "rivers._core")]
pub struct PyRunQueueConfig {
    #[pyo3(get)]
    pub max_concurrent_runs: i32,
    #[pyo3(get)]
    pub tag_concurrency_limits: Vec<Py<PyTagConcurrencyLimit>>,
    #[pyo3(get)]
    pub dequeue_interval: String,
    #[pyo3(get)]
    pub start_timeout: String,
    parsed_interval: Duration,
    parsed_start_timeout: Duration,
}

#[pymethods]
impl PyRunQueueConfig {
    #[new]
    #[pyo3(signature = (max_concurrent_runs=10, tag_concurrency_limits=vec![], dequeue_interval="250ms", start_timeout="180s"))]
    fn new(
        max_concurrent_runs: i32,
        tag_concurrency_limits: Vec<Py<PyTagConcurrencyLimit>>,
        dequeue_interval: &str,
        start_timeout: &str,
    ) -> PyResult<Self> {
        let parsed_interval = crate::utils::parse_duration("dequeue_interval", dequeue_interval)?;
        let parsed_start_timeout = crate::utils::parse_duration("start_timeout", start_timeout)?;
        Ok(Self {
            max_concurrent_runs,
            tag_concurrency_limits,
            dequeue_interval: dequeue_interval.to_string(),
            start_timeout: start_timeout.to_string(),
            parsed_interval,
            parsed_start_timeout,
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "RunQueueConfig(max_concurrent_runs={}, tag_concurrency_limits=[...{}], dequeue_interval='{}', start_timeout='{}')",
            self.max_concurrent_runs,
            self.tag_concurrency_limits.len(),
            self.dequeue_interval,
            self.start_timeout,
        )
    }
}

impl PyRunQueueConfig {
    pub fn to_core(&self, py: Python<'_>) -> RunQueueConfig {
        RunQueueConfig {
            max_concurrent_runs: self.max_concurrent_runs,
            tag_concurrency_limits: self
                .tag_concurrency_limits
                .iter()
                .map(|l| TagConcurrencyLimit::from(&*l.borrow(py)))
                .collect(),
            dequeue_interval: self.parsed_interval,
            start_timeout: self.parsed_start_timeout,
        }
    }
}

#[pyclass(name = "TagConcurrencyLimit", frozen, get_all, module = "rivers._core")]
pub struct PyTagConcurrencyLimit {
    pub key: String,
    pub limit: u32,
    pub value: Option<String>,
    pub per_unique_value: bool,
}

#[pymethods]
impl PyTagConcurrencyLimit {
    #[new]
    #[pyo3(signature = (key, limit, value=None, per_unique_value=false))]
    fn new(key: String, limit: u32, value: Option<String>, per_unique_value: bool) -> Self {
        Self {
            key,
            limit,
            value,
            per_unique_value,
        }
    }

    fn __repr__(&self) -> String {
        let mid = match (&self.value, self.per_unique_value) {
            (Some(v), _) => format!(", value='{v}'"),
            (None, true) => ", per_unique_value=True".into(),
            (None, false) => String::new(),
        };
        format!(
            "TagConcurrencyLimit(key='{}'{mid}, limit={})",
            self.key, self.limit
        )
    }
}

impl From<&PyTagConcurrencyLimit> for TagConcurrencyLimit {
    fn from(py_limit: &PyTagConcurrencyLimit) -> Self {
        TagConcurrencyLimit {
            key: py_limit.key.clone(),
            value: py_limit.value.clone(),
            per_unique_value: py_limit.per_unique_value,
            limit: py_limit.limit,
        }
    }
}

pub struct K8sParams {
    pub image: Option<String>,
    pub namespace: String,
    pub service_account: String,
    pub module: String,
    pub surreal_endpoint: String,
    pub run_cpu: String,
    pub run_memory: String,
    pub worker_cpu: String,
    pub worker_memory: String,
}

impl K8sParams {
    fn build(
        &self,
        code_location_id: String,
    ) -> PyResult<rivers_k8s::run_backend::K8sRunBackendConfig> {
        let image = self.image.clone().ok_or_else(|| {
            ConfigurationError::new_err(
                "Kubernetes run backend requires a code-location image. \
                 Set RIVERS_CODE_LOCATION_IMAGE env var or pass image= to \
                 RunBackendConfig.kubernetes()",
            )
        })?;
        // `code_location_name` is operator-stamped into the daemon pod's env
        // by the CodeLocation reconciler — not a user-facing knob, so it's
        // read at the boundary rather than threaded through the Python API.
        let code_location_name = rivers_k8s::env::detect_code_location_name().unwrap_or_default();
        Ok(rivers_k8s::run_backend::K8sRunBackendConfig {
            image,
            namespace: self.namespace.clone(),
            service_account: self.service_account.clone(),
            module: self.module.clone(),
            surreal_endpoint: self.surreal_endpoint.clone(),
            default_run_cpu: self.run_cpu.clone(),
            default_run_memory: self.run_memory.clone(),
            default_worker_cpu: self.worker_cpu.clone(),
            default_worker_memory: self.worker_memory.clone(),
            labels: BTreeMap::default(),
            code_location_name,
            code_location_id,
        })
    }
}

pub enum BackendKind {
    Local,
    // K8sParams is large (~216 bytes); box it so the other variants stay cheap.
    Kubernetes(Box<K8sParams>),
}

#[pyclass(name = "RunBackendConfig", frozen, module = "rivers._core")]
pub struct PyRunBackendConfig {
    inner: BackendKind,
}

#[pymethods]
impl PyRunBackendConfig {
    #[staticmethod]
    fn local() -> Self {
        Self {
            inner: BackendKind::Local,
        }
    }

    #[staticmethod]
    #[pyo3(signature = (
        image=None,
        *,
        namespace=None,
        service_account = rivers_k8s::defaults::SERVICE_ACCOUNT,
        run_cpu = rivers_k8s::defaults::RUN_CPU,
        run_memory = rivers_k8s::defaults::RUN_MEMORY,
        worker_cpu = rivers_k8s::defaults::WORKER_CPU,
        worker_memory = rivers_k8s::defaults::WORKER_MEMORY,
    ))]
    fn kubernetes(
        image: Option<String>,
        namespace: Option<&str>,
        service_account: &str,
        run_cpu: &str,
        run_memory: &str,
        worker_cpu: &str,
        worker_memory: &str,
    ) -> Self {
        Self {
            inner: BackendKind::Kubernetes(Box::new(K8sParams {
                image: image.or_else(rivers_k8s::env::detect_code_location_image),
                namespace: namespace
                    .map(str::to_string)
                    .unwrap_or_else(rivers_k8s::env::detect_namespace),
                service_account: service_account.to_string(),
                module: rivers_k8s::env::detect_module(),
                surreal_endpoint: rivers_k8s::env::detect_surreal_endpoint(),
                run_cpu: run_cpu.to_string(),
                run_memory: run_memory.to_string(),
                worker_cpu: worker_cpu.to_string(),
                worker_memory: worker_memory.to_string(),
            })),
        }
    }

    fn is_kubernetes(&self) -> bool {
        matches!(self.inner, BackendKind::Kubernetes(_))
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            BackendKind::Local => "RunBackendConfig.local()".to_string(),
            BackendKind::Kubernetes(k) => format!(
                "RunBackendConfig.kubernetes(image='{}', namespace='{}')",
                k.image.as_deref().unwrap_or(""),
                k.namespace,
            ),
        }
    }
}

impl PyRunBackendConfig {
    /// Build the K8s run-backend config when this is a Kubernetes config;
    /// returns `None` for local. `code_location_id` is the identity resolved
    /// by `CodeRepository::resolve_inner` — passed in rather than re-read
    /// from env so every code path sees the same snapshot.
    pub fn build_k8s_config(
        &self,
        code_location_id: String,
    ) -> PyResult<Option<rivers_k8s::run_backend::K8sRunBackendConfig>> {
        match &self.inner {
            BackendKind::Local => Ok(None),
            BackendKind::Kubernetes(k) => k.build(code_location_id).map(Some),
        }
    }
}
