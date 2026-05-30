//! rivers Python extension module — PyO3 entry point, tracing setup, and module registration.
//!
//! Initializes a `TeeWriter`-backed tracing subscriber for per-step log capture,
//! configures OpenTelemetry export and `pyo3-pylogger` bridge, then registers all
//! submodules (assets, executor, storage, daemon, etc.) into the `rivers._core` package.
#![allow(clippy::too_many_arguments, clippy::type_complexity)]

use opentelemetry::trace::TracerProvider;
use pyo3::prelude::*;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[macro_use]
pub(crate) mod utils;

mod assets;
pub mod automation;
pub mod backends;
mod completion_queue;
pub mod composition;
pub mod concurrency;
mod config;
pub mod context;
pub mod daemon;
pub mod errors;
pub mod executor;
pub mod grpc_server;
pub mod hooks;
mod job;
pub mod log_capture;
pub mod metadata;
pub mod net;
pub mod partitions;
mod repository;
pub mod result_types;
pub(crate) mod runtime;
pub mod schema;
pub mod shutdown;
pub mod storage;
mod task;

use assets::register_asset_module;
use automation::{register_automation_module, register_schedule_module, register_sensor_module};
use hooks::register_hooks_module;
use partitions::register_partition_module;
use task::register_task_module;

use crate::completion_queue::PyCompletionQueue;
use crate::composition::{PyInvokedNodeOutput, PyMappedOutput};
use crate::executor::register_executor_module;
use crate::job::register_job_module;
use crate::{repository::register_repository_module, storage::register_storage_module};

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    // First call wins; .ok() ignores duplicate inits from tests.
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("rivers=info,rivers_core=info,rivers_ui=info,warn"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .compact()
        .with_writer(log_capture::TeeWriter);

    // Optional OTel layer — activates when OTEL_EXPORTER_OTLP_ENDPOINT is set
    let mut otel_build_err = None;
    let otel_layer = if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
        match opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .build()
        {
            Ok(exp) => {
                let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
                    .with_batch_exporter(exp)
                    .with_resource(
                        opentelemetry_sdk::Resource::builder()
                            .with_service_name("rivers")
                            .build(),
                    )
                    .build();
                let tracer = provider.tracer("rivers");
                Some(tracing_opentelemetry::layer().with_tracer(tracer))
            }
            Err(err) => {
                otel_build_err = Some(err.to_string());
                None
            }
        }
    } else {
        None
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(otel_layer)
        .try_init()
        .ok();

    if let Some(err) = otel_build_err {
        report_otel_build_error(&err);
    }

    pyo3_pylogger::setup_logging(m.py(), "rivers")?;

    // Set root logger level to DEBUG so Python doesn't filter before the HostHandler;
    // actual filtering is done by the Rust EnvFilter above.
    m.py().run(
        c"import logging; logging.basicConfig(level=logging.DEBUG)",
        None,
        None,
    )?;

    errors::register_exceptions(m)?;
    register_asset_module(m)?;
    register_automation_module(m)?;
    register_partition_module(m)?;
    register_repository_module(m)?;
    register_executor_module(m)?;
    register_job_module(m)?;
    register_task_module(m)?;
    register_storage_module(m)?;
    register_hooks_module(m)?;
    register_schedule_module(m)?;
    register_sensor_module(m)?;

    m.add_class::<PyInvokedNodeOutput>()?;
    m.add_class::<PyMappedOutput>()?;
    m.add_class::<PyCompletionQueue>()?;
    m.add_class::<executor::ops::MappedResultsIter>()?;
    m.add_class::<context::io::PyOutputContext>()?;
    m.add_class::<context::io::PyInputContext>()?;
    m.add_class::<metadata::MetadataValue>()?;
    m.add_class::<schema::PySchemaWrapper>()?;
    m.add_class::<daemon::PyAutomationDaemon>()?;
    m.add_class::<concurrency::PyRunQueueConfig>()?;
    m.add_class::<concurrency::PyTagConcurrencyLimit>()?;
    m.add_class::<concurrency::PyRunBackendConfig>()?;
    m.add_class::<result_types::PyOutput>()?;
    m.add_class::<result_types::PyObservation>()?;
    m.add_class::<result_types::PyMaterialization>()?;
    m.add_class::<result_types::PyDynamicOutput>()?;
    m.add_class::<executor::parallel::worker::PyFuncRef>()?;
    m.add_class::<executor::parallel::worker::PyIOHandlerRef>()?;
    m.add_class::<executor::parallel::worker::PyIOLoadSpec>()?;
    m.add_class::<executor::parallel::worker::PyCollectLoadSpec>()?;
    m.add_class::<executor::parallel::worker::PyCollectStreamLoadSpec>()?;
    m.add_class::<executor::parallel::worker::PyWorkerResult>()?;
    m.add_class::<executor::parallel::worker::WorkerCollectStreamIter>()?;
    m.add_function(pyo3::wrap_pyfunction!(
        executor::parallel::worker::worker_execute_step,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        executor::parallel::worker::_reconstruct_func_ref,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        executor::parallel::worker::_reconstruct_io_handler_ref,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        executor::parallel::worker::_reconstruct_partition_key,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        executor::parallel::worker::_reconstruct_partitions_definition,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        executor::parallel::worker::_reconstruct_partition_mapping,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        executor::parallel::worker::_reconstruct_partition_context,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(runtime::runtime_info, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        daemon::eval_schedule_in_subprocess,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        daemon::eval_sensor_in_subprocess,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        shutdown::py_install_signal_handler,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(shutdown::py_wait_for_exit, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(shutdown::py_drain_in_flight, m)?)?;

    Ok(())
}

fn report_otel_build_error(err: &str) {
    tracing::error!(
        error = %err,
        "Failed to build OTLP span exporter; OpenTelemetry export disabled",
    );
}
