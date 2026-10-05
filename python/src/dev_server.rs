//! Host side of `rivers dev`: the embedded SurrealDB server that the UI and
//! the code-location child connect to over loopback, the UI server, and the
//! supervisor of the code-location child.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use pyo3::exceptions::{PyKeyboardInterrupt, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use rivers_core::assets::graph::{GraphTopology, TopologyNode};
use tokio_util::sync::CancellationToken;

use crate::dev_supervisor::{ChildSpec, Outcome, Supervisor};
use crate::runtime::rt;
use crate::storage::PyStorage;

/// Open client connections hold a graceful shutdown; past this the listener
/// is dropped.
const STOP_GRACE: Duration = Duration::from_secs(5);
/// A dropped live query sends its kill afterwards; the wire gets this long
/// before the caller closes the connection.
const KILL_GRACE: Duration = Duration::from_millis(200);

struct UiServer {
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

#[pyclass(name = "DevHost", frozen, module = "rivers._core")]
pub struct PyDevHost {
    storage: Mutex<Option<storage_server::StorageServer>>,
    ui: Mutex<Option<UiServer>>,
    supervisor: Mutex<Option<Supervisor>>,
    /// Where every process of this session finds storage: the embedded
    /// server, or the external one the host was pointed at.
    endpoint: String,
}

#[pymethods]
impl PyDevHost {
    /// An `endpoint` wins over a `storage_path`: nothing is served then.
    #[new]
    #[pyo3(signature = (storage_path=None, endpoint=None))]
    fn new(
        py: Python<'_>,
        storage_path: Option<String>,
        endpoint: Option<String>,
    ) -> PyResult<Self> {
        if let Some(endpoint) = endpoint {
            return Ok(Self {
                storage: Mutex::new(None),
                ui: Mutex::new(None),
                supervisor: Mutex::new(None),
                endpoint,
            });
        }
        let Some(path) = storage_path else {
            return Err(PyValueError::new_err(
                "pass a storage path to serve, or the endpoint of a SurrealDB server",
            ));
        };
        std::fs::create_dir_all(&path)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to create storage dir: {e}")))?;
        let (server, addr) = py.detach(|| storage_server::start(&path)).map_err(|e| {
            PyRuntimeError::new_err(format!("embedded storage server failed to start: {e:#}"))
        })?;
        Ok(Self {
            storage: Mutex::new(Some(server)),
            ui: Mutex::new(None),
            supervisor: Mutex::new(None),
            endpoint: format!("ws://{addr}"),
        })
    }

    /// The SurrealDB endpoint of this session.
    #[getter]
    fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    #[pyo3(signature = (storage, host, port, grpc_url, synthetic=None))]
    fn start_ui(
        &self,
        storage: &PyStorage,
        host: String,
        port: u16,
        grpc_url: String,
        synthetic: Option<String>,
    ) {
        let storage_arc = Arc::clone(storage.backend());
        let graph = synthetic.as_deref().map(synthetic_graph).map(Arc::new);
        let module = std::env::var("RIVERS_MODULE").unwrap_or_default();
        let registry = rivers_ui::code_location_registry::Registry::dev_single(grpc_url, module);
        rivers_ui::dev_reload::enable();
        let stop = CancellationToken::new();
        let shutdown = stop.clone();
        let task = rt().spawn(async move {
            let auth = match rivers_ui::auth::AuthRuntime::from_env().await {
                Ok(auth) => auth,
                Err(e) => {
                    tracing::error!(target: "rivers::auth", error = %e, "invalid RIVERS_AUTH_* configuration; UI not started");
                    return;
                }
            };
            if let Err(e) =
                rivers_ui::start_server(storage_arc, graph, host, port, registry, auth, shutdown)
                    .await
            {
                tracing::error!(target: "rivers::ui", error = %e, "UI server error");
            }
        });
        if let Some(prev) = self.ui.lock().unwrap().replace(UiServer { stop, task }) {
            prev.stop.cancel();
        }
    }

    /// Start the first generation of the code location and wait until it
    /// serves; `false` when it did not come up.
    #[allow(clippy::too_many_arguments)]
    fn start_code_location(
        &self,
        py: Python<'_>,
        python: String,
        module: String,
        repo_var: String,
        host: String,
        grpc_port: u16,
        no_daemon: bool,
    ) -> PyResult<bool> {
        let mut supervisor = Supervisor::new(ChildSpec {
            python,
            module,
            repo_var,
            host,
            grpc_port,
            endpoint: self.endpoint.clone(),
            no_daemon,
        });
        let outcome = py.detach(|| rt().block_on(supervisor.start()));
        *self.supervisor.lock().unwrap() = Some(supervisor);
        swallow_interrupt(py)?;
        Ok(matches!(outcome, Outcome::Up))
    }

    /// Supervise the code location until a terminate signal: the UI replaces
    /// it with a fresh generation, retired generations are reaped as they
    /// finish.
    fn run(&self, py: Python<'_>) -> PyResult<()> {
        let Some(mut supervisor) = self.supervisor.lock().unwrap().take() else {
            return Ok(());
        };
        py.detach(|| rt().block_on(supervisor.run()));
        *self.supervisor.lock().unwrap() = Some(supervisor);
        swallow_interrupt(py)
    }

    /// Stop every generation of the code location. Idempotent.
    fn stop_code_location(&self, py: Python<'_>) -> PyResult<()> {
        let Some(mut supervisor) = self.supervisor.lock().unwrap().take() else {
            return Ok(());
        };
        py.detach(|| rt().block_on(supervisor.stop()));
        swallow_interrupt(py)
    }

    fn stop_ui(&self, py: Python<'_>) {
        let Some(ui) = self.ui.lock().unwrap().take() else {
            return;
        };
        rivers_ui::dev_reload::disable();
        py.detach(|| {
            ui.stop.cancel();
            let _ = rt().block_on(ui.task);
            std::thread::sleep(KILL_GRACE);
        });
    }

    /// Stop the UI, then the embedded storage server. Idempotent.
    fn stop(&self, py: Python<'_>) {
        self.stop_ui(py);
        let Some(server) = self.storage.lock().unwrap().take() else {
            return;
        };
        py.detach(|| server.stop());
    }
}

/// A terminate signal answered here also reached Python's handler, which
/// raises KeyboardInterrupt at the next bytecode; it has been handled.
fn swallow_interrupt(py: Python<'_>) -> PyResult<()> {
    match py.check_signals() {
        Err(e) if e.is_instance_of::<PyKeyboardInterrupt>(py) => Ok(()),
        other => other,
    }
}

fn synthetic_graph(scale: &str) -> GraphTopology {
    let n = rivers_ui::synthetic::parse_node_count(scale);
    let g = rivers_ui::synthetic::generate_synthetic_graph(n);
    GraphTopology {
        nodes: g
            .nodes
            .into_iter()
            .map(|n| TopologyNode {
                name: n.name,
                kind: n
                    .kind
                    .parse()
                    .expect("synthetic graph produced invalid NodeKind"),
                group: n.group,
                parent_graph: n.parent_graph,
            })
            .collect(),
        edges: g.edges,
    }
}

#[cfg(feature = "dev-server")]
mod storage_server {
    //! The SurrealDB server over the local RocksDB store, assembled from
    //! `surrealdb-server`'s public parts and served with our own axum so this
    //! process owns the shutdown order and learns the bound port.

    use std::future::IntoFuture;
    use std::net::SocketAddr;
    use std::sync::{Arc, mpsc};
    use std::thread::JoinHandle;
    use std::time::Duration;

    use anyhow::{Context, Result};
    use rivers_core::storage::surrealdb_backend::{DEFAULT_DATABASE, DEFAULT_NAMESPACE};
    use surrealdb_server::core::CommunityComposer;
    use surrealdb_server::core::kvs::Datastore;
    use surrealdb_server::ntw::{RouterOptions, SurrealRouter};
    use surrealdb_server::rpc;
    use tokio_util::sync::CancellationToken;

    use super::STOP_GRACE;

    /// SurrealDB runs on 10 MiB stacks; the 2 MiB default overflows in its
    /// query executor.
    const STACK_SIZE: usize = 10 * 1024 * 1024;
    const BIND_TIMEOUT: Duration = Duration::from_secs(30);

    pub(super) struct StorageServer {
        stop: CancellationToken,
        thread: JoinHandle<()>,
    }

    impl StorageServer {
        pub(super) fn stop(self) {
            self.stop.cancel();
            let _ = self.thread.join();
        }
    }

    pub(super) fn start(path: &str) -> Result<(StorageServer, SocketAddr)> {
        let url = format!("rocksdb://{path}");
        let stop = CancellationToken::new();
        let stop_for_thread = stop.clone();
        let (addr_tx, addr_rx) = mpsc::channel::<std::result::Result<SocketAddr, String>>();
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .max(4);
        let thread = std::thread::Builder::new()
            .name("rivers-dev-storage".into())
            .stack_size(STACK_SIZE)
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .thread_name("rivers-dev-storage")
                    .thread_stack_size(STACK_SIZE)
                    .worker_threads(workers)
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(e) => {
                        let _ = addr_tx.send(Err(format!("failed to build runtime: {e}")));
                        return;
                    }
                };
                runtime.block_on(async {
                    if let Err(e) = serve(&url, workers, stop_for_thread, &addr_tx).await {
                        tracing::error!(target: "rivers::dev", error = %format!("{e:#}"), "embedded storage server stopped with error");
                        let _ = addr_tx.send(Err(format!("{e:#}")));
                    }
                });
                runtime.shutdown_timeout(STOP_GRACE);
            })
            .context("spawning the storage server thread")?;

        match addr_rx.recv_timeout(BIND_TIMEOUT) {
            Ok(Ok(addr)) => Ok((StorageServer { stop, thread }, addr)),
            Ok(Err(e)) => {
                StorageServer { stop, thread }.stop();
                Err(anyhow::anyhow!(e))
            }
            Err(_) => {
                StorageServer { stop, thread }.stop();
                Err(anyhow::anyhow!(
                    "timed out waiting for the embedded storage server to bind"
                ))
            }
        }
    }

    async fn serve(
        url: &str,
        workers: usize,
        stop: CancellationToken,
        addr_tx: &mpsc::Sender<std::result::Result<SocketAddr, String>>,
    ) -> Result<()> {
        let datastore_cancel = CancellationToken::new();
        let (send, recv) = surrealdb_server::core::channel::bounded(15_000);
        let (ds, router_state) = Datastore::builder()
            .with_auth(false)
            .with_notify(send)
            .with_shutdown_cancel(datastore_cancel.clone())
            // Started below, once `check_version` has stamped a fresh store.
            .without_maintenance_tasks()
            .with_runtime_worker_threads(workers)
            .build_with_factory_path_and_router_state(url, CommunityComposer())
            .await
            .context("opening the embedded RocksDB datastore")?;
        ds.wait_until_serve_ready()
            .await
            .context("waiting for the datastore")?;
        let (_, is_new) = ds
            .check_version()
            .await
            .context("checking the storage version")?;
        if is_new {
            ds.initialise_defaults(DEFAULT_NAMESPACE, DEFAULT_DATABASE)
                .await
                .context("creating the default namespace and database")?;
        }
        ds.insert_node().await.context("registering the node")?;
        ds.start_maintenance_tasks();

        let router = SurrealRouter::build::<CommunityComposer>(
            RouterOptions::default(),
            Arc::clone(&ds),
            recv,
            datastore_cancel.clone(),
            router_state,
        )
        .await
        .context("building the SurrealDB router")?;
        let rpc_state = Arc::clone(router.rpc_state());
        let notifications = router.spawn_notifications();
        let app = router.into_router();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .context("binding the loopback listener")?;
        let addr = listener.local_addr().context("reading the bound address")?;
        tracing::info!(target: "rivers::dev", endpoint = %format!("ws://{addr}"), "embedded storage server listening");
        let _ = addr_tx.send(Ok(addr));

        let serve_cancel = CancellationToken::new();
        let serving = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(serve_cancel.clone().cancelled_owned())
        .into_future();
        tokio::pin!(serving);
        tokio::select! {
            res = &mut serving => res.context("serving the SurrealDB router")?,
            _ = stop.cancelled() => {
                // Sessions clean up their live queries while the datastore
                // still accepts work; only then does the listener drain.
                let sessions = rpc::graceful_shutdown(Arc::clone(&rpc_state));
                if tokio::time::timeout(STOP_GRACE, sessions).await.is_err() {
                    tracing::warn!(target: "rivers::dev", "clients still connected after the stop grace period");
                }
                serve_cancel.cancel();
                if tokio::time::timeout(STOP_GRACE, &mut serving).await.is_err() {
                    tracing::warn!(target: "rivers::dev", "connections still open after the stop grace period; dropping the listener");
                }
            }
        }
        datastore_cancel.cancel();
        let _ = notifications.await;
        ds.shutdown().await.ok();
        tracing::info!(target: "rivers::dev", "embedded storage server stopped");
        Ok(())
    }
}

#[cfg(not(feature = "dev-server"))]
mod storage_server {
    use std::net::SocketAddr;

    use anyhow::Result;

    pub(super) struct StorageServer;

    impl StorageServer {
        pub(super) fn stop(self) {}
    }

    pub(super) fn start(_path: &str) -> Result<(StorageServer, SocketAddr)> {
        anyhow::bail!(
            "this build of rivers has no embedded storage server; pass --surreal-endpoint"
        )
    }
}
