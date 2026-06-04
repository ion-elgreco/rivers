<img src="https://raw.githubusercontent.com/ion-elgreco/rivers/main/assets/logo_wordmark.png" alt="rivers" width="320" />

**Orchestration platform for tasks and assets, fully backed by Rust.**

rivers is a Rust-powered orchestration platform built around data assets. Define pipelines in Python; rivers resolves the graph, plans execution - no Python interpreter on the control plane.

[Documentation](https://ion-elgreco.github.io/rivers/) · [Issues](https://github.com/ion-elgreco/rivers/issues) · [Discussions](https://github.com/ion-elgreco/rivers/discussions)

## Key features

- **Asset-based orchestration** — define data assets as Python functions; rivers resolves the dependency graph automatically.
- **Rust core** — graph resolution, execution planning, partition logic, and the scheduler all run in compiled Rust.
- **Multiple asset types** — single, multi-output, graph (composing `Task`s into sub-DAGs), and external assets.
- **Partitioning** — static, time-window (daily/hourly/custom cron), multi-dimensional, and runtime-extensible dynamic partitions.
- **Pluggable IO** — built-in handlers for in-memory, pickle (any object store), and Delta Lake with merge support.
- **Parallel & distributed execution** — `Executor.parallel()` for concurrent subprocess workers, `Executor.kubernetes()` for one-pod-per-step on K8s.
- **Schedules, sensors, and automation conditions** — declarative triggers (cron, event-driven, dep-aware) executed by the rivers daemon.
- **Backfills** — partition-range execution with multi-run, single-run, and per-dimension strategies.
- **Persistent storage** — embedded SurrealDB + RocksDB for local dev, SurrealDB server for production.
- **Concurrency control** — run-queue limits, tag concurrency, and step-level concurrency pools.
- **Single-binary dev experience** — `rivers dev <module>` boots SurrealDB (embedded RocksDB), the scheduler, and the web UI on `:3000` in one process.

## Performance

Hot paths run in compiled Rust: graph resolution, partition mapping, execution planning, the scheduler. Python is the API surface only. Plan times stay sub-millisecond on graphs with thousands of nodes. The UI is Rust too — Leptos SSR + WASM on `axum`, state read straight from SurrealDB and pushed to the browser via Server-Sent Events.

## Kubernetes-native

rivers ships with a Kubernetes operator and CRDs. Declare a repo as a `CodeLocation`:

```yaml
apiVersion: rivers.io/v1alpha1
kind: CodeLocation
metadata:
  name: analytics
spec:
  image: ghcr.io/acme/pipelines
  tag: v0.2.0
  module: pipelines.analytics
```

The operator resolves the image to a digest, reconciles a `Deployment` + `Service` running `rivers serve`, registers it with the UI's discovery registry, and re-polls the registry to keep the digest fresh. Multi-arch images (`linux/amd64`, `linux/arm64`) and Helm charts are published to `ghcr.io` on every release with SLSA build-provenance attestations.

See the [installation guide](https://ion-elgreco.github.io/rivers/latest/installation/kubernetes/) for the full setup — helm install commands, common values, and an [architecture overview](https://ion-elgreco.github.io/rivers/latest/installation/overview/) with the reconciliation and run sequence diagrams.

## Install

```bash
pip install rivers
```

Optional extras for IO handlers:

```bash
pip install rivers[delta]     # Delta Lake support
pip install rivers[pyarrow]   # PyArrow table support
pip install rivers[polars]    # Polars DataFrame support
```

## Quick example

```python
import rivers as rs

@rs.Asset
def raw_data():
    return {"users": 100, "events": 5000}

@rs.Asset
def summary(raw_data: dict):
    return f"{raw_data['users']} users, {raw_data['events']} events"

repo = rs.CodeRepository(assets=[raw_data, summary])
result = repo.materialize()

print(repo.load_node("summary"))  # "100 users, 5000 events"
```

See the [Getting Started guide](https://ion-elgreco.github.io/rivers/latest/getting-started/) for partitioning, jobs, IO handlers, and the K8s executor.

## Contributing

Contributions are welcome. See [`CONTRIBUTING.md`](CONTRIBUTING.md) for development setup (`just develop`, `just test`, `just pre-commit`), code conventions, and the test matrix. The [`docs/`](docs/) directory hosts both the user-facing guides and architectural notes for contributors.
