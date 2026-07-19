"""Repository, run handle, run/backfill result types — the orchestration entry point."""

from collections.abc import Sequence
from typing import Any, TypeVar, Union, overload

from rivers._core import (
    BashTask,
    Job,
    RetryPolicy,
    RunBackendConfig,
    RunQueueConfig,
    Task,
)
from rivers._core.assets import ExternalAsset, GraphAsset, MultiAsset, SingleAsset
from rivers._core.executor import Executor
from rivers._core.partitions import (
    BackfillStrategy,
    PartitionKey,
    PartitionKeyRange,
    PartitionsDefinition,
)
from rivers._core.schedule import Schedule, ScheduleTickResult
from rivers._core.sensor import Sensor, SensorTickResult
from rivers._core.storage import Storage

T = TypeVar("T")

class RunResult:
    """Outcome of a synchronous :meth:`CodeRepository.materialize` call."""

    success: bool
    """``True`` when every requested asset materialized."""
    run_id: str
    """ID of the underlying run record."""
    materialized_assets: list[str]
    """Asset keys that were attempted in this run."""
    failed_assets: list[tuple[str, str]]
    """``(asset_key, error_message)`` pairs for steps that failed."""

class RunHandle:
    """Async handle to a submitted run — poll, wait, or cancel."""

    run_id: str
    """ID of the underlying run record."""
    @property
    def status(self) -> str:
        """Current run status (e.g. ``"Queued"``, ``"Running"``, ``"Success"``)."""
        ...

    def wait(self, timeout: float | None = None) -> RunResult:
        """Block until the run finishes and return the result.

        Raises:
            TimeoutError: ``timeout`` seconds elapsed before the run reached a terminal state.
        """
        ...

    def cancel(self) -> None:
        """Request cancellation of the run."""
        ...

class BackfillResult:
    """Outcome of :meth:`CodeRepository.backfill` (sync or after waiting)."""

    backfill_id: str
    """Unique identifier assigned to the backfill."""
    num_partitions: int
    """Total partition keys covered by the backfill."""
    num_runs: int
    """Number of runs the backfill expanded into (depends on :class:`BackfillStrategy`)."""
    status: str
    """Terminal status string (``"completed" | "failed" | "canceled" | "in_progress"``)."""
    completed: int
    """Partitions that finished successfully."""
    failed: int
    """Partitions that finished with at least one failed asset."""
    canceled: int
    """Partitions that were canceled before finishing."""
    run_ids: list[str]
    """Run IDs spawned by the backfill."""
    is_dry_run: bool
    """``True`` when produced by a planning-only invocation (no runs launched)."""
    partition_keys: list[PartitionKey]
    """Concrete partition keys the backfill resolved to."""

class BackfillStatus:
    """Snapshot of backfill state returned by :meth:`CodeRepository.get_backfill`."""

    backfill_id: str
    """Unique identifier of the backfill."""
    status: str
    """Current status (``"in_progress" | "completed" | "failed" | "canceled"``)."""
    total_partitions: int
    """Total partition count for this backfill."""
    completed_partitions: int
    """Partitions that finished successfully so far."""
    failed_partitions: int
    """Partitions that finished with at least one failed asset."""
    canceled_partitions: int
    """Partitions canceled before finishing."""
    run_ids: list[str]
    """Run IDs spawned by the backfill so far."""
    error: str | None
    """Error message if the backfill itself failed (vs. individual partitions)."""
    tags: list[tuple[str, str]]
    """Tags attached to every run launched by the backfill."""

class CodeRepository:
    """Top-level container — declares the assets, tasks, jobs, and automations
    that make up a code location.

    Pass to ``rivers`` CLI commands or interact directly via :meth:`materialize`,
    :meth:`backfill`, :meth:`evaluate_schedule`, :meth:`evaluate_sensor`. Use as
    a context manager to guarantee :meth:`shutdown` runs on exit.
    """

    @property
    def assets(self) -> dict[str, SingleAsset | MultiAsset | GraphAsset]:
        """All registered assets keyed by asset name."""
        ...

    @property
    def storage(self) -> Storage:
        """The storage backend bound to this repository (set by :meth:`resolve`)."""
        ...

    @property
    def schedules(self) -> list[Schedule]:
        """All registered schedules."""
        ...

    @property
    def sensors(self) -> list[Sensor]:
        """All registered sensors."""
        ...

    def __init__(
        self,
        assets: Sequence[Union[SingleAsset, MultiAsset, GraphAsset, ExternalAsset]],
        tasks: Sequence[Union[Task, BashTask]] | None = None,
        jobs: Sequence[Job] | None = None,
        schedules: Sequence[Schedule] | None = None,
        sensors: Sequence[Sensor] | None = None,
        default_executor: Executor | None = None,
        resources: dict[str, Any] | None = None,
        partition_defs: dict[str, PartitionsDefinition] | None = None,
        retries: dict[str, RetryPolicy] | None = None,
        default_retry_policy: "RetryPolicy | str | None" = None,
        run_queue: "RunQueueConfig | None" = None,
        run_backend: "RunBackendConfig | None" = None,
        pool_limits: dict[str, int] | None = None,
    ) -> None:
        """Declare the contents of a code location.

        Args:
            assets: All assets exposed by this repository.
            tasks: Standalone tasks that aren't asset producers.
            jobs: Custom jobs (a Job is a named selection over assets/tasks).
            schedules: Schedules attached to the repository.
            sensors: Sensors attached to the repository.
            default_executor: Executor used when a job doesn't specify one.
            resources: ``{name: Resource}`` map injected into asset/task signatures.
            partition_defs: Named partition definitions referenced by
                ``partitions_def="name"`` on assets and tasks.
            retries: Named retry policies referenced by ``retry="name"`` on
                assets and jobs.
            default_retry_policy: Repo-wide retry default (lowest precedence:
                asset > job > this) — a policy or a ``retries`` registry name.
            run_queue: Run queue limits and dequeue cadence.
            run_backend: Local vs Kubernetes run launch configuration.
            pool_limits: Initial concurrency-pool slot caps.
        """
        ...

    def resolve(self, storage: Storage | None = None) -> None:
        """Validate the graph and persist topology / metadata into ``storage``.

        Must be called before :meth:`materialize`, :meth:`backfill`, or daemon start.
        """
        ...

    def validate(self) -> None:
        """Run the storage-independent validation pipeline.

        Performs graph composition, partition / external / resource-reference
        validation, and per-job plan building — without initializing storage,
        invoking ``Resource.setup()``, resolving IO handler ``ResourceRef``\\ s,
        registering assets/pools, or persisting topology.

        Intended for CLI / IDE / UI tools that want fast feedback without the
        side effects of a full :meth:`resolve`. Always re-runs (no idempotency
        guard) so it can be called repeatedly while the user edits code.

        Raises:
            GraphValidationError: graph topology is invalid (cycles, missing
                upstream, etc.).
            AssetDefinitionError: an asset definition is malformed (e.g. an
                external asset with ``automation_condition`` lacking an
                ``observe_fn``).
            ConfigurationError: a parameter does not match an upstream asset
                or a known resource.
        """
        ...

    def _submit_run(
        self,
        selection: list[str] | None = None,
        partition_key: PartitionKey | None = None,
    ) -> "RunHandle":
        """(Internal) submit a run to the queue and return a handle."""
        ...

    def get_job(self, name: str) -> Job:
        """Look up a registered :class:`Job` by name."""
        ...

    def get_schedule(self, name: str) -> Schedule:
        """Look up a registered :class:`Schedule` by name."""
        ...

    def get_sensor(self, name: str) -> Sensor:
        """Look up a registered :class:`Sensor` by name."""
        ...

    def evaluate_schedule(
        self, name: str, execution_time: str | None = None
    ) -> ScheduleTickResult:
        """Run a schedule's evaluation function once and return the tick result.

        Args:
            name: Schedule name.
            execution_time: Override the scheduled execution timestamp (RFC 3339);
                defaults to "now".
        """
        ...

    def evaluate_sensor(
        self, name: str, cursor: str | None = None, last_tick_time: float | None = None
    ) -> SensorTickResult:
        """Run a sensor's evaluation function once and return the tick result.

        Args:
            name: Sensor name.
            cursor: Override the cursor passed to the sensor.
            last_tick_time: Override the last-tick timestamp (seconds since epoch).
        """
        ...

    def materialize(
        self,
        selection: list[str] | None = None,
        partition_key: PartitionKey | None = None,
        tags: list[tuple[str, str]] | None = None,
        raise_on_error: bool = True,
        config: dict[str, dict[str, Any]] | None = None,
        run_id_override: str | None = None,
        include_upstream: bool = False,
        resume: bool = False,
        retry: "RetryPolicy | str | None" = None,
    ) -> RunResult:
        """Materialize assets synchronously.

        Args:
            selection: Asset names to materialize (``None`` = all).
            partition_key: Partition to target (required for partitioned assets).
            tags: Run tags applied for queue / observability filtering.
            raise_on_error: Raise on first failure instead of returning a failed result.
            config: Per-asset config, keyed by asset name.
            run_id_override: Use a pre-assigned run ID (for K8s execution pods).
            include_upstream: Also materialize transitive deps (default: only ``selection``).
            resume: Skip already-completed steps from a crashed prior run with the same ID.
            retry: Run-level retry default for this materialization — a
                :class:`RetryPolicy` or a ``retries`` registry name. Assets with
                their own policy keep it.
        """
        ...

    def backfill(
        self,
        selection: list[str] | None = None,
        partition_keys: list[PartitionKey] | None = None,
        partition_range: PartitionKeyRange | None = None,
        strategy: BackfillStrategy | None = None,
        failure_policy: str = "continue",
        max_concurrency: int = 4,
        tags: list[tuple[str, str]] | None = None,
        config: dict[str, dict[str, Any]] | None = None,
        block: bool = True,
        dry_run: bool = False,
    ) -> BackfillResult:
        """Backfill partitions for the selected assets.

        Args:
            selection: Asset names to backfill (``None`` = all partitioned assets).
            partition_keys: Explicit list of keys (alternative to ``partition_range``).
            partition_range: Range / cartesian-product spec.
            strategy: How partitions are grouped into runs.
            failure_policy: ``"continue"`` or ``"stop_on_failure"``.
            max_concurrency: Cap on concurrent partition runs.
            tags: Run tags applied to every spawned run.
            config: Per-asset config, keyed by asset name.
            block: Wait for the backfill to finish before returning.
            dry_run: Plan only — return the would-be run shape without launching.
        """
        ...

    def cancel_backfill(self, backfill_id: str) -> bool:
        """Cancel a running backfill. Returns ``True`` if the in-process coordinator was signalled."""
        ...

    def get_backfill(self, backfill_id: str) -> BackfillStatus | None:
        """Look up a backfill's current status by ID, or ``None`` if not found."""
        ...

    def rerun_backfill(
        self, backfill_id: str, block: bool = True, dry_run: bool = False
    ) -> BackfillResult:
        """Re-launch the failed/canceled partitions of a previous backfill."""
        ...
    @overload
    def load_node(
        self,
        name: str,
        *,
        partition_key: PartitionKey | None = None,
        type_hint: type[T],
    ) -> T: ...
    @overload
    def load_node(
        self, name: str, *, partition_key: PartitionKey | None = None
    ) -> Any: ...
    def load_node(
        self,
        name: str,
        *,
        partition_key: PartitionKey | None = None,
        type_hint: type | None = None,
    ) -> Any:
        """Load a previously materialized asset value via its IO handler.

        Args:
            name: Asset name.
            partition_key: Partition to load (required for partitioned assets).
            type_hint: Optional target type passed to the IO handler.
        """
        ...

    def io_handler_for_output(self, name: str) -> Any:
        """Resolve the IO handler this repository would use to write ``name``'s output.

        Walks the registry chain ``node.io_handler() → default``, the same
        chain the executor uses at materialize time. Returns the resolved
        handler instance (the configured one, an instance derived from a
        ``ResourceRef``, or the shared default ``InMemoryIOHandler``).

        Useful for debugging "which handler does this asset actually use?"
        without running execution.

        Args:
            name: Asset name.

        Raises:
            NodeNotFoundError: If ``name`` is not in the resolved repository.
        """
        ...

    def observe(self, asset_names: list[str] | None = None) -> dict[str, Any]:
        """Run the observe function of external assets and return the resulting metadata.

        Args:
            asset_names: Restrict to these external assets (``None`` = all observable).
        """
        ...

    def shutdown(self) -> None:
        """Tear down resources, daemons, and gRPC/UI servers started by this repository."""
        ...

    def __enter__(self) -> CodeRepository:
        """Enter context manager — returns ``self``."""
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Any | None = None,
    ) -> bool:
        """Exit context manager — calls :meth:`shutdown`."""
        ...

    def _start_ui_server(
        self, host: str, port: int, grpc_url: str, synthetic: str | None = None
    ) -> None:
        """(Internal) start the in-process UI HTTP server."""
        ...

    def _start_grpc_server(self, host: str, port: int) -> None:
        """(Internal) start the in-process gRPC code-location server."""
        ...

__all__ = [
    "BackfillResult",
    "BackfillStatus",
    "CodeRepository",
    "RunHandle",
    "RunResult",
]
