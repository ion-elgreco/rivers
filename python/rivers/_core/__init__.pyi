import datetime
from collections.abc import Sequence
from typing import Any, Protocol, TypeVar, Union

from rivers._core.assets import (
    Asset,
    AssetExecutionContext,
    GraphAsset,
    MultiAsset,
    SelfDependency,
    SingleAsset,
)
from rivers._core.executor import Executor
from rivers._core.partitions import (
    PartitionContext,
    PartitionKey,
    PartitionMapping,
    PartitionsDefinition,
)
from rivers._core.repo import RunResult
from rivers._core.storage import Storage
from rivers._core.tasks import BashTask, Task, TaskExecutionContext

ConfigT = TypeVar("ConfigT")

class AutomationDaemon:
    """Background daemon that ticks schedules, sensors, and automation conditions.

    Started by ``rivers dev`` / ``rivers serve`` (and tests) to drive automation
    against a :class:`Storage`. ``stop()`` is safe to call from a signal handler.
    """

    def __init__(
        self,
        repo: Any,
        storage: Storage,
        *,
        max_ticks_retained: int | None = 100,
        condition_eval_interval: str = "30s",
    ) -> None:
        """Configure the daemon.

        Args:
            repo: The :class:`CodeRepository` whose automations will be ticked.
            storage: Backend used to persist tick history and run requests.
            max_ticks_retained: Cap on per-automation tick history (``None`` = unbounded).
            condition_eval_interval: How often automation conditions are evaluated
                (humantime string, e.g. ``"30s"``).
        """
        ...

    def start(self) -> None:
        """Spawn the daemon thread; returns immediately."""
        ...

    def stop(self) -> None:
        """Signal the daemon to exit and wait for it to drain."""
        ...

class RunQueueConfig:
    """Run-queue limits applied by the queue dequeuer."""

    max_concurrent_runs: int
    tag_concurrency_limits: list[TagConcurrencyLimit]
    dequeue_interval: str
    def __init__(
        self,
        max_concurrent_runs: int = 10,
        tag_concurrency_limits: list[TagConcurrencyLimit] = ...,
        dequeue_interval: str = "250ms",
    ) -> None:
        """Build a run-queue configuration.

        Args:
            max_concurrent_runs: Maximum number of runs in-flight at once.
            tag_concurrency_limits: Per-tag concurrency caps applied on top.
            dequeue_interval: Polling interval for the queue worker (humantime).
        """
        ...

class RunBackendConfig:
    """Where runs are launched — local subprocess or Kubernetes pods."""

    @staticmethod
    def local() -> RunBackendConfig:
        """Run jobs in-process / as local subprocesses."""
        ...

    @staticmethod
    def kubernetes(
        image: str | None = None,
        *,
        namespace: str | None = None,
        service_account: str = "rivers-executor",
        run_cpu: str = "500m",
        run_memory: str = "512Mi",
        worker_cpu: str = "500m",
        worker_memory: str = "512Mi",
    ) -> RunBackendConfig:
        """Launch each run as a Kubernetes Job (and steps as worker pods).

        Args:
            image: Container image for run/worker pods (defaults to the running image).
            namespace: K8s namespace; falls back to the pod's own namespace.
            service_account: Service account bound to the pods.
            run_cpu: CPU request/limit for the run pod.
            run_memory: Memory request/limit for the run pod.
            worker_cpu: CPU request/limit for step worker pods.
            worker_memory: Memory request/limit for step worker pods.
        """
        ...

    def is_kubernetes(self) -> bool:
        """``True`` when this config targets a Kubernetes run backend."""
        ...

class TagConcurrencyLimit:
    """Cap on concurrent runs that share a tag (or per-value)."""

    key: str
    limit: int
    value: str | None
    per_unique_value: bool
    def __init__(
        self,
        key: str,
        limit: int,
        value: str | None = None,
        per_unique_value: bool = False,
    ) -> None:
        """Configure a tag-based concurrency limit.

        Args:
            key: Tag key the limit applies to.
            limit: Maximum number of concurrent runs.
            value: Optional tag value; when omitted matches all values for ``key``.
            per_unique_value: When ``True`` apply ``limit`` independently per
                distinct value of ``key`` (only meaningful when ``value`` is unset).
        """
        ...

class AssetDefinitionError(Exception):
    """Raised when an ``@Asset`` definition is invalid (bad signature, conflicting flags, ...)."""

class AssetNotFoundError(Exception):
    """Raised when a referenced asset key does not exist in the repository."""

class AssetOutputValidationError(Exception):
    """Raised when an asset output fails validation (type, schema, metadata)."""

class ConfigurationError(Exception):
    """Raised when user configuration (resources, run config) cannot be resolved."""

class ExecutionError(Exception):
    """Raised when an asset/task body fails during execution."""

class GraphValidationError(Exception):
    """Raised when the asset graph fails validation (cycles, dangling deps)."""

class InvalidMetadataError(Exception):
    """Raised when a metadata value cannot be encoded as a ``MetadataValue``."""

class NodeNotFoundError(Exception):
    """Raised when a referenced node (asset/task) is missing from the graph."""

class PartitionDefinitionError(Exception):
    """Raised when a ``PartitionsDefinition`` is constructed with invalid arguments."""

class PartitionValidationError(Exception):
    """Raised when a ``PartitionKey`` is not valid for the asset's partitions def."""

class ResultDefinitionError(Exception):
    """Raised when an asset's ``Output`` / ``Observation`` is malformed."""

class ScheduleDefinitionError(Exception):
    """Raised when a ``Schedule`` is constructed with invalid arguments."""

class SensorDefinitionError(Exception):
    """Raised when a ``Sensor`` is constructed with invalid arguments."""

class StorageError(Exception):
    """Raised on storage backend failures (connection, transaction, serialization)."""

class TaskDefinitionError(Exception):
    """Raised when a ``Task`` / ``BashTask`` definition is invalid."""

class ArrowSchemaExportable(Protocol):
    """Anything implementing the Arrow C Data Interface ``__arrow_c_schema__`` method."""

    def __arrow_c_schema__(self) -> object: ...

class Schema:
    """An Arrow schema attached to an asset output for typed metadata."""

    def __init__(self, schema: ArrowSchemaExportable) -> None:
        """Wrap an Arrow-C-compatible schema source (e.g. ``pyarrow.Schema``)."""
        ...

    @property
    def names(self) -> list[str]:
        """Top-level field names in declaration order."""
        ...

    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...
    def __arrow_c_schema__(self) -> object: ...
    def to_ipc(self) -> bytes:
        """Serialize the schema as Arrow IPC bytes."""
        ...

    @staticmethod
    def from_ipc(data: bytes) -> Schema:
        """Deserialize an Arrow-IPC schema produced by :meth:`to_ipc`."""
        ...

class MappedOutput:
    """Handle to a fan-out :func:`InvokedNodeOutput.map` result.

    Pass to ``.collect()`` to wait for every mapped instance, or to
    ``.collect_stream()`` to consume them as they finish.
    """

    @property
    def node_name(self) -> str:
        """Name of the producer node whose output is being mapped."""
        ...

    @property
    def output_name(self) -> str:
        """Output identifier on the producer node."""
        ...

    def collect(self) -> "InvokedNodeOutput":
        """Wait for all mapped instances to finish; return a single aggregated output."""
        ...

    def collect_stream(self, *, ordered: bool = False) -> "InvokedNodeOutput":
        """Stream mapped instance results as they complete.

        Args:
            ordered: When ``True``, emit results in their original mapping-key order;
                otherwise emit in completion order.
        """
        ...

class InvokedNodeOutput:
    """Reference to one output of a node invocation inside a ``@Asset.from_graph``."""

    @property
    def node_name(self) -> str:
        """Name of the node whose output is referenced."""
        ...

    @property
    def output_name(self) -> str:
        """The specific output identifier on that node."""
        ...

    def map(self, task: "Task", *, max_concurrency: int | None = None) -> MappedOutput:
        """Fan ``task`` out across each value of this output.

        Args:
            task: The task to invoke once per element.
            max_concurrency: Maximum concurrent fanout instances (``None`` = unbounded).
        """
        ...

class Job:
    """A named bundle of assets/tasks executed together as one run."""

    def __init__(
        self,
        name: str,
        assets: Sequence[Union[SingleAsset, MultiAsset, GraphAsset, Task, BashTask]],
        executor: "Executor | None" = None,
        allow_incomplete_deps: bool = False,
    ) -> None:
        """Construct a job.

        Args:
            name: Unique job name within the repository.
            assets: Assets and tasks the job will materialize.
            executor: Override the repository default executor for this job.
            allow_incomplete_deps: Tolerate missing upstream deps (debug / partial
                graphs); production jobs should leave this ``False``.
        """
        ...

    def execute(
        self,
        partition_key: PartitionKey | None = None,
        tags: list[tuple[str, str]] | None = None,
        config: dict[str, dict[str, Any]] | None = None,
        raise_on_error: bool = True,
    ) -> "RunResult":
        """Run the job synchronously and return the run result.

        Args:
            partition_key: Partition to target (required for partitioned assets).
            tags: Run tags applied for queue / observability filtering;
                ``rivers/priority`` is honored for run-queue priority.
            config: Per-asset config, keyed by asset name.
            raise_on_error: Raise on first failure instead of returning a failed result.
        """
        ...

class MetadataValue:
    """A typed metadata value for asset outputs.

    Construct via the static factory methods (:meth:`text`, :meth:`url`, …) and
    pattern-match on the variant subclasses (:class:`MetadataValue.Text`,
    :class:`MetadataValue.Url`, …) when consuming.
    """

    # Variant types (for isinstance checks and attribute access)
    class Text(MetadataValue):
        """A free-form string value."""

        value: str

    class Int(MetadataValue):
        """An integer value."""

        value: int

    class Float(MetadataValue):
        """A floating-point value."""

        value: float

    class Bool(MetadataValue):
        """A boolean value."""

        value: bool

    class Url(MetadataValue):
        """A URL — rendered as a clickable link in the UI."""

        value: str

    class Path(MetadataValue):
        """A filesystem path."""

        value: str

    class Json(MetadataValue):
        """JSON-encoded payload (held as the source string)."""

        value: str

    class Markdown(MetadataValue):
        """Markdown source rendered in the UI as formatted text."""

        value: str

    class Timestamp(MetadataValue):
        """A POSIX timestamp in seconds."""

        value: float

    class Null(MetadataValue):
        """Explicit null / unset metadata value."""

    class Bytes(MetadataValue):
        """A byte count rendered with binary-prefixed units."""

        value: int

    class Duration(MetadataValue):
        """A duration in seconds rendered as a human-readable interval."""

        value: float

    class Sql(MetadataValue):
        """A SQL query (optionally tagged with its dialect for syntax highlighting)."""

        query: str
        dialect: str | None

    class CodeBlock(MetadataValue):
        """A snippet of source code (optionally tagged with a language)."""

        code: str
        language: str | None

    class Image(MetadataValue):
        """An image — value is a URL or a data URI."""

        value: str

    class Percentage(MetadataValue):
        """A percentage value in ``[0, 100]``."""

        value: float

    class List(MetadataValue):
        """A list of nested metadata values."""

        values: list[MetadataValue]

    class DateRange(MetadataValue):
        """A half-open ``[start, end)`` datetime range."""

        start: datetime.datetime
        end: datetime.datetime

    class Schema(MetadataValue):
        """An Arrow schema serialized as IPC bytes."""

        ipc_bytes: bytes

    class DataVersion(MetadataValue):
        """A content-addressable data version string."""

        value: str

    # Constructors
    @staticmethod
    def text(value: str) -> MetadataValue.Text: ...
    @staticmethod
    def int(value: int) -> MetadataValue.Int: ...
    @staticmethod
    def float_(value: float) -> MetadataValue.Float: ...
    @staticmethod
    def bool_(value: bool) -> MetadataValue.Bool: ...
    @staticmethod
    def url(value: str) -> MetadataValue.Url: ...
    @staticmethod
    def path(value: str) -> MetadataValue.Path: ...
    @staticmethod
    def json(value: str) -> MetadataValue.Json: ...
    @staticmethod
    def md(value: str) -> MetadataValue.Markdown: ...
    @staticmethod
    def timestamp(value: float) -> MetadataValue.Timestamp: ...
    @staticmethod
    def null() -> MetadataValue.Null: ...
    @staticmethod
    def bytes(value: int) -> MetadataValue.Bytes: ...
    @staticmethod
    def duration(value: float) -> MetadataValue.Duration: ...
    @staticmethod
    def sql(query: str, dialect: str | None = None) -> MetadataValue.Sql: ...
    @staticmethod
    def code_block(
        code: str, language: str | None = None
    ) -> MetadataValue.CodeBlock: ...
    @staticmethod
    def image(value: str) -> MetadataValue.Image: ...
    @staticmethod
    def percentage(value: float) -> MetadataValue.Percentage: ...
    @staticmethod
    def list_(values: list[MetadataValue]) -> MetadataValue.List: ...
    @staticmethod
    def date_range(
        start: datetime.datetime, end: datetime.datetime
    ) -> MetadataValue.DateRange: ...
    @staticmethod
    def schema(value: ArrowSchemaExportable) -> MetadataValue.Schema: ...
    @staticmethod
    def data_version(value: str) -> MetadataValue.DataVersion: ...
    def raw_value(
        self,
    ) -> (
        str
        | int
        | float
        | bool
        | None
        | list[MetadataValue]
        | tuple[datetime.datetime, datetime.datetime]
        | Schema
    ): ...

class OutputContext:
    """Per-output context passed to :meth:`BaseIOHandler.handle_output`."""

    asset_name: str
    asset_metadata: dict[str, str] | None
    partition: PartitionContext | None
    type_hint: type | None
    output_metadata: dict[str, MetadataValue] | None
    def __init__(
        self,
        asset_name: str,
        asset_metadata: dict[str, str] | None = None,
        partition: PartitionContext | None = None,
        type_hint: type | None = None,
    ) -> None:
        """Build an output context (typically only done by the executor / tests)."""
        ...

    def add_output_metadata(
        self, metadata: dict[str, str | int | float | bool | None | MetadataValue]
    ) -> None:
        """Attach metadata about the persisted output (size, path, duration, …)."""
        ...

    def register_data_version(self, version: str) -> None:
        """Record a content-addressable version string for this output."""
        ...

    def drain_data_version(self) -> str | None:
        """Pop and return the registered data version (one-shot, used by the executor)."""
        ...

class InputContext:
    """Per-input context passed to :meth:`BaseIOHandler.load_input`."""

    asset_name: str
    downstream_asset: str
    asset_metadata: dict[str, str] | None
    partition: PartitionContext | None
    type_hint: type | None
    def __init__(
        self,
        asset_name: str,
        downstream_asset: str,
        asset_metadata: dict[str, str] | None = None,
        partition: PartitionContext | None = None,
        type_hint: type | None = None,
    ) -> None:
        """Build an input context (typically only done by the executor / tests)."""
        ...

class IOHandler(Protocol):
    """Protocol describing the IO handler interface.

    At runtime, objects must be instances of ``rivers.BaseIOHandler``
    (which extends ``pydantic_settings.BaseSettings``).
    """

    def handle_output(self, context: OutputContext, obj: Any) -> None: ...
    def load_input(self, context: InputContext) -> Any: ...

class Output:
    """Per-asset materialization result.

    Return from an ``@Asset`` function to carry output metadata, data version,
    and tags alongside the materialized value.

    For multi-asset generator yields, set ``output_name`` to identify which output
    this value belongs to.
    """

    @property
    def value(self) -> Any: ...
    @property
    def output_name(self) -> str | None: ...
    @property
    def metadata(self) -> dict[str, Any] | None: ...
    @property
    def data_version(self) -> str | None: ...
    @property
    def tags(self) -> list[str] | None: ...
    def __init__(
        self,
        value: Any = None,
        *,
        output_name: str | None = None,
        metadata: dict[str, str | int | float | bool | None | MetadataValue]
        | None = None,
        data_version: str | None = None,
        tags: list[str] | None = None,
    ) -> None: ...

class Observation:
    """Per-asset observation result.

    Return from an external asset's observe function to carry observation
    metadata and data version.

    For multi-asset generator yields, set ``output_name`` to identify which output
    this observation belongs to.
    """

    @property
    def output_name(self) -> str | None: ...
    @property
    def metadata(self) -> dict[str, Any] | None: ...
    @property
    def data_version(self) -> str | None: ...
    def __init__(
        self,
        *,
        output_name: str | None = None,
        metadata: dict[str, str | int | float | bool | None | MetadataValue]
        | None = None,
        data_version: str | None = None,
    ) -> None: ...

class Materialization:
    """Per-asset materialization-only result.

    Return from an ``@Asset`` function when the asset has already persisted its
    own output (terminal side-effecting nodes, or assets that manage IO
    directly). The framework records a Materialization event with the
    supplied ``metadata`` and ``data_version`` but never invokes the IO
    handler — there is no value to round-trip through ``handle_output``.

    Use this **instead of** :class:`Output` when the asset writes directly to
    its destination (API push, message emit, external table write) and there
    is nothing meaningful to load back via ``load_input``.

    Downstream assets cannot ``load_input`` an output produced via
    ``Materialization`` — by design. Treat such assets as terminal in the graph.

    For multi-asset generator yields, set ``output_name`` to identify which
    output this materialization belongs to.

    Example::

        @rs.Asset
        def push_to_api(rows: list[dict]) -> rs.Materialization:
            response = requests.post(API_URL, json=rows)
            return rs.Materialization(
                metadata={"status_code": rs.MetadataValue.int(response.status_code)},
                data_version=response.headers["ETag"],
            )
    """

    @property
    def output_name(self) -> str | None: ...
    @property
    def metadata(self) -> dict[str, Any] | None: ...
    @property
    def data_version(self) -> str | None: ...
    @property
    def tags(self) -> list[str] | None: ...
    def __init__(
        self,
        *,
        output_name: str | None = None,
        metadata: dict[str, str | int | float | bool | None | MetadataValue]
        | None = None,
        data_version: str | None = None,
        tags: list[str] | None = None,
    ) -> None: ...

class DynamicOutput:
    """A value with an explicit mapping key for dynamic fan-out.

    When a producer asset returns a list of ``DynamicOutput``, the executor
    uses ``.key`` as the mapping key (instance name) instead of a numeric index.
    """

    @property
    def key(self) -> str: ...
    @property
    def value(self) -> Any: ...
    def __init__(self, key: str, value: Any) -> None: ...

def install_signal_handler() -> None:
    """Install Rust-side SIGTERM/SIGINT handler for graceful two-phase shutdown."""
    ...

def wait_for_exit() -> None:
    """Block until graceful shutdown completes (drain → shutdown → exit)."""
    ...

def drain_in_flight() -> None:
    """Join in-flight worker threads before finalize (an :mod:`atexit` hook)."""
    ...

def runtime_info() -> dict[str, int]:
    """Return ``{"main_workers": N, "io_workers": M}`` for the two tokio runtimes.

    Calling this eagerly initialises both runtimes if they have not been
    built yet, which also fires their ``"tokio runtime initialised"`` info
    logs. Useful for printing worker counts at test session start.
    """
    ...

__all__ = [
    "AutomationDaemon",
    "Asset",
    "AssetExecutionContext",
    "BashTask",
    "InputContext",
    "InvokedNodeOutput",
    "Job",
    "Materialization",
    "MetadataValue",
    "DynamicOutput",
    "Observation",
    "Output",
    "OutputContext",
    "Schema",
    "PartitionContext",
    "PartitionKey",
    "PartitionMapping",
    "PartitionsDefinition",
    "SelfDependency",
    "Task",
    "TaskExecutionContext",
]
