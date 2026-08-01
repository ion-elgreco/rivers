"""rivers — Rust-powered asset orchestration with a Python API.

Re-exports the public surface from the Rust ``_core`` extension and the pure-Python
helpers (resources, IO handlers, exceptions). Everything listed in :data:`__all__`
is meant to be imported from the top-level ``rivers`` namespace; submodules are
implementation detail.

Example::

    import rivers as rs

    @rs.Asset
    def my_data() -> int:
        return 42

    repo = rs.CodeRepository(assets=[my_data])
    repo.materialize()
"""

from rivers import exceptions
from rivers._core import (
    Backoff,
    Compute,
    ComputeEscalation,
    DynamicOutput,
    FailureReason,
    InputContext,
    InvokedNodeOutput,
    Job,
    Materialization,
    MetadataValue,
    Observation,
    Output,
    OutputContext,
    RetryOn,
    RetryPolicy,
    RunBackendConfig,
    RunQueueConfig,
    Schema,
    TagConcurrencyLimit,
)
from rivers._core.assets import (
    ActionConcurrency,
    ActionContext,
    ActionOrdering,
    ActionPartitioning,
    ActionResult,
    Asset,
    AssetAction,
    AssetDef,
    AssetExecutionContext,
    DepDef,
    ExternalAsset,
    GraphAsset,
    MultiAsset,
    Outcome,
    SelfDependency,
    SingleAsset,
    action,
)
from rivers._core.automation import AutomationCondition
from rivers._core.executor import Executor
from rivers._core.hooks import Hook, HookContext
from rivers._core.partitions import (
    BackfillStrategy,
    PartitionContext,
    PartitionKey,
    PartitionKeyRange,
    PartitionMapping,
    PartitionsDefinition,
)
from rivers._core.repo import (
    BackfillResult,
    BackfillStatus,
    CodeRepository,
    RunHandle,
    RunResult,
)
from rivers._core.schedule import (
    BackfillRequest,
    EvalMode,
    RunRequest,
    Schedule,
    ScheduleEvaluationContext,
    ScheduleStatus,
    ScheduleTickResult,
    SkipReason,
)
from rivers._core.sensor import (
    Sensor,
    SensorEvaluationContext,
    SensorResult,
    SensorStatus,
    SensorTickResult,
)
from rivers._core.storage import (
    AssetRecord,
    LaunchedBy,
    RunRecord,
    StaleCause,
    Storage,
    StorageType,
    StoredEvent,
    StoredTick,
)
from rivers._core.tasks import BashTask, Task, TaskExecutionContext
from rivers.io_handlers import BaseIOHandler, InMemoryIOHandler, PickleIOHandler
from rivers.resource import Resource

__all__ = [
    "exceptions",
    "action",
    "ActionConcurrency",
    "ActionContext",
    "ActionOrdering",
    "ActionPartitioning",
    "ActionResult",
    "AssetAction",
    "Outcome",
    "AssetDef",
    "Asset",
    "AssetExecutionContext",
    "DepDef",
    "BashTask",
    "ExternalAsset",
    "SingleAsset",
    "MultiAsset",
    "GraphAsset",
    "BackfillResult",
    "BackfillStatus",
    "BackfillStrategy",
    "CodeRepository",
    "RunHandle",
    "RunResult",
    "Output",
    "Observation",
    "Materialization",
    "DynamicOutput",
    "Resource",
    "Task",
    "InvokedNodeOutput",
    "Job",
    "Executor",
    "Backoff",
    "Compute",
    "ComputeEscalation",
    "FailureReason",
    "RetryOn",
    "RetryPolicy",
    "Hook",
    "HookContext",
    "OutputContext",
    "InputContext",
    "MetadataValue",
    "Schema",
    "PartitionKey",
    "PartitionKeyRange",
    "PartitionsDefinition",
    "PartitionContext",
    "PartitionMapping",
    "AssetRecord",
    "BaseIOHandler",
    "InMemoryIOHandler",
    "PickleIOHandler",
    "LaunchedBy",
    "RunRecord",
    "RunBackendConfig",
    "RunQueueConfig",
    "SelfDependency",
    "TagConcurrencyLimit",
    "TaskExecutionContext",
    "Storage",
    "StorageType",
    "StoredEvent",
    "StaleCause",
    "StoredTick",
    "BackfillRequest",
    "RunRequest",
    "Schedule",
    "ScheduleEvaluationContext",
    "ScheduleStatus",
    "ScheduleTickResult",
    "SkipReason",
    "Sensor",
    "SensorEvaluationContext",
    "SensorResult",
    "SensorStatus",
    "SensorTickResult",
    "AutomationCondition",
    "EvalMode",
]

try:
    from rivers.io_handlers.delta import DeltaAsset, DeltaIOHandler

    __all__ = [*__all__, "DeltaAsset", "DeltaIOHandler"]
except ImportError as e:
    _delta_import_error = e

    def __getattr__(name: str):
        """Point at the missing extra instead of a bare "cannot import name".

        Raises:
            ImportError: For the Delta names, naming the extra to install.
            AttributeError: For everything else, as usual.
        """
        if name in ("DeltaAsset", "DeltaIOHandler"):
            raise ImportError(
                f"{name} requires the Delta extras — install "
                f"'rivers[delta-pyarrow]' ({_delta_import_error})"
            ) from _delta_import_error
        raise AttributeError(f"module 'rivers' has no attribute '{name}'")
