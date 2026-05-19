"""``@Asset`` decorator family and the asset / dep / context types."""

import datetime
import logging
from typing import Any, Callable, Generic, List, Optional, Type, TypeVar, overload

from rivers._core import IOHandler, MetadataValue
from rivers._core.automation import AutomationCondition
from rivers._core.hooks import Hook
from rivers._core.partitions import (
    BackfillStrategy,
    PartitionContext,
    PartitionKey,
    PartitionMapping,
    PartitionsDefinition,
)

ConfigT = TypeVar("ConfigT")

_T = TypeVar("_T", bound="Asset")
_SELF = TypeVar("_SELF")

class Asset:
    """Decorator and namespace for declaring assets.

    Use ``@Asset`` (bare) or ``@Asset(...)`` (with options) on a function for a
    :class:`SingleAsset`. Use :meth:`Asset.from_multi`, :meth:`Asset.from_graph`,
    or :meth:`Asset.external` for the multi-asset, graph-asset, and external-asset
    variants. The returned object is a callable producing the asset's value
    (and is also the registered :class:`Asset` for the repository).
    """

    # used as @Asset (no parentheses)
    @overload
    def __new__(cls, f: Callable[..., Any]) -> "SingleAsset": ...

    # used as @Asset(...) (with parentheses)
    @overload
    def __new__(
        cls,
        *,
        name: str | None = ...,
        tags: list[str] | None = ...,
        kinds: str | list[str] | None = ...,
        group: str | None = ...,
        code_version: str | None = ...,
        io_handler: IOHandler | str | None = ...,
        metadata: dict[str, str] | None = ...,
        partitions_def: PartitionsDefinition | None = ...,
        deps: list["DepDef"] | None = ...,
        hooks: list[Hook] | None = ...,
        automation_condition: AutomationCondition | None = ...,
        backfill_strategy: BackfillStrategy | None = ...,
        pool: str | list[str] | None = ...,
        pool_slots: int | dict[str, int] | None = ...,
    ) -> Callable[[Callable[..., Any]], "SingleAsset"]: ...
    # @overload
    def __new__(
        cls,
        *,
        name: str | None = None,
        tags: list[str] | None = None,
        kinds: str | list[str] | None = None,
        group: str | None = None,
        code_version: str | None = None,
        io_handler: IOHandler | str | None = None,
        metadata: dict[str, str] | None = None,
        partitions_def: PartitionsDefinition | None = None,
        deps: list["DepDef"] | None = None,
        hooks: list[Hook] | None = None,
        automation_condition: AutomationCondition | None = None,
        backfill_strategy: BackfillStrategy | None = None,
        pool: str | list[str] | None = None,
        pool_slots: int | dict[str, int] | None = None,
    ) -> Callable[[Callable[..., Any]], "SingleAsset"]: ...

    # from_multi: used as decorator (no wraps) or direct call (with wraps)
    @classmethod
    @overload
    def from_multi(
        cls: Type[_T],
        wraps: Callable[..., Any],
        output_defs: list["AssetDef"] = ...,
        name: str | None = None,
        tags: list[str] | None = None,
        kinds: str | list[str] | None = None,
        group: str | None = None,
        code_version: str | None = None,
        io_handler: IOHandler | str | None = None,
        partitions_def: PartitionsDefinition | None = None,
        deps: list["DepDef"] = ...,
        hooks: list[Hook] | None = None,
        automation_condition: AutomationCondition | None = None,
    ) -> "MultiAsset": ...
    @classmethod
    @overload
    def from_multi(
        cls: Type[_T],
        wraps: None = None,
        output_defs: list["AssetDef"] = ...,
        name: str | None = None,
        tags: list[str] | None = None,
        kinds: str | list[str] | None = None,
        group: str | None = None,
        code_version: str | None = None,
        io_handler: IOHandler | str | None = None,
        partitions_def: PartitionsDefinition | None = None,
        deps: list["DepDef"] = ...,
        hooks: list[Hook] | None = None,
        automation_condition: AutomationCondition | None = None,
    ) -> Callable[[Callable[..., Any]], "MultiAsset"]: ...

    # from_graph: used as decorator (no wraps) or direct call (with wraps)
    @classmethod
    @overload
    def from_graph(
        cls: Type[_T],
        wraps: Callable[..., Any],
        name: str | None = None,
        tags: list[str] | None = None,
        kinds: str | list[str] | None = None,
        group: str | None = None,
        code_version: str | None = None,
        io_handler: IOHandler | str | None = None,
        node_io_handler: IOHandler | str | None = None,
        metadata: dict[str, str] | None = None,
        partitions_def: PartitionsDefinition | None = None,
        deps: list["DepDef"] | None = None,
        hooks: list[Hook] | None = None,
        automation_condition: AutomationCondition | None = None,
    ) -> "GraphAsset": ...
    @classmethod
    @overload
    def from_graph(
        cls: Type[_T],
        wraps: None = None,
        name: str | None = None,
        tags: list[str] | None = None,
        kinds: str | list[str] | None = None,
        group: str | None = None,
        code_version: str | None = None,
        io_handler: IOHandler | str | None = None,
        node_io_handler: IOHandler | str | None = None,
        metadata: dict[str, str] | None = None,
        partitions_def: PartitionsDefinition | None = None,
        deps: list["DepDef"] | None = None,
        hooks: list[Hook] | None = None,
        automation_condition: AutomationCondition | None = None,
    ) -> Callable[[Callable[..., Any]], "GraphAsset"]: ...

    # external: used as decorator (no wraps) or direct call (with wraps)
    @classmethod
    @overload
    def external(
        cls: Type[_T],
        wraps: Callable[..., Any],
        *,
        name: str | None = None,
        io_handler: IOHandler | str,
        tags: list[str] | None = None,
        kinds: str | list[str] | None = None,
        group: str | None = None,
        metadata: dict[str, str] | None = None,
        partitions_def: PartitionsDefinition | None = None,
        automation_condition: AutomationCondition | None = None,
    ) -> "ExternalAsset": ...
    @classmethod
    @overload
    def external(
        cls: Type[_T],
        wraps: None = None,
        *,
        name: str | None = None,
        io_handler: IOHandler | str,
        tags: list[str] | None = None,
        kinds: str | list[str] | None = None,
        group: str | None = None,
        metadata: dict[str, str] | None = None,
        partitions_def: PartitionsDefinition | None = None,
        automation_condition: AutomationCondition | None = None,
    ) -> "ExternalAsset": ...
    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Invoke the asset's user function directly (used in tests / graphs)."""
        ...

    def _asset_fn(self) -> Callable:
        """(Internal) return the wrapped user function for the executor."""
        ...

    @property
    def _name(self) -> Optional[str]:
        """Asset name as configured by the user (without defaulting to ``__name__``)."""
        ...

    @property
    def name(self) -> str:
        """Resolved asset name."""
        ...

    @property
    def tags(self) -> list[str] | None:
        """Asset-level tags."""
        ...

    @property
    def kinds(self) -> list[str]:
        """Asset kinds (compute kind / type tags)."""
        ...

    @property
    def group(self) -> str | None:
        """Group the asset belongs to (UI grouping, ``None`` if ungrouped)."""
        ...

    @property
    def metadata(self) -> dict[str, str] | None:
        """Static asset metadata configured at definition time."""
        ...

    @property
    def code_version(self) -> str | None:
        """Code version string used to detect stale materializations."""
        ...

    @property
    def partitions_def(self) -> PartitionsDefinition | None:
        """Partitions definition, or ``None`` for non-partitioned assets."""
        ...

    @property
    def observe_fn(self) -> Callable | None:
        """The observe callable for external assets (else ``None``)."""
        ...

    @property
    def is_async(self) -> bool:
        """``True`` when the underlying user function is a coroutine function."""
        ...

    @property
    def is_single(self) -> bool:
        """``True`` for :class:`SingleAsset` instances."""
        ...

    @property
    def is_multi(self) -> bool:
        """``True`` for :class:`MultiAsset` instances."""
        ...

    @property
    def is_graph(self) -> bool:
        """``True`` for :class:`GraphAsset` instances."""
        ...

    @property
    def is_external(self) -> bool:
        """``True`` for :class:`ExternalAsset` instances."""
        ...

    @property
    def hooks(self) -> list[Hook] | None:
        """Hooks attached to this asset."""
        ...

    @property
    def automation_condition(self) -> AutomationCondition | None:
        """Automation condition driving auto-materialization, if any."""
        ...

    @property
    def partition_mapping(self) -> dict[str, PartitionMapping] | None:
        """Per-dependency partition-mapping overrides keyed by dep name."""
        ...

    @property
    def pool(self) -> list[tuple[str, int]]:
        """Concurrency pools and slot counts this asset claims when materializing."""
        ...

class SingleAsset(Asset):
    """An asset producing a single output (``@Asset`` of a function)."""

class MultiAsset(Asset):
    """An asset whose function emits multiple typed outputs.

    Construct via :meth:`Asset.from_multi`. Output definitions are accessible
    via :attr:`output_defs`.
    """

    @property
    def output_defs(self) -> List[AssetDef]:
        """Per-output asset definitions declared on this multi-asset."""
        ...

class GraphAsset(Asset):
    """An asset whose value is computed by composing tasks (``@Asset.from_graph``)."""

class ExternalAsset(Asset):
    """An asset materialized outside rivers whose state we observe / track.

    The decorated function is the optional ``observe`` callable that returns
    metadata / data version for the externally-produced value.
    """

    def __call__(self, f: Callable[..., Any]) -> "ExternalAsset":
        """Decorator form — attach the observe callable to this external asset."""
        ...

class AssetDef:
    """Output / dep definition used inside :meth:`Asset.from_multi` and :class:`DepDef`."""

    name: str
    tags: list[str] | None
    kinds: list[str]
    group: str | None
    code_version: str | None
    io_handler: IOHandler | str | None
    metadata: dict[str, str] | None
    partitions_def: PartitionsDefinition | None
    partition_mapping: dict[str | AssetDef, PartitionMapping] | None
    pool: list[tuple[str, int]]

    def __init__(
        self,
        name: str,
        tags: list[str] | None = ...,
        kinds: str | list[str] | None = ...,
        group: str | None = ...,
        code_version: str | None = ...,
        io_handler: IOHandler | str | None = ...,
        metadata: dict[str, str] | None = ...,
        partitions_def: PartitionsDefinition | None = ...,
        partition_mapping: dict[str | AssetDef, PartitionMapping] | None = ...,
        pool: str | list[str] | None = ...,
        pool_slots: int | dict[str, int] | None = ...,
        deps: list["DepDef"] = ...,
    ) -> None:
        """Build an asset definition shared between multi-asset outputs and deps."""
        ...

    @property
    def deps(self) -> list["DepDef"]:
        """Per-output dependencies declared on this output.

        When used inside :meth:`Asset.from_multi`, input deps (from
        :meth:`AssetDef.input`) merge into the multi-asset's function-level
        input set, and lineage-only deps (:meth:`AssetDef.dep`) become edges
        to this specific output.
        """
        ...

    @staticmethod
    def input(
        name: str,
        partition_mapping: PartitionMapping | None = None,
        io_handler: IOHandler | str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> "DepDef":
        """Declare an input — a dep whose value is loaded into the asset function."""
        ...

    @staticmethod
    def dep(
        name: str,
        partition_mapping: PartitionMapping | None = None,
    ) -> "DepDef":
        """Declare a non-loaded dep — establishes ordering only, no value injected."""
        ...

class DepDef:
    """One upstream dependency edge — see :meth:`AssetDef.input` / :meth:`AssetDef.dep`."""

    name: str
    """Upstream asset name."""
    partition_mapping: PartitionMapping | None
    """Optional override of how the downstream partition maps upstream."""
    metadata: dict[str, str] | None
    """Per-edge metadata forwarded to the IO handler."""
    is_input: bool
    """``True`` for :meth:`AssetDef.input`, ``False`` for :meth:`AssetDef.dep`."""

class SelfDependency(Generic[_SELF]):
    """Marker for an asset depending on its own previous partition."""

    def get_inner(self) -> _SELF | None:
        """Return the loaded prior value, or ``None`` for the first partition."""
        ...

class AssetExecutionContext(Generic[ConfigT]):
    """Context object injected into asset functions that declare a ``context`` parameter."""

    asset_name: str
    """Name of the asset being materialized."""
    tags: list[str] | None
    """Asset-level tags."""
    kinds: list[str]
    """Asset kinds."""
    group: str | None
    """Asset group, if any."""
    code_version: str | None
    """Asset code version."""
    is_multi_asset: bool
    """``True`` when this context belongs to a :class:`MultiAsset`."""
    output_selection: list[str]
    """For multi-assets, the subset of outputs being materialized."""
    asset_metadata: dict[str, str] | None
    """Static asset metadata."""
    partition: PartitionContext | None
    """Partition currently being materialized."""
    def mark_partition_failed(self, partition_key: PartitionKey, error: str) -> None:
        """Record that a specific partition failed inside a multi-partition step."""
        ...

    def __init__(
        self,
        asset_name: str,
        tags: list[str] | None = None,
        kinds: list[str] | None = None,
        group: str | None = None,
        code_version: str | None = None,
        asset_metadata: dict[str, str] | None = None,
        partition: PartitionContext | None = None,
        is_multi_asset: bool = False,
        output_selection: list[str] | None = None,
        config: ConfigT | None = None,
    ) -> None:
        """Construct a context (typically only the executor calls this)."""
        ...

    @property
    def has_partition_key(self) -> bool:
        """``True`` when the asset is being materialized for a specific partition."""
        ...

    @property
    def partition_key(self) -> str:
        """String form of the current partition key (raises when not partitioned)."""
        ...

    @property
    def partition_time_window(
        self,
    ) -> tuple[datetime.datetime, datetime.datetime] | None:
        """Time window for time-windowed partitions, else ``None``."""
        ...

    @property
    def log(self) -> logging.Logger:
        """Logger that ships records back through the run event log."""
        ...

    def add_output_metadata(
        self,
        metadata: dict[
            str, str | int | float | bool | None | MetadataValue
        ],  # TODO: | MetadataValue
    ) -> None:
        """Attach metadata about the materialized output (size, path, schema, ...)."""
        ...

    def register_data_version(self, version: str) -> None:
        """Record a content-addressable version string for this output."""
        ...

    @property
    def output_metadata(
        self,
    ) -> dict[str, Any] | None:  # TODO: dict[str, MetadataValue]
        """Metadata accumulated via :meth:`add_output_metadata` so far."""
        ...

    def drain_data_version(self) -> str | None:
        """Pop and return the registered data version (one-shot, used by the executor)."""
        ...

    @property
    def config(self) -> ConfigT:
        """Resolved asset config (typed via the generic parameter)."""
        ...
