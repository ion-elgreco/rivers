"""Partition keys, definitions, ranges, and dep mappings."""

import datetime
from collections.abc import Mapping

class PartitionKey:
    """A specific key within an asset's :class:`PartitionsDefinition`.

    Use :meth:`single` for one-dimensional partitions and :meth:`multi` for
    multi-dimensional ones. The two variants are exposed as nested classes for
    pattern matching / ``isinstance`` checks.
    """

    class Single(PartitionKey):
        """One-dimensional key (e.g. a date string, a customer id)."""

        key: list[str]

    class Multi(PartitionKey):
        """Multi-dimensional key — a mapping of dimension name to value list."""

        keys: dict[str, list[str]]

    class Set(PartitionKey):
        """Explicit key set bundling a sparse backfill group — internal /
        transport form, returned by :meth:`from_json`."""

        keys: list[PartitionKey]

    @staticmethod
    def single(key: str | list[str]) -> PartitionKey.Single:
        """Build a single-dimension key from a value or list of values."""
        ...

    @staticmethod
    def multi(keys: Mapping[str, str | list[str]]) -> PartitionKey.Multi:
        """Build a multi-dimension key from a ``{dimension: value(s)}`` mapping."""
        ...

    def to_json(self) -> str:
        """Serialize the key as a JSON string suitable for CLI / RPC transport."""
        ...

    @staticmethod
    def from_json(s: str) -> PartitionKey:
        """Parse a key produced by :meth:`to_json`."""
        ...

    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...

class BackfillStrategy:
    """How a backfill expands its partition range into runs."""

    class MultiRun(BackfillStrategy):
        """One run per partition key — maximum parallelism."""

    class SingleRun(BackfillStrategy):
        """All partition keys handled inside a single run."""

    class PerDimension(BackfillStrategy):
        """Different strategies per dimension of a multi-partitioned asset."""

        multi_run_dims: list[str]
        single_run_dims: list[str]

    @staticmethod
    def multi_run() -> BackfillStrategy.MultiRun:
        """Build the all-multi-run strategy."""
        ...

    @staticmethod
    def single_run() -> BackfillStrategy.SingleRun:
        """Build the all-single-run strategy."""
        ...

    @staticmethod
    def per_dimension(
        multi_run: list[str], single_run: list[str]
    ) -> BackfillStrategy.PerDimension:
        """Mix strategies per dimension.

        Args:
            multi_run: Dimension names handled with one run per value.
            single_run: Dimension names collapsed into a single run.
        """
        ...

    def __eq__(self, other: object) -> bool: ...

class PartitionKeyRange:
    """An inclusive range of partition keys for backfills / lookups."""

    @staticmethod
    def single(from_key: str, to_key: str) -> PartitionKeyRange:
        """Single-dimension range from ``from_key`` to ``to_key`` (inclusive),
        ordered by the partition definition at resolve time."""
        ...

    @staticmethod
    def multi(
        dimensions: dict[str, tuple[str, str] | list[str]],
    ) -> PartitionKeyRange:
        """Multi-dimension range — each dimension is either a ``(from, to)`` tuple
        or an explicit list of keys."""
        ...

    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...

class PartitionsDefinition:
    """How an asset is split into independently-materializable partitions."""

    class Static(PartitionsDefinition):
        """A fixed list of string keys."""

        keys: list[str]

    class TimeWindow(PartitionsDefinition):
        """Partitions aligned to a cron schedule or fixed interval."""

        cron_schedule: str | None
        interval_seconds: float | None
        start: datetime.datetime
        end: datetime.datetime | None
        fmt: str

    class Multi(PartitionsDefinition):
        """Cartesian product of named child definitions."""

        dimensions: list[tuple[str, PartitionsDefinition]]

    class Dynamic(PartitionsDefinition):
        """Runtime-extensible partitions; values stored in :class:`Storage`."""

        name: str

    @staticmethod
    def static_(keys: list[str]) -> PartitionsDefinition.Static:
        """Define a static partitions set from a list of keys.

        Raises:
            PartitionDefinitionError: If ``keys`` is empty, or a key contains
                a character reserved by the canonical display form
                (``|`` or ``,``).
        """
        ...

    @staticmethod
    def daily(
        start: datetime.datetime,
        end: datetime.datetime | None = None,
        fmt: str | None = None,
    ) -> PartitionsDefinition.TimeWindow:
        """Daily time-window partitions starting at ``start``."""
        ...

    @staticmethod
    def hourly(
        start: datetime.datetime,
        end: datetime.datetime | None = None,
        fmt: str | None = None,
    ) -> PartitionsDefinition.TimeWindow:
        """Hourly time-window partitions starting at ``start``."""
        ...

    @staticmethod
    def time_window(
        start: datetime.datetime,
        cron_schedule: str | None = None,
        interval_seconds: float | None = None,
        end: datetime.datetime | None = None,
        fmt: str | None = None,
    ) -> PartitionsDefinition.TimeWindow:
        """Custom time-window partitions defined by a cron schedule or interval.

        Provide exactly one of ``cron_schedule`` or ``interval_seconds``.

        Raises:
            PartitionDefinitionError: If ``interval_seconds`` is not positive
                or is below one nanosecond, or if ``fmt`` renders keys
                containing a character reserved by the canonical display
                form (``|`` or ``,``).
        """
        ...

    @staticmethod
    def multi(
        dimensions: dict[str, PartitionsDefinition],
    ) -> PartitionsDefinition.Multi:
        """Combine multiple definitions into a multi-dimensional partition space.

        Raises:
            PartitionDefinitionError: If ``dimensions`` is empty, a dimension
                is itself Multi, or a dimension name is empty or contains a
                character reserved by the canonical display form
                (``|``, ``,`` or ``=``).
        """
        ...

    @staticmethod
    def dynamic(name: str) -> PartitionsDefinition.Dynamic:
        """Create a named dynamic partitions def whose keys are added at runtime."""
        ...

    def get_partition_keys(self) -> list[PartitionKey]:
        """Enumerate all currently-known keys for this definition."""
        ...

    def validate_partition_key(self, key: PartitionKey) -> bool:
        """Return ``True`` if ``key`` is valid for this definition."""
        ...

class PartitionContext:
    """The partition(s) being materialized in the current step."""

    keys: list[PartitionKey]
    """All partition keys this step is responsible for."""
    key: PartitionKey
    """Convenience getter for ``keys[0]`` — the canonical/first key."""
    definition: PartitionsDefinition
    """The definition the keys belong to."""
    def __init__(
        self, keys: list[PartitionKey], definition: PartitionsDefinition
    ) -> None:
        """Construct a context (typically only the executor calls this).

        Raises:
            ValueError: If ``keys`` is empty.
        """
        ...

    def time_window(self) -> tuple[datetime.datetime, datetime.datetime] | None:
        """Return the half-open ``(start, end)`` time window for time-windowed keys, else ``None``."""
        ...

class PartitionMapping:
    """Maps a downstream partition key to the upstream partition keys it depends on."""

    class Identity(PartitionMapping):
        """Same key on both sides (the default for matched partition definitions)."""

    class AllPartitions(PartitionMapping):
        """Every downstream key depends on every upstream key."""

    class Static(PartitionMapping):
        """Fixed downstream-key → upstream-key mapping."""

        mapping: dict[str, str]

    class TimeWindow(PartitionMapping):
        """Offset upstream/downstream by ``offset`` time-window steps."""

        offset: int

    class Multi(PartitionMapping):
        """Per-dimension mapping for multi-partitioned assets."""

        dimension_mappings: dict[str, tuple[str, PartitionMapping]]

    class MultiToSingle(PartitionMapping):
        """Project a multi-dim partition onto a single dimension when crossing assets."""

        dimension_name: str

    class SpecificPartitions(PartitionMapping):
        """Always depend on a fixed set of upstream keys."""

        partition_keys: list[str]

    class ForKeys(PartitionMapping):
        """Depend on the upstream keys listed in ``selectors`` (keys or ranges)."""

        selectors: list[PartitionKey | PartitionKeyRange]

    class Subset(PartitionMapping):
        """Pass-through mapping: depend on the subset of upstream keys that exist."""

    @staticmethod
    def identity() -> PartitionMapping.Identity:
        """Build an identity mapping."""
        ...

    @staticmethod
    def all_partitions() -> PartitionMapping.AllPartitions:
        """Build an "all upstream partitions" mapping."""
        ...

    @staticmethod
    def static_(mapping: dict[str, str]) -> PartitionMapping.Static:
        """Build a fixed mapping from downstream → upstream key strings."""
        ...

    @staticmethod
    def time_window(offset: int) -> PartitionMapping.TimeWindow:
        """Offset the downstream by ``offset`` upstream windows (negative = lag);
        shifts outside the upstream's ``[start, end)`` range fail the run."""
        ...

    @staticmethod
    def multi(
        dimension_mappings: dict[str, PartitionMapping | tuple[str, PartitionMapping]],
    ) -> PartitionMapping.Multi:
        """Combine per-dimension mappings into a single multi mapping."""
        ...

    @staticmethod
    def multi_to_single(
        dimension_name: str,
        partition_mapping: PartitionMapping | None = None,
    ) -> PartitionMapping.MultiToSingle:
        """Project a multi-dim partition onto the named dimension before mapping upstream."""
        ...

    @staticmethod
    def specific_partitions(
        partition_keys: list[str],
    ) -> PartitionMapping.SpecificPartitions:
        """Always depend on the given fixed set of upstream keys."""
        ...

    @staticmethod
    def for_keys(
        selectors: list[PartitionKey | PartitionKeyRange],
    ) -> PartitionMapping.ForKeys:
        """Depend on a heterogeneous list of upstream keys / ranges."""
        ...

    @staticmethod
    def subset() -> PartitionMapping.Subset:
        """Build a subset mapping (depend only on existing upstream keys)."""
        ...
