"""Storage backend types — events, runs, assets, ticks, and concurrency pools."""

import enum
from collections.abc import Coroutine
from typing import Any

from rivers._core.partitions import PartitionKey

class StorageType(enum.IntEnum):
    """Backend variant for a :class:`Storage` instance."""

    Memory = ...
    """Pure in-memory store; data is lost on shutdown."""
    Embedded = ...
    """SurrealDB+RocksDB embedded in the process — single-process access only."""
    Remote = ...
    """SurrealDB server / TiKV — supports multi-process access."""

class StoredEvent:
    """One persisted run-event record (materialization, observation, error, ...)."""

    id: str
    event_type: str
    asset_key: str | None
    run_id: str
    partition_key: "PartitionKey | None"
    timestamp: int
    metadata: list[tuple[str, str]]
    data_version: str | None
    code_version: str | None
    input_data_versions: list[tuple[str, str]]

class StaleCause:
    """One reason an asset is considered stale relative to its deps / code version."""

    asset_key: str
    category: str
    reason: str
    dependency: str | None

class AssetRecord:
    """Materialization-state snapshot for one asset key."""

    asset_key: str
    """The asset's name."""
    tags: list[str]
    """Tags configured on the asset."""
    kinds: list[str]
    """Asset kinds (compute kind / type tags)."""
    group: str | None
    """Group the asset belongs to (UI grouping)."""
    code_version: str | None
    """Currently-declared code version on the asset definition."""
    last_event_id: str | None
    """ID of the most recent materialization event (``None`` if never materialized)."""
    last_run_id: str | None
    """Run ID that produced the most recent materialization."""
    last_timestamp: int | None
    """Nanosecond timestamp of the most recent materialization."""
    last_data_version: str | None
    """Data version recorded by the most recent materialization."""
    last_materialization_code_version: str | None
    """Code version that was active during the most recent materialization."""
    last_input_data_versions: list[tuple[str, str]]
    """``(upstream_asset, data_version)`` pairs observed at the most recent materialization."""
    pool: list[tuple[str, int]]
    """Concurrency pools and slot counts the asset claims when materializing."""

class StoredTick:
    """One persisted schedule / sensor / automation-condition tick."""

    id: str
    automation_name: str
    automation_type: str
    status: str
    timestamp: int
    run_ids: list[str]
    backfill_ids: list[str]
    skip_reason: str | None
    error: str | None
    cursor: str | None

class LaunchedBy:
    """Origin of a run. Construct via the classmethod factories; discriminate on `.kind`."""

    @classmethod
    def manual(cls) -> "LaunchedBy":
        """Run was launched manually (CLI / API call)."""
        ...

    @classmethod
    def schedule(cls, name: str) -> "LaunchedBy":
        """Run was launched by the schedule named ``name``."""
        ...

    @classmethod
    def sensor(cls, name: str) -> "LaunchedBy":
        """Run was launched by the sensor named ``name``."""
        ...

    @classmethod
    def backfill(cls, backfill_id: str) -> "LaunchedBy":
        """Run was launched as part of backfill ``backfill_id``."""
        ...

    @classmethod
    def condition(cls) -> "LaunchedBy":
        """Run was launched by an automation condition."""
        ...
    @property
    def kind(self) -> str:
        """One of "manual", "schedule", "sensor", "backfill", "condition"."""

    @property
    def name(self) -> str | None:
        """Schedule or sensor name; None for other variants."""

    @property
    def backfill_id(self) -> str | None:
        """Backfill id; None for other variants."""

    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...
    def __repr__(self) -> str: ...

class RunRecord:
    """Persisted record describing one run."""

    run_id: str
    job_name: str | None
    """Name of the user-defined ``Job`` this run targets. ``None`` for ad-hoc
    runs (``repo.materialize()``, sensors that drive an asset selection)."""
    status: str
    start_time: int
    end_time: int | None
    tags: list[tuple[str, str]]
    node_names: list[str]
    priority: int
    partition_key: "PartitionKey | None"
    block_reason: str | None
    launched_by: LaunchedBy
    """Origin of this run (manual / schedule / sensor / backfill / condition)."""

class PoolLimit:
    """Configuration of a concurrency pool."""

    pool_key: str
    slot_limit: int
    lease_duration_secs: int

class PoolInfo:
    """Live pool stats — limit + claimed/pending counters."""

    pool_key: str
    slot_limit: int
    lease_duration_secs: int
    claimed_count: int
    pending_count: int

class SlotHolder:
    """A run/step currently holding pool slots."""

    run_id: str
    step_key: str
    slots_consumed: int
    claimed_at: int
    lease_expires_at: int

class PoolBlockDetail:
    """Per-pool block detail — used inside :class:`BlockReason`."""

    pool_key: str
    claimed: int
    limit: int

class BlockReason:
    """Why a step couldn't claim its pool slots immediately."""

    kind: str
    pool_key: str
    claimed: int
    limit: int
    pools: list[PoolBlockDetail]

class ConcurrencyClaimStatus:
    """Result of attempting to claim concurrency-pool slots for a step."""

    status: str
    position: int
    reason: BlockReason | None
    @property
    def is_claimed(self) -> bool:
        """``True`` when the claim succeeded (slots are now leased)."""
        ...

class Storage:
    """Persistent storage backend — events, runs, assets, ticks, KV, pools, queue.

    Construct via the static factories (:meth:`memory`, :meth:`embedded`,
    :meth:`connect`); the underlying SurrealDB connection is shared across
    rivers components.
    """

    @property
    def type(self) -> StorageType:
        """Backend variant — see :class:`StorageType`."""
        ...

    @staticmethod
    def embedded(path: str) -> Storage:
        """Open or create an embedded SurrealDB+RocksDB database at ``path``."""
        ...

    @staticmethod
    def memory() -> Storage:
        """Build an in-memory SurrealDB store (data lost on shutdown)."""
        ...

    @staticmethod
    def connect(
        endpoint: str,
        *,
        username: str | None = None,
        password: str | None = None,
        namespace: str | None = None,
        database: str | None = None,
    ) -> Storage:
        """Connect to a remote SurrealDB endpoint (e.g. ``ws://host:8000``).

        Each parameter resolves via: explicit kwarg → ``RIVERS_SURREAL_*`` env
        var → default. So in K8s, where Helm/operator inject the env vars
        from a Secret, callers can simply ``Storage.connect(endpoint)``.

        ``username`` and ``password`` (when both resolve to non-empty values)
        authenticate as a database-scoped user against ``namespace`` /
        ``database`` — matching a ``DEFINE USER ... ON DATABASE`` definition.
        Omit both for an ``--unauthenticated`` SurrealDB. ``namespace``
        defaults to ``"rivers"`` and ``database`` to ``"main"``.
        """
        ...

    @staticmethod
    def migrate_embedded(path: str) -> None:
        """Apply pending storage schema migrations to an embedded database at
        ``path``, bringing it to this build's schema version.

        Idempotent — a no-op when already current. Runs any data-heal steps
        under a cross-process lease. Backs ``rivers db migrate``.
        """
        ...

    @staticmethod
    def migrate_remote(
        endpoint: str,
        *,
        username: str | None = None,
        password: str | None = None,
        namespace: str | None = None,
        database: str | None = None,
    ) -> None:
        """Remote counterpart of :meth:`migrate_embedded`.

        Same field resolution as :meth:`connect` (kwarg → ``RIVERS_SURREAL_*``
        env → default).
        """
        ...

    def get_events_for_asset(
        self, asset_key: str, limit: int = 100
    ) -> list[StoredEvent]:
        """Return up to ``limit`` events for ``asset_key`` (newest first)."""
        ...

    def get_events_for_run(self, run_id: str) -> list[StoredEvent]:
        """Return all events emitted during ``run_id`` (oldest first)."""
        ...

    def get_latest_materialization(
        self, asset_key: str, partition: str | None = None
    ) -> StoredEvent | None:
        """Return the latest materialization event for ``asset_key`` (optionally a partition)."""
        ...

    def get_asset_record(self, asset_key: str) -> AssetRecord | None:
        """Return the asset's current state record, or ``None`` if it has never materialized."""
        ...

    def get_asset_records(self) -> list[AssetRecord]:
        """Return state records for every asset in the catalog."""
        ...

    def compute_staleness(self) -> dict[str, tuple[str, list[StaleCause]]]:
        """Compute current staleness for every asset, keyed by asset_key.

        ``stale_status`` is no longer persisted on ``AssetRecord``; call this
        to get the live result. Each entry is ``(status, causes)`` where
        status is one of ``"UpToDate"``, ``"Stale"``, ``"Missing"``.
        """
        ...

    def get_assets_by_tag(self, tag: str) -> list[AssetRecord]:
        """Return asset records carrying ``tag``."""
        ...

    def get_assets_by_kind(self, kind: str) -> list[AssetRecord]:
        """Return asset records of the given kind (compute kind / type tag)."""
        ...

    def get_assets_by_group(self, group: str) -> list[AssetRecord]:
        """Return asset records belonging to ``group``."""
        ...

    def get_run(self, run_id: str) -> RunRecord | None:
        """Look up a run by ID, or ``None`` if not found."""
        ...

    def get_runs(self, limit: int = 100, status: str | None = None) -> list[RunRecord]:
        """Return up to ``limit`` runs, optionally filtered by status string."""
        ...

    def get_ticks(self, automation_name: str, limit: int = 100) -> list[StoredTick]:
        """Return up to ``limit`` ticks for the given schedule / sensor / automation condition."""
        ...

    def kv_get(self, key: str) -> bytes | None:
        """Read a raw KV value (used for graph topology, daemon cursors, etc.)."""
        ...

    def kv_set(self, key: str, value: bytes) -> None:
        """Write a raw KV value."""
        ...

    def add_dynamic_partitions(
        self, partitions_def_name: str, partition_keys: list[str]
    ) -> None:
        """Add keys to a :class:`PartitionsDefinition.Dynamic` instance.

        Raises:
            StorageError: If a key is empty or contains a character reserved
                by the canonical display form (``|`` or ``,``).
        """
        ...

    def delete_dynamic_partition(
        self, partitions_def_name: str, partition_key: str
    ) -> None:
        """Remove a single key from a dynamic partitions def."""
        ...

    def get_dynamic_partitions(self, partitions_def_name: str) -> list[str]:
        """List currently-known keys for a dynamic partitions def."""
        ...

    def has_dynamic_partition(
        self, partitions_def_name: str, partition_key: str
    ) -> bool:
        """Return ``True`` if ``partition_key`` is present in the dynamic partitions def."""
        ...

    def get_materialized_partitions(self, asset_key: str) -> list[PartitionKey]:
        """Return every partition key that has at least one materialization recorded."""
        ...

    def count_materialized_partitions(self, asset_key: str) -> int:
        """Count materialized partitions for ``asset_key`` via an aggregate query."""
        ...

    def set_pool_limit(
        self, pool_key: str, limit: int, lease_duration: str = "5m"
    ) -> None:
        """Upsert a concurrency pool's slot limit and lease duration (humantime string)."""
        ...

    def get_pool_limits(self) -> list[PoolLimit]:
        """Return every configured pool limit."""
        ...

    def get_all_pool_infos(self) -> list[PoolInfo]:
        """Return live pool info (limit + claimed/pending) for every pool."""
        ...

    def get_pool_info(self, pool_key: str) -> PoolInfo:
        """Return live pool info for a single pool — raises if the pool is unknown."""
        ...

    def _claim_concurrency_slots(
        self,
        pools: list[tuple[str, int]],
        run_id: str,
        step_key: str,
        priority: int = 0,
        lease_duration: str = "5m",
    ) -> ConcurrencyClaimStatus:
        """(Internal) atomically claim slots across ``pools`` for a step."""
        ...

    def _free_concurrency_slots(self, run_id: str, step_key: str) -> None:
        """(Internal) release slots held by ``(run_id, step_key)``."""
        ...

    def _free_concurrency_slots_for_run(self, run_id: str) -> None:
        """(Internal) release every slot held by any step of ``run_id``."""
        ...

    def _renew_slot_lease(
        self, run_id: str, step_key: str, lease_duration: str = "5m"
    ) -> int:
        """(Internal) extend the lease on the slots held by ``(run_id, step_key)``."""
        ...

    def _free_expired_leases(self) -> int:
        """(Internal) sweep expired leases; return the number of slots freed."""
        ...

    def get_queued_runs(self) -> list[RunRecord]:
        """Return every run currently in the ``Queued`` state."""
        ...

    def get_pool_slot_holders(self, pool_key: str) -> list[SlotHolder]:
        """Return active holders of slots in ``pool_key`` (run/step + lease info)."""
        ...

    def cancel_queued_run(self, run_id: str) -> bool:
        """Cancel a run that hasn't started yet. Returns ``False`` if not in the queue."""
        ...

    def is_cancelled(self, run_id: str) -> bool:
        """Return ``True`` when cancellation has been requested for ``run_id``."""
        ...

    def request_cancellation(self, run_id: str) -> None:
        """Mark ``run_id`` as cancellation-requested (cooperative)."""
        ...

    def set_run_outcome(
        self,
        run_id: str,
        status: str,
        completed_steps: int,
        total_steps: int,
        message: str | None = None,
    ) -> None:
        """Persist the terminal status (``"Success" | "Failure" | "Cancelled"``) of a run."""
        ...

    def get_run_progress(self, run_id: str) -> tuple[int, int]:
        """Return ``(completed_steps, total_steps)`` for an in-flight run."""
        ...

    # Async variants
    def async_get_events_for_asset(
        self, asset_key: str, limit: int = 100
    ) -> Coroutine[Any, Any, list[StoredEvent]]: ...
    def async_get_events_for_run(
        self, run_id: str
    ) -> Coroutine[Any, Any, list[StoredEvent]]: ...
    def async_get_latest_materialization(
        self, asset_key: str, partition: str | None = None
    ) -> Coroutine[Any, Any, StoredEvent | None]: ...
    def async_get_asset_record(
        self, asset_key: str
    ) -> Coroutine[Any, Any, AssetRecord | None]: ...
    def async_get_asset_records(self) -> Coroutine[Any, Any, list[AssetRecord]]: ...
    def async_get_assets_by_tag(
        self, tag: str
    ) -> Coroutine[Any, Any, list[AssetRecord]]: ...
    def async_get_assets_by_kind(
        self, kind: str
    ) -> Coroutine[Any, Any, list[AssetRecord]]: ...
    def async_get_assets_by_group(
        self, group: str
    ) -> Coroutine[Any, Any, list[AssetRecord]]: ...
    def async_get_run(self, run_id: str) -> Coroutine[Any, Any, RunRecord | None]: ...
    def async_get_runs(
        self, limit: int = 100, status: str | None = None
    ) -> Coroutine[Any, Any, list[RunRecord]]: ...
    def async_get_ticks(
        self, automation_name: str, limit: int = 100
    ) -> Coroutine[Any, Any, list[StoredTick]]: ...
    def async_kv_get(self, key: str) -> Coroutine[Any, Any, bytes | None]: ...
    def async_kv_set(self, key: str, value: bytes) -> Coroutine[Any, Any, None]: ...
    def async_add_dynamic_partitions(
        self, partitions_def_name: str, partition_keys: list[str]
    ) -> Coroutine[Any, Any, None]: ...
    def async_delete_dynamic_partition(
        self, partitions_def_name: str, partition_key: str
    ) -> Coroutine[Any, Any, None]: ...
    def async_get_dynamic_partitions(
        self, partitions_def_name: str
    ) -> Coroutine[Any, Any, list[str]]: ...
    def async_has_dynamic_partition(
        self, partitions_def_name: str, partition_key: str
    ) -> Coroutine[Any, Any, bool]: ...

__all__ = [
    "AssetRecord",
    "BlockReason",
    "ConcurrencyClaimStatus",
    "PoolBlockDetail",
    "PoolInfo",
    "PoolLimit",
    "RunRecord",
    "SlotHolder",
    "StaleCause",
    "Storage",
    "StorageType",
    "StoredEvent",
    "StoredTick",
]
