# Storage

## `Storage`

SurrealDB-backed storage for events, runs, asset records, and key-value data.

### `Storage.memory()`

Create an in-memory storage instance (useful for tests).

```python
storage = rs.Storage.memory()
```

### `Storage.embedded(path)`

Create an embedded storage backed by RocksDB at the given path.

```python
storage = rs.Storage.embedded(".rivers/storage")
```

### `Storage.connect(endpoint, *, username=None, password=None, namespace=None, database=None)`

Connect to a remote SurrealDB endpoint (e.g. `ws://host:8000`). Used by the K8s `serve` / `execute` CLI commands.

```python
storage = rs.Storage.connect("ws://surrealdb.rivers.svc.cluster.local:8000")
```

Each parameter resolves via: explicit kwarg → `RIVERS_SURREAL_*` env var → default. So in Kubernetes, where Helm/operator inject the env vars from a Secret, callers can simply `Storage.connect(endpoint)`.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `endpoint` | `str` | required | SurrealDB endpoint URL (e.g. `ws://host:8000`). |
| `username` | `str \| None` | `None` | DB-scoped user. Omit (with `password`) for an `--unauthenticated` SurrealDB. |
| `password` | `str \| None` | `None` | Password for the DB-scoped user. |
| `namespace` | `str \| None` | `"rivers"` | SurrealDB namespace. |
| `database` | `str \| None` | `"main"` | SurrealDB database name. |

When `username` and `password` both resolve to non-empty values, they authenticate as a database-scoped user against `namespace` / `database` — matching a `DEFINE USER ... ON DATABASE` definition.

## Schema versioning & migration

Each persistent database carries a **schema stamp** — the schema version it was last migrated to, plus a `min_reader` and `min_writer` floor (the oldest rivers build allowed to read, and to write, that version). Opening a store checks this stamp against the build's schema version and **refuses an incompatible open instead of silently migrating**.

**The open contract** (`floor ≤ build ≤ version`):

| Situation | Read open (UI) | Read/write open (code locations, daemon) |
|-----------|----------------|-------------------------------------------|
| Build at the database's version | proceeds | proceeds |
| Build newer than the database | refused — run `rivers db migrate` | refused — run `rivers db migrate` |
| Database ahead, build ≥ its floor | proceeds (reader/writer split) | proceeds |
| Build below the relevant floor | refused — upgrade rivers | refused — upgrade rivers |

The reader/writer split lets a write-breaking migration for newer writers run **without locking out an older read-only UI**: a `Read` open is gated by `min_reader`, a read/write open by `min_writer` (and `min_writer ≥ min_reader` always holds).

An **uninitialized** store (no stamp) is bootstrapped by whichever process opens it first — the UI included — so a fresh deployment shows an empty UI without waiting for a code location.

### `rivers db migrate`

Applies pending schema migrations, bringing the database to the running build's schema version. Idempotent (a no-op when already current), and serialized across processes by a short-lived lease so two openers can't migrate the same store at once. Run it after upgrading rivers when a code location or the UI reports that the database needs migration.

```bash
rivers db migrate                              # embedded (--storage-path, default .rivers/storage/)
rivers db migrate --surreal-endpoint ws://surrealdb:8000   # remote
```

In K8s, run it as an explicit init/job step before rolling out upgraded code locations. `rivers dev` instead offers to migrate interactively when it finds the database behind the build.

### `Storage.migrate_embedded(path)` / `Storage.migrate_remote(endpoint, ...)`

The programmatic form of `rivers db migrate` — open-migrate-close. `migrate_remote` takes the same `username` / `password` / `namespace` / `database` resolution as `Storage.connect`.

```python
rs.Storage.migrate_embedded(".rivers/storage")
rs.Storage.migrate_remote("ws://surrealdb.rivers.svc.cluster.local:8000")
```

### `storage.type`

Returns a `StorageType` enum (`StorageType.Memory`, `StorageType.Embedded`, or `StorageType.Remote`).

## Sync Methods

All query methods block the calling thread until the result is ready.

| Method | Return Type |
|--------|-------------|
| `get_events_for_asset(asset_key, limit=100)` | `list[StoredEvent]` |
| `get_events_for_run(run_id)` | `list[StoredEvent]` |
| `get_latest_materialization(asset_key, partition=None)` | `StoredEvent \| None` |
| `get_asset_record(asset_key)` | `AssetRecord \| None` |
| `get_asset_records()` | `list[AssetRecord]` |
| `compute_staleness()` | `dict[str, tuple[str, list[StaleCause]]]` |
| `get_assets_by_tag(tag)` | `list[AssetRecord]` |
| `get_assets_by_kind(kind)` | `list[AssetRecord]` |
| `get_assets_by_group(group)` | `list[AssetRecord]` |
| `get_run(run_id)` | `RunRecord \| None` |
| `get_runs(limit=100, status=None)` | `list[RunRecord]` |
| `get_ticks(automation_name, limit=100)` | `list[StoredTick]` |
| `kv_get(key)` | `bytes \| None` |
| `kv_set(key, value)` | `None` |
| `add_dynamic_partitions(name, keys)` | `None` |
| `delete_dynamic_partition(name, key)` | `None` |
| `get_dynamic_partitions(name)` | `list[str]` |
| `has_dynamic_partition(name, key)` | `bool` |
| `get_materialized_partitions(asset_key)` | `list[PartitionKey]` |

Dynamic keys registered before the reserved-character guard existed (`|`/`,`) still classify, but cannot round-trip display-string paths (UI, gRPC). The storage migration logs a warning for all such keys and records them under the `reserved_dynamic_keys` entry in the `kv` table. Delete each offending key and re-register it under a clean name.

### `compute_staleness()`

```python
def compute_staleness(self) -> dict[str, tuple[str, list[StaleCause]]]
```

Compute current staleness for every asset, keyed by asset_key. Each entry is `(status, causes)` where status is one of `"UpToDate"`, `"Stale"`, or `"Missing"`. Staleness is no longer persisted on `AssetRecord` — call this to get the live result.

## Async Methods

Every sync method has an `async_` prefixed counterpart that returns a Python awaitable. These use `pyo3_async_runtimes::tokio::future_into_py` under the hood — the Rust future runs on Tokio without holding the GIL, making them safe to use in async Python code.

```python
import asyncio
import rivers as rs

async def main():
    storage = rs.Storage.memory()
    await storage.async_kv_set("key", b"value")
    result = await storage.async_kv_get("key")
    print(result)  # b"value"

asyncio.run(main())
```

| Async Method | Return Type |
|--------------|-------------|
| `async_get_events_for_asset(asset_key, limit=100)` | `list[StoredEvent]` |
| `async_get_events_for_run(run_id)` | `list[StoredEvent]` |
| `async_get_latest_materialization(asset_key, partition=None)` | `StoredEvent \| None` |
| `async_get_asset_record(asset_key)` | `AssetRecord \| None` |
| `async_get_asset_records()` | `list[AssetRecord]` |
| `async_get_assets_by_tag(tag)` | `list[AssetRecord]` |
| `async_get_assets_by_kind(kind)` | `list[AssetRecord]` |
| `async_get_assets_by_group(group)` | `list[AssetRecord]` |
| `async_get_run(run_id)` | `RunRecord \| None` |
| `async_get_runs(limit=100, status=None)` | `list[RunRecord]` |
| `async_get_ticks(automation_name, limit=100)` | `list[StoredTick]` |
| `async_kv_get(key)` | `bytes \| None` |
| `async_kv_set(key, value)` | `None` |
| `async_add_dynamic_partitions(name, keys)` | `None` |
| `async_delete_dynamic_partition(name, key)` | `None` |
| `async_get_dynamic_partitions(name)` | `list[str]` |
| `async_has_dynamic_partition(name, key)` | `bool` |

## Data Classes

### `StoredEvent`

| Field | Type |
|-------|------|
| `id` | `str` |
| `event_type` | `str` |
| `asset_key` | `str \| None` |
| `run_id` | `str` |
| `partition_key` | `str \| None` |
| `timestamp` | `int` |
| `metadata` | `list[tuple[str, str]]` |
| `data_version` | `str \| None` |
| `code_version` | `str \| None` |
| `input_data_versions` | `list[tuple[str, str]]` |

### `AssetRecord`

| Field | Type |
|-------|------|
| `asset_key` | `str` |
| `tags` | `list[str]` |
| `kinds` | `list[str]` |
| `group` | `str \| None` |
| `code_version` | `str \| None` |
| `last_event_id` | `str \| None` |
| `last_run_id` | `str \| None` |
| `last_timestamp` | `int \| None` |
| `last_data_version` | `str \| None` |
| `last_materialization_code_version` | `str \| None` |
| `last_input_data_versions` | `list[tuple[str, str]]` |
| `pool` | `list[tuple[str, int]]` |

!!! note
    Staleness is no longer persisted on `AssetRecord`. Call [`Storage.compute_staleness()`](#compute_staleness) to get the live `(status, causes)` per asset.

### `StaleCause`

| Field | Type |
|-------|------|
| `asset_key` | `str` |
| `category` | `str` |
| `reason` | `str` |
| `dependency` | `str \| None` |

### `RunRecord`

| Field | Type |
|-------|------|
| `run_id` | `str` |
| `job_name` | `str \| None` |
| `status` | `str` |
| `start_time` | `int` |
| `end_time` | `int \| None` |
| `tags` | `list[tuple[str, str]]` |
| `node_names` | `list[str]` |
| `priority` | `int` |
| `partition_key` | `PartitionKey \| None` |
| `block_reason` | `str \| None` |
| `launched_by` | `LaunchedBy` |

### `LaunchedBy`

Tagged union describing where a run originated. Build via classmethod factories and discriminate on `.kind`.

```python
rs.LaunchedBy.manual()
rs.LaunchedBy.schedule("nightly")
rs.LaunchedBy.sensor("file_sensor")
rs.LaunchedBy.backfill("bf-2024-q1")
rs.LaunchedBy.condition()
```

| Property | Type | Description |
|----------|------|-------------|
| `kind` | `str` | One of `"manual"`, `"schedule"`, `"sensor"`, `"backfill"`, `"condition"`. |
| `name` | `str \| None` | Schedule or sensor name (else `None`). |
| `backfill_id` | `str \| None` | Backfill id (else `None`). |

### `StoredTick`

| Field | Type |
|-------|------|
| `id` | `str` |
| `automation_name` | `str` |
| `automation_type` | `str` |
| `status` | `str` |
| `timestamp` | `int` |
| `run_ids` | `list[str]` |
| `backfill_ids` | `list[str]` |
| `skip_reason` | `str \| None` |
| `error` | `str \| None` |
| `cursor` | `str \| None` |

### `PoolLimit`

Configuration for a concurrency pool.

| Field | Type |
|-------|------|
| `pool_key` | `str` |
| `slot_limit` | `int` |
| `lease_duration_secs` | `int` |

### `PoolInfo`

Runtime info for a concurrency pool (config + current usage).

| Field | Type |
|-------|------|
| `pool_key` | `str` |
| `slot_limit` | `int` |
| `lease_duration_secs` | `int` |
| `claimed_count` | `int` |
| `pending_count` | `int` |

### `PoolBlockDetail`

Detail for a single pool that blocked a concurrency claim.

| Field | Type |
|-------|------|
| `pool_key` | `str` |
| `claimed` | `int` |
| `limit` | `int` |

### `BlockReason`

Why a step was blocked from claiming concurrency slots.

| Field | Type | Description |
|-------|------|-------------|
| `kind` | `str` | `"pool_full"` or `"pools_full"` |
| `pool_key` | `str` | The blocking pool (first pool if multiple) |
| `claimed` | `int` | Current claimed slots |
| `limit` | `int` | Pool slot limit |
| `pools` | `list[PoolBlockDetail]` | All blocking pools (only for `pools_full`) |

### `ConcurrencyClaimStatus`

Result of attempting to claim concurrency slots.

| Field | Type | Description |
|-------|------|-------------|
| `status` | `str` | `"claimed"` or `"pending"` |
| `position` | `int` | Queue position (meaningful when pending) |
| `reason` | `BlockReason \| None` | Block reason (only when pending) |
| `is_claimed` | `bool` | Property: `True` if slots were claimed |

### `SlotHolder`

A run/step currently holding pool slots — surfaced by `get_pool_slot_holders()`.

| Field | Type |
|-------|------|
| `run_id` | `str` |
| `step_key` | `str` |
| `slots_consumed` | `int` |
| `claimed_at` | `int` |
| `lease_expires_at` | `int` |

## Pool Methods

| Method | Return Type | Description |
|--------|-------------|-------------|
| `set_pool_limit(pool_key, limit, lease_duration="5m")` | `None` | Set (upsert) a pool's slot limit and lease duration. `lease_duration` is a human-readable duration string (e.g. `"5m"`, `"1h"`). |
| `get_pool_limits()` | `list[PoolLimit]` | Get all configured pool limits. |
| `get_all_pool_infos()` | `list[PoolInfo]` | Live info (limit + claimed/pending) for every configured pool. |
| `get_pool_info(pool_key)` | `PoolInfo` | Get runtime info for a single pool. Raises if the pool is unknown. |
| `get_pool_slot_holders(pool_key)` | `list[SlotHolder]` | Active slot holders (run/step + lease info) for the named pool. |

## Run-queue Methods

| Method | Return Type | Description |
|--------|-------------|-------------|
| `get_queued_runs()` | `list[RunRecord]` | Every run currently in the `Queued` state. |
| `cancel_queued_run(run_id)` | `bool` | Cancel a not-yet-started run. Returns `False` if the run is not in the queue. |
| `is_cancelled(run_id)` | `bool` | Cooperative cancellation flag — set by `request_cancellation()`. |
| `request_cancellation(run_id)` | `None` | Mark the run as cancellation-requested (executor checks between steps). |
| `set_run_outcome(run_id, status, completed_steps, total_steps, message=None)` | `None` | Persist the terminal status of a run (`"Success" \| "Failure" \| "Cancelled"`). |
| `get_run_progress(run_id)` | `tuple[int, int]` | `(completed_steps, total_steps)` for an in-flight run. |

The claim/release methods (`_claim_concurrency_slots`, `_free_concurrency_slots`, `_free_concurrency_slots_for_run`) and the lease management methods (`_renew_slot_lease`, `_free_expired_leases`) are internal — called by the executor during step scheduling and background lease management, not by user code.

### Executor Integration

Pool limits are enforced during execution. When a step with `pool` configuration runs, the executor:

1. **Claims slots** before execution — blocks (with backoff polling) until all required pools have capacity.
2. **Starts lease renewal** — a background task renews the lease every `lease_duration / 3` seconds.
3. **Executes the step** — the Python function runs while slots are held.
4. **Releases slots** on completion or failure — slots are freed immediately.
5. **Run-level cleanup** — after all steps finish, `free_concurrency_slots_for_run` removes any leaked slots.

Per-backend behavior:

- **InProcess**: Sequential claim → execute → release per step.
- **Async**: Each concurrent task claims independently inside its JoinSet task.
- **Parallel**: Pool-requiring steps run in loky subprocesses with claim-gated concurrency via a tokio JoinSet — each task does `claim → submit+collect → release` so the pool limit naturally throttles how many steps execute simultaneously. Non-pool steps use the existing batch pipeline.

Mapped (fan-out) steps claim per instance, not per parent step.

#### Lease Expiry

Concurrency slots use lease-based expiry. Each claimed slot has a `lease_expires_at` timestamp. Expired leases are automatically excluded from capacity checks, so a crashed process's slots are reclaimed without manual intervention. The `_renew_slot_lease` method extends the lease during execution, and `_free_expired_leases` performs garbage collection of stale rows. The coordinator daemon calls `free_expired_leases` on every tick as defense-in-depth GC.
