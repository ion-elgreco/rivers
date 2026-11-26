# Asset Migrations

When the shape of an asset's stored data changes, code that expects the new shape cannot read the old data. Increase the asset's `schema_version` and add a migration to the new version. When the new code registers, rivers holds the runs that write or read the asset, waits for the runs that already started, and migrates the stored data once. Then the runs go on.

```python
import rivers as rs
from rivers.integrations.duckdb import DuckDBResource


def add_currency(ctx: rs.MigrationContext, warehouse: DuckDBResource) -> None:
    with warehouse.get_connection() as con:
        con.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS currency TEXT DEFAULT 'EUR'")


@rs.Asset(schema_version=2, migrations=[rs.AssetMigration(to=2)(add_currency)])
def orders(warehouse: DuckDBResource) -> None:
    with warehouse.get_connection() as con:
        con.execute("CREATE OR REPLACE TABLE orders AS SELECT id, amount, currency FROM raw_orders")
```

## Versions

Each asset declares the version of its stored data with `schema_version`, `1` by default. rivers keeps the version that the stored data is at, in a [version record](../api-reference/storage.md#assetversionrecord) per asset. Only an asset that declares a version above 1 has a record, or an asset whose [partition keys changed](#changing-the-partition-keys) while it had stored data.

- An asset without stored data starts at the version that it declares. There is nothing to migrate.
- Data that was stored before the asset declared a version is at version 1.
- Each version from 2 to `schema_version` needs one migration. Keep a migration in the code until every store has applied it. A store that needs a migration that the code no longer has fails with `no migration to version 3`.

`schema_version` is not `code_version`. A new `code_version` makes the stored data stale. A new `schema_version` says that the stored data must change before the code can use it. Most changes of `code_version` need no migration.

## Kinds of migration

| Kind | Declaration | What it does |
|------|-------------|--------------|
| Convert | `rs.AssetMigration(to=2)(fn)` | Runs `fn` once, in a run of the asset's built-in `migrate` action, to change the stored data where it is. |
| Rebuild | `rs.AssetMigration(to=2, rebuild=True)` | Runs the asset's own function again: one run, or a backfill over the materialized partitions. |
| Version only | `rs.AssetMigration(to=2)` | Records the version. The new data arrives through normal materializations. |

A convert function receives a [`MigrationContext`](../api-reference/context.md#migrationcontext), then resources by name. For a partitioned asset, the convert runs once, for the whole asset. When the migration changes the partition keys, `ctx.partition` holds the partitions to fill (see [Changing the partition keys](#changing-the-partition-keys)). A retry runs the function again from the start, so the function must be safe to run twice: in SQL, use `IF NOT EXISTS` and `CREATE OR REPLACE`. When the convert changes values that readers use, return `rs.ActionResult.materialized()`: the readers then become stale.

See [`AssetMigration`](../api-reference/assets.md#assetmigration) for all parameters, and [`@migration`](../api-reference/assets.md#migration) for class-form assets.

### Convert or rebuild

Does the stored data already hold everything that the new version needs?

- **Yes: convert.** It is fast, and it does not run the asset's function again.
- **No: rebuild.** It costs as much as a backfill of the materialized partitions, and it needs the upstream data for that period. If upstream no longer keeps the old data, only a convert can change those partitions.

| Change | Choice | Why |
|--------|--------|-----|
| A new column, calculated from other columns | Convert | Calculate it in SQL. |
| A renamed column, or a value in a new unit | Convert | The values are stored. |
| A new column with values from upstream | Rebuild | The values are not stored. |
| Medians or distinct counts instead of sums | Rebuild | Stored sums cannot give them. |

### Rebuild or a new `code_version`

A rebuild runs the same runs as a new `code_version` and a backfill. The difference is what rivers does around them:

| | New `code_version` and a backfill | `rebuild=True` |
|---|---|---|
| Readers during the runs | Read a mix of old and new partitions. | Wait until all partitions are done. |
| Runs that started with the old code | Can still write old data. | End first. Then the rebuild starts. |
| Readers that read all partitions | Can run many times during the backfill. | Run once, at the end. |
| Done once | You start the backfill and check that it ended. | rivers starts it once and records it as done. |

Use a new `code_version` when only the logic changes and readers can read a mix for a while. Use a migration when they cannot.

## When new code registers

1. **Pending.** The new code registers when its code location starts (`resolve()`), and the version record of the asset goes to `pending`. From then on, runs that write or read the asset wait. New code never runs against old data.
2. **Draining.** The daemon of the code location takes the migration. It waits until no started run of the code location writes or reads the asset, and until each asset that this asset reads has finished its own migration.
3. **Running.** The daemon runs the migrations from the stored version to the declared version, in order. A rebuild of an asset without stored data only records the version.
4. **Ready.** The stored data is at the declared version. The held runs go on, and automation evaluates the asset again.

One daemon runs each migration. If that daemon stops, the migration waits about a minute. Then a daemon whose code declares the new version takes it over, also the same daemon after a restart.

## Starting later

With `start_at`, the deploy comes first and the migration later, for example a rebuild at night:

```python
from datetime import datetime
from zoneinfo import ZoneInfo

FRIDAY_NIGHT = datetime(2026, 11, 27, 22, 0, tzinfo=ZoneInfo("Europe/Amsterdam"))


@rs.Asset(
    schema_version=2,
    migrations=[rs.AssetMigration(to=2, rebuild=True, start_at=FRIDAY_NIGHT)],
)
def orders(warehouse: DuckDBResource) -> None: ...
```

- The daemon starts the migration at `start_at`. When that time has passed by the time the code registers, the migration starts at once.
- The asset and its readers still wait from the deploy on: the new code expects the new data, and the old code no longer runs. A held run gives the start time, for example `orders waits for its migration to version 2, which starts at 2026-11-27T21:00:00Z`. Schedules keep adding runs to the run queue while they wait.
- `start_at` needs a time zone. A datetime without one raises `AssetDefinitionError`.
- A migration that applies several versions starts at the latest `start_at` of those versions.
- **Start now** on the asset page, the gRPC `StartMigration`, and `repo.migrate()` start the migration before its time.
- The time is the same in each environment that runs the code, staging too. To start sooner there, start the migration by hand, or read the time from an environment variable.

## Changing the partition keys

A new partitions definition can name other partitions than the stored ones, for example hours instead of days (see [Changing a partitions definition](partitions.md#changing-a-partitions-definition)). In the deploy that changes the definition, raise `schema_version` and add a migration to the new version:

```python
from datetime import datetime


def by_hour(ctx: rs.MigrationContext, warehouse: DuckDBResource) -> None:
    ctx.log.info("fills %d hours", len(ctx.partition.keys))
    with warehouse.get_connection() as con:
        con.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS hour TIMESTAMP")
        con.execute("UPDATE orders SET hour = date_trunc('hour', created_at)")


@rs.Asset(
    partitions_def=rs.PartitionsDefinition.hourly(start=datetime(2024, 1, 1)),  # was daily
    schema_version=2,
    migrations=[rs.AssetMigration(to=2)(by_hour)],
)
def orders(context: rs.AssetExecutionContext, warehouse: DuckDBResource) -> None: ...
```

Without the migration, an asset with stored data gets the state `blocked`. It waits as an asset that migrates does, and the reason names the migration to add.

The migration fills the partitions of the new definition that the stored partitions cover:

| Change | Partitions to fill |
|--------|--------------------|
| Time windows to other time windows | Each new partition whose whole window the stored partitions cover. Daily to hourly: the 24 hours of each stored day. Hourly to daily: each day whose 24 hours are all stored. |
| Multi-dimensional, when only one dimension changes, from time windows to time windows | The same, for each value of the other dimensions. A value that a static dimension no longer has fills nothing. |
| Partitions to no partitions | The whole asset, when a partition is stored. |
| Each other change | rivers cannot find them. Name them with `partitions=`, else a convert or a rebuild fails the migration. |

`partitions=[...]` names the partitions to fill, also when rivers can find them. `partitions=[]` starts the asset empty.

The kind of migration tells how rivers fills them:

| Kind | Fills the partitions |
|------|----------------------|
| Convert | In one run of `migrate`. `ctx.partition.keys` lists the partitions to fill. rivers records a materialization of each of them, whatever the function returns, except the ones that the function marks with `ctx.mark_partition_failed(key, error)`. |
| Rebuild | In a backfill. |
| Version only | None: the asset starts empty. |

Then the stored partitions of the definition before go: rivers records one deletion of the whole asset, at the time when the work started, so that the partitions that the work wrote stay. A partition that the work did not fill holds no data. Automation or a backfill materializes it with the new code.

- When the daemon takes the migration, it cancels the queued runs and the backfills of the asset that were made before the deploy: they name the old keys. It waits for the started runs, as for each migration.
- Only the step to the declared version changes the keys. The steps before it see the old keys. A rebuild among them fails the migration, because the code defines only the new partitions.
- A change of the partition keys is breaking, also with `breaking=False`.
- A migration that keeps the partition keys ignores `partitions=`, with a warning in the daemon log.

## While an asset migrates

A run that writes or reads the asset waits:

| Run | While the asset migrates |
|-----|--------------------------|
| A run in the [run queue](../api-reference/concurrency.md#runqueueconfig) | Stays queued, BLOCKED with the reason, for example `orders is migrating to version 2`. It starts after the migration. |
| A daemon run without a run queue | Fails at its start, with the reason. |
| A launch from the UI or gRPC | Is refused, with the reason. |
| `repo.materialize()`, `repo.run_action()`, `Job.execute()`, `repo.backfill()` | Raises [`AssetMigrationPendingError`](../api-reference/exceptions.md). |

Automation conditions start no run of the asset or of its readers. The tick is `Skipped` with the reason, and the condition fires again after the migration (see [Automation conditions](scheduling.md#automation-conditions)). Schedules and sensors are not paused: their runs wait in the run queue, or fail at their start without one.

## When a migration fails

A convert that fails after its retries, or whose run is canceled, fails the migration. The asset and its readers stay held. The asset page shows the error and a **Retry** button. Correct the cause, deploy the fix if needed, then retry: in the UI, with the gRPC `RetryMigration`, or with `repo.migrate()`. A retry runs the convert again from the start.

A rebuild does not fail the migration. When runs of the rebuild fail, the migration still ends. A partition that the rebuild did not write then holds no data, so that no reader reads data of the old version. The same applies to an asset without partitions whose rebuild run failed. Automation or a backfill materializes it again with the new code.

A run that never ends keeps the migration waiting. The asset page lists the runs that the migration waits for. Cancel them.

## Rolling back

**Before the migration runs.** Code that you roll back does not declare the new version, so nothing runs the migration, and the asset stays held. Cancel the migration with **Cancel migration** on the asset page, or with the gRPC `CancelMigration`. The stored data keeps its version, and the runs go on. While the code still declares the new version, cancel is refused, because the migration would start again.

**After the migration.** The stored data is at the new version. Code that declares an older version cannot write the asset: its runs fail with `this code declares orders version 1; the stored data is at version 2`, and in-process calls raise `AssetVersionMismatchError`. Migrations go forward only. To undo a change, add a migration to the next version that changes the data back.

## Rolling deploys

During a rolling deploy, pods with the old code and pods with the new code run at the same time. The old code does not run the migration. Its runs wait while the asset migrates. After the migration, its runs that write the asset fail.

!!! warning "Upgrade rivers first"
    A rivers release without asset migrations does not check versions. During a rollout, its pods can write the asset while the asset migrates. Upgrade every code location to a rivers release with asset migrations first. Raise `schema_version` in a later deploy.

## Without a daemon

[`repo.migrate()`](../api-reference/repository.md#migrate) runs the migrations in this process, for tests and scripts:

```python
repo = rs.CodeRepository(assets=[orders], resources={"warehouse": warehouse})
repo.resolve(storage=storage)
(record,) = repo.migrate(["orders"])
assert (record.state, record.applied_version) == ("ready", 2)
```

## In the UI

- **Asset page.** STATUS shows `migrating`, `migration-failed`, or `migration-needed` for an asset whose partition keys changed without a migration. A panel shows the stored version and the target version, then when the migration starts, what it waits for, what it runs (with links to the runs or the backfill, and the number of partitions that it fills), or why it failed or cannot start. It has **Retry** for a failed migration, **Start now** for a migration that starts later, and **Cancel migration** after a rollback.
- **Graph.** The node of an asset that migrates has a `migrating`, `migration failed`, or `migration needed` chip.

The gRPC `GetAssetsInfo` returns the declared `schema_version` and the `stored_version` of each asset, with `start_at` for a migration that starts later.

## Limits

- rivers finds the partitions to fill only when one dimension changes. For a change of two dimensions, name them with `partitions=`.
- `MigrationContext` does not give the partitions definition before the change. The convert's code knows it.
- Readers in other code locations are not held, and the migration does not wait for their runs.
- `breaking=` has no effect yet. rivers records it for readers in other code locations.
