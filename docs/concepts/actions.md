# Asset Actions

An **action** is a named operation on an asset beyond `materialize`: `optimize`,
`vacuum`, `merge`, custom updates. Actions execute through the existing run spine —
run records, retries, cancellation, log capture, and the run page all work — and are
schedulable through the existing `Job` + `Schedule` machinery.

Actions never invoke the producing function and never pull in upstream: the plan has
one step per target. An action needs only the asset's identity and its IO handler,
which every produced asset has.

## Defining actions

In a class-based asset, mark classmethods with `@rs.action`; shared verbs live in
mixins and are inherited:

```python
class TableMaintenance(rs.Asset):
    io_handler = WAREHOUSE

    @rs.action(outcome=rs.Outcome.Unchanged, concurrency=rs.ActionConcurrency.Exclusive)
    @classmethod
    def compact(cls, ctx: rs.ActionContext) -> None:
        h = ctx.io_handler
        compact_small_files(h.asset_uri(ctx.asset_name))


class Orders(TableMaintenance):
    @classmethod
    def materialize(cls, ctx) -> pl.DataFrame: ...
```

For Delta tables the standard maintenance verbs ship built in — subclass
[`DeltaAsset`](../api-reference/delta.md#deltaasset) instead of hand-writing them.

The decorator form attaches reusable `AssetAction` objects, mirroring `hooks=[...]`:

```python
delta_optimize = rs.AssetAction(
    name="optimize",
    outcome=rs.Outcome.Unchanged,
    concurrency=rs.ActionConcurrency.Exclusive,
)(_optimize)


@rs.Asset(actions=[delta_optimize], kinds="delta")
def orders() -> pl.DataFrame: ...
```

Multi-assets register class-level actions on **every output**; per-output additions
and overrides go on the `AssetDef` (`rs.AssetDef(actions=[...])`). Graph assets'
actions target the graph's persisted output and never re-run the composition.
External assets accept **no user-defined actions** — they carry exactly one built-in
action, `observe`.

## Running actions

```python
repo.run_action("optimize", selection=["orders", "customers"])
repo.run_action("compact", partition_key=rs.PartitionKey.single("2026-07-25"))
```

With no `selection`, the action runs over every asset that defines it. Every targeted
asset must define the verb — anything else is a validation error.

Partition-key rules are per-verb, declared with `partitioning=`. The default,
`ActionPartitioning.Required`, matches materialize: partitioned assets need a key.
`ActionPartitioning.Keyless` marks a whole-asset verb — it runs without a key even
on partitioned assets, and a supplied key is rejected up front. `Optional` accepts
both: keyed runs are partition-scoped, keyless runs cover the whole asset.

Multi-partition actions are ordinary backfills; child runs inherit the verb, and
`rerun_backfill` preserves it:

```python
repo.backfill(
    selection=["events"],
    partition_range=rs.PartitionKeyRange.single("2024-01-01", "2024-06-30"),
    action="compact",
    max_concurrency=4,
)
```

Scheduling costs nothing new — the verb lives on the `Job`:

```python
rs.Job(name="nightly_optimize", assets=[Orders, Customers], action="optimize")
rs.Schedule(cron_schedule="0 3 * * *", job_name="nightly_optimize")
```

The UI shows a button per action on the asset page, and action runs display their
verb in the runs list and run header. A verb declaring `Outcome.Unmaterialize`
renders as a danger button and always routes through the confirmation dialog,
which names the verb and says it clears materialization state — nothing else in
the product distinguishes a destructive verb from a benign one.

## ActionContext

Actions receive a single `ActionContext` — no upstream inputs:

| Field | Purpose |
|---|---|
| `asset_name`, `action`, `run_id` | identity |
| `partition_key` / `partition` | the partition being acted on |
| `io_handler` | the asset's resolved handler — the action's config bag (same storage options and URI resolution the write path uses) |
| `asset_metadata` | per-asset overrides IO resolution honors |
| `config` | typed config from an `ActionContext[Config]` annotation (see below) |
| `log` | Python logger |

It also carries `mark_partition_failed(key, error)`, which reports one key of a
batched run as failed while the rest still complete — see
[the context reference](../api-reference/context.md#mark_partition_failedpartition_key-error-1).

Parameters after the context are **resources, injected by name** — the same rule
materialize functions use; a parameter matching no resource is a resolution error:

```python
@rs.action(outcome=rs.Outcome.Unchanged)
@classmethod
def compact(cls, ctx: rs.ActionContext, warehouse: DuckDB) -> None:
    warehouse.execute(f"VACUUM {ctx.asset_name}")
```

Handlers must carry **configuration, not live connections** — an action may run in a
worker or pod and must be able to resolve its table there (see
[IO Handlers](io-handlers.md)).

### Action config

Annotate the context parameter with a Pydantic model to receive typed config —
defaults from the model, per-asset overrides from `run_action(config=...)` (keys
are target asset names; per-output for multi-assets):

```python
class TuneConfig(BaseModel):
    target_size_mb: int = 128

class EventLog(rs.Asset):
    @classmethod
    def materialize(cls) -> pl.DataFrame: ...

    @rs.action(outcome=rs.Outcome.Unchanged)
    @classmethod
    def tune(cls, ctx: rs.ActionContext[TuneConfig]) -> None:
        compact(
            ctx.io_handler.asset_table_uri(ctx.asset_name, ctx.asset_metadata),
            target_mb=ctx.config.target_size_mb,
        )

repo.run_action("tune", config={"event_log": {"target_size_mb": 512}})
```

Without the generic annotation, `ctx.config` is `None`.

## Outcomes

The declared `outcome` states what the action does to orchestration state — not to
physical bytes (a Delta `optimize` rewrites every file and is still `Unchanged`):

| Outcome | Meaning |
|---|---|
| `Unchanged` | No materialization-state change; downstream is never invalidated |
| `MayMaterialize` | Reports at runtime whether data changed (merges) |
| `Unmaterialize` | Clears materialization state (delete) |

`MayMaterialize` actions report what actually happened with an `ActionResult` —
this is what keeps a no-op merge from cascading the whole downstream graph:

```python
@rs.action(outcome=rs.Outcome.MayMaterialize)
@classmethod
def merge_late_arrivals(cls, ctx) -> rs.ActionResult:
    late = fetch_late_arrivals(ctx.partition_key)
    if late.is_empty():
        return rs.ActionResult.unchanged()          # downstream stays put
    merge_into(ctx.io_handler.asset_table_uri(ctx.asset_name, ctx.asset_metadata), late)
    return rs.ActionResult.materialized(metadata={"rows_merged": len(late)})
```

`materialized()` emits a real `Materialization` event — a merge is
indistinguishable downstream from a normal materialize, which is exactly
correct. An action declaring `Unchanged` that reports `materialized()` fails
the step, as does returning anything other than an `ActionResult` or `None`.

An action run's steps emit `ActionCompleted` events; the built-in `observe` emits the
same `Observation` events it always has. Per-action `retry=` takes an inline
`RetryPolicy` or the name of one registered in `CodeRepository(retries={...})` — the
same two spellings asset-level retry accepts.

## Conventional verbs

Verb names are free-form — behavior keys off the declared `outcome`, never the name
(`materialize`, `observe`, and `compose` are reserved). For the common maintenance
verbs, use these declarations so a verb behaves the way its name promises:

| Verb | Declaration |
|------|-------------|
| `optimize`, `vacuum` | `Outcome.Unchanged` + `ActionConcurrency.Exclusive` + `ActionPartitioning.Keyless` — rewrites bytes table-wide, never state |
| `merge`, `refresh` | `Outcome.MayMaterialize` — report an `ActionResult` |
| `delete`, `purge` | `Outcome.Unmaterialize` + `ActionConcurrency.Exclusive` + `ActionOrdering.DownstreamFirst` + `ActionPartitioning.Optional` |

Delta assets get `optimize`, `vacuum`, and `delete` with exactly these declarations
built in — subclass [`DeltaAsset`](../api-reference/delta.md#deltaasset).

## Delete

An `Unmaterialize` action clears materialization state — the run emits `Deletion`
events, the asset record's data version is cleared (per partition for
partition-scoped deletes), and condition evaluation sees the asset as unmaterialized:

```python
class GdprDeletable(rs.Asset):
    @rs.action(
        outcome=rs.Outcome.Unmaterialize,
        concurrency=rs.ActionConcurrency.Exclusive,
        ordering=rs.ActionOrdering.DownstreamFirst,
    )
    @classmethod
    def delete(cls, ctx) -> None:
        h = ctx.io_handler
        DeltaTable(
            h.asset_table_uri(ctx.asset_name, ctx.asset_metadata),
            storage_options=h.storage_options,
        ).delete(h.partition_predicate(ctx.asset_metadata, ctx.partition))


repo.run_action("delete", selection=["events", "event_rollups"], partition_key=pk)
```

`DownstreamFirst` deletes downstream before upstream — a failure midway leaves a
rollup missing rather than a rollup derived from deleted source data. Ordering bounds
partial failure; it cannot make a multi-asset delete atomic.

**Delete acts on exactly what you name.** There is no implicit lineage expansion in
v1 — downstream left out of the selection keeps its data. Returning
`rs.ActionResult.unchanged()` from a delete ("nothing to delete") preserves state and
completes without a `Deletion` event; the flagship GDPR purge over a date range is a
backfill-shaped delete (`repo.backfill(..., action="delete")`).

## Observation is an action

`repo.observe()` runs the built-in `observe` action over the observable external
assets — through the run spine, so observations get run records, log capture, and a
run page. Observation metadata lands on the run's `Observation` events. Schedule it
like any action: `rs.Job(name="obs", assets=[VendorFeed], action="observe")`.

## What actions deliberately don't do

- **Hooks never fire for action runs.** `hooks=[...]` on an asset belongs to its
  materialize path — a `Deletion` or `ActionCompleted` is not a materialization, and
  firing success hooks for one would misreport data as fresh. An action that needs a
  side effect performs it in its own body.
- **Actions never inherit a materialize retry policy.** Auto-retrying a
  half-completed merge has different safety properties from retrying a pure
  materialize, so a verb declares its own `retry=`, defaulting to none; a job-level
  `retry` together with `action=` is rejected rather than silently dropped.
- **Upstream is never pulled in.** An action plan has one step per named target.
