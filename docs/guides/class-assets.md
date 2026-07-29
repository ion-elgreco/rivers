# Class-Based Assets

Every asset can be defined as a class instead of a decorated function. The class body
lists what the asset is and does: its configuration as class attributes, its verbs as
classmethods. Subclass one of the four bases — `rs.Asset`, `rs.MultiAsset`,
`rs.GraphAsset`, `rs.ExternalAsset` — and register the class directly:

```python
import rivers as rs


class Orders(rs.Asset):
    io_handler = delta_handler
    kinds = "table"
    group = "sales"

    @classmethod
    def materialize(cls, ctx: rs.AssetExecutionContext):
        return load_orders()


repo = rs.CodeRepository(assets=[Orders])
```

Both forms produce the same objects — a class desugars at registration into exactly
what the decorator form builds, so resolution, execution, scheduling, and the UI see
no difference. Use whichever reads better; mix them freely in one repository.

## Rules

- **Methods are `@classmethod`.** This is mechanical, not style: the parallel executor
  re-imports `Module.Class.method` in worker processes, and a bound classmethod needs
  no instance lifecycle. A plain `def materialize(self)` is rejected at registration —
  its first parameter would be read as a dependency.
- **The asset name** is the snake_cased class name (`EnrichedOrders` → `enriched_orders`);
  a `name = "..."` class attribute overrides it.
- **Configuration attributes** map to the decorator parameters of the *same kind*:
  `io_handler`, `tags`, `kinds`, `group`, `code_version`, `metadata`, `partitions_def`,
  `deps`, `hooks`, `automation_condition`, `pool`, `retry`, `compute`, … Each is a typed
  attribute on the base classes, so your editor autocompletes them inside the class body
  and a type checker flags wrong value types (`kinds = 123`). The kinds don't all accept
  the same set — `Asset.from_multi()` takes no `pool`, `ExternalAsset` takes no `retry`,
  and so on; declaring one a kind can't carry is a registration-time error rather than a
  silent drop.
- **Actions** are `@rs.action` classmethods in the body, or an `actions = [...]` list of
  `AssetAction` objects, mirroring the decorator's `actions=` argument.
- **Dependencies** are `materialize` parameters, exactly as in the decorator form.
- **Registration is explicit** — `CodeRepository(assets=[Orders])`. Desugaring happens
  there, not at class creation, so defining a class has no import-time side effects.

## Sharing through inheritance

Shared configuration and helpers live in base classes. A base class is just a class —
it only becomes an asset if you list it in `assets=[...]`:

```python
class WarehouseAsset(rs.Asset):
    """Shared handler + plumbing for every warehouse table."""

    io_handler = delta_handler

    @classmethod
    def _clean(cls, df):        # helper — not a verb, ignored by registration
        return df.drop_nulls()


class Orders(WarehouseAsset):
    @classmethod
    def materialize(cls, ctx):
        return cls._clean(load_orders())


class Customers(WarehouseAsset):
    group = "crm"               # subclass attributes override base ones

    @classmethod
    def materialize(cls, ctx):
        return cls._clean(load_customers())
```

Fifty Delta-backed assets inherit the handler by naming their base class. Standard MRO
rules apply: the most derived definition of an attribute or verb wins. A `materialize`
defined on the base is inherited too — `cls` binds to the subclass, in-process and in
parallel workers alike, so template bases can parameterize by class attributes:

```python
class TableSnapshot(rs.Asset):
    table = ""

    @classmethod
    def materialize(cls, ctx):
        return snapshot(cls.table)


class UsersSnapshot(TableSnapshot):
    table = "users"


class OrdersSnapshot(TableSnapshot):
    table = "orders"
```

Listing a class with no executable verb (`materialize` / `observe` / `compose`) is a
registration-time error — that's what distinguishes an accidentally listed mixin from
an asset.

## The other three kinds

**Multi assets** gain the most: outputs are attributes, so names aren't repeated as
strings and per-output configuration sits next to the name it configures. `AssetDef`
without `name=` takes the attribute name; an explicit `name=` wins:

```python
class Ingest(rs.MultiAsset):
    customers = rs.AssetDef(io_handler=delta_customers)
    orders = rs.AssetDef(io_handler=delta_orders, group="sales")

    @classmethod
    def materialize(cls, ctx):
        yield rs.Output(load_customers(), output_name="customers")
        yield rs.Output(load_orders(), output_name="orders")
```

**Graph assets** define `compose`, traced under the composition context exactly like
the decorator form:

```python
class ValidatedPipeline(rs.GraphAsset):
    io_handler = output_io

    @classmethod
    def compose(cls, enriched_orders: dict):
        validate_data(enriched_orders)
        return compute_margins(enriched_orders)
```

**External assets** define `observe` (optional — a verb-less external is a plain
reference to data produced elsewhere):

```python
class VendorFeed(rs.ExternalAsset):
    io_handler = feed_handler

    @classmethod
    def observe(cls, ctx) -> rs.Observation:
        return rs.Observation(metadata={"rows": rs.MetadataValue.int(1)}, data_version=etag())
```

External assets are observed, not produced — defining `materialize` on an
`ExternalAsset` subclass is an error; model data you operate on as a regular asset.

## Actions

The class body lists every verb the asset supports — `materialize` plus any
[actions](../concepts/actions.md), marked with `@rs.action`. Shared verbs inherit
through base classes like everything else, and a subclass override replaces the
inherited action:

```python
class DeltaAsset(rs.Asset):
    io_handler = WAREHOUSE

    @rs.action(outcome=rs.Outcome.Unchanged, concurrency=rs.ActionConcurrency.Exclusive)
    @classmethod
    def optimize(cls, ctx: rs.ActionContext) -> None: ...

    @rs.action(outcome=rs.Outcome.Unchanged)
    @classmethod
    def vacuum(cls, ctx: rs.ActionContext) -> None: ...


class Orders(DeltaAsset):        # inherits optimize + vacuum
    @classmethod
    def materialize(cls, ctx) -> pl.DataFrame: ...
```

Reusable `AssetAction` objects can also be assigned as class attributes
(`optimize = delta_optimize`). Shadowing an inherited action with a non-action
attribute is a registration-time error, not a silent removal.

## Jobs and selections

Jobs accept the classes anywhere an asset instance is accepted; a multi-asset class
expands to its output names:

```python
rs.Job(name="nightly", assets=[Orders, Ingest])
```

## Parallel execution

Class-form assets work across the loky process boundary by reference: workers re-import
the defining module and rebuild the IO handler from the class attribute, so handlers
follow the same contract as always — **configuration, not live connections** (see
[IO Handlers](../concepts/io-handlers.md)). Classes defined inside a function body
(tests, notebooks, `__main__` scripts) can't be re-imported and are pickled by value
instead; both routes are chosen automatically.
