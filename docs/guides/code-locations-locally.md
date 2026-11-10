# Several code locations in `rivers dev`

Two teams can keep their assets in different code locations and still see one lineage. This guide runs two code locations on one storage with `rivers dev`. The `warehouse` code location owns `raw_orders`, and the `analytics` code location builds `order_count` from it.

## The two code locations

```python title="warehouse.py"
from obstore.store import LocalStore

import rivers as rs

tables = rs.PickleIOHandler(store=LocalStore(prefix="/tmp/shared", mkdir=True))


@rs.Asset(io_handler=tables)
def raw_orders() -> list[int]:
    return [12, 7, 30]


repo = rs.CodeRepository(assets=[raw_orders])
```

```python title="analytics.py"
from obstore.store import LocalStore

import rivers as rs

warehouse_tables = rs.PickleIOHandler(store=LocalStore(prefix="/tmp/shared", mkdir=True))

raw_orders = rs.Asset.remote("raw_orders", code_location="warehouse", io_handler=warehouse_tables)


@rs.Asset(automation_condition=rs.AutomationCondition.eager())
def order_count(raw_orders: list[int]) -> int:
    return len(raw_orders)


repo = rs.CodeRepository(assets=[raw_orders, order_count])
```

Both IO handlers use the same directory, so `analytics` reads the files that `warehouse` writes. [Remote assets](../concepts/assets.md#remote-assets) describes the options of `Asset.remote()`.

## Start them

Start the code location that serves the storage first:

```bash
rivers dev warehouse --code-location-id warehouse
```

It prints the storage endpoint:

```text
Storage: ws://127.0.0.1:52378
```

Start the second code location on that storage, with a different gRPC port:

```bash
rivers dev analytics --code-location-id analytics \
  --surreal-endpoint ws://127.0.0.1:52378 \
  --grpc-port 3011 --no-ui
```

`--code-location-id` gives each code location its own data in storage, and its name in the code location directory. In `rivers dev`, the namespace is `dev`, so `code_location="warehouse"` finds the first code location.

Open <http://127.0.0.1:3000>. The code location menu lists both code locations:

- The lineage view of `analytics` shows `raw_orders` with a dotted border. The node links to the asset in `warehouse`.
- The **Lineage** tab of `raw_orders` in `warehouse` lists `order_count` under **In other code locations**.

## Materialize across the two

Materialize `raw_orders` from the UI, or from the command line:

```bash
rivers materialize warehouse --code-location-id warehouse \
  --surreal-endpoint ws://127.0.0.1:52378
```

The condition daemon of `analytics` sees the new materialization and starts a run of `order_count`. The run loads `raw_orders` through `warehouse_tables`.

## Reload code

The **Reload code location** button shows only on the code location of its own `rivers dev`. With `--no-ui`, reload `analytics` with a restart. To get its button, give it a UI on another port instead:

```bash
rivers dev analytics --code-location-id analytics \
  --surreal-endpoint ws://127.0.0.1:52378 \
  --port 3010 --grpc-port 3011
```

Both UIs list both code locations.

## Stop them

Stop `analytics` first. The storage stops with the `rivers dev` that serves it, but a code location that uses that storage does not stop. The storage endpoint changes each time `warehouse` starts, so start the other code locations again with the new endpoint.
