"""dlt integration: dlt sources as multi-assets, loaded into DuckDB or a sink under ``tmp_path``.

The ``DltSourceAssets`` and the asset functions that load them are module-level:
a dlt source does not pickle, so parallel workers import them from this module.
"""

import importlib
import json
import os
import sys
from typing import Any

import pytest

dlt = pytest.importorskip("dlt")
duckdb = pytest.importorskip("duckdb")

import pyarrow as pa  # noqa: E402

import rivers as rs  # noqa: E402
from rivers.integrations.dlt import DltSourceAssets, output_def  # noqa: E402
from rivers.integrations.duckdb import DuckDBIOHandler, DuckDBResource  # noqa: E402

from _helpers import make_repo  # noqa: E402

# Loky workers import this module to run the asset functions, so dlt's
# environment config is set here for them too.
os.environ.setdefault("RUNTIME__DLTHUB_TELEMETRY", "false")
# A failed load job of the "partial" pipeline lets the other jobs finish.
os.environ["PARTIAL__LOAD__RAISE_ON_FAILED_JOBS"] = "false"

OUTPUTS = {"shop_customers", "shop_orders", "shop_notes"}


@dlt.source(name="shop")
def shop(region: str = "eu", broken: str = ""):
    @dlt.resource(primary_key="id", write_disposition="merge")
    def customers():
        pid = os.getpid()
        yield [
            {"id": 1, "name": "ann", "pid": pid, "tags": ["new", "vip"]},
            {"id": 2, "name": "bob", "pid": pid, "tags": ["new"]},
        ]

    @dlt.resource(table_name="orderRows")
    def orders():
        if broken == "orders":
            raise ValueError("orders API is down")
        yield [
            {"id": 10, "customer_id": 1, "amount": 5.5, "region": region},
            {"id": 11, "customer_id": 2, "amount": 7.25, "region": region},
        ]

    @dlt.transformer(data_from=customers)
    def notes(rows):
        for row in rows:
            yield {"customer_id": row["id"], "note": f"hello {row['name']}"}

    return customers, orders, notes


@dlt.source(name="stale")
def stale():
    @dlt.resource
    def leftovers():
        yield {"id": 1}

    return leftovers


@dlt.source(name="feed")
def feed(title: str = "new"):
    @dlt.resource(primary_key="id")
    def posts(cursor=dlt.sources.incremental("id", initial_value=0)):
        yield [{"id": i, "title": title} for i in (1, 2, 3)]

    return posts


@dlt.destination(name="sink")
def jsonl_sink(items, table, path: str = dlt.config.value) -> None:
    with open(path, "a", encoding="utf-8") as file:
        file.writelines(json.dumps(item) + "\n" for item in items)


class Warehouse(rs.Resource):
    """The destination that the pipelines load, and their directory.

    The destination is the DuckDB file ``warehouse.duckdb``, or with ``sink``,
    a custom destination that appends the rows to ``sink.jsonl``.
    """

    directory: str
    pipeline_name: str = "shop"
    restore: bool = True
    sink: bool = False

    def pipeline(self) -> dlt.Pipeline:
        destination = (
            jsonl_sink(path=os.path.join(self.directory, "sink.jsonl"))
            if self.sink
            else dlt.destinations.duckdb(
                os.path.join(self.directory, "warehouse.duckdb")
            )
        )
        pipeline = dlt.pipeline(
            pipeline_name=self.pipeline_name,
            pipelines_dir=os.path.join(self.directory, "pipelines"),
            destination=destination,
            dataset_name="shop_data",
        )
        pipeline.config.restore_from_destination = self.restore
        return pipeline


class ShopApi(rs.Resource):
    region: str


SHOP = DltSourceAssets(shop())

SHOP_TABLES_SOURCE = shop()


def shop_tables_def(resource) -> rs.AssetDef:
    """The dlt guide's ``asset_def`` for ``DuckDBIOHandler``, with this module's dataset."""
    metadata = output_def(resource).metadata
    table = SHOP_TABLES_SOURCE.schema.naming.normalize_table_identifier(
        metadata["dlt/table"]
    )
    return output_def(
        resource,
        metadata=metadata | {"duckdb/schema": "shop_data", "duckdb/table": table},
    )


SHOP_TABLES = DltSourceAssets(SHOP_TABLES_SOURCE, asset_def_fn=shop_tables_def)

FEED = DltSourceAssets(feed())


def load_shop(context: rs.AssetExecutionContext, warehouse: Warehouse):
    yield from SHOP.load(context, warehouse.pipeline())


def load_broken_shop(context: rs.AssetExecutionContext, warehouse: Warehouse):
    yield from SHOP.load(context, warehouse.pipeline(), shop(broken="orders"))


def load_api_shop(
    context: rs.AssetExecutionContext, warehouse: Warehouse, api: ShopApi
):
    yield from SHOP.load(context, warehouse.pipeline(), shop(region=api.region))


def load_regional_shop(context: rs.AssetExecutionContext, warehouse: Warehouse):
    yield from SHOP.load(
        context, warehouse.pipeline(), shop(region=context.partition_key)
    )


def load_shop_tables(context: rs.AssetExecutionContext, warehouse: Warehouse):
    yield from SHOP_TABLES.load(context, warehouse.pipeline())


def load_feed(context: rs.AssetExecutionContext, warehouse: Warehouse):
    yield from FEED.load(context, warehouse.pipeline())


def _repo(
    tmp_path, storage, *assets, executor=None, resources=None, **warehouse
) -> rs.CodeRepository:
    """``assets`` next to an independent asset, with the warehouse under ``tmp_path``.

    Two steps in one level send both to the parallel executor's workers.
    """

    @rs.Asset
    def bystander() -> rs.Materialization:
        return rs.Materialization()

    return make_repo(
        [*assets, bystander],
        storage,
        executor=executor,
        resources={
            "warehouse": Warehouse(directory=str(tmp_path), **warehouse),
            **(resources or {}),
        },
    )


def _rows(tmp_path, sql: str) -> list[tuple]:
    with duckdb.connect(str(tmp_path / "warehouse.duckdb")) as con:
        return con.sql(sql).fetchall()


def _tables(tmp_path) -> set[str]:
    rows = _rows(
        tmp_path,
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'shop_data'",
    )
    return {name for (name,) in rows}


def _entries(event) -> dict[str, tuple[str, Any]]:
    """``(kind, value)`` of each metadata entry of a stored event."""
    entries = {}
    for key, value in event.metadata:
        [(kind, body)] = json.loads(value).items()
        entries[key] = (kind, body["value"])
    return entries


def _materializations(storage, run_id: str) -> dict[str, dict[str, tuple[str, Any]]]:
    return {
        event.asset_key: _entries(event)
        for event in storage.get_events_for_run(run_id)
        if event.event_type == "Materialization"
    }


def _values(entries: dict[str, tuple[str, Any]]) -> dict[str, Any]:
    return {key: value for key, (_, value) in entries.items()}


def _failures(storage, run_id: str) -> dict[str, str]:
    return {
        event.asset_key: dict(event.metadata)["error"]
        for event in storage.get_events_for_run(run_id)
        if event.event_type == "StepFailure"
    }


def test_missing_dlt_names_the_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "dlt", None)
    monkeypatch.delitem(sys.modules, "rivers.integrations.dlt")
    with pytest.raises(ImportError, match=r"pip install rivers\[dlt\]"):
        importlib.import_module("rivers.integrations.dlt")


def test_one_output_per_selected_resource():
    assert [
        (output.name, output.kinds, output.metadata) for output in SHOP.output_defs
    ] == [
        (
            "shop_customers",
            ["dlt"],
            {"dlt/resource": "customers", "dlt/table": "customers"},
        ),
        ("shop_orders", ["dlt"], {"dlt/resource": "orders", "dlt/table": "orderRows"}),
        ("shop_notes", ["dlt"], {"dlt/resource": "notes", "dlt/table": "notes"}),
    ]
    assert {output: r.name for output, r in SHOP.resources.items()} == {
        "shop_customers": "customers",
        "shop_orders": "orders",
        "shop_notes": "notes",
    }

    assets = SHOP.assets(name="raw", group="raw")(load_shop)
    assert assets.name == "raw"
    assert [(output.name, output.group) for output in assets.output_defs] == [
        ("shop_customers", "raw"),
        ("shop_orders", "raw"),
        ("shop_notes", "raw"),
    ]

    orders_only = DltSourceAssets(shop().with_resources("orders"))
    assert [output.name for output in orders_only.output_defs] == ["shop_orders"]


def test_output_def_sets_or_replaces_fields():
    @dlt.source(name="events")
    def events():
        @dlt.resource(table_name=lambda event: event["type"])
        def by_type():
            yield {"type": "click"}

        return by_type

    [resource] = events().selected_resources.values()
    assert output_def(resource).metadata == {
        "dlt/resource": "by_type",
        "dlt/table": "by_type",
    }

    output = output_def(
        resource, name="clicks", kinds=["dlt", "duckdb"], pool="events", group="raw"
    )
    assert (output.name, output.kinds, output.group, output.pool) == (
        "clicks",
        ["dlt", "duckdb"],
        "raw",
        [("events", 1)],
    )
    assert output.metadata == {"dlt/resource": "by_type", "dlt/table": "by_type"}


def test_materialize_loads_every_resource(tmp_path, storage, executor_env):
    in_worker = isinstance(executor_env[0], rs.Executor.Parallel)
    repo = _repo(
        tmp_path,
        storage,
        SHOP.assets(name="shop")(load_shop),
        executor=executor_env[0],
    )
    result = repo.materialize()

    assert result.success
    customers = _rows(
        tmp_path, "SELECT id, name, pid FROM shop_data.customers ORDER BY id"
    )
    assert [row[:2] for row in customers] == [(1, "ann"), (2, "bob")]
    assert {pid != os.getpid() for *_, pid in customers} == {in_worker}
    assert _rows(
        tmp_path, "SELECT value FROM shop_data.customers__tags ORDER BY value"
    ) == [("new",), ("new",), ("vip",)]
    assert _rows(
        tmp_path, "SELECT id, customer_id, amount FROM shop_data.order_rows ORDER BY id"
    ) == [(10, 1, 5.5), (11, 2, 7.25)]
    assert _rows(
        tmp_path, "SELECT customer_id, note FROM shop_data.notes ORDER BY customer_id"
    ) == [(1, "hello ann"), (2, "hello bob")]

    [(load_id,)] = _rows(
        tmp_path, "SELECT DISTINCT _dlt_load_id FROM shop_data.customers"
    )
    metadata = _materializations(storage, result.run_id)
    assert set(metadata) == {*OUTPUTS, "bystander"}
    assert {key: kind for key, (kind, _) in metadata["shop_customers"].items()} == {
        "dlt/dataset": "Text",
        "dlt/load_id": "Text",
        "dlt/rows_loaded": "Int",
        "dlt/row_counts": "Json",
        "dlt/columns": "Json",
    }

    customers_meta = _values(metadata["shop_customers"])
    assert customers_meta["dlt/load_id"] == load_id
    assert customers_meta["dlt/dataset"] == "shop_data"
    assert customers_meta["dlt/rows_loaded"] == 2
    assert json.loads(customers_meta["dlt/row_counts"]) == {
        "customers": 2,
        "customers__tags": 3,
    }
    assert json.loads(customers_meta["dlt/columns"]) == {
        "customers": {
            "id": "bigint",
            "name": "text",
            "pid": "bigint",
            "_dlt_load_id": "text",
            "_dlt_id": "text",
        },
        "customers__tags": {
            "value": "text",
            "_dlt_root_id": "text",
            "_dlt_parent_id": "text",
            "_dlt_list_idx": "bigint",
            "_dlt_id": "text",
        },
    }

    orders_meta = _values(metadata["shop_orders"])
    assert orders_meta["dlt/load_id"] == load_id
    assert orders_meta["dlt/rows_loaded"] == 2
    assert json.loads(orders_meta["dlt/row_counts"]) == {"order_rows": 2}
    assert json.loads(orders_meta["dlt/columns"])["order_rows"]["amount"] == "double"
    assert json.loads(_values(metadata["shop_notes"])["dlt/row_counts"]) == {"notes": 2}


def test_selected_outputs_load_only_their_resources(tmp_path, storage, executor_env):
    repo = _repo(
        tmp_path,
        storage,
        SHOP.assets(name="shop")(load_shop),
        executor=executor_env[0],
    )
    result = repo.materialize(selection=["shop_notes", "bystander"])

    assert result.success
    tables = _tables(tmp_path)
    assert "notes" in tables
    assert not tables & {"customers", "customers__tags", "order_rows"}
    assert _rows(
        tmp_path, "SELECT customer_id, note FROM shop_data.notes ORDER BY customer_id"
    ) == [(1, "hello ann"), (2, "hello bob")]

    metadata = _materializations(storage, result.run_id)
    assert set(metadata) == {"shop_notes", "bystander"}
    assert _values(metadata["shop_notes"])["dlt/rows_loaded"] == 2


def test_load_runs_the_given_source(tmp_path, storage, executor_env):
    repo = _repo(
        tmp_path,
        storage,
        SHOP.assets(name="shop")(load_api_shop),
        executor=executor_env[0],
        resources={"api": ShopApi(region="us")},
    )

    assert repo.materialize().success
    assert _rows(tmp_path, "SELECT DISTINCT region FROM shop_data.order_rows") == [
        ("us",)
    ]


def test_extract_failure_fails_every_selected_output(tmp_path, storage, executor_env):
    repo = _repo(
        tmp_path,
        storage,
        SHOP.assets(name="shop")(load_broken_shop),
        executor=executor_env[0],
    )
    result = repo.materialize(raise_on_error=False)

    assert not result.success
    failures = _failures(storage, result.run_id)
    assert set(failures) == OUTPUTS
    assert all("orders API is down" in error for error in failures.values())
    assert set(_materializations(storage, result.run_id)) == {"bystander"}
    assert not _tables(tmp_path) & {"customers", "order_rows", "notes"}


def test_failed_load_job_fails_every_output_by_default(tmp_path, storage, executor_env):
    repo = _repo(
        tmp_path,
        storage,
        SHOP.assets(name="shop")(load_shop),
        executor=executor_env[0],
    )
    assert repo.materialize().success
    with duckdb.connect(str(tmp_path / "warehouse.duckdb")) as con:
        con.execute("DROP TABLE shop_data.order_rows")

    result = repo.materialize(raise_on_error=False)

    assert not result.success
    failures = _failures(storage, result.run_id)
    assert set(failures) == OUTPUTS
    assert all("step=load" in error for error in failures.values())
    assert set(_materializations(storage, result.run_id)) == {"bystander"}


def test_failed_load_job_keeps_the_loaded_resources(tmp_path, storage, executor_env):
    repo = _repo(
        tmp_path,
        storage,
        SHOP.assets(name="shop")(load_shop),
        executor=executor_env[0],
        pipeline_name="partial",
    )
    assert repo.materialize().success
    with duckdb.connect(str(tmp_path / "warehouse.duckdb")) as con:
        con.execute("DROP TABLE shop_data.order_rows")

    result = repo.materialize(raise_on_error=False)

    assert not result.success
    failures = _failures(storage, result.run_id)
    assert set(failures) == {"shop_orders"}
    assert "DestinationHasFailedJobs" in failures["shop_orders"]
    metadata = _materializations(storage, result.run_id)
    assert set(metadata) == {"shop_customers", "shop_notes", "bystander"}
    load_ids = _rows(
        tmp_path, "SELECT DISTINCT _dlt_load_id FROM shop_data.notes ORDER BY 1"
    )
    assert len(load_ids) == 2
    assert _values(metadata["shop_notes"])["dlt/load_id"] == load_ids[-1][0]
    assert _rows(tmp_path, "SELECT count(*) FROM shop_data.notes") == [(4,)]
    assert "order_rows" not in _tables(tmp_path)


@pytest.mark.parametrize("restore", [True, False], ids=["restore", "no-restore"])
def test_pending_packages_are_aborted_before_a_run(tmp_path, storage, restore):
    pending = Warehouse(directory=str(tmp_path), restore=restore).pipeline()
    pending.extract(feed(title="old"))
    pending.normalize()
    assert pending.list_normalized_load_packages()

    repo = _repo(
        tmp_path, storage, FEED.assets(name="feed")(load_feed), restore=restore
    )

    assert repo.materialize().success
    # The abort rewinds the incremental cursor, so the run extracts the rows again.
    assert _rows(tmp_path, "SELECT id, title FROM shop_data.posts ORDER BY id") == [
        (1, "new"),
        (2, "new"),
        (3, "new"),
    ]
    after = Warehouse(directory=str(tmp_path), restore=restore).pipeline()
    assert not after.list_normalized_load_packages()


def test_sink_keeps_the_incremental_state(tmp_path, storage, executor_env):
    repo = _repo(
        tmp_path,
        storage,
        FEED.assets(name="feed")(load_feed),
        executor=executor_env[0],
        sink=True,
    )

    rows_loaded = []
    for _ in range(2):
        result = repo.materialize()
        assert result.success
        posts = _materializations(storage, result.run_id)["feed_posts"]
        rows_loaded.append(_values(posts)["dlt/rows_loaded"])

    assert rows_loaded == [3, 0]
    with open(tmp_path / "sink.jsonl", encoding="utf-8") as file:
        assert [json.loads(line) for line in file] == [
            {"id": 1, "title": "new"},
            {"id": 2, "title": "new"},
            {"id": 3, "title": "new"},
        ]


def test_partition_builds_the_source(tmp_path, storage):
    assets = SHOP.assets(
        name="shop", partitions_def=rs.PartitionsDefinition.static_(["eu", "us"])
    )(load_regional_shop)
    repo = _repo(tmp_path, storage, assets)

    for region in ("eu", "us"):
        assert repo.materialize(
            selection=["shop_orders"], partition_key=rs.PartitionKey.single(region)
        ).success

    assert _rows(
        tmp_path,
        "SELECT region, id, amount FROM shop_data.order_rows ORDER BY region, id",
    ) == [("eu", 10, 5.5), ("eu", 11, 7.25), ("us", 10, 5.5), ("us", 11, 7.25)]
    for region in ("eu", "us"):
        event = storage.get_latest_materialization("shop_orders", region)
        assert event is not None
        assert _entries(event)["dlt/rows_loaded"] == ("Int", 2)


def test_asset_def_fn_shapes_the_outputs(tmp_path, storage):
    def asset_def(resource) -> rs.AssetDef:
        return output_def(
            resource,
            name=f"raw_{resource.name}",
            group="sales" if resource.name == "orders" else None,
            deps=[rs.AssetDef.dep("crm_export")]
            if resource.name == "customers"
            else [],
        )

    raw = DltSourceAssets(shop(), asset_def_fn=asset_def)

    @raw.assets(name="raw", group="raw")
    def raw_shop(context: rs.AssetExecutionContext, warehouse: Warehouse):
        yield from raw.load(context, warehouse.pipeline())

    @rs.Asset
    def crm_export() -> rs.Materialization:
        return rs.Materialization()

    assert [
        (output.name, output.group, output.metadata["dlt/resource"])
        for output in raw_shop.output_defs
    ] == [
        ("raw_customers", "raw", "customers"),
        ("raw_orders", "sales", "orders"),
        ("raw_notes", "raw", "notes"),
    ]

    repo = _repo(tmp_path, storage, crm_export, raw_shop)
    result = repo.materialize(selection=["raw_customers"], include_upstream=True)

    assert result.success
    assert set(_materializations(storage, result.run_id)) == {
        "crm_export",
        "raw_customers",
    }
    assert _rows(tmp_path, "SELECT id FROM shop_data.customers ORDER BY id") == [
        (1,),
        (2,),
    ]


def test_downstream_reads_the_table_with_an_io_handler(tmp_path, storage, executor_env):
    warehouse = DuckDBResource(database=str(tmp_path / "warehouse.duckdb"))
    assets = SHOP_TABLES.assets(
        name="shop", io_handler=DuckDBIOHandler(resource=warehouse)
    )(load_shop_tables)

    @rs.Asset(io_handler=executor_env[1]())
    def loaded(shop_customers: pa.Table, shop_orders: pa.Table) -> dict:
        customers = shop_customers.select(["id", "name"]).sort_by("id")
        orders = shop_orders.select(["id", "customer_id", "amount"]).sort_by("id")
        return {
            "customer_columns": shop_customers.column_names,
            "customers": customers.to_pylist(),
            "orders": orders.to_pylist(),
        }

    assert {output.name: output.metadata for output in SHOP_TABLES.output_defs} == {
        "shop_customers": {
            "dlt/resource": "customers",
            "dlt/table": "customers",
            "duckdb/schema": "shop_data",
            "duckdb/table": "customers",
        },
        "shop_orders": {
            "dlt/resource": "orders",
            "dlt/table": "orderRows",
            "duckdb/schema": "shop_data",
            "duckdb/table": "order_rows",
        },
        "shop_notes": {
            "dlt/resource": "notes",
            "dlt/table": "notes",
            "duckdb/schema": "shop_data",
            "duckdb/table": "notes",
        },
    }

    repo = _repo(tmp_path, storage, assets, loaded, executor=executor_env[0])

    assert repo.materialize().success
    # the root table: dlt loads the `tags` lists into shop_data.customers__tags
    assert repo.load_node("loaded") == {
        "customer_columns": ["id", "name", "pid", "_dlt_load_id", "_dlt_id"],
        "customers": [{"id": 1, "name": "ann"}, {"id": 2, "name": "bob"}],
        "orders": [
            {"id": 10, "customer_id": 1, "amount": 5.5},
            {"id": 11, "customer_id": 2, "amount": 7.25},
        ],
    }


def test_source_assets_errors():
    with pytest.raises(TypeError, match="source must be a dlt source, .* got Dlt"):
        DltSourceAssets(shop)
    with pytest.raises(ValueError, match="'shop' has no selected resources"):
        DltSourceAssets(shop().with_resources())
    with pytest.raises(
        ValueError,
        match="'customers' and 'orders' both map to the output 'same'; "
        "pass an asset_def_fn that names them apart",
    ):
        DltSourceAssets(shop(), asset_def_fn=lambda r: output_def(r, name="same"))
    with pytest.raises(ValueError, match="'customers' has no name"):
        DltSourceAssets(shop(), asset_def_fn=lambda r: rs.AssetDef())


def test_load_errors(tmp_path):
    context = rs.AssetExecutionContext(
        "shop", is_multi_asset=True, output_selection=sorted(OUTPUTS)
    )
    pipeline = Warehouse(directory=str(tmp_path)).pipeline()

    with pytest.raises(
        TypeError, match="pipeline must be a dlt pipeline, .* got method"
    ):
        next(SHOP.load(context, Warehouse(directory=str(tmp_path)).pipeline))
    with pytest.raises(TypeError, match="source must be a dlt source, got list"):
        next(SHOP.load(context, pipeline, []))
    with pytest.raises(
        ValueError,
        match=r"the dlt source 'stale' has no resources \['customers', 'notes', 'orders'\]",
    ):
        next(SHOP.load(context, pipeline, stale()))
    assert not os.path.exists(tmp_path / "warehouse.duckdb")
