"""dbt builds run through rivers: rows in the warehouse, and what rivers records.

Tests that take ``run_with`` run with both executors. The parallel executor runs
a step in a worker only when its level has another step, so these runs add a
sibling step beside the dbt step. Tests that take ``is_async`` also run an async
body, which both executors run in this process.
"""

import json
import os
import shutil
import threading

import pytest

pytest.importorskip("dbt.version")
pytest.importorskip("duckdb")
pa = pytest.importorskip("pyarrow")

import rivers as rs  # noqa: E402
from rivers.integrations.dbt import DbtNode, DbtProject, DbtResource  # noqa: E402
from rivers.integrations.duckdb import DuckDBIOHandler  # noqa: E402

from ._shop import (  # noqa: E402
    DAYS,
    PAYMENTS,
    SHOP_OUTPUTS,
    daily_assets,
    failures,
    materializations,
    metadata_kinds,
    payments_asset,
    repository,
    rows,
    shop_assets,
    sibling_asset,
    source_asset,
    tables,
    warehouse,
)


def _shop_repo(
    shop, storage, run_with, amounts=PAYMENTS, dbt=None, is_async=False
) -> rs.CodeRepository:
    assets = [
        payments_asset(shop, amounts),
        shop_assets(shop, is_async=is_async, io_handler=run_with.io_handler()),
        sibling_asset("payments", io_handler=run_with.io_handler()),
    ]
    return repository(shop, assets, storage, run_with.executor, dbt)


def _ran_in_worker(metadata: dict) -> bool:
    [pid] = {metadata[name]["pid"] for name in metadata if name in SHOP_OUTPUTS}
    return pid != os.getpid()


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_build_materializes_every_output(shop, storage, run_with, is_async):
    repo = _shop_repo(shop, storage, run_with, is_async=is_async)

    result = repo.materialize()

    assert result.success
    assert rows(shop, "SELECT id, name, paid FROM customers ORDER BY id") == [
        (1, "Ann", 12),
        (2, "Bob", 3),
        (3, "Cid", 0),
    ]
    assert rows(
        shop, "SELECT id, name FROM snapshots.customers_snapshot ORDER BY id"
    ) == [(1, "Ann"), (2, "Bob"), (3, "Cid")]

    metadata = materializations(storage, result.run_id)
    assert set(metadata) == SHOP_OUTPUTS | {"payments", "sibling"}
    assert _ran_in_worker(metadata) == (run_with.in_worker and not is_async)
    assert metadata["raw_customers"]["dbt/rows_affected"] == 3
    assert metadata["raw_customers"]["dbt/message"] == "INSERT 3"
    assert metadata["customers"]["dbt/execution_time"] > 0
    assert len({metadata[name]["dbt/invocation_id"] for name in SHOP_OUTPUTS}) == 1
    assert json.loads(metadata["stg_customers"]["dbt/tests"]) == {
        "not_null_stg_customers_id": "pass",
        "unique_stg_customers_id": "pass",
    }
    assert json.loads(metadata["stg_orders"]["dbt/tests"]) == {
        "warn_large_orders": "warn"
    }
    assert "dbt/tests" not in metadata["raw_orders"]


@pytest.mark.parametrize(
    "config",
    [None, {"resources": {"dbt": {"profile": "shop"}}}],
    ids=["defined", "overridden"],
)
def test_a_resource_from_the_project_builds_in_its_target(
    shop, storage, run_with, config
):
    database = (shop.parent / "warehouse.duckdb").as_posix()
    with (shop / "profiles.yml").open("a") as profile:
        profile.write(
            f"    prod:\n      type: duckdb\n      path: '{database}'\n"
            "      schema: prod\n      threads: 4\n"
        )
    project = DbtProject(shop, profiles_dir=shop, target="prod")
    repo = _shop_repo(shop, storage, run_with, dbt=DbtResource(project_dir=project))

    result = repo.materialize(config=config)

    assert result.success
    metadata = materializations(storage, result.run_id)
    assert _ran_in_worker(metadata) == run_with.in_worker
    assert rows(shop, "SELECT id, name, paid FROM prod.customers ORDER BY id") == [
        (1, "Ann", 12),
        (2, "Bob", 3),
        (3, "Cid", 0),
    ]
    assert "main.customers" not in tables(shop)


def test_a_run_reuses_the_parse_in_the_project_target_path(shop, storage, run_with):
    shutil.rmtree(shop / "target")
    project = DbtProject(
        shop, target_path="build", profiles_dir=shop, source_asset_fn=source_asset
    )
    project.prepare()

    @project.assets(exclude="daily_orders", io_handler=run_with.io_handler())
    def shop_def(context: rs.AssetExecutionContext, dbt: DbtResource):
        context.add_output_metadata({"pid": os.getpid()})
        yield from dbt.cli(["build"], context)

    assets = [
        payments_asset(shop),
        shop_def,
        sibling_asset("payments", io_handler=run_with.io_handler()),
    ]
    repo = repository(shop, assets, storage, run_with.executor)

    result = repo.materialize()

    assert result.success
    metadata = materializations(storage, result.run_id)
    assert _ran_in_worker(metadata) == run_with.in_worker
    log = "".join(row.logs or "" for row in storage.get_run_logs(result.run_id))
    assert "Running with dbt=" in log
    assert "Unable to do partial parsing" not in log


def test_build_records_typed_metadata(shop, storage):
    repo = repository(shop, [payments_asset(shop), shop_assets(shop)], storage)

    result = repo.materialize()

    assert metadata_kinds(storage, result.run_id, "raw_customers") == {
        "pid": "Int",
        "dbt/invocation_id": "Text",
        "dbt/execution_time": "Duration",
        "dbt/message": "Text",
        "dbt/rows_affected": "Int",
    }
    kinds = metadata_kinds(storage, result.run_id, "stg_customers")
    assert kinds["dbt/tests"] == "Json"


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_build_logs_dbt_output(shop, storage, caplog, is_async):
    assets = [payments_asset(shop), shop_assets(shop, is_async=is_async)]
    repo = repository(shop, assets, storage)

    with caplog.at_level("INFO"):
        result = repo.materialize()

    assert result.success
    messages = [(record.levelname, record.getMessage()) for record in caplog.records]
    assert any(
        level == "INFO" and "OK created sql table model main.customers" in message
        for level, message in messages
    )
    assert any(
        level == "WARNING" and "warn_large_orders" in message
        for level, message in messages
    )
    log = "".join(row.logs or "" for row in storage.get_run_logs(result.run_id))
    assert "OK created sql table model main.customers" in log


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_a_downstream_model_runs_alone(shop, storage, run_with, is_async):
    repo = _shop_repo(shop, storage, run_with, is_async=is_async)
    assert repo.materialize().success

    # Edit an upstream table by hand: rebuilding it would undo the edit.
    with warehouse(shop).get_connection() as con:
        con.execute("UPDATE main.stg_payments SET amount = 100 WHERE order_id = 10")
    result = repo.materialize(["customers", "sibling"])

    assert result.success
    metadata = materializations(storage, result.run_id)
    assert set(metadata) == {"customers", "sibling"}
    assert _ran_in_worker(metadata) == (run_with.in_worker and not is_async)
    assert rows(shop, "SELECT id, paid FROM customers ORDER BY id") == [
        (1, 107),
        (2, 3),
        (3, 0),
    ]


def test_include_upstream_follows_refs_through_ephemeral_models(shop, storage):
    repo = repository(shop, [payments_asset(shop), shop_assets(shop)], storage)

    result = repo.materialize(["customers"], include_upstream=True)

    assert result.success
    # customers reads stg_orders and stg_payments through the ephemeral order_totals.
    assert set(materializations(storage, result.run_id)) == {
        "payments",
        "raw_customers",
        "raw_orders",
        "stg_customers",
        "stg_orders",
        "stg_payments",
        "customers",
    }


def test_a_failed_model_fails_with_what_it_skipped(shop, storage, run_with):
    repo = _shop_repo(shop, storage, run_with, amounts=["5", "oops", "3"])

    result = repo.materialize(raise_on_error=False)

    assert not result.success
    metadata = materializations(storage, result.run_id)
    assert set(metadata) == {
        "payments",
        "sibling",
        "raw_customers",
        "raw_orders",
        "stg_customers",
        "stg_orders",
        "customers_snapshot",
    }
    assert _ran_in_worker(metadata) == run_with.in_worker
    failed = failures(storage, result.run_id)
    assert set(failed) == {"stg_payments", "customers"}
    assert "dbt build failed with exit code 1" in failed["customers"]
    assert "stg_payments: Runtime Error in model stg_payments" in failed["customers"]
    assert "customers: skipped" in failed["customers"]
    assert "main.customers" not in tables(shop)


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_a_failed_test_fails_the_output_it_tests(shop, storage, run_with, is_async):
    repo = _shop_repo(
        shop, storage, run_with, amounts=["5", "-4", "3"], is_async=is_async
    )

    result = repo.materialize(raise_on_error=False)

    assert not result.success
    failed = failures(storage, result.run_id)
    assert set(failed) == {"stg_payments", "customers"}
    assert (
        "assert_payments_positive: Got 1 result, configured to fail if != 0"
        in failed["stg_payments"]
    )
    metadata = materializations(storage, result.run_id)
    assert set(metadata) == SHOP_OUTPUTS - {"stg_payments", "customers"} | {
        "payments",
        "sibling",
    }
    assert _ran_in_worker(metadata) == (run_with.in_worker and not is_async)
    # dbt built the table before its test failed.
    assert rows(shop, "SELECT amount FROM stg_payments ORDER BY order_id") == [
        (5,),
        (-4,),
        (3,),
    ]


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_the_body_must_take_the_resource_as_a_parameter(
    shop, storage, run_with, is_async
):
    project = DbtProject(shop, profiles_dir=shop, source_asset_fn=source_asset)
    define = project.assets(exclude="daily_orders", io_handler=run_with.io_handler())

    def shop_def(context: rs.AssetExecutionContext):
        dbt = DbtResource(project_dir=project)
        yield from dbt.cli(["build"], context)

    async def async_shop_def(context: rs.AssetExecutionContext):
        dbt = DbtResource(project_dir=project)
        async for materialization in dbt.cli(["build"], context):
            yield materialization

    body = async_shop_def if is_async else shop_def
    assets = [
        payments_asset(shop),
        define(body),
        sibling_asset("payments", io_handler=run_with.io_handler()),
    ]
    repo = repository(shop, assets, storage, run_with.executor)

    result = repo.materialize(raise_on_error=False)

    assert not result.success
    error = (
        f"TypeError: '{body.__name__}' must take the DbtResource as a parameter, e.g. "
        "dbt: DbtResource; a DbtResource made in the body is not bound to its outputs"
    )
    assert failures(storage, result.run_id) == dict.fromkeys(SHOP_OUTPUTS, error)
    assert set(materializations(storage, result.run_id)) == {"payments", "sibling"}
    assert tables(shop) == {"raw.payments"}


def test_tests_the_definition_excludes_stay_excluded_in_a_subset(shop, storage):
    payments = payments_asset(shop, amounts=["5", "-4", "3"])
    project = DbtProject(shop, profiles_dir=shop, source_asset_fn=source_asset)

    @project.assets(exclude="daily_orders assert_payments_positive")
    def shop_def(context: rs.AssetExecutionContext, dbt: DbtResource):
        yield from dbt.cli(["build"], context)

    repo = repository(shop, [payments, shop_def], storage)

    result = repo.materialize(["payments", "stg_payments"])

    assert result.success
    assert set(materializations(storage, result.run_id)) == {
        "payments",
        "stg_payments",
    }
    assert rows(shop, "SELECT amount FROM stg_payments ORDER BY order_id") == [
        (5,),
        (-4,),
        (3,),
    ]


def test_partitions_pass_the_day_to_dbt(shop, storage, run_with):
    sibling = sibling_asset(
        "stg_orders", io_handler=run_with.io_handler(), partitions_def=DAYS
    )
    assets = [
        payments_asset(shop),
        shop_assets(shop, io_handler=run_with.io_handler()),
        daily_assets(shop, io_handler=run_with.io_handler()),
        sibling,
    ]
    repo = repository(shop, assets, storage, run_with.executor)
    assert repo.materialize(["stg_orders"], include_upstream=True).success

    for day in ("2024-01-02", "2024-01-01", "2024-01-02"):
        result = repo.materialize(
            ["daily_orders", "sibling"], partition_key=rs.PartitionKey.single(day)
        )
        assert result.success
        metadata = materializations(storage, result.run_id)
        assert set(metadata) == {"daily_orders", "sibling"}
        in_worker = metadata["daily_orders"]["pid"] != os.getpid()
        assert in_worker == run_with.in_worker

    assert rows(
        shop,
        "SELECT CAST(order_date AS VARCHAR), orders, amount FROM daily_orders ORDER BY 1",
    ) == [("2024-01-01", 1, 5), ("2024-01-02", 2, 10)]


def test_a_dep_on_another_definition_has_the_name_of_its_output(
    shop, storage, run_with
):
    project = DbtProject(
        shop,
        profiles_dir=shop,
        asset_def_fn=lambda node: node.output_def(name=f"shop_{node.asset_name}"),
        source_asset_fn=source_asset,
    )

    @project.assets(exclude="daily_orders", io_handler=run_with.io_handler())
    def shop_def(context: rs.AssetExecutionContext, dbt: DbtResource):
        context.add_output_metadata({"pid": os.getpid()})
        yield from dbt.cli(["build"], context)

    @project.assets(select="daily_orders", io_handler=run_with.io_handler())
    def daily_def(context: rs.AssetExecutionContext, dbt: DbtResource):
        context.add_output_metadata({"pid": os.getpid()})
        yield from dbt.cli(["build"], context)

    assets = [
        payments_asset(shop),
        shop_def,
        daily_def,
        sibling_asset("payments", io_handler=run_with.io_handler()),
        sibling_asset(
            "shop_stg_orders", name="daily_sibling", io_handler=run_with.io_handler()
        ),
    ]
    repo = repository(shop, assets, storage, run_with.executor)

    result = repo.materialize()

    assert result.success
    metadata = materializations(storage, result.run_id)
    assert set(metadata) == {f"shop_{name}" for name in SHOP_OUTPUTS} | {
        "shop_daily_orders",
        "payments",
        "sibling",
        "daily_sibling",
    }
    for name in ("shop_customers", "shop_daily_orders"):
        assert (metadata[name]["pid"] != os.getpid()) == run_with.in_worker
    built = [
        event.asset_key
        for event in storage.get_events_for_run(result.run_id)
        if event.event_type == "Materialization"
    ]
    assert built.index("shop_stg_orders") < built.index("shop_daily_orders")
    assert rows(
        shop,
        "SELECT CAST(order_date AS VARCHAR), orders, amount FROM daily_orders",
    ) == [("2024-01-01", 1, 5)]


def test_a_step_runs_when_the_project_functions_cannot_be_pickled(
    shop, storage, run_with
):
    lock = threading.Lock()

    def asset_def(node: DbtNode) -> rs.AssetDef:
        with lock:
            return node.output_def()

    project = DbtProject(
        shop, profiles_dir=shop, asset_def_fn=asset_def, source_asset_fn=source_asset
    )

    @project.assets(exclude="daily_orders", io_handler=run_with.io_handler())
    def shop_def(context: rs.AssetExecutionContext, dbt: DbtResource):
        context.add_output_metadata({"pid": os.getpid()})
        yield from dbt.cli(["build"], context)

    assets = [
        payments_asset(shop),
        shop_def,
        sibling_asset("payments", io_handler=run_with.io_handler()),
    ]
    repo = repository(shop, assets, storage, run_with.executor)

    result = repo.materialize(raise_on_error=False)

    assert failures(storage, result.run_id) == {}
    metadata = materializations(storage, result.run_id)
    assert set(metadata) == SHOP_OUTPUTS | {"payments", "sibling"}
    assert _ran_in_worker(metadata) == run_with.in_worker
    assert rows(shop, "SELECT id, name, paid FROM customers ORDER BY id") == [
        (1, "Ann", 12),
        (2, "Bob", 3),
        (3, "Cid", 0),
    ]


def test_downstream_assets_load_models_through_the_io_handler(shop, storage):
    shop_def = shop_assets(shop, io_handler=DuckDBIOHandler(resource=warehouse(shop)))

    @rs.Asset
    def paid_total(customers: pa.Table) -> int:
        return int(sum(customers.column("paid").to_pylist()))

    repo = repository(shop, [payments_asset(shop), shop_def, paid_total], storage)

    assert repo.materialize().success
    assert repo.load_node("paid_total") == 15
