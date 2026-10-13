"""dbt builds run through rivers: rows in the warehouse, and what rivers records.

Tests that take ``run_with`` run with both executors. The parallel executor runs
a step in a worker only when its level has another step, so these runs add a
sibling step beside the dbt step.
"""

import json
import os

import pytest

pytest.importorskip("dbt.version")
pytest.importorskip("duckdb")
pa = pytest.importorskip("pyarrow")

import rivers as rs  # noqa: E402
from rivers.integrations.dbt import DbtProject, DbtResource  # noqa: E402
from rivers.integrations.duckdb import DuckDBIOHandler  # noqa: E402

from ._shop import (  # noqa: E402
    DAYS,
    PAYMENTS,
    SHOP_OUTPUTS,
    SourceTranslator,
    daily_assets,
    failures,
    materializations,
    metadata_kinds,
    payments_asset,
    repository,
    rows,
    shop_assets,
    sibling_asset,
    tables,
    warehouse,
)


def _shop_repo(shop, storage, run_with, amounts=PAYMENTS) -> rs.CodeRepository:
    assets = [
        payments_asset(shop, amounts),
        shop_assets(shop, io_handler=run_with.io_handler()),
        sibling_asset("payments", io_handler=run_with.io_handler()),
    ]
    return repository(shop, assets, storage, run_with.executor)


def _ran_in_worker(metadata: dict) -> bool:
    [pid] = {metadata[name]["pid"] for name in metadata if name in SHOP_OUTPUTS}
    return pid != os.getpid()


def test_build_materializes_every_output(shop, storage, run_with):
    repo = _shop_repo(shop, storage, run_with)

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
    assert _ran_in_worker(metadata) == run_with.in_worker
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


def test_build_logs_dbt_output(shop, storage, caplog):
    repo = repository(shop, [payments_asset(shop), shop_assets(shop)], storage)

    with caplog.at_level("INFO"):
        assert repo.materialize().success

    messages = [(record.levelname, record.getMessage()) for record in caplog.records]
    assert any(
        level == "INFO" and "OK created sql table model main.customers" in message
        for level, message in messages
    )
    assert any(
        level == "WARNING" and "warn_large_orders" in message
        for level, message in messages
    )


def test_a_downstream_model_runs_alone(shop, storage, run_with):
    repo = _shop_repo(shop, storage, run_with)
    assert repo.materialize().success

    # Edit an upstream table by hand: rebuilding it would undo the edit.
    with warehouse(shop).get_connection() as con:
        con.execute("UPDATE main.stg_payments SET amount = 100 WHERE order_id = 10")
    result = repo.materialize(["customers", "sibling"])

    assert result.success
    metadata = materializations(storage, result.run_id)
    assert set(metadata) == {"customers", "sibling"}
    assert _ran_in_worker(metadata) == run_with.in_worker
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


def test_a_failed_test_fails_the_output_it_tests(shop, storage, run_with):
    repo = _shop_repo(shop, storage, run_with, amounts=["5", "-4", "3"])

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
    assert _ran_in_worker(metadata) == run_with.in_worker
    # dbt built the table before its test failed.
    assert rows(shop, "SELECT amount FROM stg_payments ORDER BY order_id") == [
        (5,),
        (-4,),
        (3,),
    ]


def test_tests_the_definition_excludes_stay_excluded_in_a_subset(shop, storage):
    payments = payments_asset(shop, amounts=["5", "-4", "3"])
    project = DbtProject(shop, profiles_dir=shop)

    @project.assets(
        exclude="daily_orders assert_payments_positive", translator=SourceTranslator()
    )
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


def test_downstream_assets_load_models_through_the_io_handler(shop, storage):
    shop_def = shop_assets(shop, io_handler=DuckDBIOHandler(resource=warehouse(shop)))

    @rs.Asset
    def paid_total(customers: pa.Table) -> int:
        return int(sum(customers.column("paid").to_pylist()))

    repo = repository(shop, [payments_asset(shop), shop_def, paid_total], storage)

    assert repo.materialize().success
    assert repo.load_node("paid_total") == 15
