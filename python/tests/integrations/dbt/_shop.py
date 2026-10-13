"""rivers definitions over the ``shop`` dbt project, shared by the dbt tests.

``raw.payments``, the project's source, is written by the ``payments`` asset.
"""

import json
import os
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

import rivers as rs
from rivers.integrations.dbt import DbtNode, DbtProject, DbtResource
from rivers.integrations.duckdb import DuckDBIOHandler, DuckDBResource

SHOP = Path(__file__).parent / "shop"
PAYMENTS = ["5", "7", "3"]
DAYS = rs.PartitionsDefinition.static_(["2024-01-01", "2024-01-02"])
SHOP_OUTPUTS = {
    "raw_customers",
    "raw_orders",
    "stg_customers",
    "stg_orders",
    "stg_payments",
    "customers",
    "customers_snapshot",
}


def write_profile(project_dir: Path) -> None:
    """Point the project's profile at ``warehouse.duckdb`` next to the project."""
    database = (project_dir.parent / "warehouse.duckdb").as_posix()
    (project_dir / "profiles.yml").write_text(
        "shop:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: duckdb\n"
        f"      path: '{database}'\n"
        "      threads: 4\n"
    )


def source_asset(source: DbtNode) -> str:
    """The rivers asset that loads a dbt source: the one named like its table."""
    return source.name


def warehouse(project_dir: Path) -> DuckDBResource:
    return DuckDBResource(database=str(project_dir.parent / "warehouse.duckdb"))


def payments_asset(project_dir: Path, amounts: list[str] = PAYMENTS) -> rs.SingleAsset:
    """The asset that loads ``raw.payments``; amounts are text, cast by ``stg_payments``."""

    @rs.Asset(
        io_handler=DuckDBIOHandler(resource=warehouse(project_dir), schema_name="raw")
    )
    def payments() -> pa.Table:
        return pa.table({"order_id": [10, 11, 12], "amount": amounts})

    return payments


def shop_assets(
    project_dir: Path, *, is_async: bool = False, **kwargs: Any
) -> rs.MultiAsset:
    """Every resource but ``daily_orders``, built with ``dbt build``.

    The body is a generator, or with ``is_async`` an async generator. Each
    output records the ``pid`` of the process that ran dbt.
    """
    project = DbtProject(
        project_dir, profiles_dir=project_dir, source_asset_fn=source_asset
    )
    define = project.assets(exclude="daily_orders", **kwargs)

    if is_async:

        @define
        async def shop(context: rs.AssetExecutionContext, dbt: DbtResource):
            context.add_output_metadata({"pid": os.getpid()})
            async for materialization in dbt.cli(["build"], context):
                yield materialization

        return shop

    @define
    def shop(context: rs.AssetExecutionContext, dbt: DbtResource):
        context.add_output_metadata({"pid": os.getpid()})
        yield from dbt.cli(["build"], context)

    return shop


def daily_assets(project_dir: Path, **kwargs: Any) -> rs.MultiAsset:
    """``daily_orders``, partitioned by day: the day goes to dbt as a var."""
    project = DbtProject(project_dir, profiles_dir=project_dir)

    @project.assets(select="daily_orders", partitions_def=DAYS, **kwargs)
    def daily(context: rs.AssetExecutionContext, dbt: DbtResource):
        context.add_output_metadata({"pid": os.getpid()})
        day = json.dumps({"day": context.partition_key})
        yield from dbt.cli(["build", "--vars", day], context)

    return daily


def sibling_asset(dep: str, **kwargs: Any) -> rs.SingleAsset:
    """A step beside a dbt step: two steps in one level run in parallel workers."""

    @rs.Asset(deps=[rs.AssetDef.dep(dep)], **kwargs)
    def sibling() -> rs.Materialization:
        return rs.Materialization()

    return sibling


def repository(
    project_dir: Path,
    assets: list,
    storage,
    executor=None,
    dbt: DbtResource | None = None,
) -> rs.CodeRepository:
    repo = rs.CodeRepository(
        assets=assets,
        resources={
            "dbt": dbt or DbtResource(project_dir=project_dir, profiles_dir=project_dir)
        },
        default_executor=executor or rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    return repo


def rows(project_dir: Path, sql: str) -> list[tuple]:
    with duckdb.connect(
        str(project_dir.parent / "warehouse.duckdb"), read_only=True
    ) as con:
        return con.sql(sql).fetchall()


def tables(project_dir: Path) -> set[str]:
    return {
        f"{schema}.{table}"
        for schema, table in rows(
            project_dir,
            "SELECT table_schema, table_name FROM information_schema.tables",
        )
    }


def materializations(storage, run_id: str) -> dict[str, dict[str, Any]]:
    """The metadata values of each asset the run materialized, by asset name."""
    return {
        event.asset_key: {key: _decode(raw)[1] for key, raw in event.metadata}
        for event in storage.get_events_for_run(run_id)
        if event.event_type == "Materialization"
    }


def failures(storage, run_id: str) -> dict[str, str]:
    """The error of each asset that failed in the run, by asset name."""
    return {
        event.asset_key: dict(event.metadata)["error"]
        for event in storage.get_events_for_run(run_id)
        if event.event_type == "StepFailure"
    }


def metadata_kinds(storage, run_id: str, asset: str) -> dict[str, str]:
    """The kind of each metadata value of the asset's materialization, e.g. ``"Duration"``."""
    [event] = [
        event
        for event in storage.get_events_for_run(run_id)
        if event.event_type == "Materialization" and event.asset_key == asset
    ]
    return {key: _decode(raw)[0] for key, raw in event.metadata}


def _decode(raw: str) -> tuple[str, Any]:
    [(kind, body)] = json.loads(raw).items()
    return kind, body.get("value", body)
