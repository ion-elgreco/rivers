"""Runs and backfills on a PostgreSQL store, each in a schema of its own.

Skipped without ``RIVERS_TEST_POSTGRES_URL``.
"""

import asyncio
from urllib.parse import urlsplit, urlunsplit

import pytest
import rivers as rs
from rivers.exceptions import StorageError

from _helpers import daily_pd


def _assets(io_handler, is_async):
    if is_async:

        @rs.Asset(io_handler=io_handler)
        async def numbers() -> list[int]:
            await asyncio.sleep(0)
            return [1, 2, 3]

    else:

        @rs.Asset(io_handler=io_handler)
        def numbers() -> list[int]:
            return [1, 2, 3]

    @rs.Asset(io_handler=io_handler)
    def total(numbers: list[int]) -> int:
        return sum(numbers)

    return [numbers, total]


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_a_materialization_is_recorded(postgres_url, executor_env, is_async):
    executor, io_handler = executor_env
    storage = rs.Storage.open(postgres_url)
    assert storage.type == rs.StorageType.Postgres

    repo = rs.CodeRepository(
        assets=_assets(io_handler(), is_async), default_executor=executor
    )
    repo.resolve(storage=storage)
    result = repo.materialize()

    assert result.success
    (run,) = storage.get_runs()
    assert (run.run_id, run.status) == (result.run_id, "Success")
    for asset in ("numbers", "total"):
        events = {e.event_type for e in storage.get_events_for_asset(asset)}
        assert {"StepStart", "StepSuccess", "Materialization"} <= events
    assert sorted(r.asset_key for r in storage.get_asset_records()) == [
        "numbers",
        "total",
    ]


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_a_partitioned_backfill_completes(postgres_url, executor_env, is_async):
    executor, _ = executor_env
    if is_async:

        @rs.Asset(partitions_def=daily_pd())
        async def daily(context: rs.AssetExecutionContext) -> int:
            await asyncio.sleep(0)
            return 1

    else:

        @rs.Asset(partitions_def=daily_pd())
        def daily(context: rs.AssetExecutionContext) -> int:
            return 1

    storage = rs.Storage.open(postgres_url)
    repo = rs.CodeRepository(assets=[daily], default_executor=executor)
    repo.resolve(storage=storage)
    keys = [rs.PartitionKey.single(f"2024-01-1{day}") for day in range(3)]
    result = repo.backfill(selection=["daily"], partition_keys=keys, max_concurrency=2)

    assert result.status == "CompletedSuccess"
    assert (result.completed, result.failed) == (3, 0)
    assert sorted(str(k) for k in storage.get_materialized_partitions("daily")) == [
        "2024-01-10",
        "2024-01-11",
        "2024-01-12",
    ]
    assert repo.get_backfill(result.backfill_id).completed_partitions == 3


def test_a_wrong_password_shows_the_cause_without_the_password(postgres_url: str):
    parts = urlsplit(postgres_url)
    host = parts.netloc.rsplit("@", 1)[1]
    wrong = urlunsplit(
        parts._replace(netloc=f"{parts.username}:not-the-password@{host}")
    )

    with pytest.raises(StorageError) as error:
        rs.Storage.open(wrong)

    message = str(error.value)
    assert message.startswith(f"connecting to PostgreSQL at {parts.scheme}://"), message
    assert "password authentication failed" in message, message
    assert "not-the-password" not in message
