"""A backfill of 127,200 partitions against a store with a 1 MiB message limit.

An embedded store has no message limit, so only a SurrealDB server shows a
message that grows with the number of partitions. The tests run when
``RIVERS_TEST_SURREAL_ENDPOINT`` names a WebSocket endpoint, for example
``ws://127.0.0.1:18000``.
"""

import os
import time
import uuid
from datetime import datetime, timedelta

import pytest
import rivers as rs
from rivers._core import AutomationDaemon

ENDPOINT = os.environ.get("RIVERS_TEST_SURREAL_ENDPOINT")
MESSAGE_LIMIT = 1 << 20

ZONES = [f"{zone:03d}" for zone in range(1, 266)]
START = datetime(2024, 1, 1)
HOURS = 480
PARTITIONS = len(ZONES) * HOURS

pytestmark = [
    pytest.mark.skipif(
        not ENDPOINT,
        reason="needs a SurrealDB server: set RIVERS_TEST_SURREAL_ENDPOINT",
    ),
    # 265 runs against a server: about 50 s alone, more beside other tests.
    pytest.mark.serial,
    pytest.mark.timeout(600),
]


@pytest.fixture
def limited_storage():
    """A store on the test server, in a database of its own."""
    return rs.Storage.connect(
        ENDPOINT,
        database=f"t{uuid.uuid4().hex}",
        max_message_size=MESSAGE_LIMIT,
    )


def _trips_repo(storage, seen_batches):
    @rs.Asset(
        name="trips",
        partitions_def=rs.PartitionsDefinition.multi(
            {
                "hour": rs.PartitionsDefinition.hourly(
                    start=START, end=START + timedelta(hours=HOURS)
                ),
                "zone": rs.PartitionsDefinition.static_(ZONES),
            }
        ),
        backfill_strategy=rs.BackfillStrategy.per_dimension(
            multi_run=["zone"], single_run=["hour"]
        ),
    )
    def trips(context: rs.AssetExecutionContext) -> None:
        zones = {key.keys["zone"][0] for key in context.partition.keys}
        seen_batches.append((sorted(zones), len(context.partition.keys)))

    repo = rs.CodeRepository(
        assets=[trips],
        default_executor=rs.Executor.in_process(),
        run_queue=rs.RunQueueConfig(max_concurrent_runs=8, dequeue_interval="100ms"),
    )
    repo.resolve(storage=storage)
    return repo


def _all_hours():
    first = START.strftime("%Y-%m-%dT%H:00")
    last = (START + timedelta(hours=HOURS - 1)).strftime("%Y-%m-%dT%H:00")
    return rs.PartitionKeyRange.multi({"hour": (first, last)})


def _assert_every_partition_completed(status, seen_batches):
    assert status.status == "CompletedSuccess", status
    assert status.total_partitions == PARTITIONS
    assert status.completed_partitions == PARTITIONS
    assert (status.failed_partitions, status.canceled_partitions) == (0, 0)
    assert len(status.run_ids) == len(ZONES)
    assert sorted(seen_batches) == [([zone], HOURS) for zone in ZONES]


def test_backfill_of_127_200_partitions_stays_under_the_message_limit(
    limited_storage,
):
    seen_batches = []
    repo = _trips_repo(limited_storage, seen_batches)

    result = repo.backfill(
        selection=["trips"], partition_range=_all_hours(), block=True
    )

    assert (result.num_partitions, result.num_runs) == (PARTITIONS, len(ZONES))
    _assert_every_partition_completed(
        repo.get_backfill(result.backfill_id), seen_batches
    )


def test_queued_backfill_of_127_200_partitions_stays_under_the_message_limit(
    limited_storage,
):
    seen_batches = []
    repo = _trips_repo(limited_storage, seen_batches)

    launched = repo.backfill(
        selection=["trips"], partition_range=_all_hours(), block=False
    )
    assert launched.num_partitions == PARTITIONS
    assert launched.num_runs == len(ZONES)

    daemon = AutomationDaemon(
        repo=repo, storage=limited_storage, condition_eval_interval="1s"
    )
    daemon.start()
    try:
        deadline = time.monotonic() + 480
        status = repo.get_backfill(launched.backfill_id)
        while (
            status.status in ("Requested", "InProgress") and time.monotonic() < deadline
        ):
            time.sleep(1)
            status = repo.get_backfill(launched.backfill_id)
    finally:
        daemon.stop()

    _assert_every_partition_completed(status, seen_batches)
