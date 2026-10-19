"""A run's cost must not grow with the number of keys in a static definition."""

import statistics
import time

import pytest

import rivers as rs


def _per_run_seconds(storage, n_keys: int, launch: str) -> float:
    keys = [f"k{i:07d}" for i in range(n_keys)]

    @rs.Asset(name="events", partitions_def="keys")
    def events(context: rs.AssetExecutionContext) -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[events],
        partition_defs={"keys": rs.PartitionsDefinition.static_(keys)},
    )
    repo.resolve(storage=storage)
    targets = [rs.PartitionKey.single(k) for k in keys[:: n_keys // 10][:10]]

    def run(pk: rs.PartitionKey) -> None:
        if launch == "materialize":
            repo.materialize(selection=["events"], partition_key=pk)
        else:
            repo.backfill(selection=["events"], partition_keys=[pk], block=True)

    run(targets[0])
    samples = []
    for pk in targets:
        started = time.perf_counter()
        run(pk)
        samples.append(time.perf_counter() - started)
    repo.shutdown()
    return statistics.median(samples)


@pytest.mark.parametrize("launch", ["materialize", "backfill"])
def test_run_cost_independent_of_static_key_count(launch):
    small = _per_run_seconds(rs.testing.memory_storage(), 1_000, launch)
    large = _per_run_seconds(rs.testing.memory_storage(), 1_000_000, launch)
    assert large < small * 5, (
        f"1k keys: {small * 1000:.1f} ms, 1M keys: {large * 1000:.1f} ms"
    )
