"""A backfill's bookkeeping per partition must not grow with the backfill's size."""

import statistics

import rivers as rs


def _median_gap_seconds(tmp_path, n_partitions: int, tail: int) -> float:
    """Median idle time between consecutive child runs of one backfill.

    Args:
        tmp_path: Directory for the embedded store.
        n_partitions: Partitions in the backfill, one run each.
        tail: Number of trailing gaps to take the median of.

    Returns:
        The median time between one run ending and the next one starting, in seconds.
    """
    keys = [f"k{i:05d}" for i in range(n_partitions)]

    @rs.Asset(name="events", partitions_def="keys")
    def events(context: rs.AssetExecutionContext) -> int:
        return 1

    storage = rs.testing.embedded_storage(str(tmp_path / f"store-{n_partitions}"))
    repo = rs.CodeRepository(
        assets=[events],
        partition_defs={"keys": rs.PartitionsDefinition.static_(keys)},
    )
    repo.resolve(storage=storage)

    result = repo.backfill(
        selection=["events"],
        partition_keys=[rs.PartitionKey.single(k) for k in keys],
        block=True,
    )
    assert result.completed == n_partitions

    runs = sorted(storage.get_runs(limit=n_partitions + 10), key=lambda r: r.start_time)
    gaps = [b.start_time - a.end_time for a, b in zip(runs, runs[1:])]
    repo.shutdown()
    return statistics.median(gaps[-tail:]) / 1e9


def test_backfill_bookkeeping_independent_of_backfill_size(tmp_path):
    small = _median_gap_seconds(tmp_path, 100, tail=50)
    large = _median_gap_seconds(tmp_path, 1_500, tail=50)
    assert large < small * 3, (
        f"100 partitions: {small * 1000:.2f} ms between runs, "
        f"1,500 partitions: {large * 1000:.2f} ms"
    )
