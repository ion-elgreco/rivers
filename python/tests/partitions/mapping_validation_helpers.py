from datetime import datetime, timedelta
from typing import Any

import pytest

import rivers as rs

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DAILY_START = datetime(2024, 1, 1)
STATIC_KEYS = ["a", "b", "c"]

# (definition, keys it has, keys it lacks). The lacking keys are off the grid,
# outside [start, end), or a real key written another way.
KEY_LOOKUP_GRIDS = [
    pytest.param(
        lambda: rs.PartitionsDefinition.static_(STATIC_KEYS),
        ["a", "c"],
        ["zz", "A"],
        id="static",
    ),
    pytest.param(
        lambda: rs.PartitionsDefinition.hourly(
            start=DAILY_START, end=DAILY_START + timedelta(hours=48)
        ),
        ["2024-01-01T05:00", "2024-01-02T23:00"],
        ["2024-01-01T05:30", "2024-01-03T00:00", "2023-12-31T23:00", "2024-1-01T05:00"],
        id="cron",
    ),
    pytest.param(
        lambda: rs.PartitionsDefinition.time_window(
            start=DAILY_START,
            interval_seconds=3600,
            end=DAILY_START + timedelta(hours=48),
        ),
        ["2024-01-01T05:00:00", "2024-01-02T23:00:00"],
        [
            "2024-01-01T05:30:00",
            "2024-01-03T00:00:00",
            "2023-12-31T23:00:00",
            "2024-1-01T05:00:00",
        ],
        id="interval",
    ),
    pytest.param(
        lambda: rs.PartitionsDefinition.time_window(
            start=DAILY_START,
            cron_schedule="0 9 * * 1-5",
            end=DAILY_START + timedelta(days=14),
            fmt="%Y-%m-%d %H:%M",
        ),
        ["2024-01-05 09:00", "2024-01-12 09:00"],
        ["2024-01-06 09:00", "2024-01-05 10:00", "2024-1-05 09:00"],
        id="weekdays",
    ),
]


def make_repo(assets, tasks=None, resolve=True):
    """Build and resolve a CodeRepository."""
    repo = rs.CodeRepository(assets=assets, tasks=tasks)
    if resolve:
        repo.resolve()
    return repo


def _identity_edge(down_def, up_def):
    """Upstream→downstream pair with no explicit mapping (Identity default)."""

    @rs.Asset(partitions_def=up_def)
    def upstream() -> Any:
        return 1

    @rs.Asset(partitions_def=down_def)
    def downstream(upstream: Any) -> Any:
        return upstream

    return [upstream, downstream]


def validate_mapped_edge(down_def, up_def, mapping):
    """Validate an upstream→downstream pair joined by ``mapping``."""

    @rs.Asset(partitions_def=up_def)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=down_def,
        deps=[rs.AssetDef.input("upstream", partition_mapping=mapping)],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    rs.CodeRepository(assets=[upstream, downstream]).validate()
