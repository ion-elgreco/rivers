from datetime import datetime
from typing import Any

import rivers as rs

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DAILY_START = datetime(2024, 1, 1)
STATIC_KEYS = ["a", "b", "c"]


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
