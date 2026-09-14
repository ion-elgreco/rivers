"""rivers code location used by the Kubernetes benchmark.

``N`` no-op assets, the same workload `bench_defs.py` gives Dagster and
`bench_flows.py` gives Prefect, so "code location ready" measures the same job
in every cluster.

No ``from __future__ import annotations`` here. Under PEP 563 every annotation
becomes a string, and rivers then rejects a correctly typed asset with
"returned value of type 'int' but expected 'int'".
"""

import os

from rivers import Asset, CodeRepository

N_ASSETS = int(os.environ.get("BENCH_N_ASSETS", "10"))


def _make_asset(i: int):
    def _asset() -> int:
        return i

    _asset.__name__ = f"asset_{i}"
    return Asset(name=f"asset_{i}")(_asset)


repo = CodeRepository(assets=[_make_asset(i) for i in range(N_ASSETS)])
