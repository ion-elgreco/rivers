"""Dagster code location used by the Kubernetes benchmark."""

import os

import dagster as dg

N_ASSETS = int(os.environ.get("BENCH_N_ASSETS", "10"))


def _make_asset(i):
    @dg.asset(name=f"asset_{i}")
    def _asset() -> int:
        return i

    return _asset


@dg.op
def noop_op():
    return 1


@dg.job
def noop_job():
    noop_op()


defs = dg.Definitions(assets=[_make_asset(i) for i in range(N_ASSETS)], jobs=[noop_job])
