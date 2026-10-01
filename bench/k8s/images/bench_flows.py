"""Prefect deployments used by the Kubernetes benchmark.

``N`` no-op flows, the same workload `bench_defs.py` gives Dagster and
`bench_pipeline.py` gives rivers.

Prefect has no code location object. A flow reaches a worker by being
registered as a deployment against the server, which is a client call rather
than a cluster resource — so this module is run once after the worker is up,
and the time it takes is Prefect's "code location ready".
"""

import os
import subprocess
import sys

from prefect import flow

N_ASSETS = int(os.environ.get("BENCH_N_ASSETS", "10"))
WORK_POOL = os.environ.get("BENCH_WORK_POOL", "bench")
# `build=False` still wants a reference to record on the deployment. The worker
# never pulls it here; nothing in this benchmark runs a flow.
IMAGE = os.environ.get("BENCH_FLOW_IMAGE", "prefecthq/prefect:3.8.5-python3.11")


def _make_flow(i: int):
    @flow(name=f"asset_{i}")
    def _flow() -> int:
        return i

    return _flow


flows = [_make_flow(i) for i in range(N_ASSETS)]


def ensure_work_pool() -> None:
    """Create the work pool if the worker has not yet.

    The worker creates it on startup, but its Deployment reports ready before
    that lands, so registering straight after would race it. `--overwrite`
    makes this safe either way.

    The pool must be `kubernetes`: a `process` pool rejects the image
    reference every deployment carries.
    """
    # `-m prefect` rather than the `prefect` script, so this works wherever
    # the interpreter is rather than wherever PATH points.
    subprocess.run(
        [
            sys.executable,
            "-m",
            "prefect",
            "work-pool",
            "create",
            WORK_POOL,
            "--type",
            "kubernetes",
            "--overwrite",
        ],
        check=True,
        stdout=sys.stderr,
    )


def register() -> int:
    """Register every flow as a deployment. Returns how many were registered."""
    ensure_work_pool()
    for one in flows:
        one.deploy(
            name="bench",
            work_pool_name=WORK_POOL,
            image=IMAGE,
            build=False,
            push=False,
            print_next_steps=False,
            ignore_warnings=True,
        )
    return len(flows)


if __name__ == "__main__":
    print(f"registered {register()} deployments", flush=True)
