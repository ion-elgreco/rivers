"""Minimal pipeline for K8s integration tests.

Three jobs exercise different execution paths:
  - k8s_inprocess_job: assets override to in_process executor via metadata
  - k8s_step_job: Kubernetes step executor with S3-backed PickleIOHandler (full K8s flow)
  - k8s_graph_job: graph asset with `rivers/node/executor=in_process` so the
    graph asset gets one outer step pod and its internal tasks run in-process
    within that pod (the "outer pod / inner in-process" pattern documented
    in `docs/guides/graph-assets.md`).

The repo's default executor is Kubernetes. The in-process job assets use
metadata={"rivers/executor": "in_process"} to override per-asset.

Configured with RunQueueConfig + RunBackendConfig.kubernetes() so that
gRPC Materialize/ExecuteJob calls go through the daemon's run coordinator,
which creates Run CRs via K8sRunBackend.
"""

import os
import time

import obstore.store
from rivers import (
    Asset,
    CodeRepository,
    Compute,
    ComputeEscalation,
    Executor,
    InMemoryIOHandler,
    Job,
    Output,
    PickleIOHandler,
    RetryOn,
    RetryPolicy,
    RunBackendConfig,
    RunQueueConfig,
    Task,
)

S3_ENDPOINT = os.environ.get("RIVERS_S3_ENDPOINT", "http://minio.rivers.svc:9000")
S3_BUCKET = os.environ.get("RIVERS_S3_BUCKET", "rivers-io")

s3_store = obstore.store.S3Store(
    S3_BUCKET,
    endpoint_url=S3_ENDPOINT,
    access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "rivers"),
    secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "riverstest"),
    region="us-east-1",
    skip_signature=False,
    virtual_hosted_style_request=False,
    client_options={"allow_http": "true"},
)
s3_io = PickleIOHandler(store=s3_store, prefix="k8s-test")
mem_io = InMemoryIOHandler()

INPROCESS_META = {"rivers/executor": "in_process"}


# --- In-process job (assets override executor via metadata) ---


@Asset(io_handler=mem_io, metadata=INPROCESS_META)
def source_data():
    return Output(value={"records": 42, "timestamp": time.time()})


@Asset(io_handler=mem_io, metadata=INPROCESS_META)
def transform_data(source_data: dict):
    return Output(value={"processed": True, "count": source_data["records"]})


@Asset(io_handler=mem_io, metadata=INPROCESS_META)
def final_report(transform_data: dict):
    return Output(value={"status": "complete", "processed": transform_data["processed"]})


k8s_inprocess_job = Job(
    name="k8s_inprocess_job",
    assets=[source_data, transform_data, final_report],
)


# --- K8s step executor job (full flow with S3 IO) ---


@Asset(io_handler=s3_io)
def s3_source():
    return Output(value={"records": 100, "origin": "s3_test"})


@Asset(io_handler=s3_io)
def s3_transform(s3_source: dict):
    return Output(value={"transformed": True, "count": s3_source["records"]})


@Asset(io_handler=s3_io)
def s3_report(s3_transform: dict):
    return Output(value={"status": "complete", "transformed": s3_transform["transformed"]})


k8s_step_job = Job(
    name="k8s_step_job",
    assets=[s3_source, s3_transform, s3_report],
)


# --- Failing job (in-process; asset always raises) ---


@Asset(io_handler=mem_io, metadata=INPROCESS_META)
def always_fails():
    raise RuntimeError("intentional failure for k8s integration test")


k8s_failing_job = Job(
    name="k8s_failing_job",
    assets=[always_fails],
)


# --- Slow job (in-process; sleeps long enough for timeout/cancel/delete tests) ---


@Asset(io_handler=mem_io, metadata=INPROCESS_META)
def slow_asset():
    time.sleep(120)
    return Output(value={"done": True})


k8s_slow_job = Job(
    name="k8s_slow_job",
    assets=[slow_asset],
)


# --- Resume job (in-process; slow enough that an executor kill lands mid-run
# — a too-fast run completes inside the kill window and stores its outcome,
# which the operator honors instead of restarting) ---


@Asset(io_handler=mem_io, metadata=INPROCESS_META)
def resume_slow():
    time.sleep(20)
    return Output(value={"resumed": True})


k8s_resume_job = Job(
    name="k8s_resume_job",
    assets=[resume_slow],
)


# --- Graph asset with internal tasks running in-process ---
#
# This proves the "outer pod / inner in-process" pattern end-to-end on K8s:
# the graph asset itself runs as one step pod (via the default kubernetes
# executor), and its internal tasks run in-process inside that pod thanks
# to `rivers/node/executor=in_process`. Internal task outputs are still
# persisted through `s3_io` so a downstream task in the graph can read
# from the upstream one's S3 object.


@Task(io_handler=s3_io)
def graph_inner_load() -> dict:
    return {"records": 7}


@Task(io_handler=s3_io)
def graph_inner_transform(graph_inner_load: dict) -> dict:
    return {"records": graph_inner_load["records"], "doubled": graph_inner_load["records"] * 2}


@Asset.from_graph(
    name="graph_pipeline",
    io_handler=s3_io,
    metadata={"rivers/node/executor": "in_process"},
)
def graph_pipeline():
    return graph_inner_transform(graph_inner_load())


k8s_graph_job = Job(
    name="k8s_graph_job",
    assets=[graph_pipeline],
)


# --- Retry jobs (K8s step executor; see tests/integration/kubernetes/test_k8s_retry.py) ---


@Asset(io_handler=s3_io, retry=RetryPolicy(max_retries=2))
def retry_always_fails():
    raise RuntimeError("intentional failure, retried until the budget runs out")


k8s_retry_exhausted_job = Job(
    name="k8s_retry_exhausted_job",
    assets=[retry_always_fails],
)


def _allocate_mib(mib: int) -> bytearray:
    """Commit real pages (zero-page COW never charges the cgroup)."""
    buf = bytearray(mib * 1024 * 1024)
    for i in range(0, len(buf), 4096):
        buf[i] = 1
    return buf


@Asset(
    io_handler=s3_io,
    retry=RetryPolicy(
        max_retries=2,
        retry_on=RetryOn.TRANSIENT,
        escalate=ComputeEscalation(factor=2.0, max_memory="1Gi"),
    ),
)
def oom_hungry():
    # ~250MiB touched: over the 256Mi base limit, comfortably under the
    # escalated 512Mi — OOMKilled on attempt 1, succeeds on attempt 2.
    buf = _allocate_mib(250)
    return Output(value={"allocated_mib": len(buf) // (1024 * 1024)})


k8s_oom_escalation_job = Job(
    name="k8s_oom_escalation_job",
    assets=[oom_hungry],
)


@Asset(io_handler=s3_io)
def oom_no_retry():
    # No retry policy: the OOM-killed pod writes no event, so this run only
    # terminates because the poll falls back to the Job status.
    buf = _allocate_mib(250)
    return Output(value={"allocated_mib": len(buf) // (1024 * 1024)})


k8s_oom_no_retry_job = Job(
    name="k8s_oom_no_retry_job",
    assets=[oom_no_retry],
)


@Asset(io_handler=s3_io, retry=RetryPolicy(max_retries=1, retry_on=[ValueError]))
def exc_match_listed():
    raise ValueError("listed exception type — retried once, then budget spent")


k8s_exc_listed_job = Job(
    name="k8s_exc_listed_job",
    assets=[exc_match_listed],
)


@Asset(io_handler=s3_io, retry=RetryPolicy(max_retries=1, retry_on=[ConnectionError]))
def exc_match_unlisted():
    raise ValueError("unlisted exception type — fails without retrying")


k8s_exc_unlisted_job = Job(
    name="k8s_exc_unlisted_job",
    assets=[exc_match_unlisted],
)


# --- Per-asset compute (step pod sized by the asset, not the executor) ---


@Asset(io_handler=s3_io, compute=Compute(cpu="300m", memory="384Mi"))
def sized_step():
    return Output(value={"sized": True})


k8s_compute_job = Job(
    name="k8s_compute_job",
    assets=[sized_step],
)


all_assets = [
    source_data, transform_data, final_report,
    s3_source, s3_transform, s3_report,
    always_fails, slow_asset, resume_slow,
    graph_pipeline,
    retry_always_fails, oom_hungry, oom_no_retry,
    exc_match_listed, exc_match_unlisted,
    sized_step,
]

all_tasks = [graph_inner_load, graph_inner_transform]

repo = CodeRepository(
    assets=all_assets,
    tasks=all_tasks,
    jobs=[
        k8s_inprocess_job, k8s_step_job, k8s_failing_job, k8s_slow_job, k8s_resume_job,
        k8s_graph_job,
        k8s_retry_exhausted_job, k8s_oom_escalation_job, k8s_oom_no_retry_job,
        k8s_exc_listed_job, k8s_exc_unlisted_job,
        k8s_compute_job,
    ],
    default_executor=Executor.kubernetes(
        worker_cpu="250m",
        worker_memory="256Mi",
    ),
    run_queue=RunQueueConfig(max_concurrent_runs=3),
    run_backend=RunBackendConfig.kubernetes(
        run_cpu="250m",
        run_memory="256Mi",
        worker_cpu="250m",
        worker_memory="256Mi",
    ),
)
