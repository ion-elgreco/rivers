"""Dagster definitions loaded by the comparison benchmarks over a ModuleTarget.

Size and interval come from the environment so the same module serves every
point in a scale sweep.
"""

import os
import time

import dagster as dg

N_ASSETS = int(os.environ.get("BENCH_N_ASSETS", "1"))
N_SENSORS = int(os.environ.get("BENCH_N_SENSORS", "0"))
SENSOR_INTERVAL = int(os.environ.get("BENCH_SENSOR_INTERVAL", "30"))
# The parallel and cancellation benchmarks run a real multiprocess job, which
# Dagster rebuilds in each subprocess from this module. Their shape has to
# reach those children, and the environment is what crosses that boundary.
N_SLEEP = int(os.environ.get("BENCH_N_SLEEP", "0"))
SLEEP_MS = float(os.environ.get("BENCH_SLEEP_MS", "50"))
CANCEL_SLEEP_S = float(os.environ.get("BENCH_CANCEL_SLEEP_S", "60"))
# Partitions on the asset the backfill benchmark targets. The backfill daemon
# launches its runs through this code location, so the asset has to live here.
N_PARTITIONS = int(os.environ.get("BENCH_N_PARTITIONS", "0"))
# The log benchmark's line count and the exact line. Both arrive through the
# environment because this module is loaded by a code server whose working
# directory is this one, so it cannot import the benchmark harness.
N_LINES = int(os.environ.get("BENCH_N_LINES", "0"))
LOG_LINE = os.environ.get("BENCH_LOG_LINE", "bench log line")


@dg.op
def noop_op():
    pass


@dg.job
def noop_job():
    noop_op()


def _make_asset(i: int):
    @dg.asset(name=f"asset_{i}")
    def _asset() -> int:
        return i

    return _asset


def _make_sensor(i: int):
    @dg.sensor(
        name=f"sensor_{i}",
        job=noop_job,
        minimum_interval_seconds=SENSOR_INTERVAL,
        default_status=dg.DefaultSensorStatus.RUNNING,
    )
    def _sensor(_context: dg.SensorEvaluationContext):
        return dg.SkipReason("noop")

    return _sensor


def _make_sleep_asset(i: int):
    @dg.asset(name=f"sleep_{i}")
    def _asset() -> None:
        time.sleep(SLEEP_MS / 1000)

    return _asset


def parallel_job() -> dg.JobDefinition:
    """A multiprocess job over ``BENCH_N_SLEEP`` sleeping assets.

    `dagster.reconstructable` needs an importable module-level factory, because
    every executor subprocess rebuilds the job from it. That is why this lives
    here rather than in the driver, which runs as ``__main__``.
    """
    sleepers = [_make_sleep_asset(i) for i in range(N_SLEEP)]
    return dg.Definitions(
        assets=sleepers,
        jobs=[dg.define_asset_job("parallel", executor_def=dg.multiprocess_executor)],
    ).resolve_job_def("parallel")


@dg.op
def long_sleep_op() -> None:
    """One step long enough that a cancellation always lands mid-run."""
    time.sleep(CANCEL_SLEEP_S)


@dg.op
def log_lines_op() -> None:
    """Write ``BENCH_N_LINES`` lines to stdout, which the run's log capture picks up."""
    for _ in range(N_LINES):
        print(LOG_LINE)


@dg.job(name="log_job")
def log_job():
    """The job the log benchmark launches.

    Dagster persists step output only for a launched run, so this benchmark
    goes through the run launcher rather than `materialize`.
    """
    log_lines_op()


@dg.job(name="cancel_job")
def cancel_job():
    """The job the cancellation benchmark launches and then terminates.

    It is registered here rather than built in the driver because the run
    launcher starts it through this code location's gRPC server, which can only
    serve what the module exposes.
    """
    long_sleep_op()


def _partitioned_assets() -> list:
    """The backfill target: one asset over ``BENCH_N_PARTITIONS`` static keys."""
    if not N_PARTITIONS:
        return []
    keys = [f"key_{i:08d}" for i in range(N_PARTITIONS)]

    @dg.asset(name="partitioned", partitions_def=dg.StaticPartitionsDefinition(keys))
    def partitioned() -> int:
        return 1

    return [partitioned]


assets = [_make_asset(i) for i in range(N_ASSETS)] + _partitioned_assets()
sensors = [_make_sensor(i) for i in range(N_SENSORS)]

defs = dg.Definitions(
    assets=assets, jobs=[noop_job, cancel_job, log_job], sensors=sensors
)
