"""Pipeline whose sensor evaluation is slow, so the daemon takes a while to
stop scheduling after a terminate signal.

Env vars:
    EVAL_MARKER — file appended to when a sensor evaluation starts
    EVAL_SLEEP  — seconds each sensor evaluation sleeps (default 4)
"""

import os
import time

import rivers as rs


class _NullIO(rs.BaseIOHandler):
    def handle_output(self, context, obj):
        pass

    def load_input(self, context):
        return None


@rs.Asset(io_handler=_NullIO())
def noop():
    return 1


job = rs.Job(name="noop_job", assets=[noop], executor=rs.Executor.in_process())


@rs.Sensor(
    job_name="noop_job", minimum_interval="1s", default_status=rs.SensorStatus.Running
)
def slow_sensor(context: rs.SensorEvaluationContext):
    with open(os.environ["EVAL_MARKER"], "a") as f:
        f.write("started\n")
    time.sleep(float(os.environ.get("EVAL_SLEEP", "4")))
    return rs.SkipReason("slow")


repo = rs.CodeRepository(assets=[noop], jobs=[job], sensors=[slow_sensor])
