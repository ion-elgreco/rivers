import sys

if sys.version_info >= (3, 11):
    from typing import reveal_type
else:
    from typing_extensions import reveal_type

import rivers as rs


class DuckHandler:
    def handle_output(self, context, obj):
        pass

    def load_input(self, context):
        return None


# Asset bare decorator
@rs.Asset
def bare():
    return 1


reveal_type(bare)


# Asset with parens
@rs.Asset(name="named")
def named():
    return 1


reveal_type(named)  # noqa: F821


# from_graph decorator
@rs.Asset
def inner():
    return 1


@rs.Asset.from_graph()
def g(inner: int) -> int:
    return inner


reveal_type(g)


# from_graph direct call
g2 = rs.Asset.from_graph(lambda inner: inner)
reveal_type(g2)


# from_multi decorator
@rs.Asset.from_multi(output_defs=[rs.AssetDef("x")])
def m():
    return {"x": 1}


reveal_type(m)

# from_multi direct call
m2 = rs.Asset.from_multi(lambda: {"x": 1}, output_defs=[rs.AssetDef("x")])
reveal_type(m2)


# external Direct call (no decorator)
ext = rs.Asset.external(name="ext", io_handler=DuckHandler())
reveal_type(ext)


# external Decorator usage
@rs.Asset.external(name="ext2", io_handler=DuckHandler())
def ext2():
    return None


reveal_type(ext2)


# Schedule as decorator (with args, no func)
@rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job")
def sched_decorated(context: rs.ScheduleEvaluationContext):
    return rs.RunRequest()


reveal_type(sched_decorated)

# Schedule direct construction (no func)
sched_plain = rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job")
reveal_type(sched_plain)


# Sensor as decorator (with args, no func)
@rs.Sensor(job_name="my_job")
def sens_decorated(context: rs.SensorEvaluationContext):
    return rs.RunRequest()


reveal_type(sens_decorated)

# Sensor direct construction (no func)
sens_plain = rs.Sensor(name="my_sensor")
reveal_type(sens_plain)


# load_node with type_hint returns T
repo = rs.CodeRepository(assets=[bare])
load_any = repo.load_node("bare")
reveal_type(load_any)

load_typed = repo.load_node("bare", type_hint=int)
reveal_type(load_typed)

load_typed_str = repo.load_node("bare", type_hint=str)
reveal_type(load_typed_str)
