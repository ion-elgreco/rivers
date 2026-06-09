"""Tests for tasks integrated into the dependency graph via CodeRepository and Job."""

import rivers as rs


def test_task_depends_on_asset():
    """A Task that depends on an Asset receives the asset's output."""

    @rs.Asset
    def source_data() -> int:
        return 42

    @rs.Task
    def process(source_data: int) -> str:
        return f"processed: {source_data}"

    repo = rs.CodeRepository(
        assets=[source_data],
        tasks=[process],
        jobs=[
            rs.Job(
                name="pipeline",
                assets=[source_data, process],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("pipeline").execute()
    assert repo.load_node("source_data") == 42
    assert repo.load_node("process") == "processed: 42"


def test_asset_depends_on_task():
    """An Asset that depends on a Task receives the task's output."""

    @rs.Task
    def compute() -> int:
        return 10

    @rs.Asset
    def downstream(compute: int) -> int:
        return compute * 2

    repo = rs.CodeRepository(
        assets=[downstream],
        tasks=[compute],
        jobs=[
            rs.Job(
                name="pipeline",
                assets=[compute, downstream],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("pipeline").execute()
    assert repo.load_node("compute") == 10
    assert repo.load_node("downstream") == 20


def test_bash_task_as_root():
    """A BashTask with no deps executes and downstream asset receives its output."""

    greet = rs.BashTask(name="greet", command="echo hello")

    @rs.Asset
    def use_greeting(greet: str) -> str:
        return f"greeting: {greet}"

    repo = rs.CodeRepository(
        assets=[use_greeting],
        tasks=[greet],
        jobs=[
            rs.Job(
                name="pipeline",
                assets=[greet, use_greeting],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("pipeline").execute()
    assert repo.load_node("greet") == "hello"
    assert repo.load_node("use_greeting") == "greeting: hello"


def test_mixed_chain_asset_task_asset():
    """Asset → Task → Asset chain works correctly."""

    @rs.Asset
    def raw() -> int:
        return 5

    @rs.Task
    def transform(raw: int) -> int:
        return raw * 3

    @rs.Asset
    def final_result(transform: int) -> str:
        return f"result={transform}"

    repo = rs.CodeRepository(
        assets=[raw, final_result],
        tasks=[transform],
        jobs=[
            rs.Job(
                name="pipeline",
                assets=[raw, transform, final_result],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("pipeline").execute()
    assert repo.load_node("raw") == 5
    assert repo.load_node("transform") == 15
    assert repo.load_node("final_result") == "result=15"


def test_task_standalone_no_deps():
    """A Task with no dependencies runs standalone."""

    @rs.Task
    def standalone() -> str:
        return "done"

    repo = rs.CodeRepository(
        assets=[],
        tasks=[standalone],
        jobs=[
            rs.Job(
                name="pipeline",
                assets=[standalone],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("pipeline").execute()
    assert repo.load_node("standalone") == "done"


def test_bash_task_standalone():
    """A BashTask with no dependencies runs standalone in a job."""

    echo = rs.BashTask(name="echo_task", command="echo world")

    repo = rs.CodeRepository(
        assets=[],
        tasks=[echo],
        jobs=[
            rs.Job(
                name="pipeline",
                assets=[echo],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("pipeline").execute()
    assert repo.load_node("echo_task") == "world"


def test_multiple_tasks_in_chain():
    """Multiple tasks chained together: Task → Task → Asset."""

    @rs.Task
    def step_one() -> int:
        return 1

    @rs.Task
    def step_two(step_one: int) -> int:
        return step_one + 1

    @rs.Asset
    def output(step_two: int) -> str:
        return f"final={step_two}"

    repo = rs.CodeRepository(
        assets=[output],
        tasks=[step_one, step_two],
        jobs=[
            rs.Job(
                name="pipeline",
                assets=[step_one, step_two, output],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("pipeline").execute()
    assert repo.load_node("step_one") == 1
    assert repo.load_node("step_two") == 2
    assert repo.load_node("output") == "final=2"


def test_task_with_context():
    """A Task can receive AssetExecutionContext."""

    @rs.Task
    def my_task(context: rs.AssetExecutionContext) -> str:
        return f"name={context.asset_name}"

    repo = rs.CodeRepository(
        assets=[],
        tasks=[my_task],
        jobs=[
            rs.Job(
                name="pipeline",
                assets=[my_task],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("pipeline").execute()
    assert repo.load_node("my_task") == "name=my_task"


def test_task_with_task_execution_context():
    """A Task can receive TaskExecutionContext."""

    @rs.Task
    def my_task(context: rs.TaskExecutionContext) -> str:
        return f"name={context.task_name}"

    repo = rs.CodeRepository(
        assets=[],
        tasks=[my_task],
        jobs=[
            rs.Job(
                name="pipeline",
                assets=[my_task],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("pipeline").execute()
    assert repo.load_node("my_task") == "name=my_task"


def test_task_execution_context_properties():
    """TaskExecutionContext exposes task_name, tags, and partition helpers."""

    @rs.Task(tags=["fast"])
    def my_task(context: rs.TaskExecutionContext) -> dict:
        return {
            "task_name": context.task_name,
            "tags": context.tags,
            "has_partition_key": context.has_partition_key,
            "repr": repr(context),
        }

    repo = rs.CodeRepository(
        assets=[],
        tasks=[my_task],
        jobs=[
            rs.Job(
                name="pipeline",
                assets=[my_task],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("pipeline").execute()
    out = repo.load_node("my_task")
    assert out["task_name"] == "my_task"
    assert out["tags"] == ["fast"]
    assert out["has_partition_key"] is False
    assert out["repr"] == "TaskExecutionContext(task_name='my_task')"


def test_task_execution_context_with_partition():
    """TaskExecutionContext receives partition context when task is partitioned."""
    parts = rs.PartitionsDefinition.static_(["x", "y"])

    @rs.Task(partitions_def=parts)
    def my_task(context: rs.TaskExecutionContext) -> str:
        return f"key={context.partition_key}"

    repo = rs.CodeRepository(
        assets=[],
        tasks=[my_task],
        jobs=[
            rs.Job(
                name="pipeline",
                assets=[my_task],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("pipeline").execute(partition_key=rs.PartitionKey.single("x"))
    assert (
        repo.load_node("my_task", partition_key=rs.PartitionKey.single("x")) == "key=x"
    )


def test_task_execution_context_log():
    """TaskExecutionContext.log returns a logger with code-repo.tasks prefix."""
    import logging

    @rs.Task
    def my_task(context: rs.TaskExecutionContext) -> str:
        log = context.log
        assert isinstance(log, logging.Logger)
        assert log.name == "code-repo.tasks.my_task"
        return "ok"

    repo = rs.CodeRepository(
        assets=[],
        tasks=[my_task],
        jobs=[
            rs.Job(
                name="pipeline",
                assets=[my_task],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("pipeline").execute()
    assert repo.load_node("my_task") == "ok"


def test_task_execution_context_with_deps():
    """TaskExecutionContext works alongside upstream dependencies."""

    @rs.Asset
    def source() -> int:
        return 42

    @rs.Task
    def process(context: rs.TaskExecutionContext, source: int) -> str:
        return f"{context.task_name}: {source}"

    repo = rs.CodeRepository(
        assets=[source],
        tasks=[process],
        jobs=[
            rs.Job(
                name="pipeline",
                assets=[source, process],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("pipeline").execute()
    assert repo.load_node("process") == "process: 42"


# ---------------------------------------------------------------------------
# Partitioned tasks
# ---------------------------------------------------------------------------


def test_partitioned_task_receives_partition_context():
    """A partitioned Task receives PartitionContext via AssetExecutionContext."""
    parts = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Task(partitions_def=parts)
    def my_task(context: rs.AssetExecutionContext) -> str:
        return f"key={context.partition_key}"

    repo = rs.CodeRepository(
        assets=[],
        tasks=[my_task],
        jobs=[
            rs.Job(
                name="pipeline",
                assets=[my_task],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("pipeline").execute(partition_key=rs.PartitionKey.single("a"))
    assert (
        repo.load_node("my_task", partition_key=rs.PartitionKey.single("a")) == "key=a"
    )


def test_partitioned_task_chain_with_asset():
    """Partitioned asset → partitioned task chain with Identity mapping."""
    parts = rs.PartitionsDefinition.static_(["x", "y"])

    @rs.Asset(partitions_def=parts)
    def source(context: rs.AssetExecutionContext) -> str:
        return f"src-{context.partition_key}"

    @rs.Task(partitions_def=parts)
    def process(context: rs.AssetExecutionContext, source: str) -> str:
        return f"processed({source})-{context.partition_key}"

    repo = rs.CodeRepository(
        assets=[source],
        tasks=[process],
        jobs=[
            rs.Job(
                name="pipeline",
                assets=[source, process],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    pk = rs.PartitionKey.single("x")
    repo.get_job("pipeline").execute(partition_key=pk)
    assert repo.load_node("source", partition_key=pk) == "src-x"
    assert repo.load_node("process", partition_key=pk) == "processed(src-x)-x"


def test_unpartitioned_task_with_all_partitions_mapping():
    """Unpartitioned task depends on partitioned asset via AllPartitions."""
    parts = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(partitions_def=parts)
    def source() -> int:
        return 1

    @rs.Task(partition_mapping={"source": rs.PartitionMapping.all_partitions()})
    def aggregate(source: int) -> str:
        return f"got={source}"

    repo = rs.CodeRepository(
        assets=[source],
        tasks=[aggregate],
    )
    repo.resolve()


def test_partitioned_task_materialize():
    """Partitioned task can be materialized with partition_key via repo.materialize."""
    parts = rs.PartitionsDefinition.static_(["p1", "p2"])

    @rs.Task(partitions_def=parts)
    def compute(context: rs.AssetExecutionContext) -> str:
        return f"computed-{context.partition_key}"

    @rs.Asset(partitions_def=parts)
    def sink(context: rs.AssetExecutionContext, compute: str) -> str:
        return f"sink({compute})"

    repo = rs.CodeRepository(assets=[sink], tasks=[compute])
    repo.resolve()
    result = repo.materialize(
        ["compute", "sink"], partition_key=rs.PartitionKey.single("p1")
    )
    assert result.success
    pk = rs.PartitionKey.single("p1")
    assert repo.load_node("compute", partition_key=pk) == "computed-p1"
    assert repo.load_node("sink", partition_key=pk) == "sink(computed-p1)"


def test_dynamic_partitioned_task(storage):
    """Task with dynamic partitions_def works correctly."""
    dyn = rs.PartitionsDefinition.dynamic("tenants")

    @rs.Task(partitions_def=dyn)
    def process(context: rs.AssetExecutionContext) -> str:
        return f"tenant={context.partition_key}"

    repo = rs.CodeRepository(assets=[], tasks=[process])
    repo.resolve(storage=storage)
    storage.add_dynamic_partitions("tenants", ["acme"])
    result = repo.materialize(["process"], partition_key=rs.PartitionKey.single("acme"))
    assert result.success
    assert (
        repo.load_node("process", partition_key=rs.PartitionKey.single("acme"))
        == "tenant=acme"
    )


def test_task_context_repr():
    """TaskExecutionContext repr includes task_name."""
    ctx = rs.TaskExecutionContext("my_task")
    assert repr(ctx) == "TaskExecutionContext(task_name='my_task')"
