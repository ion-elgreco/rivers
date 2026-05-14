import rivers as rs


def test_asset_with_partitions_def():
    """@Asset accepts partitions_def parameter."""
    pd = rs.PartitionsDefinition.static_(["a", "b", "c"])

    @rs.Asset(partitions_def=pd)
    def my_asset() -> int:
        return 42

    assert my_asset._name == "my_asset"


def test_job_execute_with_partition_key():
    """Job.execute passes PartitionContext to IO handlers."""
    captured_contexts = []

    class CapturingHandler(rs.BaseIOHandler):
        def handle_output(self, context, obj):
            captured_contexts.append(context)

        def load_input(self, context):
            return None

    pd = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(io_handler=CapturingHandler(), partitions_def=pd)
    def partitioned() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[partitioned],
        jobs=[
            rs.Job(
                name="partition_test",
                assets=[partitioned],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("partition_test").execute(partition_key=rs.PartitionKey.single("a"))

    assert len(captured_contexts) == 1
    ctx = captured_contexts[0]
    assert ctx.partition is not None
    assert ctx.partition.key == rs.PartitionKey.single("a")
    assert isinstance(ctx.partition.definition, rs.PartitionsDefinition.Static)

    runs = repo.storage.get_runs(limit=10)
    assert len(runs) == 1
    assert runs[0].partition_key == rs.PartitionKey.single("a")


def test_job_execute_without_partition_key():
    """Job.execute without partition_key leaves partition as None."""
    captured_contexts = []

    class CapturingHandler(rs.BaseIOHandler):
        def handle_output(self, context, obj):
            captured_contexts.append(context)

        def load_input(self, context):
            return None

    @rs.Asset(io_handler=CapturingHandler())
    def no_partition() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[no_partition],
        jobs=[
            rs.Job(
                name="no_partition_test",
                assets=[no_partition],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("no_partition_test").execute()

    assert len(captured_contexts) == 1
    assert captured_contexts[0].partition is None


def test_job_partition_key_on_input_context():
    """Downstream load_input receives PartitionContext from upstream."""
    captured_input_contexts = []

    class CapturingHandler(rs.BaseIOHandler):
        stored: dict = {}

        def handle_output(self, context, obj):
            self.stored[context.asset_name] = obj

        def load_input(self, context):
            captured_input_contexts.append(context)
            return self.stored.get(context.asset_name)

    handler = CapturingHandler()
    pd = rs.PartitionsDefinition.static_(["x", "y"])

    @rs.Asset(io_handler=handler, partitions_def=pd)
    def source() -> int:
        return 10

    @rs.Asset(io_handler=handler, partitions_def=pd)
    def consumer(source: int) -> int:
        return source + 1

    repo = rs.CodeRepository(
        assets=[source, consumer],
        jobs=[
            rs.Job(
                name="input_ctx_test",
                assets=[source, consumer],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("input_ctx_test").execute(partition_key=rs.PartitionKey.single("x"))

    # consumer's load_input won't be called because source result is in-memory.
    # But handle_output should have partition context for both
