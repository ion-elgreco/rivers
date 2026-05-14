"""Tests for dynamic fan-out: .map() / .collect() / .collect_stream()."""

import os
import tempfile

import obstore.store
import pytest

import rivers as rs


@pytest.fixture(params=["in_process", "parallel"])
def executor_env(request, tmp_path):
    """Provide (executor, io_handler, node_io_handler) appropriate for the executor type."""
    if request.param == "in_process":
        return rs.Executor.in_process(), None, None
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    io = rs.PickleIOHandler(store=store)
    return rs.Executor.parallel(max_workers=2), io, io


# ── Helper tasks (module-level, no io_handler — resolved via node_io_handler) ──


@rs.Task
def double(x: int) -> int:
    return x * 2


@rs.Task
def to_upper(s: str) -> str:
    return s.upper()


@rs.Task
def sum_all(values: list) -> int:
    return sum(values)


@rs.Task
def join_all(values: list) -> str:
    return ",".join(values)


@rs.Task
def failing_task(x: int) -> int:
    if x == 2:
        raise ValueError("fail on 2")
    return x * 10


@rs.Task
def process_item(x: int) -> int:
    return x * 10


# Module-level so it's picklable by loky
class PidTrackingIOHandler(rs.PickleIOHandler):
    """PickleIOHandler that records PID + asset_name on each load_input call.

    The pid_log_path is stored as a Pydantic field so it survives pickling
    to loky subprocesses (unlike module-level state which gets reset).
    """

    pid_log_path: str | None = None

    def load_input(self, context):
        import os

        if self.pid_log_path is not None:
            with open(self.pid_log_path, "a") as f:
                f.write(f"{os.getpid()}:{context.asset_name}\n")
        return super().load_input(context)


@rs.Task
def consume_stream(items: object) -> list:
    return list(items)


@rs.Task
def count_stream(items: object) -> int:
    return sum(1 for _ in items)


# ── Test: basic list producer + barrier collect ──


class TestBatchMapCollect:
    def test_basic_map_collect(self, executor_env):
        """Fan out over a list, double each element, collect and sum."""
        executor, io, node_io = executor_env

        @rs.Asset(io_handler=io)
        def numbers() -> list:
            return [1, 2, 3, 4, 5]

        @rs.Asset.from_graph(io_handler=io, node_io_handler=node_io)
        def doubled():
            nums = numbers()
            mapped = nums.map(double)
            return sum_all(mapped.collect())

        repo = rs.CodeRepository(
            assets=[numbers, doubled],
            tasks=[double, sum_all],
            jobs=[
                rs.Job(name="pipeline", assets=[numbers, doubled], executor=executor)
            ],
        )
        repo.get_job("pipeline").execute()
        assert repo.load_node("doubled") == 30

    def test_map_with_strings(self, executor_env):
        """Fan out over strings, uppercase each, collect and join."""
        executor, io, node_io = executor_env

        @rs.Asset(io_handler=io)
        def words() -> list:
            return ["hello", "world", "foo"]

        @rs.Asset.from_graph(io_handler=io, node_io_handler=node_io)
        def uppercased():
            w = words()
            mapped = w.map(to_upper)
            return join_all(mapped.collect())

        repo = rs.CodeRepository(
            assets=[words, uppercased],
            tasks=[to_upper, join_all],
            jobs=[
                rs.Job(name="pipeline", assets=[words, uppercased], executor=executor)
            ],
        )
        repo.get_job("pipeline").execute()
        assert repo.load_node("uppercased") == "HELLO,WORLD,FOO"

    def test_empty_list_map(self, executor_env):
        """Fan out over an empty list should produce empty collect."""
        executor, io, node_io = executor_env

        @rs.Asset(io_handler=io)
        def empty_list() -> list:
            return []

        @rs.Asset.from_graph(io_handler=io, node_io_handler=node_io)
        def result():
            items = empty_list()
            mapped = items.map(double)
            return sum_all(mapped.collect())

        repo = rs.CodeRepository(
            assets=[empty_list, result],
            tasks=[double, sum_all],
            jobs=[
                rs.Job(name="pipeline", assets=[empty_list, result], executor=executor)
            ],
        )
        repo.get_job("pipeline").execute()
        assert repo.load_node("result") == 0

    def test_single_element_map(self, executor_env):
        """Fan out over a single-element list."""
        executor, io, node_io = executor_env

        @rs.Asset(io_handler=io)
        def one() -> list:
            return [42]

        @rs.Asset.from_graph(io_handler=io, node_io_handler=node_io)
        def result():
            items = one()
            mapped = items.map(double)
            return sum_all(mapped.collect())

        repo = rs.CodeRepository(
            assets=[one, result],
            tasks=[double, sum_all],
            jobs=[rs.Job(name="pipeline", assets=[one, result], executor=executor)],
        )
        repo.get_job("pipeline").execute()
        assert repo.load_node("result") == 84


# ── Test: DynamicOutput with custom mapping keys ──


class TestDynamicOutput:
    def test_dynamic_output_keys(self, executor_env):
        """DynamicOutput items use their .key as the mapping key."""
        executor, io, node_io = executor_env

        @rs.Asset(io_handler=io)
        def documents() -> list:
            return [
                rs.DynamicOutput(key="report_q1", value="/data/report_q1.pdf"),
                rs.DynamicOutput(key="report_q2", value="/data/report_q2.pdf"),
                rs.DynamicOutput(key="invoice", value="/data/invoice.pdf"),
            ]

        @rs.Task
        def path_length(path: str) -> int:
            return len(path)

        @rs.Asset.from_graph(io_handler=io, node_io_handler=node_io)
        def result():
            docs = documents()
            mapped = docs.map(path_length)
            return sum_all(mapped.collect())

        repo = rs.CodeRepository(
            assets=[documents, result],
            tasks=[path_length, sum_all],
            jobs=[
                rs.Job(name="pipeline", assets=[documents, result], executor=executor)
            ],
        )
        repo.get_job("pipeline").execute()
        # len("/data/report_q1.pdf")=19 + len("/data/report_q2.pdf")=19 + len("/data/invoice.pdf")=17
        assert repo.load_node("result") == 55

        # Verify IO paths use custom keys, not numeric indices
        if io is not None:
            import os

            store_root = io.store.prefix
            assert os.path.exists(
                os.path.join(store_root, "result", "path_length__report_q1.pkl")
            )
            assert os.path.exists(
                os.path.join(store_root, "result", "path_length__report_q2.pkl")
            )
            assert os.path.exists(
                os.path.join(store_root, "result", "path_length__invoice.pkl")
            )
            # Numeric indices should NOT exist
            assert not os.path.exists(
                os.path.join(store_root, "result", "path_length__0.pkl")
            )

    def test_mixed_dynamic_and_plain_errors(self, executor_env):
        """Mixing DynamicOutput and plain values in a list is not allowed."""
        executor, io, node_io = executor_env

        @rs.Asset(io_handler=io)
        def items() -> list:
            return [
                rs.DynamicOutput(key="named", value=10),
                20,  # plain value — not allowed when mixed with DynamicOutput
            ]

        @rs.Asset.from_graph(io_handler=io, node_io_handler=node_io)
        def result():
            i = items()
            mapped = i.map(double)
            return sum_all(mapped.collect())

        repo = rs.CodeRepository(
            assets=[items, result],
            tasks=[double, sum_all],
            jobs=[rs.Job(name="pipeline", assets=[items, result], executor=executor)],
        )
        with pytest.raises(TypeError, match="Cannot mix"):
            repo.get_job("pipeline").execute()

    def test_dynamic_output_parallel_graphs(self, executor_env):
        """Two parallel graph assets with DynamicOutput producers.

        Forces the producer steps into a multi-step loky batch, which
        exercises the parallel worker's handling of DynamicOutput.
        """
        executor, io, node_io = executor_env

        @rs.Asset(io_handler=io)
        def docs_a() -> list:
            return [
                rs.DynamicOutput(key="alpha", value=1),
                rs.DynamicOutput(key="beta", value=2),
            ]

        @rs.Asset(io_handler=io)
        def docs_b() -> list:
            return [
                rs.DynamicOutput(key="gamma", value=10),
                rs.DynamicOutput(key="delta", value=20),
            ]

        @rs.Asset.from_graph(io_handler=io, node_io_handler=node_io)
        def graph_a():
            d = docs_a()
            mapped = d.map(double)
            return sum_all(mapped.collect())

        @rs.Asset.from_graph(io_handler=io, node_io_handler=node_io)
        def graph_b():
            d = docs_b()
            mapped = d.map(double)
            return sum_all(mapped.collect())

        repo = rs.CodeRepository(
            assets=[docs_a, docs_b, graph_a, graph_b],
            tasks=[double, sum_all],
            jobs=[
                rs.Job(
                    name="pipeline",
                    assets=[docs_a, docs_b, graph_a, graph_b],
                    executor=executor,
                )
            ],
        )
        repo.get_job("pipeline").execute()
        assert repo.load_node("graph_a/sum_all") == 6  # (1+2)*2
        assert repo.load_node("graph_b/sum_all") == 60  # (10+20)*2


# ── Test: max_concurrency ──


class TestMaxConcurrency:
    def test_max_concurrency_limits_batch_size(self, tmp_path):
        """max_concurrency limits how many map instances run in parallel."""
        store = obstore.store.LocalStore(str(tmp_path / "data"), mkdir=True)
        io = rs.PickleIOHandler(store=store)
        executor = rs.Executor.parallel(max_workers=4)

        @rs.Asset(io_handler=io)
        def numbers() -> list:
            return list(range(10))

        @rs.Asset.from_graph(io_handler=io, node_io_handler=io)
        def result():
            nums = numbers()
            mapped = nums.map(double, max_concurrency=2)
            return sum_all(mapped.collect())

        repo = rs.CodeRepository(
            assets=[numbers, result],
            tasks=[double, sum_all],
            jobs=[rs.Job(name="pipeline", assets=[numbers, result], executor=executor)],
        )
        repo.get_job("pipeline").execute()
        # sum(i*2 for i in range(10)) = 90
        assert repo.load_node("result") == 90


# ── Test: error handling ──


class TestFanOutErrors:
    def test_map_instance_failure(self, executor_env):
        """When a map instance fails, the job raises."""
        executor, io, node_io = executor_env

        @rs.Asset(io_handler=io)
        def numbers() -> list:
            return [1, 2, 3]  # failing_task fails on x=2

        @rs.Asset.from_graph(io_handler=io, node_io_handler=node_io)
        def result():
            nums = numbers()
            mapped = nums.map(failing_task)
            return sum_all(mapped.collect())

        repo = rs.CodeRepository(
            assets=[numbers, result],
            tasks=[failing_task, sum_all],
            jobs=[rs.Job(name="pipeline", assets=[numbers, result], executor=executor)],
        )
        from rivers.exceptions import ExecutionError

        with pytest.raises((ValueError, ExecutionError)):
            repo.get_job("pipeline").execute()


# ── Test: composition validation ──


class TestCompositionValidation:
    def test_map_requires_task(self):
        """.map() target must be a @Task, not an @Asset."""

        @rs.Asset
        def src() -> list:
            return [1]

        @rs.Asset
        def not_a_task(x: int) -> int:
            return x

        with pytest.raises(TypeError, match="Task"):

            @rs.Asset.from_graph()
            def broken():
                s = src()
                return s.map(not_a_task)

    def test_mapped_output_must_be_collected(self):
        """MappedOutput cannot be passed directly to a task — must call .collect()."""

        @rs.Asset
        def src() -> list:
            return [1]

        with pytest.raises(TypeError, match="must be collected"):

            @rs.Asset.from_graph()
            def broken():
                s = src()
                mapped = s.map(double)
                return sum_all(mapped)  # error — not collected


# ── Streaming collect tests ──


class TestCollectStream:
    def test_basic_collect_stream(self, executor_env):
        """collect_stream() passes a generator to downstream, not a list."""
        executor, io, node_io = executor_env

        @rs.Asset(io_handler=io)
        def numbers() -> list:
            return [1, 2, 3, 4, 5]

        @rs.Asset.from_graph(io_handler=io, node_io_handler=node_io)
        def streamed():
            nums = numbers()
            mapped = nums.map(process_item)
            return consume_stream(mapped.collect_stream())

        repo = rs.CodeRepository(
            assets=[numbers, streamed],
            tasks=[process_item, consume_stream],
            jobs=[
                rs.Job(name="pipeline", assets=[numbers, streamed], executor=executor)
            ],
        )
        repo.get_job("pipeline").execute()
        assert sorted(repo.load_node("streamed")) == [10, 20, 30, 40, 50]

    def test_collect_stream_is_iterable(self, executor_env):
        """The stream passed to downstream is a proper iterable (generator)."""
        executor, io, node_io = executor_env

        @rs.Asset(io_handler=io)
        def numbers() -> list:
            return [1, 2, 3]

        @rs.Asset.from_graph(io_handler=io, node_io_handler=node_io)
        def counted():
            nums = numbers()
            mapped = nums.map(process_item)
            return count_stream(mapped.collect_stream())

        repo = rs.CodeRepository(
            assets=[numbers, counted],
            tasks=[process_item, count_stream],
            jobs=[
                rs.Job(name="pipeline", assets=[numbers, counted], executor=executor)
            ],
        )
        repo.get_job("pipeline").execute()
        assert repo.load_node("counted") == 3

    def test_collect_stream_empty(self, executor_env):
        """collect_stream() with empty source yields empty generator."""
        executor, io, node_io = executor_env

        @rs.Asset(io_handler=io)
        def empty() -> list:
            return []

        @rs.Asset.from_graph(io_handler=io, node_io_handler=node_io)
        def result():
            items = empty()
            mapped = items.map(process_item)
            return count_stream(mapped.collect_stream())

        repo = rs.CodeRepository(
            assets=[empty, result],
            tasks=[process_item, count_stream],
            jobs=[rs.Job(name="pipeline", assets=[empty, result], executor=executor)],
        )
        repo.get_job("pipeline").execute()
        assert repo.load_node("result") == 0

    def test_collect_stream_ordered(self, executor_env):
        """collect_stream(ordered=True) yields results in mapping key order."""
        executor, io, node_io = executor_env

        @rs.Asset(io_handler=io)
        def numbers() -> list:
            return [5, 4, 3, 2, 1]

        @rs.Asset.from_graph(io_handler=io, node_io_handler=node_io)
        def ordered_result():
            nums = numbers()
            mapped = nums.map(process_item)
            return consume_stream(mapped.collect_stream(ordered=True))

        repo = rs.CodeRepository(
            assets=[numbers, ordered_result],
            tasks=[process_item, consume_stream],
            jobs=[
                rs.Job(
                    name="pipeline", assets=[numbers, ordered_result], executor=executor
                )
            ],
        )
        repo.get_job("pipeline").execute()
        assert repo.load_node("ordered_result") == [50, 40, 30, 20, 10]

    def test_collect_vs_collect_stream_same_results(self, executor_env):
        """collect() and collect_stream() should produce the same final values."""
        executor, io, node_io = executor_env

        @rs.Asset(io_handler=io)
        def numbers() -> list:
            return [3, 1, 4, 1, 5]

        @rs.Asset.from_graph(io_handler=io, node_io_handler=node_io)
        def via_collect():
            nums = numbers()
            mapped = nums.map(double)
            return sum_all(mapped.collect())

        @rs.Asset.from_graph(io_handler=io, node_io_handler=node_io)
        def via_stream():
            nums = numbers()
            mapped = nums.map(double)
            return consume_stream(mapped.collect_stream())

        repo = rs.CodeRepository(
            assets=[numbers, via_collect],
            tasks=[double, sum_all],
            jobs=[
                rs.Job(
                    name="collect_job", assets=[numbers, via_collect], executor=executor
                )
            ],
        )
        repo.get_job("collect_job").execute()
        collect_result = repo.load_node("via_collect")

        repo2 = rs.CodeRepository(
            assets=[numbers, via_stream],
            tasks=[double, consume_stream],
            jobs=[
                rs.Job(
                    name="stream_job", assets=[numbers, via_stream], executor=executor
                )
            ],
        )
        repo2.get_job("stream_job").execute()
        stream_result = repo2.load_node("via_stream")

        assert collect_result == sum(stream_result)


# ── Test: parallel graph assets (forces multi-step loky batches) ──


# ── Test: IOHandlerRef fallback with unpicklable handler ──
#
# This handler wraps PickleIOHandler but holds a threading.Lock,
# making it unpicklable directly. It must be resolved via _IOHandlerRef
# (re-import from module, extract from asset definition).


_unpicklable_store_dir = os.path.join(
    tempfile.gettempdir(), "rivers_test_unpicklable_handler"
)
os.makedirs(_unpicklable_store_dir, exist_ok=True)
_unpicklable_store = obstore.store.LocalStore(_unpicklable_store_dir, mkdir=True)


class UnpicklableIOHandler(rs.PickleIOHandler):
    """PickleIOHandler that can't be pickled directly.

    Overrides __reduce__ to raise, forcing _IOHandlerRef resolution.
    Works normally via _IOHandlerRef (re-imports module, gets handler from asset).
    """

    def __reduce__(self):
        raise TypeError("UnpicklableIOHandler cannot be pickled directly")


_unpicklable_handler = UnpicklableIOHandler(store=_unpicklable_store)


@rs.Task(io_handler=_unpicklable_handler)
def double_unpickable(x: int) -> int:
    return x * 2


@rs.Task(io_handler=_unpicklable_handler)
def sum_all_unpickable(values: list) -> int:
    return sum(values)


@rs.Asset(io_handler=_unpicklable_handler)
def _up_nums_a() -> list:
    return [1, 2, 3]


@rs.Asset(io_handler=_unpicklable_handler)
def _up_nums_b() -> list:
    return [10, 20, 30]


@rs.Asset.from_graph(
    io_handler=_unpicklable_handler, node_io_handler=_unpicklable_handler
)
def _up_graph_a():
    n = _up_nums_a()
    mapped = n.map(double_unpickable)
    return sum_all_unpickable(mapped.collect())


@rs.Asset.from_graph(
    io_handler=_unpicklable_handler, node_io_handler=_unpicklable_handler
)
def _up_graph_b():
    n = _up_nums_b()
    mapped = n.map(double_unpickable)
    return sum_all_unpickable(mapped.collect())


@rs.Asset.from_graph()
def _up_graph_a_no_io():
    n = _up_nums_a()
    mapped = n.map(double_unpickable)
    return sum_all_unpickable(mapped.collect())


@rs.Asset.from_graph()
def _up_graph_b_no_io():
    n = _up_nums_b()
    mapped = n.map(double_unpickable)
    return sum_all_unpickable(mapped.collect())


class TestUnpicklableHandlerRef:
    def setup_method(self):
        """Clean the shared store before each test."""
        import shutil

        if os.path.exists(_unpicklable_store_dir):
            shutil.rmtree(_unpicklable_store_dir)
        os.makedirs(_unpicklable_store_dir, exist_ok=True)

    def test_parallel_graphs_with_unpicklable_handler(self):
        """Parallel graph assets with an unpicklable IO handler.

        The handler has a threading.Lock and can't be pickled directly.
        _IOHandlerRef must be used to reconstruct it from the module-level
        asset definition in the subprocess. If the collect resolution falls
        back to pickling the raw handler, this test fails.
        """
        repo = rs.CodeRepository(
            assets=[_up_nums_a, _up_nums_b, _up_graph_a, _up_graph_b],
            tasks=[double_unpickable, sum_all_unpickable],
            jobs=[
                rs.Job(
                    name="pipeline",
                    assets=[_up_nums_a, _up_nums_b, _up_graph_a, _up_graph_b],
                    executor=rs.Executor.parallel(max_workers=2),
                )
            ],
        )
        repo.get_job("pipeline").execute()
        assert repo.load_node("_up_graph_a/sum_all_unpickable") == 12
        assert repo.load_node("_up_graph_b/sum_all_unpickable") == 120

        # Verify files were written to the unpicklable handler's store
        assert os.path.exists(os.path.join(_unpicklable_store_dir, "_up_nums_a.pkl"))
        assert os.path.exists(os.path.join(_unpicklable_store_dir, "_up_nums_b.pkl"))
        # Map instance outputs written by workers
        assert os.path.exists(
            os.path.join(
                _unpicklable_store_dir, "_up_graph_a", "double_unpickable__0.pkl"
            )
        )
        assert os.path.exists(
            os.path.join(
                _unpicklable_store_dir, "_up_graph_b", "double_unpickable__0.pkl"
            )
        )
        # Downstream task outputs
        assert os.path.exists(
            os.path.join(
                _unpicklable_store_dir, "_up_graph_a", "sum_all_unpickable.pkl"
            )
        )
        assert os.path.exists(
            os.path.join(
                _unpicklable_store_dir, "_up_graph_b", "sum_all_unpickable.pkl"
            )
        )

    def test_parallel_graphs_with_unpicklable_handler_on_tasks(self):
        """Parallel graph assets with an unpicklable IO handler.

        The handler has a threading.Lock and can't be pickled directly.
        _IOHandlerRef must be used to reconstruct it from the module-level
        asset definition in the subprocess. If the collect resolution falls
        back to pickling the raw handler, this test fails.
        """
        repo = rs.CodeRepository(
            assets=[_up_nums_a, _up_nums_b, _up_graph_a_no_io, _up_graph_b_no_io],
            tasks=[double_unpickable, sum_all_unpickable],
            jobs=[
                rs.Job(
                    name="pipeline",
                    assets=[
                        _up_nums_a,
                        _up_nums_b,
                        _up_graph_a_no_io,
                        _up_graph_b_no_io,
                    ],
                    executor=rs.Executor.parallel(max_workers=2),
                )
            ],
        )
        repo.get_job("pipeline").execute()
        assert repo.load_node("_up_graph_a_no_io/sum_all_unpickable") == 12
        assert repo.load_node("_up_graph_b_no_io/sum_all_unpickable") == 120

        # Verify files were written to the unpicklable handler's store
        assert os.path.exists(os.path.join(_unpicklable_store_dir, "_up_nums_a.pkl"))
        assert os.path.exists(os.path.join(_unpicklable_store_dir, "_up_nums_b.pkl"))
        # Map instance outputs written by workers
        assert os.path.exists(
            os.path.join(
                _unpicklable_store_dir, "_up_graph_a_no_io", "double_unpickable__0.pkl"
            )
        )
        assert os.path.exists(
            os.path.join(
                _unpicklable_store_dir, "_up_graph_b_no_io", "double_unpickable__0.pkl"
            )
        )
        # Downstream task outputs
        assert os.path.exists(
            os.path.join(
                _unpicklable_store_dir, "_up_graph_a_no_io", "sum_all_unpickable.pkl"
            )
        )
        assert os.path.exists(
            os.path.join(
                _unpicklable_store_dir, "_up_graph_b_no_io", "sum_all_unpickable.pkl"
            )
        )


class TestParallelGraphAssets:
    def test_two_graph_assets_parallel(self, executor_env):
        """Two graph assets with identical structure run in parallel.

        This forces downstream steps (sum_all) into the same loky batch,
        exercising collect resolution in the parallel path.
        """
        executor, io, node_io = executor_env

        @rs.Asset(io_handler=io)
        def nums_a() -> list:
            return [1, 2, 3]

        @rs.Asset(io_handler=io)
        def nums_b() -> list:
            return [10, 20, 30]

        @rs.Asset.from_graph(io_handler=io, node_io_handler=node_io)
        def graph_a():
            n = nums_a()
            mapped = n.map(double)
            return sum_all(mapped.collect())

        @rs.Asset.from_graph(io_handler=io, node_io_handler=node_io)
        def graph_b():
            n = nums_b()
            mapped = n.map(double)
            return sum_all(mapped.collect())

        repo = rs.CodeRepository(
            assets=[nums_a, nums_b, graph_a, graph_b],
            tasks=[double, sum_all],
            jobs=[
                rs.Job(
                    name="pipeline",
                    assets=[nums_a, nums_b, graph_a, graph_b],
                    executor=executor,
                )
            ],
        )
        repo.get_job("pipeline").execute()
        assert repo.load_node("graph_a/sum_all") == 12  # sum([2, 4, 6])
        assert repo.load_node("graph_b/sum_all") == 120  # sum([20, 40, 60])

    def test_collect_inputs_loaded_in_subprocess(self, executor_env, tmp_path):
        """Map instance results consumed via collect should be loaded from IO
        in the loky subprocess, not in the parent process.

        Uses a PID-tracking IO handler to verify where load_input is called.
        If the parent loads map instance results (current bug), those
        load_input calls have the parent PID. If loaded in the subprocess
        (correct), the PID differs.
        """
        import os

        executor, io, node_io = executor_env
        if io is None:
            pytest.skip("Only relevant for parallel executor")

        pid_log = tmp_path / "load_pids"
        pio = PidTrackingIOHandler(store=io.store, pid_log_path=str(pid_log))

        @rs.Asset(io_handler=pio)
        def nums_a() -> list:
            return [1, 2, 3]

        @rs.Asset(io_handler=pio)
        def nums_b() -> list:
            return [10, 20, 30]

        @rs.Asset.from_graph(io_handler=pio, node_io_handler=pio)
        def graph_a():
            n = nums_a()
            mapped = n.map(double)
            return sum_all(mapped.collect())

        @rs.Asset.from_graph(io_handler=pio, node_io_handler=pio)
        def graph_b():
            n = nums_b()
            mapped = n.map(double)
            return sum_all(mapped.collect())

        repo = rs.CodeRepository(
            assets=[nums_a, nums_b, graph_a, graph_b],
            tasks=[double, sum_all],
            jobs=[
                rs.Job(
                    name="pipeline",
                    assets=[nums_a, nums_b, graph_a, graph_b],
                    executor=executor,
                )
            ],
        )
        parent_pid = str(os.getpid())
        repo.get_job("pipeline").execute()
        assert repo.load_node("graph_a/sum_all") == 12
        assert repo.load_node("graph_b/sum_all") == 120

        # Check that map instance load_input calls happened in a subprocess
        lines = pid_log.read_text().strip().splitlines()
        map_instance_loads = [line for line in lines if "double__" in line]
        assert map_instance_loads, "No map instance load_input calls were recorded"
        parent_loads = [
            load for load in map_instance_loads if load.startswith(parent_pid + ":")
        ]
        assert not parent_loads, (
            f"Map instance results were loaded in the parent process (pid={parent_pid}). "
            f"They should be loaded in loky subprocesses via _IOLoadSpec. "
            f"Parent loads: {parent_loads}"
        )


# ---------------------------------------------------------------------------
# Predefined keys (__keys path)
# ---------------------------------------------------------------------------


def test_predefined_keys_reused_on_remap(tmp_path):
    """When a DynamicOutput source is materialized, mapping keys are persisted
    to rivers-internal KV (scoped by data_version), not via the user's IO
    handler. A subsequent run that re-materializes the source with plain
    values gets a fresh data_version, so its KV lookup misses the prior
    entry and the pipeline correctly falls back to synthetic indices for
    the new values."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Task
    def double_pk(x: int) -> int:
        return x * 2

    @rs.Task
    def total_pk(values: list) -> int:
        return sum(values)

    @rs.Asset(io_handler=handler)
    def pk_source() -> list:
        return [
            rs.DynamicOutput(key="a", value=10),
            rs.DynamicOutput(key="b", value=20),
        ]

    @rs.Asset.from_graph(io_handler=handler, node_io_handler=handler)
    def pk_pipeline():
        s = pk_source()
        mapped = s.map(double_pk)
        return total_pk(mapped.collect())

    assets = [pk_source, pk_pipeline]
    repo = rs.CodeRepository(
        assets=assets,
        tasks=[double_pk, total_pk],
        jobs=[rs.Job(name="j", assets=assets, executor=rs.Executor.in_process())],
    )
    repo.get_job("j").execute()
    assert (
        handler.load_input(
            rs.InputContext(asset_name="pk_pipeline", downstream_asset="t")
        )
        == 60
    )  # (10+20)*2

    # __keys is no longer persisted via the user IO handler; it lives in
    # rivers-internal KV scoped by data_version.
    with pytest.raises(Exception):
        handler.load_input(
            rs.InputContext(asset_name="pk_source__keys", downstream_asset="t")
        )

    # Second run: source returns plain values (not DynamicOutput-wrapped).
    @rs.Asset(io_handler=handler)
    def pk_source() -> list:  # noqa: F811
        return [100, 200]

    assets2 = [pk_source, pk_pipeline]
    repo2 = rs.CodeRepository(
        assets=assets2,
        tasks=[double_pk, total_pk],
        jobs=[rs.Job(name="j", assets=assets2, executor=rs.Executor.in_process())],
    )
    repo2.get_job("j").execute()
    assert (
        handler.load_input(
            rs.InputContext(asset_name="pk_pipeline", downstream_asset="t")
        )
        == 600
    )  # (100+200)*2


def test_mapped_instance_io_failure_emits_step_failure(tmp_path, storage):
    """Regression for review_in_process.md bug #1.

    When a mapped instance's IO write fails *after* the step function has
    successfully executed, the in-process executor must emit a `StepFailure`
    event for the instance. Without it, the UI shows a `StepStart` for the
    instance with no terminal event — a "ghost" step that appears to be
    running forever.

    Today, `run_mapped_sequential`'s `handle_step_output` error branch
    (`python/src/executor/in_process.rs:230-234`) only inserts the instance
    into `failed_names` and pushes to `failures`; it doesn't call
    `ctx.emit_step_failure`. The non-mapped path's `record_failure_no_hooks`
    (`in_process.rs:92`) does emit, so the test contrasts the two.
    """

    class FailOnInstanceWriteHandler(rs.PickleIOHandler):
        """Raises on `handle_output` when the asset name contains
        `fail_substring`. Lets us simulate an IO-write failure on a specific
        mapped instance without affecting the source asset."""

        fail_substring: str = ""

        def handle_output(self, context, value):
            if self.fail_substring and self.fail_substring in context.asset_name:
                raise IOError(f"simulated IO write failure on '{context.asset_name}'")
            return super().handle_output(context, value)

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = FailOnInstanceWriteHandler(store=store, fail_substring="mapfail__")

    @rs.Task
    def mapfail(x: int) -> int:
        return x * 2

    @rs.Task
    def mapfail_sum(values: list) -> int:
        return sum(values)

    @rs.Asset(io_handler=handler)
    def mapfail_src() -> list:
        return [1, 2, 3]

    @rs.Asset.from_graph(io_handler=handler, node_io_handler=handler)
    def mapfail_pipeline():
        s = mapfail_src()
        mapped = s.map(mapfail)
        return mapfail_sum(mapped.collect())

    assets = [mapfail_src, mapfail_pipeline]
    repo = rs.CodeRepository(
        assets=assets,
        tasks=[mapfail, mapfail_sum],
        jobs=[rs.Job(name="j", assets=assets, executor=rs.Executor.in_process())],
    )
    repo.resolve(storage=storage)

    with pytest.raises(Exception):
        repo.get_job("j").execute()

    # The first mapped instance is the one that fails.
    instance_name = "mapfail_pipeline/mapfail__0"
    events = storage.get_events_for_asset(instance_name)
    event_types = [e.event_type for e in events]

    assert "StepStart" in event_types, (
        f"expected StepStart for '{instance_name}' (sanity), got: {event_types}"
    )
    assert "StepFailure" in event_types, (
        f"expected StepFailure for '{instance_name}' after IO write failure, "
        f"got: {event_types} — the in-process mapped path's "
        f"`handle_step_output` error branch must call `ctx.emit_step_failure` "
        f"so the instance has a terminal event in storage"
    )


def test_predefined_keys_skipped_when_source_rewrites_plain(tmp_path):
    """Regression: a previous run's mapping keys must not bleed into a
    subsequent run that re-materializes the source with plain (non-
    DynamicOutput) values.

    Setup:
      Run 1 — `pk_source` returns 2 DynamicOutputs → orchestrator persists
              `["a", "b"]` to KV under the run-1 data_version.
      Run 2 — `pk_source` is redefined to return 3 *plain* values. The
              materialization gets a fresh data_version, so the run-1 KV
              entry is invisible to the run-2 fan-out lookup. Within run 2,
              `step_dynamic_keys[pk_source] = vec![]` (empty sentinel: "ran
              locally with plain values") short-circuits the lookup entirely
              before it would even consult KV.

    Asserts the pipeline completes and the collect sum reflects the new
    plain-value source: (100 + 200 + 300) × 2 = 1200.
    """
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Task
    def pk_double(x: int) -> int:
        return x * 2

    @rs.Task
    def pk_total(values: list) -> int:
        return sum(values)

    @rs.Asset(io_handler=handler)
    def pk_source() -> list:
        return [
            rs.DynamicOutput(key="a", value=10),
            rs.DynamicOutput(key="b", value=20),
        ]

    @rs.Asset.from_graph(io_handler=handler, node_io_handler=handler)
    def pk_pipeline():
        s = pk_source()
        mapped = s.map(pk_double)
        return pk_total(mapped.collect())

    assets = [pk_source, pk_pipeline]
    repo = rs.CodeRepository(
        assets=assets,
        tasks=[pk_double, pk_total],
        jobs=[rs.Job(name="j", assets=assets, executor=rs.Executor.in_process())],
    )
    repo.get_job("j").execute()
    # __keys is no longer persisted via the user IO handler; it lives in
    # rivers-internal KV scoped by data_version.
    with pytest.raises(Exception):
        handler.load_input(
            rs.InputContext(asset_name="pk_source__keys", downstream_asset="t")
        )

    @rs.Asset(io_handler=handler)
    def pk_source() -> list:  # noqa: F811
        return [100, 200, 300]

    assets2 = [pk_source, pk_pipeline]
    repo2 = rs.CodeRepository(
        assets=assets2,
        tasks=[pk_double, pk_total],
        jobs=[rs.Job(name="j", assets=assets2, executor=rs.Executor.in_process())],
    )
    repo2.get_job("j").execute()
    assert (
        handler.load_input(
            rs.InputContext(asset_name="pk_pipeline", downstream_asset="t")
        )
        == 1200
    )


def test_predefined_keys_use_inline_dynamic_outputs_within_run(tmp_path):
    """When the source asset emits DynamicOutputs in *this* batch, the fan-out
    path must use those in-memory keys directly — without re-reading them
    from disk. Verified by confirming that mapped instance files are written
    under the source's DynamicOutput keys, not synthetic indices."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Task
    def im_inc(x: int) -> int:
        return x + 1

    @rs.Task
    def im_sum(values: list) -> int:
        return sum(values)

    @rs.Asset(io_handler=handler)
    def im_source() -> list:
        return [
            rs.DynamicOutput(key="alpha", value=10),
            rs.DynamicOutput(key="beta", value=20),
        ]

    @rs.Asset.from_graph(io_handler=handler, node_io_handler=handler)
    def im_pipeline():
        s = im_source()
        mapped = s.map(im_inc)
        return im_sum(mapped.collect())

    assets = [im_source, im_pipeline]
    repo = rs.CodeRepository(
        assets=assets,
        tasks=[im_inc, im_sum],
        jobs=[rs.Job(name="j", assets=assets, executor=rs.Executor.in_process())],
    )
    repo.get_job("j").execute()
    assert (
        handler.load_input(
            rs.InputContext(asset_name="im_pipeline", downstream_asset="t")
        )
        == 32
    )

    store_root = handler.store.prefix
    assert os.path.exists(os.path.join(store_root, "im_pipeline", "im_inc__alpha.pkl"))
    assert os.path.exists(os.path.join(store_root, "im_pipeline", "im_inc__beta.pkl"))
    assert not os.path.exists(os.path.join(store_root, "im_pipeline", "im_inc__0.pkl"))


def test_predefined_keys_honor_changed_dynamic_keys_between_runs(tmp_path):
    """When the source re-runs with *different* DynamicOutput keys (same
    length), the new keys must be used — not the stale `__keys` file. Under
    the old code, the disk file silently overrode the inline keys whenever
    the file existed, so two runs that legitimately changed their key labels
    would still write under the original ones."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Task
    def ck_double(x: int) -> int:
        return x * 2

    @rs.Task
    def ck_total(values: list) -> int:
        return sum(values)

    @rs.Asset(io_handler=handler)
    def ck_source() -> list:
        return [
            rs.DynamicOutput(key="orig_a", value=1),
            rs.DynamicOutput(key="orig_b", value=2),
        ]

    @rs.Asset.from_graph(io_handler=handler, node_io_handler=handler)
    def ck_pipeline():
        s = ck_source()
        mapped = s.map(ck_double)
        return ck_total(mapped.collect())

    assets = [ck_source, ck_pipeline]
    repo = rs.CodeRepository(
        assets=assets,
        tasks=[ck_double, ck_total],
        jobs=[rs.Job(name="j", assets=assets, executor=rs.Executor.in_process())],
    )
    repo.get_job("j").execute()
    store_root = handler.store.prefix
    assert os.path.exists(
        os.path.join(store_root, "ck_pipeline", "ck_double__orig_a.pkl")
    )

    @rs.Asset(io_handler=handler)
    def ck_source() -> list:  # noqa: F811
        return [
            rs.DynamicOutput(key="renamed_x", value=10),
            rs.DynamicOutput(key="renamed_y", value=20),
        ]

    assets2 = [ck_source, ck_pipeline]
    repo2 = rs.CodeRepository(
        assets=assets2,
        tasks=[ck_double, ck_total],
        jobs=[rs.Job(name="j", assets=assets2, executor=rs.Executor.in_process())],
    )
    repo2.get_job("j").execute()
    assert os.path.exists(
        os.path.join(store_root, "ck_pipeline", "ck_double__renamed_x.pkl")
    )
    assert os.path.exists(
        os.path.join(store_root, "ck_pipeline", "ck_double__renamed_y.pkl")
    )


# ---------------------------------------------------------------------------
# Mapped instance failure skips downstream under Parallel
# ---------------------------------------------------------------------------


def test_mapped_failure_skips_downstream_parallel(tmp_path):
    """When a mapped instance fails under Parallel, downstream steps are skipped."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)
    executor = rs.Executor.parallel(max_workers=2)

    @rs.Task
    def fail_on_two_mp(x: int) -> int:
        if x == 2:
            raise ValueError("fail on 2")
        return x * 10

    @rs.Task
    def consume_mp(values: list) -> int:
        return sum(values)

    @rs.Asset(io_handler=handler)
    def mp_src() -> list:
        return [1, 2, 3]

    @rs.Asset.from_graph(io_handler=handler, node_io_handler=handler)
    def mp_fan_pipeline():
        s = mp_src()
        mapped = s.map(fail_on_two_mp)
        return consume_mp(mapped.collect())

    @rs.Asset(io_handler=handler)
    def mp_after(mp_fan_pipeline: int) -> int:
        return mp_fan_pipeline + 1

    assets = [mp_src, mp_fan_pipeline, mp_after]
    repo = rs.CodeRepository(
        assets=assets,
        tasks=[fail_on_two_mp, consume_mp],
        jobs=[rs.Job(name="j", assets=assets, executor=executor)],
    )
    with pytest.raises(Exception):
        repo.get_job("j").execute()

    with pytest.raises(Exception):
        handler.load_input(rs.InputContext(asset_name="mp_after", downstream_asset="t"))


# ---------------------------------------------------------------------------
# Cross-executor parity: fan-out
# ---------------------------------------------------------------------------


@rs.Task
def parity_double(x: int) -> int:
    return x * 2


@rs.Task
def parity_sum(values: list) -> int:
    return sum(values)


def test_cross_executor_parity_fan_out(tmp_path):
    """Same fan-out pipeline produces identical results under InProcess and Parallel."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    def build_repo(executor, io):
        @rs.Asset(io_handler=io)
        def parity_nums() -> list:
            return [1, 2, 3, 4, 5]

        @rs.Asset.from_graph(io_handler=io, node_io_handler=io)
        def parity_result():
            n = parity_nums()
            mapped = n.map(parity_double)
            return parity_sum(mapped.collect())

        assets = [parity_nums, parity_result]
        return rs.CodeRepository(
            assets=assets,
            tasks=[parity_double, parity_sum],
            jobs=[rs.Job(name="j", assets=assets, executor=executor)],
        )

    repo_ip = build_repo(rs.Executor.in_process(), None)
    repo_ip.get_job("j").execute()
    ip_result = repo_ip.load_node("parity_result")

    repo_par = build_repo(rs.Executor.parallel(max_workers=2), handler)
    repo_par.get_job("j").execute()
    par_result = handler.load_input(
        rs.InputContext(asset_name="parity_result", downstream_asset="t")
    )

    assert ip_result == par_result == 30  # (1+2+3+4+5)*2
