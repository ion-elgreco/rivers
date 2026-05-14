import obstore
import obstore.store

import rivers as rs


def test_job_with_pickle_handler(tmp_path):
    """Assets with io_handler persist to store AND return results."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def source() -> list:
        return [1, 2, 3]

    @rs.Asset(io_handler=handler)
    def doubled(source: list) -> list:
        return [x * 2 for x in source]

    repo = rs.CodeRepository(
        assets=[source, doubled],
        jobs=[
            rs.Job(
                name="pickle_handler",
                assets=[source, doubled],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("pickle_handler").execute()

    # In-memory results are correct
    assert repo.load_node("source") == [1, 2, 3]
    assert repo.load_node("doubled") == [2, 4, 6]

    # Files were persisted
    assert (tmp_path / "source.pkl").exists()
    assert (tmp_path / "doubled.pkl").exists()

    # Can load back from store
    ctx = rs.InputContext(
        asset_name="source", downstream_asset="test", asset_metadata=None
    )
    assert handler.load_input(ctx) == [1, 2, 3]


def test_job_with_memory_store():
    """Assets with io_handler using MemoryStore."""
    store = obstore.store.MemoryStore()
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def val() -> int:
        return 99

    repo = rs.CodeRepository(
        assets=[val],
        jobs=[
            rs.Job(name="memory_store", assets=[val], executor=rs.Executor.in_process())
        ],
    )
    repo.get_job("memory_store").execute()
    assert repo.load_node("val") == 99

    # Can load back from memory store
    ctx = rs.InputContext(
        asset_name="val", downstream_asset="test", asset_metadata=None
    )
    assert handler.load_input(ctx) == 99


def test_job_mixed_io_handlers(tmp_path):
    """Mix of assets with and without io_handler."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def persisted() -> int:
        return 42

    @rs.Asset
    def not_persisted(persisted: int) -> int:
        return persisted + 1

    repo = rs.CodeRepository(
        assets=[persisted, not_persisted],
        jobs=[
            rs.Job(
                name="mixed_handlers",
                assets=[persisted, not_persisted],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("mixed_handlers").execute()

    assert repo.load_node("persisted") == 42
    assert repo.load_node("not_persisted") == 43

    # Only persisted asset has a file
    assert (tmp_path / "persisted.pkl").exists()
    assert not (tmp_path / "not_persisted.pkl").exists()


def test_job_chain_with_io_handler():
    """Chain of three assets all with io_handler — downstream gets in-memory result."""
    store = obstore.store.MemoryStore()
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def a() -> int:
        return 10

    @rs.Asset(io_handler=handler)
    def b(a: int) -> int:
        return a * 2

    @rs.Asset(io_handler=handler)
    def c(b: int) -> int:
        return b + 5

    repo = rs.CodeRepository(
        assets=[a, b, c],
        jobs=[
            rs.Job(
                name="chain_handler",
                assets=[a, b, c],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("chain_handler").execute()

    assert repo.load_node("a") == 10
    assert repo.load_node("b") == 20
    assert repo.load_node("c") == 25


def test_job_with_io_handler_parallel(tmp_path):
    """IO handlers work with Parallel executor too."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def mp_src() -> list:
        return [10, 20, 30]

    @rs.Asset(io_handler=handler)
    def mp_out(mp_src: list) -> int:
        return sum(mp_src)

    repo = rs.CodeRepository(
        assets=[mp_src, mp_out],
        jobs=[
            rs.Job(
                name="parallel_handler",
                assets=[mp_src, mp_out],
                executor=rs.Executor.parallel(max_workers=2),
            )
        ],
    )
    repo.get_job("parallel_handler").execute()

    assert repo.load_node("mp_src") == [10, 20, 30]
    assert repo.load_node("mp_out") == 60
    assert (tmp_path / "mp_src.pkl").exists()
    assert (tmp_path / "mp_out.pkl").exists()


def test_job_with_in_memory_handler():
    """Custom IOHandler works in job execution."""
    handler = rs.InMemoryIOHandler()

    @rs.Asset(io_handler=handler)
    def x() -> str:
        return "hello"

    @rs.Asset(io_handler=handler)
    def y(x: str) -> str:
        return x + " world"

    repo = rs.CodeRepository(
        assets=[x, y],
        jobs=[
            rs.Job(
                name="in_memory_handler",
                assets=[x, y],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("in_memory_handler").execute()

    assert repo.load_node("x") == "hello"
    assert repo.load_node("y") == "hello world"
    assert handler._storage["x"] == "hello"
    assert handler._storage["y"] == "hello world"


def test_output_context_has_metadata():
    """OutputContext carries metadata from the asset."""
    captured_contexts = []

    class CapturingHandler(rs.BaseIOHandler):
        def handle_output(self, context, obj):
            captured_contexts.append(context)

        def load_input(self, context):
            return None

    meta = {"table": "users", "format": "parquet"}

    @rs.Asset(io_handler=CapturingHandler(), metadata=meta)
    def with_meta() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[with_meta],
        jobs=[
            rs.Job(
                name="output_context_metadata",
                assets=[with_meta],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("output_context_metadata").execute()

    assert len(captured_contexts) == 1
    assert captured_contexts[0].asset_name == "with_meta"
    assert captured_contexts[0].asset_metadata == meta
