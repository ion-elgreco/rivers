import rivers as rs

from _helpers import DictIOHandler


def test_self_dependency_first_run():
    """On first run, get_inner() returns None because no data exists yet."""
    handler = DictIOHandler()

    @rs.Asset(io_handler=handler)
    def my_asset(self: rs.SelfDependency[list]) -> list:
        prev = self.get_inner()
        assert prev is None
        return [1, 2, 3]

    repo = rs.CodeRepository(assets=[my_asset])
    repo.materialize()
    assert repo.load_node("my_asset") == [1, 2, 3]


def test_self_dependency_subsequent_run():
    """On second run, get_inner() returns the previously stored output."""
    handler = DictIOHandler()

    @rs.Asset(io_handler=handler)
    def my_asset(self: rs.SelfDependency[list]) -> list:
        prev = self.get_inner()
        if prev is None:
            return [1, 2, 3]
        return prev + [4, 5, 6]

    repo = rs.CodeRepository(assets=[my_asset])

    repo.materialize()
    assert repo.load_node("my_asset") == [1, 2, 3]

    repo.materialize()
    assert repo.load_node("my_asset") == [1, 2, 3, 4, 5, 6]


def test_self_dependency_works_with_default_io_handler():
    """SelfDependency works with the default InMemoryIOHandler."""

    @rs.Asset
    def my_asset(self: rs.SelfDependency[list]) -> list:
        prev = self.get_inner()
        if prev is None:
            return [1, 2, 3]
        return prev + [4]

    repo = rs.CodeRepository(assets=[my_asset])
    repo.materialize()
    assert repo.load_node("my_asset") == [1, 2, 3]

    repo.materialize()
    assert repo.load_node("my_asset") == [1, 2, 3, 4]


def test_self_dependency_no_graph_cycle():
    """Asset with self-dep doesn't create a cycle in the graph."""
    handler = DictIOHandler()

    @rs.Asset(io_handler=handler)
    def cyclic(self: rs.SelfDependency[int]) -> int:
        prev = self.get_inner()
        return (prev or 0) + 1

    # Should not raise — no cycle
    repo = rs.CodeRepository(assets=[cyclic])
    repo.materialize()
    assert repo.load_node("cyclic") == 1


def test_self_dependency_with_other_deps():
    """Asset has both self-dep and normal upstream deps."""
    handler = DictIOHandler()

    @rs.Asset
    def source() -> int:
        return 10

    @rs.Asset(io_handler=handler)
    def accumulator(self: rs.SelfDependency[int], source: int) -> int:
        prev = self.get_inner()
        return (prev or 0) + source

    repo = rs.CodeRepository(assets=[source, accumulator])

    repo.materialize()
    assert repo.load_node("accumulator") == 10

    repo.materialize()
    assert repo.load_node("accumulator") == 20


def test_self_dependency_class_getitem():
    """SelfDependency[T] works as a type annotation."""
    alias = rs.SelfDependency[int]
    assert alias.__origin__ is rs.SelfDependency  # type: ignore
    assert alias.__args__ == (int,)  # type: ignore
