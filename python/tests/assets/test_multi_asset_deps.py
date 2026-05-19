"""Tests for the ``deps`` argument on ``AssetDef`` inside ``Asset.from_multi``.

Per-output deps interact with the multi-asset's top-level ``deps=`` as follows:

* ``AssetDef.input(...)`` declared per-output merges into the multi-asset's
  function-level input set (the function fires once, so input deps cannot be
  truly per-output). Conflicting declarations of the same input across the
  top-level and per-output lists raise ``AssetDefinitionError``.
* ``AssetDef.dep(...)`` (lineage-only) declared per-output adds a graph edge
  to that specific output only, while top-level lineage-only deps apply to
  every output.
"""

import pytest
import rivers as rs
from _helpers import CapturingHandler, DictIOHandler


class TestAssetDefAPI:
    def test_accepts_deps_kwarg(self):
        upstream = rs.AssetDef.dep("upstream")
        inp = rs.AssetDef.input("loaded")
        ad = rs.AssetDef("out", deps=[upstream, inp])

        assert len(ad.deps) == 2
        assert {d.name for d in ad.deps} == {"upstream", "loaded"}

    def test_deps_defaults_to_empty(self):
        ad = rs.AssetDef("out")
        assert ad.deps == []


class TestLineageDeps:
    """Lineage-only deps (``AssetDef.dep(...)``) add graph edges without
    being loaded as function arguments."""

    def test_creates_dependency_edge(self, storage):
        """The multi-asset's single function call is held until every
        upstream referenced by ANY output has materialized — if per-output
        deps were dropped, the multi step would only wait for upstream_a
        (whichever output created the step first)."""
        handler = DictIOHandler()
        call_order: list[str] = []

        @rs.Asset(io_handler=handler)
        def upstream_a():
            call_order.append("upstream_a")
            return 10

        @rs.Asset(io_handler=handler)
        def upstream_b():
            call_order.append("upstream_b")
            return 20

        @rs.Asset.from_multi(
            output_defs=[
                rs.AssetDef(
                    "out_a", io_handler=handler, deps=[rs.AssetDef.dep("upstream_a")]
                ),
                rs.AssetDef(
                    "out_b", io_handler=handler, deps=[rs.AssetDef.dep("upstream_b")]
                ),
            ],
        )
        def m():
            call_order.append("multi")
            yield rs.Output(value=1, output_name="out_a")
            yield rs.Output(value=2, output_name="out_b")

        repo = rs.CodeRepository(
            assets=[upstream_a, upstream_b, m],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)
        result = repo.materialize()

        assert result.success
        multi_idx = call_order.index("multi")
        assert call_order.index("upstream_a") < multi_idx
        assert call_order.index("upstream_b") < multi_idx

    def test_does_not_require_function_param(self):
        """A lineage-only dep adds a graph edge without being loaded, so
        the multi-asset function does NOT need a parameter named after it.
        Only ``AssetDef.input(...)`` deps are matched against fn params."""
        handler = DictIOHandler()

        @rs.Asset(io_handler=handler)
        def upstream() -> int:
            return 42

        @rs.Asset.from_multi(
            output_defs=[
                rs.AssetDef(
                    "out", io_handler=handler, deps=[rs.AssetDef.dep("upstream")]
                ),
            ],
        )
        def m():
            yield rs.Output(value=99, output_name="out")

        repo = rs.CodeRepository(
            assets=[upstream, m], default_executor=rs.Executor.in_process()
        )
        result = repo.materialize()

        assert result.success
        assert handler.store["out"] == 99


class TestInputDeps:
    """Input deps (``AssetDef.input(...)``) merge into the multi-asset's
    function-level input set — they're loaded as kwargs to the fn."""

    def test_loads_into_function(self):
        handler = CapturingHandler()
        handler.input_contexts = []
        handler.store = {}

        @rs.Asset(io_handler=handler)
        def upstream() -> int:
            return 7

        @rs.Asset.from_multi(
            output_defs=[
                rs.AssetDef(
                    "out", io_handler=handler, deps=[rs.AssetDef.input("upstream")]
                ),
            ],
        )
        def m(upstream: int):
            yield rs.Output(value=upstream * 2, output_name="out")

        repo = rs.CodeRepository(
            assets=[upstream, m], default_executor=rs.Executor.in_process()
        )
        result = repo.materialize()

        assert result.success
        assert handler.store["out"] == 14

    def test_must_match_function_param(self):
        """Same rule as top-level input deps: the name must match a
        function parameter, else error at definition time."""

        @rs.Asset
        def upstream() -> int:
            return 1

        with pytest.raises(Exception, match=r"does not match any parameter"):

            @rs.Asset.from_multi(
                output_defs=[
                    rs.AssetDef("out", deps=[rs.AssetDef.input("upstream")]),
                ],
            )
            def m():
                yield rs.Output(value=1, output_name="out")


class TestTopLevelInteraction:
    """Interactions between top-level ``deps=`` on ``from_multi`` and
    per-output ``deps=`` on each ``AssetDef``."""

    def test_lineage_deps_union(self, storage):
        """Top-level lineage deps apply to every output; per-output
        lineage deps add additional edges scoped to that output."""
        handler = DictIOHandler()

        @rs.Asset(io_handler=handler)
        def shared_upstream() -> int:
            return 1

        @rs.Asset(io_handler=handler)
        def a_only() -> int:
            return 2

        @rs.Asset(io_handler=handler)
        def b_only() -> int:
            return 3

        @rs.Asset.from_multi(
            deps=[rs.AssetDef.dep("shared_upstream")],
            output_defs=[
                rs.AssetDef(
                    "out_a", io_handler=handler, deps=[rs.AssetDef.dep("a_only")]
                ),
                rs.AssetDef(
                    "out_b", io_handler=handler, deps=[rs.AssetDef.dep("b_only")]
                ),
            ],
        )
        def m():
            yield rs.Output(value=10, output_name="out_a")
            yield rs.Output(value=20, output_name="out_b")

        repo = rs.CodeRepository(
            assets=[shared_upstream, a_only, b_only, m],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)
        result = repo.materialize()

        assert result.success
        order = result.materialized_assets
        assert order.index("shared_upstream") < order.index("out_a")
        assert order.index("shared_upstream") < order.index("out_b")
        assert order.index("a_only") < order.index("out_a")
        assert order.index("b_only") < order.index("out_b")

    def test_top_input_plus_per_output_lineage(self):
        """Top-level input dep loads as a function param; per-output
        lineage dep adds an edge but is NOT passed to the function."""
        handler = DictIOHandler()

        @rs.Asset(io_handler=handler)
        def loaded() -> int:
            return 5

        @rs.Asset(io_handler=handler)
        def gated() -> int:
            return 0

        @rs.Asset.from_multi(
            deps=[rs.AssetDef.input("loaded")],
            output_defs=[
                rs.AssetDef("out", io_handler=handler, deps=[rs.AssetDef.dep("gated")]),
            ],
        )
        def m(loaded: int):
            yield rs.Output(value=loaded + 100, output_name="out")

        repo = rs.CodeRepository(
            assets=[loaded, gated, m], default_executor=rs.Executor.in_process()
        )
        result = repo.materialize()
        assert result.success
        assert handler.store["out"] == 105

    def test_same_input_dep_dedups_when_compatible(self):
        """Declaring the same input dep at top level and per-output is a
        no-op when the declarations match — loaded exactly once."""
        handler = DictIOHandler()

        @rs.Asset(io_handler=handler)
        def upstream() -> int:
            return 4

        @rs.Asset.from_multi(
            deps=[rs.AssetDef.input("upstream")],
            output_defs=[
                rs.AssetDef(
                    "out", io_handler=handler, deps=[rs.AssetDef.input("upstream")]
                ),
            ],
        )
        def m(upstream: int):
            yield rs.Output(value=upstream + 1, output_name="out")

        repo = rs.CodeRepository(
            assets=[upstream, m], default_executor=rs.Executor.in_process()
        )
        result = repo.materialize()
        assert result.success
        assert handler.store["out"] == 5


class TestConflicts:
    """The same dep declared with different per-edge settings at top-level
    vs per-output should raise — silent overwrites are bad."""

    def test_input_dep_partition_mapping_conflict_raises(self):
        @rs.Asset
        def upstream() -> int:
            return 1

        with pytest.raises(Exception, match=r"conflicts with an earlier declaration"):

            @rs.Asset.from_multi(
                deps=[
                    rs.AssetDef.input(
                        "upstream", partition_mapping=rs.PartitionMapping.identity()
                    )
                ],
                output_defs=[
                    rs.AssetDef(
                        "out",
                        deps=[
                            rs.AssetDef.input(
                                "upstream",
                                partition_mapping=rs.PartitionMapping.all_partitions(),
                            )
                        ],
                    ),
                ],
            )
            def m(upstream: int):
                yield rs.Output(value=upstream, output_name="out")

    def test_input_dep_metadata_conflict_raises(self):
        @rs.Asset
        def upstream() -> int:
            return 1

        with pytest.raises(Exception, match=r"metadata that conflicts"):

            @rs.Asset.from_multi(
                deps=[rs.AssetDef.input("upstream", metadata={"k": "v1"})],
                output_defs=[
                    rs.AssetDef(
                        "out",
                        deps=[rs.AssetDef.input("upstream", metadata={"k": "v2"})],
                    ),
                ],
            )
            def m(upstream: int):
                yield rs.Output(value=upstream, output_name="out")

    def test_lineage_partition_mapping_conflict_with_explicit_dict_raises(self):
        """If the user supplies both ``AssetDef(partition_mapping={...})``
        and a ``dep(..., partition_mapping=...)`` for the same name with
        different mappings, raise rather than silently pick one."""

        with pytest.raises(Exception, match=r"conflicts with the entry"):

            @rs.Asset.from_multi(
                output_defs=[
                    rs.AssetDef(
                        "out",
                        partition_mapping={"upstream": rs.PartitionMapping.identity()},
                        deps=[
                            rs.AssetDef.dep(
                                "upstream",
                                partition_mapping=rs.PartitionMapping.all_partitions(),
                            )
                        ],
                    ),
                ],
            )
            def m():
                yield rs.Output(value=1, output_name="out")


class TestPartitionMappings:
    def test_per_output_lineage_dep_partition_mapping_flows_to_output_def(self):
        """A partition_mapping carried on a per-output ``dep`` shows up in
        the inner output's ``partition_mapping`` dict via ``output_defs``."""

        @rs.Asset.from_multi(
            output_defs=[
                rs.AssetDef(
                    "out",
                    deps=[
                        rs.AssetDef.dep(
                            "upstream", partition_mapping=rs.PartitionMapping.identity()
                        )
                    ],
                ),
            ],
        )
        def m():
            yield rs.Output(value=1, output_name="out")

        pm = m.output_defs[0].partition_mapping
        assert pm is not None
        assert pm["upstream"] == rs.PartitionMapping.identity()
