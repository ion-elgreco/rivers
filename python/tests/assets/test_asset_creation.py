import pytest

import rivers as rs


def test_single_asset_simple():
    @rs.Asset
    def foo():
        return 10

    assert isinstance(foo, rs.SingleAsset)
    assert not isinstance(foo, rs.MultiAsset)


def test_single_asset_name_derived():
    @rs.Asset
    def foo():
        return 10

    assert foo.name == "foo"
    assert foo._name == "foo"


def test_single_asset_with_values():
    @rs.Asset(
        name="hi",
        tags=["a", "b"],
        kinds="deltalake",
        group="NDIA",
        code_version="v1.0.0",
        io_handler=None,
        metadata=None,
    )
    def foo():
        return 10

    assert foo.name == "hi"
    assert foo.tags == ["a", "b"]


def test_single_asset_use_function():
    @rs.Asset()
    def foo():
        return 10

    assert foo._asset_fn() == 10


def test_multi_asset_name_derived():
    defs = [rs.AssetDef("foo"), rs.AssetDef("bar")]

    @rs.Asset.from_multi(
        output_defs=defs,
    )
    def foo():
        return 10

    assert isinstance(foo, rs.MultiAsset)
    assert not isinstance(foo, rs.SingleAsset)
    assert foo._name == "foo"
    assert foo.output_defs == defs


def test_multi_asset_with_values():
    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("foo"), rs.AssetDef("bar")],
        name="my_multi_asset",
        tags=["a", "b"],
        kinds="deltalake",
        group="NDIA",
        code_version="v1.0.0",
        io_handler=None,
    )
    def foo():
        return 10

    assert foo._name == "my_multi_asset"


def test_multi_asset_use_function():
    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("foo"), rs.AssetDef("bar")],
        name="my_multi_asset",
        tags=["a", "b"],
        kinds="deltalake",
        group="NDIA",
        code_version="v1.0.0",
        io_handler=None,
    )
    def foo():
        return 10

    assert foo._asset_fn() == 10


# ---------------------------------------------------------------------------
# Property exposure: hooks, automation_condition, partition_mapping
# ---------------------------------------------------------------------------


class DuckHandler(rs.BaseIOHandler):
    def handle_output(self, context, obj):
        pass

    def load_input(self, context):
        return None


def test_single_asset_exposes_hooks():
    @rs.Hook.success
    def my_hook(ctx):
        pass

    @rs.Asset(hooks=[my_hook])
    def a():
        return 1

    assert a.hooks is not None
    assert len(a.hooks) == 1
    assert isinstance(a.hooks[0], rs.Hook.Success)


def test_single_asset_exposes_automation_condition():
    cond = rs.AutomationCondition.on_missing()

    @rs.Asset(automation_condition=cond)
    def a():
        return 1

    assert a.automation_condition is not None


def test_single_asset_exposes_partition_mapping():
    @rs.Asset(
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.identity()
            )
        ]
    )
    def a(upstream: int):
        return 1

    assert a.partition_mapping is not None
    assert "upstream" in a.partition_mapping


def test_single_asset_none_when_not_set():
    @rs.Asset
    def a():
        return 1

    assert a.hooks is None
    assert a.automation_condition is None
    assert a.partition_mapping is None


def test_multi_asset_exposes_hooks():
    @rs.Hook.success
    def my_hook(ctx):
        pass

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("x")],
        hooks=[my_hook],
    )
    def m():
        return {"x": 1}

    assert m.hooks is not None
    assert len(m.hooks) == 1


def test_multi_asset_exposes_automation_condition():
    cond = rs.AutomationCondition.on_missing()

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("x")],
        automation_condition=cond,
    )
    def m():
        return {"x": 1}

    assert m.automation_condition is not None


def test_graph_asset_exposes_hooks():
    @rs.Hook.success
    def my_hook(ctx):
        pass

    @rs.Asset
    def inner():
        return 1

    @rs.Asset.from_graph(hooks=[my_hook])
    def g(inner: int) -> int:
        return inner

    assert g.hooks is not None
    assert len(g.hooks) == 1


def test_graph_asset_exposes_automation_condition():
    cond = rs.AutomationCondition.on_missing()

    @rs.Asset
    def inner():
        return 1

    @rs.Asset.from_graph(automation_condition=cond)
    def g(inner: int) -> int:
        return inner

    assert g.automation_condition is not None


def test_external_asset_exposes_automation_condition():
    cond = rs.AutomationCondition.on_missing()

    @rs.Asset.external(
        name="src",
        io_handler=DuckHandler(),
        automation_condition=cond,
    )
    def src(context: rs.AssetExecutionContext):
        return rs.Observation(data_version="v1")

    assert src.automation_condition is not None


def test_external_asset_hooks_is_none():
    """External assets don't support hooks."""
    ext = rs.Asset.external(name="src", io_handler=DuckHandler())
    assert ext.hooks is None


# ---------------------------------------------------------------------------
# Getter accessibility: all common getters on every asset variant
# ---------------------------------------------------------------------------


def _make_single(populated):
    if populated:

        @rs.Asset(
            name="s", tags=["t"], kinds=["k"], group="grp", metadata={"key": "val"}
        )
        def s():
            return 1
    else:

        @rs.Asset
        def s():
            return 1

    return s


def _make_graph(populated):
    @rs.Asset
    def inner():
        return 1

    if populated:

        @rs.Asset.from_graph(
            name="g", tags=["t"], kinds=["k"], group="grp", metadata={"key": "val"}
        )
        def g(inner: int) -> int:
            return inner
    else:

        @rs.Asset.from_graph()
        def g(inner: int) -> int:
            return inner

    return g


@pytest.mark.parametrize(
    "make_asset,expected_name",
    [
        pytest.param(_make_single, "s", id="single"),
        pytest.param(_make_graph, "g", id="graph"),
    ],
)
@pytest.mark.parametrize("populated", [True, False], ids=["all_getters", "defaults"])
def test_asset_getters(make_asset, expected_name, populated):
    """name/tags/kinds/group/metadata are exposed on Single and GraphAssets,
    in both populated and default forms."""
    asset = make_asset(populated)

    assert asset.name == expected_name
    if populated:
        assert asset.tags == ["t"]
        assert asset.kinds == ["k"]
        assert asset.group == "grp"
        assert asset.metadata == {"key": "val"}
    else:
        assert asset.tags is None
        assert asset.kinds == []
        assert asset.group is None
        assert asset.metadata is None


def test_multi_asset_all_getters():
    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("x")],
        name="m",
        tags=["t"],
        kinds=["k"],
        group="grp",
    )
    def m():
        return {"x": 1}

    assert m.name == "m"
    assert m.tags is None  # Multi returns None for tags
    assert m.kinds == []  # Multi returns empty kinds
    assert m.group is None  # Multi returns None for group
    assert m.metadata is None  # Multi returns None for metadata


def test_external_asset_all_getters():
    ext = rs.Asset.external(
        name="ext",
        io_handler=DuckHandler(),
        tags=["t"],
        kinds=["k"],
        group="grp",
        metadata={"key": "val"},
    )

    assert ext.name == "ext"
    assert ext.tags == ["t"]
    assert ext.kinds == ["k"]
    assert ext.group == "grp"
    assert ext.metadata == {"key": "val"}


def test_external_asset_getters_defaults():
    ext = rs.Asset.external(name="ext", io_handler=DuckHandler())

    assert ext.name == "ext"
    assert ext.tags is None
    assert ext.kinds == []
    assert ext.group is None
    assert ext.metadata is None


# ---------------------------------------------------------------------------
# Additional getters: code_version, partitions_def, observe_fn
# ---------------------------------------------------------------------------


def test_single_asset_code_version():
    @rs.Asset(code_version="v2")
    def a():
        return 1

    assert a.code_version == "v2"


def test_single_asset_code_version_default():
    @rs.Asset
    def a():
        return 1

    assert a.code_version is None


def test_graph_asset_code_version():
    @rs.Asset
    def inner():
        return 1

    @rs.Asset.from_graph(code_version="v3")
    def g(inner: int) -> int:
        return inner

    assert g.code_version == "v3"


def test_external_asset_code_version_is_none():
    ext = rs.Asset.external(name="ext", io_handler=DuckHandler())
    assert ext.code_version is None


def test_single_asset_partitions_def():
    pdef = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(partitions_def=pdef)
    def a():
        return 1

    assert a.partitions_def is not None


def test_single_asset_partitions_def_default():
    @rs.Asset
    def a():
        return 1

    assert a.partitions_def is None


def test_external_asset_observe_fn():
    @rs.Asset.external(name="ext", io_handler=DuckHandler())
    def observe():
        return None

    assert observe.observe_fn is not None


def test_single_asset_observe_fn_is_none():
    @rs.Asset
    def a():
        return 1

    assert a.observe_fn is None
