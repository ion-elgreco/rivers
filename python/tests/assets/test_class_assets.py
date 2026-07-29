"""Class-form asset definitions: the pure-Python adapter.

A class-form asset desugars at registration into the same objects the
decorator form produces — these tests pin the desugaring rules: name
derivation, config collection across the MRO (mixins), verb validation,
attribute outputs on multi assets, and parity across all four kinds.
"""

import asyncio

import obstore.store
import pytest

import rivers as rs
from rivers._core.assets import desugar, is_asset_class, node_names
from rivers.exceptions import AssetDefinitionError

IP = rs.Executor.in_process()


def _pickle_handler(tmp_path):
    return rs.PickleIOHandler(store=obstore.store.LocalStore(str(tmp_path), mkdir=True))


# ---------------------------------------------------------------------------
# Single assets
# ---------------------------------------------------------------------------


def test_single_class_asset_materializes():
    class EnrichedOrders(rs.Asset):
        @classmethod
        def materialize(cls) -> int:
            return 42

    repo = rs.CodeRepository(assets=[EnrichedOrders], default_executor=IP)
    repo.materialize()
    assert repo.load_node("enriched_orders") == 42


def test_single_name_attribute_overrides_derived_name():
    class Orders(rs.Asset):
        name = "orders_v2"

        @classmethod
        def materialize(cls) -> int:
            return 1

    repo = rs.CodeRepository(assets=[Orders], default_executor=IP)
    repo.materialize()
    assert repo.load_node("orders_v2") == 1
    assert node_names(Orders) == ["orders_v2"]


def test_single_dep_by_param_name_and_mixed_forms():
    @rs.Asset
    def base_value() -> int:
        return 40

    class Derived(rs.Asset):
        @classmethod
        def materialize(cls, base_value: int) -> int:
            return base_value + 2

    class Final(rs.Asset):
        @classmethod
        def materialize(cls, derived: int) -> int:
            return derived * 10

    repo = rs.CodeRepository(assets=[base_value, Derived, Final], default_executor=IP)
    repo.materialize()
    assert repo.load_node("derived") == 42
    assert repo.load_node("final") == 420


def test_single_config_attributes_reach_context():
    class Configured(rs.Asset):
        tags = ["prod"]
        kinds = "table"
        group = "analytics"
        code_version = "v3"
        metadata = {"priority": "high"}

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext) -> dict:
            return {
                "tags": list(context.tags),
                "kinds": list(context.kinds),
                "group": context.group,
                "code_version": context.code_version,
                "metadata": dict(context.asset_metadata),
            }

    repo = rs.CodeRepository(assets=[Configured], default_executor=IP)
    repo.materialize()
    got = repo.load_node("configured")
    assert got["tags"] == ["prod"]
    assert "table" in got["kinds"]
    assert got["group"] == "analytics"
    assert got["code_version"] == "v3"
    assert got["metadata"]["priority"] == "high"


def test_single_async_classmethod():
    class AsyncOrders(rs.Asset):
        @classmethod
        async def materialize(cls) -> int:
            await asyncio.sleep(0)
            return 7

    repo = rs.CodeRepository(assets=[AsyncOrders], default_executor=IP)
    repo.materialize()
    assert repo.load_node("async_orders") == 7


def test_single_partitioned_class_asset():
    class Daily(rs.Asset):
        partitions_def = rs.PartitionsDefinition.static_(["a", "b"])

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext) -> str:
            return f"got:{context.partition_key}"

    pk = rs.PartitionKey.single("a")
    repo = rs.CodeRepository(assets=[Daily], default_executor=IP)
    repo.materialize(partition_key=pk)
    assert repo.load_node("daily", partition_key=pk) == "got:a"


# ---------------------------------------------------------------------------
# Mixins / inheritance
# ---------------------------------------------------------------------------


def test_mixin_shares_handler_and_helpers(tmp_path):
    handler = _pickle_handler(tmp_path)

    class StoreAsset(rs.Asset):
        io_handler = handler

        @classmethod
        def _payload(cls) -> int:
            return 21

    class Left(StoreAsset):
        @classmethod
        def materialize(cls) -> int:
            return cls._payload()

    class Right(StoreAsset):
        @classmethod
        def materialize(cls) -> int:
            return cls._payload() * 2

    repo = rs.CodeRepository(assets=[Left, Right], default_executor=IP)
    repo.materialize()
    assert repo.load_node("left") == 21
    assert repo.load_node("right") == 42


def test_subclass_config_overrides_base():
    class Base(rs.Asset):
        group = "base_group"

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext) -> str:
            return context.group

    class Child(Base):
        group = "child_group"

    repo = rs.CodeRepository(assets=[Child], default_executor=IP)
    repo.materialize()
    assert repo.load_node("child") == "child_group"


def test_template_base_materialize_binds_subclass_cls():
    class SeedBase(rs.Asset):
        seed = 0

        @classmethod
        def materialize(cls) -> int:
            return cls.seed

    class SeedOne(SeedBase):
        seed = 100

    class SeedTwo(SeedBase):
        seed = 200

    repo = rs.CodeRepository(assets=[SeedOne, SeedTwo], default_executor=IP)
    repo.materialize()
    assert repo.load_node("seed_one") == 100
    assert repo.load_node("seed_two") == 200


def test_mixin_is_not_an_asset_until_listed():
    class Mixin(rs.Asset):
        group = "shared"

    assert is_asset_class(Mixin)
    with pytest.raises(AssetDefinitionError, match="defines no materialize"):
        desugar(Mixin)


# ---------------------------------------------------------------------------
# Multi assets
# ---------------------------------------------------------------------------


def test_multi_attribute_outputs_dict_return():
    class Ingest(rs.MultiAsset):
        customers = rs.AssetDef()
        orders = rs.AssetDef()

        @classmethod
        def materialize(cls):
            return {"customers": 10, "orders": 20}

    repo = rs.CodeRepository(assets=[Ingest], default_executor=IP)
    repo.materialize()
    assert repo.load_node("customers") == 10
    assert repo.load_node("orders") == 20
    assert node_names(Ingest) == ["customers", "orders"]


def test_near_miss_names_are_allowed_for_non_config_values():
    """The typo guard must not fire on things that structurally can't be config.

    A class body is a general namespace: `MultiAsset` outputs, action verbs and
    helper methods all legitimately use names that near-miss a config key.
    """

    class Ingest(rs.MultiAsset):
        # `kind` near-misses `kinds`, `partitions` near-misses `partitions_def`
        kind = rs.AssetDef()
        partitions = rs.AssetDef()

        @classmethod
        def materialize(cls):
            return {"kind": 1, "partitions": 2}

    repo = rs.CodeRepository(assets=[Ingest], default_executor=IP)
    repo.materialize()
    assert repo.load_node("kind") == 1
    assert repo.load_node("partitions") == 2

    def _tag(ctx):
        return None

    tag_action = rs.AssetAction(name="tag", outcome=rs.Outcome.Unchanged)(_tag)

    class Orders(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        actions = [tag_action]

        @classmethod
        def materialize(cls):
            return 1

        # near-misses `retry`, but it is a method, not configuration
        @classmethod
        def retries(cls) -> int:
            return 3

    repo2 = rs.CodeRepository(assets=[Orders], default_executor=IP)
    repo2.materialize()
    assert repo2.load_node("orders") == 1
    assert Orders.retries() == 3


def test_near_miss_config_value_still_rejected():
    """A plain data value under a near-miss name is the case the guard is for."""

    class Orders(rs.Asset):
        kind = "delta"

        @classmethod
        def materialize(cls):
            return 1

    with pytest.raises(AssetDefinitionError, match="did you mean 'kinds'"):
        desugar(Orders)


def test_multi_generator_yielding_outputs():
    class GenIngest(rs.MultiAsset):
        g_cust = rs.AssetDef()
        g_ord = rs.AssetDef()

        @classmethod
        def materialize(cls):
            yield rs.Output(1, output_name="g_cust")
            yield rs.Output(9, output_name="g_ord")

    repo = rs.CodeRepository(assets=[GenIngest], default_executor=IP)
    repo.materialize()
    assert repo.load_node("g_cust") == 1
    assert repo.load_node("g_ord") == 9


def test_multi_explicit_assetdef_name_wins():
    class Renamed(rs.MultiAsset):
        attr_name = rs.AssetDef(name="explicit_name")

        @classmethod
        def materialize(cls):
            return {"explicit_name": 5}

    repo = rs.CodeRepository(assets=[Renamed], default_executor=IP)
    repo.materialize()
    assert repo.load_node("explicit_name") == 5
    assert node_names(Renamed) == ["explicit_name"]


def test_multi_per_output_config():
    class PerOutput(rs.MultiAsset):
        alpha = rs.AssetDef(group="g_alpha")
        beta = rs.AssetDef(group="g_beta")

        @classmethod
        def materialize(cls):
            return {"alpha": 1, "beta": 2}

    asset = desugar(PerOutput)
    defs = {d.name: d for d in asset.output_defs}
    assert defs["alpha"].group == "g_alpha"
    assert defs["beta"].group == "g_beta"


def test_multi_without_assetdef_attrs_is_an_error():
    class Empty(rs.MultiAsset):
        @classmethod
        def materialize(cls):
            return {}

    with pytest.raises(AssetDefinitionError, match="no AssetDef attributes"):
        desugar(Empty)


def test_multi_shared_assetdef_instance_is_an_error():
    shared = rs.AssetDef()

    class Aliased(rs.MultiAsset):
        first = shared
        second = shared

        @classmethod
        def materialize(cls):
            return {}

    with pytest.raises(AssetDefinitionError, match="share one AssetDef instance"):
        desugar(Aliased)


def test_assetdef_without_name_outside_class_form_is_an_error():
    with pytest.raises(AssetDefinitionError, match="requires name="):
        rs.Asset.from_multi(output_defs=[rs.AssetDef()])(lambda: {})


# ---------------------------------------------------------------------------
# Graph assets
# ---------------------------------------------------------------------------


def test_graph_class_asset_composes():
    @rs.Asset
    def g_source() -> int:
        return 5

    @rs.Task
    def g_double(g_source: int) -> int:
        return g_source * 2

    class GPipeline(rs.GraphAsset):
        @classmethod
        def compose(cls, g_source: int):
            return g_double(g_source)

    repo = rs.CodeRepository(
        assets=[g_source, GPipeline], tasks=[g_double], default_executor=IP
    )
    assert "g_pipeline" in repo.assets
    repo.materialize()
    assert repo.load_node("g_pipeline") == 10


# ---------------------------------------------------------------------------
# External assets
# ---------------------------------------------------------------------------


def test_external_class_asset_observe(tmp_path):
    class VendorFeed(rs.ExternalAsset):
        io_handler = _pickle_handler(tmp_path)

        @classmethod
        def observe(cls) -> rs.Observation:
            return rs.Observation(
                metadata={"rows": rs.MetadataValue.int(1)}, data_version="abc"
            )

    repo = rs.CodeRepository(assets=[VendorFeed], default_executor=IP)
    out = repo.observe()
    assert out is not None


def test_external_without_observe_is_allowed(tmp_path):
    class Reference(rs.ExternalAsset):
        io_handler = _pickle_handler(tmp_path)

    repo = rs.CodeRepository(assets=[Reference], default_executor=IP)
    assert "reference" in repo.assets


# ---------------------------------------------------------------------------
# Definition errors
# ---------------------------------------------------------------------------


def test_plain_method_materialize_is_an_error():
    class Plain(rs.Asset):
        def materialize(self) -> int:  # missing @classmethod
            return 1

    with pytest.raises(AssetDefinitionError, match="@classmethod"):
        desugar(Plain)


def test_observe_on_regular_asset_is_an_error():
    class Confused(rs.Asset):
        @classmethod
        def materialize(cls) -> int:
            return 1

        @classmethod
        def observe(cls):
            return None

    with pytest.raises(AssetDefinitionError, match="only ExternalAsset"):
        desugar(Confused)


def test_materialize_on_external_asset_is_an_error(tmp_path):
    class Wrong(rs.ExternalAsset):
        io_handler = _pickle_handler(tmp_path)

        @classmethod
        def materialize(cls) -> int:
            return 1

    with pytest.raises(AssetDefinitionError, match="observed, not produced"):
        desugar(Wrong)


def test_assetdef_attr_on_single_asset_is_an_error():
    class NotMulti(rs.Asset):
        extra = rs.AssetDef()

        @classmethod
        def materialize(cls) -> int:
            return 1

    with pytest.raises(AssetDefinitionError, match="MultiAsset subclass"):
        desugar(NotMulti)


def test_registering_the_base_class_is_an_error():
    with pytest.raises(AssetDefinitionError, match="base class"):
        rs.CodeRepository(assets=[rs.Asset])


def test_registering_a_non_asset_class_is_an_error():
    class NotAnAsset:
        pass

    with pytest.raises(AssetDefinitionError, match="does not subclass"):
        rs.CodeRepository(assets=[NotAnAsset])


# ---------------------------------------------------------------------------
# Jobs over class-form assets
# ---------------------------------------------------------------------------


def test_job_accepts_class_form_assets():
    class JobOrders(rs.Asset):
        @classmethod
        def materialize(cls) -> int:
            return 3

    class JobIngest(rs.MultiAsset):
        j_left = rs.AssetDef()
        j_right = rs.AssetDef()

        @classmethod
        def materialize(cls):
            return {"j_left": 1, "j_right": 2}

    repo = rs.CodeRepository(
        assets=[JobOrders, JobIngest],
        jobs=[rs.Job(name="j", assets=[JobOrders, JobIngest], executor=IP)],
    )
    repo.get_job("j").execute()
    assert repo.load_node("job_orders") == 3
    assert repo.load_node("j_left") == 1
    assert repo.load_node("j_right") == 2


# ---------------------------------------------------------------------------
# Declared-but-unsupported config, actions lists, cross-class AssetDef reuse
# ---------------------------------------------------------------------------


def test_unsupported_config_attribute_is_an_error():
    """A base that can't carry an attribute must say so — silently dropping it
    left the asset configured differently than the class body reads."""

    class Ingest(rs.MultiAsset):
        out_a = rs.AssetDef()
        pool = "writers"

        @classmethod
        def materialize(cls):
            return {"out_a": 1}

    with pytest.raises(AssetDefinitionError, match="pool"):
        desugar(Ingest)


@pytest.mark.parametrize(
    ("wrong", "right"),
    [
        ("partitions", "partitions_def"),
        ("partition_def", "partitions_def"),
        ("automation", "automation_condition"),
        ("tag", "tags"),
        ("kind", "kinds"),
        ("pools", "pool"),
        ("retries", "retry"),
        ("backfill", "backfill_strategy"),
    ],
)
def test_misspelled_config_attribute_is_an_error(wrong, right):
    """A near-miss declaration must not read as an ordinary user attribute.

    ``rs.Asset(partitions=...)`` raises TypeError; the class body silently
    registered an unpartitioned asset instead, so the two forms disagreed on
    the same typo.
    """
    body = {
        wrong: rs.PartitionsDefinition.static_(["p1"]),
        "materialize": classmethod(lambda cls: 1),
    }
    Orders = type("Orders", (rs.Asset,), body)

    with pytest.raises(AssetDefinitionError, match=right):
        desugar(Orders)


def test_actions_list_attribute_registers_the_actions():
    """`actions = [...]` mirrors the decorator's `actions=[...]` — it used to
    be silently ignored, so the verbs never existed."""

    def _touch(ctx):
        return None

    touch = rs.AssetAction(name="touch", outcome=rs.Outcome.Unchanged)(_touch)

    class Table(rs.Asset):
        actions = [touch]

        @classmethod
        def materialize(cls):
            return 1

    asset = desugar(Table)
    repo = rs.CodeRepository(assets=[asset], default_executor=IP)
    repo.materialize()
    assert repo.run_action("touch").success


def test_assetdef_shared_across_classes_is_an_error():
    """One AssetDef reused by two classes named both outputs the same, and the
    second silently replaced the first in the graph."""
    shared = rs.AssetDef()

    class First(rs.MultiAsset):
        out = shared

        @classmethod
        def materialize(cls):
            return {"out": 1}

    class Second(rs.MultiAsset):
        other = shared

        @classmethod
        def materialize(cls):
            return {"other": 2}

    with pytest.raises(AssetDefinitionError, match="out"):
        rs.CodeRepository(assets=[First, Second], default_executor=IP).resolve()


def test_declared_config_reads_back_off_the_asset():
    """Attributes the stubs declare must exist at runtime — `retry`, `compute`
    and `backfill_strategy` raised AttributeError while type-checking clean."""

    class Orders(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        retry = rs.RetryPolicy(max_retries=3)
        compute = rs.Compute(cpu="2", memory="4Gi")
        backfill_strategy = rs.BackfillStrategy.single_run()
        pool = "writers"

        @classmethod
        def materialize(cls):
            return 1

    asset = desugar(Orders)
    assert asset.retry.max_retries == 3
    assert asset.compute.cpu == "2"
    assert asset.backfill_strategy is not None
    # pool_slots folds into pool, which is what reads back.
    assert asset.pool == [("writers", 1)]


def test_named_retry_reads_back_as_the_name():
    """A `retry="name"` reference reads back as the name until the repository
    resolves it against its registry."""

    class Orders(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        retry = "flaky"

        @classmethod
        def materialize(cls):
            return 1

    assert desugar(Orders).retry == "flaky"
