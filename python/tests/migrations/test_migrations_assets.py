"""``schema_version`` and ``migrations`` on decorator, class, multi and graph
assets: defaults, read-back, the merge per multi-asset output, and the checks
at definition."""

import pytest

import rivers as rs
from rivers._core.assets import desugar
from rivers.exceptions import AssetDefinitionError

FORMS = pytest.mark.parametrize("form", ["decorator", "class", "multi", "graph"])


def add_currency(ctx) -> None:
    return None


async def add_currency_async(ctx) -> None:
    return None


def _orders(form, **declared):
    """An asset named ``orders`` (for ``multi``, the one output of ``ingest``)
    that declares ``declared`` as keywords or as class attributes."""
    if form == "decorator":

        @rs.Asset(**declared)
        def orders() -> int:
            return 1

        return orders

    if form == "class":
        body = {"materialize": classmethod(lambda cls: 1), **declared}
        return desugar(type("Orders", (rs.Asset,), body))

    if form == "multi":

        @rs.Asset.from_multi(output_defs=[rs.AssetDef("orders")], **declared)
        def ingest():
            return {"orders": 1}

        return ingest

    @rs.Task
    def load() -> int:
        return 1

    @rs.Asset.from_graph(**declared)
    def orders():
        return load()

    return orders


def _declared(asset):
    """``orders``' version and its migrations as ``(to, kind, breaking)``."""
    if asset.is_multi:
        (output,) = asset.output_defs
        version, migrations = output.schema_version, output.migrations
    else:
        version, migrations = asset.schema_version, asset.migrations or []
    return version, [(m.to, m.kind, m.breaking) for m in migrations]


@FORMS
def test_defaults(form):
    asset = _orders(form)

    assert _declared(asset) == (1, [])
    assert asset.migrations is None


@FORMS
@pytest.mark.parametrize(
    "body", [add_currency, add_currency_async], ids=["sync", "async"]
)
def test_declared_values_read_back(form, body):
    asset = _orders(
        form,
        schema_version=4,
        migrations=[
            rs.AssetMigration(to=2, breaking=False)(body),
            rs.AssetMigration(to=3, rebuild=True),
            rs.AssetMigration(to=4),
        ],
    )

    assert _declared(asset) == (
        4,
        [(2, "convert", False), (3, "rebuild", True), (4, "version_only", True)],
    )


@FORMS
@pytest.mark.parametrize("version", [0, -1])
def test_schema_version_below_1_is_an_error(form, version):
    name = "ingest" if form == "multi" else "orders"

    with pytest.raises(
        AssetDefinitionError,
        match=rf"asset '{name}': schema_version must be 1 or more, got {version}",
    ):
        _orders(form, schema_version=version)


@FORMS
def test_schema_version_above_the_stored_range_is_an_error(form):
    """Storage keeps versions as 32-bit unsigned numbers."""
    name = "ingest" if form == "multi" else "orders"

    with pytest.raises(
        AssetDefinitionError,
        match=rf"asset '{name}': schema_version must be at most 4294967295, got {2**32}",
    ):
        _orders(form, schema_version=2**32)


@FORMS
@pytest.mark.parametrize("to", [1, 3])
def test_migration_outside_2_and_schema_version_is_an_error(form, to):
    with pytest.raises(
        AssetDefinitionError,
        match=rf"asset 'orders': migration to version {to} is not between 2 and "
        r"schema_version 2",
    ):
        _orders(form, schema_version=2, migrations=[rs.AssetMigration(to=to)])


@FORMS
def test_two_migrations_to_one_version_is_an_error(form):
    with pytest.raises(
        AssetDefinitionError, match="asset 'orders': two migrations go to version 2"
    ):
        _orders(
            form,
            schema_version=2,
            migrations=[
                rs.AssetMigration(to=2)(add_currency),
                rs.AssetMigration(to=2, rebuild=True),
            ],
        )


@FORMS
@pytest.mark.parametrize("rebuild", [True, False], ids=["rebuild", "version-only"])
def test_migration_without_a_function_takes_no_retry(form, rebuild):
    """A rebuild runs the asset's own function with the asset's retry, and a
    version-only migration runs nothing, so a retry on either never applies."""
    migration = rs.AssetMigration(to=2, rebuild=rebuild, retry=rs.RetryPolicy())

    with pytest.raises(
        AssetDefinitionError,
        match="asset 'orders': migration to version 2 has no function, so it takes "
        "no retry=",
    ):
        _orders(form, schema_version=2, migrations=[migration])


def test_a_convert_takes_a_retry():
    migration = rs.AssetMigration(to=2, retry="flaky")(add_currency)

    asset = _orders("decorator", schema_version=2, migrations=[migration])

    assert asset.migrations == [migration]


def test_a_decorator_without_a_name_is_checked_when_it_wraps():
    """The check waits for the asset's name, so that the message names it."""
    versioned = rs.Asset(schema_version=0)

    with pytest.raises(
        AssetDefinitionError, match="asset 'orders': schema_version must be 1 or more"
    ):

        @versioned
        def orders() -> int:
            return 1

    with pytest.raises(
        AssetDefinitionError, match="asset 'named': schema_version must be 1 or more"
    ):
        rs.Asset(name="named", schema_version=0)


# ---------------------------------------------------------------------------
# Multi-asset outputs
# ---------------------------------------------------------------------------


def test_multi_outputs_take_the_top_level_version_unless_they_declare_one():
    @rs.Asset.from_multi(
        schema_version=2,
        output_defs=[rs.AssetDef("customers"), rs.AssetDef("orders", schema_version=3)],
    )
    def ingest():
        return {"customers": 1, "orders": 2}

    assert ingest.schema_version == 2
    assert {d.name: d.schema_version for d in ingest.output_defs} == {
        "customers": 2,
        "orders": 3,
    }


def test_multi_output_migrations_merge_with_the_top_level_ones():
    """A top-level migration applies to every output; an output's own
    migration to the same version replaces it for that output."""
    shared_split = rs.AssetMigration(to=2)(add_currency)
    shared_rebuild = rs.AssetMigration(to=3, rebuild=True)
    own_split = rs.AssetMigration(to=2, breaking=False)(add_currency_async)

    @rs.Asset.from_multi(
        schema_version=3,
        migrations=[shared_split, shared_rebuild],
        output_defs=[
            rs.AssetDef("customers"),
            rs.AssetDef("orders", migrations=[own_split]),
        ],
    )
    def ingest():
        return {"customers": 1, "orders": 2}

    merged = {d.name: d.migrations for d in ingest.output_defs}
    assert merged["customers"] == [shared_split, shared_rebuild]
    assert merged["orders"] == [shared_rebuild, own_split]
    assert ingest.migrations is None


def test_multi_checks_each_output_against_its_own_version():
    """A top-level migration to version 3 suits ``orders`` (version 3) but
    not ``customers``, which takes the top-level version 2."""
    with pytest.raises(
        AssetDefinitionError,
        match="asset 'customers': migration to version 3 is not between 2 and "
        "schema_version 2",
    ):

        @rs.Asset.from_multi(
            schema_version=2,
            migrations=[rs.AssetMigration(to=3, rebuild=True)],
            output_defs=[
                rs.AssetDef("customers"),
                rs.AssetDef("orders", schema_version=3),
            ],
        )
        def ingest():
            return {"customers": 1, "orders": 2}


def test_multi_output_version_below_1_names_the_output():
    with pytest.raises(
        AssetDefinitionError,
        match="asset 'orders': schema_version must be 1 or more, got 0",
    ):

        @rs.Asset.from_multi(output_defs=[rs.AssetDef("orders", schema_version=0)])
        def ingest():
            return {"orders": 1}


def test_asset_def_defaults():
    ad = rs.AssetDef("orders")

    assert ad.schema_version is None
    assert ad.migrations == []


# ---------------------------------------------------------------------------
# External and remote assets
# ---------------------------------------------------------------------------


def test_external_and_remote_assets_read_as_version_1():
    """rivers does not own their data, so they declare no versions."""
    external = rs.Asset.external(name="feed", io_handler=rs.InMemoryIOHandler())
    remote = rs.Asset.remote("orders", code_location="sales")

    for asset in (external, remote):
        assert asset.schema_version == 1
        assert asset.migrations is None


@pytest.mark.parametrize(
    "declared",
    [{"schema_version": 2}, {"migrations": [rs.AssetMigration(to=2)]}],
    ids=["schema_version", "migrations"],
)
def test_external_asset_takes_no_versions(declared):
    """Like every keyword an asset kind does not take, it is a TypeError in
    the decorator form; the class form raises AssetDefinitionError."""
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        rs.Asset.external(name="feed", io_handler=rs.InMemoryIOHandler(), **declared)
