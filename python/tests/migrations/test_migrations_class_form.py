"""Migrations in a class-form asset body: ``@rs.migration`` classmethods,
``AssetMigration`` attributes and the ``migrations = [...]`` list, collected
across the MRO."""

import datetime

import pytest

import rivers as rs
from rivers._core.assets import desugar
from rivers.exceptions import AssetDefinitionError


def convert(cls, ctx) -> None:
    return None


async def convert_async(cls, ctx) -> None:
    return None


def _materialize(cls) -> int:
    return 1


def _migrations(asset):
    return sorted((m.to, m.kind, m.breaking, m.description) for m in asset.migrations)


@pytest.mark.parametrize(
    ("body", "is_async"),
    [(convert, False), (convert_async, True)],
    ids=["sync", "async"],
)
def test_marker_registers_a_convert(body, is_async):
    marked = rs.migration(to=2, breaking=False, description="add currency")(
        classmethod(body)
    )
    Orders = type(
        "Orders",
        (rs.Asset,),
        {"schema_version": 2, "materialize": classmethod(_materialize), "add": marked},
    )

    (migration,) = desugar(Orders).migrations

    assert (
        migration.to,
        migration.kind,
        migration.breaking,
        migration.description,
    ) == (
        2,
        "convert",
        False,
        "add currency",
    )
    *_, func, detected = migration.__reduce__()[1]
    assert func.__func__ is body
    assert func.__self__ is Orders
    assert detected is is_async


def test_marker_keeps_the_start_time():
    friday_night = datetime.datetime(2026, 11, 27, 21, 0, tzinfo=datetime.timezone.utc)
    marked = rs.migration(to=2, start_at=friday_night)(classmethod(convert))
    Orders = type(
        "Orders",
        (rs.Asset,),
        {"schema_version": 2, "materialize": classmethod(_materialize), "add": marked},
    )

    (migration,) = desugar(Orders).migrations

    assert migration.start_at == friday_night


def test_marker_start_time_needs_a_time_zone():
    with pytest.raises(AssetDefinitionError, match="start_at needs a time zone"):
        rs.migration(to=2, start_at=datetime.datetime(2026, 11, 27, 22, 0))


def test_list_and_attribute_register_migrations_without_a_function():
    class Orders(rs.Asset):
        schema_version = 4
        migrations = [rs.AssetMigration(to=3, rebuild=True)]
        to_v4 = rs.AssetMigration(to=4, description="new readers only")

        @classmethod
        def materialize(cls) -> int:
            return 1

        @rs.migration(to=2)
        @classmethod
        def add_currency(cls, ctx) -> None: ...

    assert _migrations(desugar(Orders)) == [
        (2, "convert", True, None),
        (3, "rebuild", True, None),
        (4, "version_only", True, "new readers only"),
    ]


@pytest.mark.parametrize("base_form", ["marker", "list"])
def test_subclass_overrides_a_base_migration_to_the_same_version(base_form):
    class Base(rs.Asset):
        schema_version = 2

        @classmethod
        def materialize(cls) -> int:
            return 1

    if base_form == "marker":
        Base.base_convert = rs.migration(to=2, description="base")(classmethod(convert))
    else:
        Base.migrations = [rs.AssetMigration(to=2, description="base", rebuild=True)]

    class Orders(Base):
        @rs.migration(to=2, description="orders")
        @classmethod
        def add_currency(cls, ctx) -> None: ...

    assert _migrations(desugar(Orders)) == [(2, "convert", True, "orders")]


def test_base_marker_wins_over_a_later_mixin_list():
    """``Orders.add_currency`` resolves to the base's method: the base comes
    before the mixin in the MRO, so the mixin's list entry to the same version
    loses, and its other entries stay."""

    class Table(rs.Asset):
        schema_version = 3

        @rs.migration(to=2, description="table")
        @classmethod
        def add_currency(cls, ctx) -> None: ...

    class AuditMixin:
        migrations = [
            rs.AssetMigration(to=2, rebuild=True, description="mixin"),
            rs.AssetMigration(to=3, description="mixin"),
        ]

    class Orders(Table, AuditMixin):
        @classmethod
        def materialize(cls) -> int:
            return 1

    assert _migrations(desugar(Orders)) == [
        (2, "convert", True, "table"),
        (3, "version_only", True, "mixin"),
    ]


@pytest.mark.parametrize("form", ["marker", "attribute"])
def test_list_and_attribute_to_one_version_in_one_class_is_an_error(form):
    listed = rs.AssetMigration(to=2, rebuild=True)
    body = {
        "schema_version": 2,
        "migrations": [listed],
        "materialize": classmethod(_materialize),
        "add_currency": (
            rs.migration(to=2)(classmethod(convert))
            if form == "marker"
            else rs.AssetMigration(to=2)
        ),
    }

    with pytest.raises(
        AssetDefinitionError,
        match=r"Orders\.add_currency and Orders\.migrations both declare the "
        r"migration to version 2; keep one",
    ):
        desugar(type("Orders", (rs.Asset,), body))

    body["add_currency"] = listed
    assert _migrations(desugar(type("Orders", (rs.Asset,), body))) == [
        (2, "rebuild", True, None)
    ]


def test_marker_without_classmethod_is_an_error():
    class Orders(rs.Asset):
        schema_version = 2

        @classmethod
        def materialize(cls) -> int:
            return 1

        @rs.migration(to=2)
        def add_currency(cls, ctx) -> None: ...

    with pytest.raises(
        AssetDefinitionError,
        match=r"Orders\.add_currency: @rs\.migration bodies must be defined with "
        r"@classmethod",
    ):
        desugar(Orders)


@pytest.mark.parametrize(
    ("listed", "message"),
    [
        (["rebuild"], "must contain AssetMigration objects, got 'rebuild'"),
        (3, "must be a list of AssetMigration objects"),
    ],
    ids=["element", "not-a-list"],
)
def test_migrations_list_holds_asset_migrations(listed, message):
    body = {"migrations": listed, "materialize": classmethod(_materialize)}

    with pytest.raises(AssetDefinitionError, match=rf"Orders\.migrations {message}"):
        desugar(type("Orders", (rs.Asset,), body))


def test_shadowing_an_inherited_migration_is_an_error():
    class Base(rs.Asset):
        schema_version = 2

        @rs.migration(to=2)
        @classmethod
        def add_currency(cls, ctx) -> None: ...

    class Orders(Base):
        add_currency = None

        @classmethod
        def materialize(cls) -> int:
            return 1

    with pytest.raises(
        AssetDefinitionError,
        match=r"Orders\.add_currency shadows the inherited migration 'add_currency'",
    ):
        desugar(Orders)


def test_misspelled_migrations_attribute_is_an_error():
    class Orders(rs.Asset):
        schema_version = 2
        migration = [rs.AssetMigration(to=2, rebuild=True)]

        @classmethod
        def materialize(cls) -> int:
            return 1

    with pytest.raises(
        AssetDefinitionError, match=r"Orders\.migration: did you mean 'migrations'\?"
    ):
        desugar(Orders)


def test_class_form_checks_run_with_the_class_name():
    class Orders(rs.Asset):
        schema_version = 2
        migrations = [rs.AssetMigration(to=3, rebuild=True)]

        @classmethod
        def materialize(cls) -> int:
            return 1

    with pytest.raises(
        AssetDefinitionError,
        match="asset 'orders': migration to version 3 is not between 2 and "
        "schema_version 2",
    ):
        desugar(Orders)


# ---------------------------------------------------------------------------
# Multi, graph and external classes
# ---------------------------------------------------------------------------


def test_multi_class_merges_its_migrations_into_each_output():
    class Ingest(rs.MultiAsset):
        schema_version = 3
        customers = rs.AssetDef(migrations=[rs.AssetMigration(to=3, rebuild=True)])
        orders = rs.AssetDef(schema_version=2)

        @classmethod
        def materialize(cls) -> dict[str, int]:
            return {"customers": 1, "orders": 2}

        @rs.migration(to=2, breaking=False)
        @classmethod
        def split_names(cls, ctx) -> None: ...

    defs = {d.name: d for d in desugar(Ingest).output_defs}

    assert {name: d.schema_version for name, d in defs.items()} == {
        "customers": 3,
        "orders": 2,
    }
    assert sorted((m.to, m.kind) for m in defs["customers"].migrations) == [
        (2, "convert"),
        (3, "rebuild"),
    ]
    assert [(m.to, m.kind) for m in defs["orders"].migrations] == [(2, "convert")]


def test_graph_class_declares_versions():
    @rs.Task
    def load() -> int:
        return 1

    class Report(rs.GraphAsset):
        schema_version = 2
        migrations = [rs.AssetMigration(to=2, rebuild=True)]

        @classmethod
        def compose(cls):
            return load()

    asset = desugar(Report)

    assert asset.schema_version == 2
    assert _migrations(asset) == [(2, "rebuild", True, None)]


@pytest.mark.parametrize("declared", ["schema_version", "list", "marker"])
def test_external_class_takes_no_versions(declared):
    body: dict = {"io_handler": rs.InMemoryIOHandler()}
    if declared == "schema_version":
        body["schema_version"] = 2
        message = r"Feed\.schema_version: this asset kind takes no 'schema_version'"
    elif declared == "list":
        body["migrations"] = [rs.AssetMigration(to=2, rebuild=True)]
        message = "Feed: external assets take no migrations"
    else:
        body["add_currency"] = rs.migration(to=2)(classmethod(convert))
        message = "Feed: external assets take no migrations"

    with pytest.raises(AssetDefinitionError, match=message):
        desugar(type("Feed", (rs.ExternalAsset,), body))
