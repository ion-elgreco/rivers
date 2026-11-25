"""``rs.AssetMigration``: its kinds, binding a function, and pickling."""

import pickle

import cloudpickle
import pytest

import rivers as rs
from rivers.exceptions import AssetDefinitionError


def add_currency(ctx) -> None:
    return None


async def add_currency_async(ctx) -> None:
    return None


BODIES = pytest.mark.parametrize(
    ("body", "is_async"),
    [(add_currency, False), (add_currency_async, True)],
    ids=["sync", "async"],
)


def _state(migration):
    """What the migration pickles: (to, breaking, rebuild, retry, description,
    function, is_async)."""
    return migration.__reduce__()[1]


def test_defaults():
    m = rs.AssetMigration(to=2)

    assert (m.to, m.breaking, m.rebuild, m.description) == (2, True, False, None)
    assert m.kind == "version_only"


@pytest.mark.parametrize(
    ("migration", "kind"),
    [
        (rs.AssetMigration(to=2)(add_currency), "convert"),
        (rs.AssetMigration(to=2)(add_currency_async), "convert"),
        (rs.AssetMigration(to=2, rebuild=True), "rebuild"),
        (rs.AssetMigration(to=2), "version_only"),
    ],
    ids=["convert-sync", "convert-async", "rebuild", "version-only"],
)
def test_kind(migration, kind):
    assert migration.kind == kind


@BODIES
def test_binding_returns_a_convert_and_keeps_the_settings(body, is_async):
    unbound = rs.AssetMigration(
        to=3,
        breaking=False,
        retry=rs.RetryPolicy(max_retries=2),
        description="add currency",
    )

    bound = unbound(body)

    assert (bound.to, bound.breaking, bound.rebuild, bound.description) == (
        3,
        False,
        False,
        "add currency",
    )
    assert bound.kind == "convert"
    assert unbound.kind == "version_only"
    *_, func, detected = _state(bound)
    assert func is body
    assert detected is is_async


def test_binding_twice_is_an_error():
    bound = rs.AssetMigration(to=2)(add_currency)

    with pytest.raises(
        AssetDefinitionError,
        match="migration to version 2 is already bound to a function",
    ):
        bound(add_currency)


def test_binding_a_rebuild_is_an_error():
    with pytest.raises(
        AssetDefinitionError,
        match="migration to version 3 is a rebuild: it runs the asset's own function",
    ):
        rs.AssetMigration(to=3, rebuild=True)(add_currency)


def test_retry_is_a_policy_or_a_name():
    with pytest.raises(ValueError, match="retry must be a RetryPolicy or a str"):
        rs.AssetMigration(to=2, retry=3)  # type: ignore[arg-type]


def test_repr():
    assert (
        repr(rs.AssetMigration(to=2, breaking=False)(add_currency))
        == "AssetMigration(to=2, kind='convert', breaking=False)"
    )
    assert (
        repr(rs.AssetMigration(to=3, rebuild=True))
        == "AssetMigration(to=3, kind='rebuild', breaking=True)"
    )


@pytest.mark.parametrize(
    "migration",
    [
        rs.AssetMigration(
            to=2,
            breaking=False,
            retry=rs.RetryPolicy(max_retries=2, backoff=rs.Backoff.linear(1.0)),
            description="add currency",
        )(add_currency),
        rs.AssetMigration(to=2, retry="flaky")(add_currency_async),
        rs.AssetMigration(to=3, rebuild=True, description="recompute totals"),
        rs.AssetMigration(to=4),
    ],
    ids=["convert-sync", "convert-async", "rebuild", "version-only"],
)
def test_pickle_round_trip(migration):
    restored = pickle.loads(pickle.dumps(migration))

    assert type(restored) is rs.AssetMigration
    assert _state(restored) == _state(migration)
    assert restored.kind == migration.kind


def test_cloudpickle_ships_a_local_function():
    """loky ships tasks with cloudpickle, and a class body's migration often
    has a function that is local to a script."""

    def double(ctx):
        return ctx * 2

    restored = cloudpickle.loads(cloudpickle.dumps(rs.AssetMigration(to=2)(double)))

    *_, func, is_async = _state(restored)
    assert func(21) == 42
    assert is_async is False
    assert restored.kind == "convert"


def test_asset_def_pickles_its_version_and_migrations():
    ad = rs.AssetDef(
        "orders",
        schema_version=3,
        migrations=[
            rs.AssetMigration(to=2)(add_currency),
            rs.AssetMigration(to=3, rebuild=True),
        ],
    )

    restored = pickle.loads(pickle.dumps(ad))

    assert restored == ad
    assert restored.schema_version == 3
    assert [(m.to, m.kind) for m in restored.migrations] == [
        (2, "convert"),
        (3, "rebuild"),
    ]
