"""Class-form assets with migrations still materialize.

The parallel executor ships a class defined in a test to loky workers by
value, with its class attributes: the marked classmethod, the
``migrations = [...]`` list and the ``AssetDef`` outputs. Each test has two
steps in one level, because a lone step runs in this process.
"""

import obstore.store
import pytest

import rivers as rs

EXECUTORS = {
    "in_process": rs.Executor.in_process(),
    "parallel": rs.Executor.parallel(max_workers=2),
}


def _convert(body):
    def add_currency(cls, ctx) -> None:
        return None

    async def add_currency_async(cls, ctx) -> None:
        return None

    func = add_currency if body == "sync" else add_currency_async
    return rs.migration(to=2, breaking=False)(classmethod(func))


def _single(name, value, handler, body):
    return type(
        name,
        (rs.Asset,),
        {
            "io_handler": handler,
            "schema_version": 3,
            "migrations": [rs.AssetMigration(to=3, rebuild=True)],
            "add_currency": _convert(body),
            "materialize": classmethod(lambda cls: value),
        },
    )


def _multi(handler, body):
    return type(
        "Ingest",
        (rs.MultiAsset,),
        {
            "io_handler": handler,
            "schema_version": 2,
            "customers": rs.AssetDef(
                schema_version=3, migrations=[rs.AssetMigration(to=3, rebuild=True)]
            ),
            "orders": rs.AssetDef(),
            "add_currency": _convert(body),
            "materialize": classmethod(lambda cls: {"customers": 1, "orders": 2}),
        },
    )


@pytest.mark.parametrize("executor", EXECUTORS)
@pytest.mark.parametrize("body", ["sync", "async"])
@pytest.mark.parametrize("form", ["single", "multi"])
def test_class_asset_with_migrations_materializes(tmp_path, executor, body, form):
    handler = rs.PickleIOHandler(
        store=obstore.store.LocalStore(str(tmp_path), mkdir=True)
    )
    if form == "single":
        assets = [_single("Left", 1, handler, body), _single("Right", 2, handler, body)]
        expected = {"left": 1, "right": 2}
    else:
        assets = [_multi(handler, body), _single("Right", 3, handler, body)]
        expected = {"customers": 1, "orders": 2, "right": 3}
    repo = rs.CodeRepository(assets=assets, default_executor=EXECUTORS[executor])

    assert repo.materialize().success

    assert {name: repo.load_node(name) for name in expected} == expected
