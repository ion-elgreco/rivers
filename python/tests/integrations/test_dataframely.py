"""dataframely integration: assets typed with a dataframely schema validate their output.

A valid frame is stored as dataframely returns it, with the schema as
``dataframely/schema`` metadata; an invalid frame fails the step with
dataframely's report. Every case runs on both executors: the parallel executor
validates a sync step in its loky worker.
"""

import importlib
import json
import os
import sys
from typing import Annotated

import pytest

dy = pytest.importorskip("dataframely")
pl = pytest.importorskip("polars")

from polars.testing import assert_frame_equal  # noqa: E402

import rivers as rs  # noqa: E402
from rivers.integrations import output_validator  # noqa: E402
from rivers.integrations.dataframely import validate_output  # noqa: E402


class Orders(dy.Schema):
    """Orders placed in the shop."""

    id = dy.Int64(primary_key=True, min=0, description="Order id")
    amount = dy.Float64(nullable=True, min_exclusive=0)
    status = dy.String(regex="^(open|closed)$")

    @dy.rule()
    def open_orders_have_amount(cls) -> pl.Expr:
        return (pl.col("status") != "open") | pl.col("amount").is_not_null()


class Prices(dy.Schema):
    price = dy.Float64()

    class Config:
        cast = True


GOOD_ROWS = {"id": [1, 2], "amount": [9.5, None], "status": ["open", "closed"]}
BAD_ROWS = {"id": [1, -1], "amount": [None, -3.0], "status": ["open", "lost"]}

ORDERS_SCHEMA = {
    "name": "Orders",
    "description": "Orders placed in the shop.",
    "columns": [
        {
            "name": "id",
            "type": "Int64",
            "nullable": False,
            "primary_key": True,
            "unique": False,
            "description": "Order id",
            "checks": ["min=0"],
        },
        {
            "name": "amount",
            "type": "Float64",
            "nullable": True,
            "primary_key": False,
            "unique": False,
            "description": None,
            "checks": ["min_exclusive=0", "inf", "nan"],
        },
        {
            "name": "status",
            "type": "String",
            "nullable": False,
            "primary_key": False,
            "unique": False,
            "description": None,
            "checks": ["regex='^(open|closed)$'"],
        },
    ],
    "checks": ["open_orders_have_amount"],
}

HINTS = [
    pytest.param(dy.DataFrame[Orders], id="dataframe"),
    pytest.param(Annotated[pl.DataFrame, Orders], id="annotated"),
]


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("hint", HINTS)
def test_valid_frame_is_stored_with_its_schema(env, tmp_path, hint, is_async):
    pid_file = tmp_path / "pid"
    frame = pl.DataFrame(GOOD_ROWS)

    repo, result = env.materialize(
        env.asset(hint, frame, is_async=is_async, pid_file=pid_file)
    )

    assert result.success
    if not is_async:
        ran_in_worker = int(pid_file.read_text()) != os.getpid()
        assert ran_in_worker == env.parallel
    assert_frame_equal(repo.load_node("orders"), frame)
    assert env.stored_json("orders", "dataframely/schema") == ORDERS_SCHEMA


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("hint", HINTS)
def test_invalid_frame_fails_with_the_dataframely_report(env, hint, is_async):
    frame = pl.DataFrame(BAD_ROWS)

    _, result = env.materialize(env.asset(hint, frame, is_async=is_async))

    assert not result.success
    with pytest.raises(dy.exc.ValidationError) as report:
        Orders.validate(frame)
    error = dict(result.failed_assets)["orders"]
    assert error == (
        "AssetOutputValidationError: Asset 'orders' returned a value that does "
        f"not match '{hint}': {report.value}"
    )
    for rule in ("'min'", "'min_exclusive'", "'regex'", "'open_orders_have_amount'"):
        assert rule in error
    assert env.storage.get_latest_materialization("orders") is None


def test_wrong_column_type_fails_without_config_cast(env):
    frame = pl.DataFrame({**GOOD_ROWS, "id": ["1", "2"]})

    _, result = env.materialize(env.asset(dy.DataFrame[Orders], frame))

    assert not result.success
    with pytest.raises(dy.exc.SchemaError) as report:
        Orders.validate(frame)
    assert dict(result.failed_assets)["orders"] == (
        "AssetOutputValidationError: Asset 'orders' returned a value that does "
        f"not match '{dy.DataFrame[Orders]}': {report.value}"
    )


@pytest.mark.parametrize(
    "hint",
    [
        pytest.param(dy.DataFrame[Prices], id="dataframe"),
        pytest.param(Annotated[pl.DataFrame, Prices], id="annotated"),
    ],
)
def test_config_cast_stores_the_cast_frame(env, hint):
    @rs.Asset(io_handler=env.handler)
    def prices() -> hint:
        return pl.DataFrame({"price": [1, 2]})

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("sale_prices", io_handler=env.handler, type_hint=hint)]
    )
    def load():
        return {"sale_prices": pl.DataFrame({"price": [1, 2]})}

    repo, result = env.materialize(prices, load)

    assert result.success
    expected = pl.DataFrame({"price": [1.0, 2.0]})
    assert_frame_equal(repo.load_node("prices"), expected)
    assert_frame_equal(repo.load_node("sale_prices"), expected)


def test_columns_outside_the_schema_are_dropped(env):
    frame = pl.DataFrame({**GOOD_ROWS, "note": ["a", "b"]})

    repo, result = env.materialize(env.asset(dy.DataFrame[Orders], frame))

    assert result.success
    assert_frame_equal(repo.load_node("orders"), pl.DataFrame(GOOD_ROWS))


def test_lazy_frame_is_checked_for_columns_now_and_for_rows_when_collected(env):
    good = pl.DataFrame(GOOD_ROWS)

    @rs.Asset(io_handler=env.handler)
    def lazy_orders() -> dy.LazyFrame[Orders]:
        return good.lazy()

    @rs.Asset(io_handler=env.handler)
    def lazy_bad_rows() -> dy.LazyFrame[Orders]:
        return pl.DataFrame(BAD_ROWS).lazy()

    @rs.Asset(io_handler=env.handler)
    def lazy_missing_column() -> dy.LazyFrame[Orders]:
        return good.drop("status").lazy()

    repo, result = env.materialize(lazy_orders, lazy_bad_rows, lazy_missing_column)

    assert not result.success
    assert_frame_equal(repo.load_node("lazy_orders").collect(), good)
    assert env.stored_json("lazy_orders", "dataframely/schema") == ORDERS_SCHEMA
    with pytest.raises(pl.exceptions.ComputeError, match="rules failed validation"):
        repo.load_node("lazy_bad_rows").collect()
    assert dict(result.failed_assets)["lazy_missing_column"].startswith(
        "AssetOutputValidationError: Asset 'lazy_missing_column' returned a value "
        f"that does not match '{dy.LazyFrame[Orders]}': "
        "1 missing columns for schema 'Orders': 'status'"
    )


def test_value_that_is_not_a_frame_fails_like_any_hint(env):
    _, result = env.materialize(env.asset(dy.DataFrame[Orders], GOOD_ROWS))

    assert not result.success
    assert dict(result.failed_assets)["orders"] == (
        "AssetOutputValidationError: Asset 'orders' returned value of type 'dict' "
        f"but expected '{dy.DataFrame[Orders]}'"
    )


def test_optional_hint_accepts_none_and_reports_a_bad_frame(env):
    hint = dy.DataFrame[Orders] | None
    bad = pl.DataFrame(BAD_ROWS)

    @rs.Asset(io_handler=env.handler)
    def no_orders() -> hint:
        return None

    repo, result = env.materialize(no_orders, env.asset(hint, bad))

    assert not result.success
    assert repo.load_node("no_orders") is None
    materialization = env.storage.get_latest_materialization("no_orders")
    assert "dataframely/schema" not in dict(materialization.metadata)
    with pytest.raises(dy.exc.ValidationError) as report:
        Orders.validate(bad)
    assert dict(result.failed_assets)["orders"] == (
        "AssetOutputValidationError: Asset 'orders' returned a value that does "
        f"not match '{hint}': {report.value}"
    )


def test_metadata_the_asset_sets_wins_over_the_schema_entry(env):
    @rs.Asset(io_handler=env.handler)
    def orders(context: rs.AssetExecutionContext) -> dy.DataFrame[Orders]:
        context.add_output_metadata({"dataframely/schema": "see the data contract"})
        return rs.Output(pl.DataFrame(GOOD_ROWS), metadata={"rows": 2})

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef(
                "refunds", io_handler=env.handler, type_hint=dy.DataFrame[Orders]
            )
        ],
    )
    def load(context: rs.AssetExecutionContext):
        context.add_output_metadata({"dataframely/schema": "see the data contract"})
        return {"refunds": rs.Output(pl.DataFrame(GOOD_ROWS), metadata={"rows": 2})}

    _, result = env.materialize(orders, load)

    assert result.success
    for name in ("orders", "refunds"):
        materialization = env.storage.get_latest_materialization(name)
        metadata = {k: json.loads(v) for k, v in materialization.metadata}
        assert metadata["dataframely/schema"] == {
            "Text": {"value": "see the data contract"}
        }
        assert metadata["rows"] == {"Int": {"value": 2}}


@pytest.mark.parametrize("hint", HINTS)
def test_multi_asset_output_is_validated_against_its_type_hint(env, hint):
    good, bad = pl.DataFrame(GOOD_ROWS), pl.DataFrame(BAD_ROWS)

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("orders", io_handler=env.handler, type_hint=hint),
            rs.AssetDef("refunds", io_handler=env.handler, type_hint=hint),
        ],
    )
    def load():
        yield rs.Output(good, output_name="orders")
        yield rs.Output(bad, output_name="refunds")

    repo, result = env.materialize(load)

    assert_frame_equal(repo.load_node("orders"), good)
    assert env.stored_json("orders", "dataframely/schema") == ORDERS_SCHEMA
    with pytest.raises(dy.exc.ValidationError) as report:
        Orders.validate(bad)
    assert dict(result.failed_assets) == {
        "refunds": "AssetOutputValidationError: Asset 'refunds' returned a value "
        f"that does not match '{hint}': {report.value}"
    }
    assert env.storage.get_latest_materialization("refunds") is None


def test_schema_metadata_shows_each_rule_with_its_bound():
    class Mixed(dy.Schema):
        size = dy.Int64(is_in=[1, 2, 3])
        code = dy.String(min_length=1, max_length=5, unique=True)
        at = dy.Datetime(resolution="1h")
        score = dy.Int64(check={"positive": lambda x: x > 0})

    out = validate_output(Mixed.create_empty(), Mixed)
    columns = json.loads(out.metadata["dataframely/schema"].value)["columns"]

    assert {column["name"]: column["checks"] for column in columns} == {
        "size": ["is_in=[1, 2, 3]"],
        "code": ["min_length=1", "max_length=5"],
        "at": ["resolution='1h'"],
        "score": ["check__positive"],
    }


def test_output_validator_finds_dataframely_hints_only():
    dataframely_hints = [dy.DataFrame[Orders], dy.LazyFrame[Orders], Orders]
    other_hints = [int, pl.DataFrame, pl.LazyFrame, list[int]]

    assert [output_validator(h) for h in dataframely_hints] == [validate_output] * 3
    assert [output_validator(h) for h in other_hints] == [None] * 4


def test_validate_output_declines_hints_without_a_schema():
    frame = pl.DataFrame(GOOD_ROWS)

    assert validate_output(frame, dy.DataFrame) is NotImplemented
    assert validate_output(GOOD_ROWS, dy.DataFrame[Orders]) is NotImplemented
    assert validate_output(frame.lazy(), dy.DataFrame[Orders]) is NotImplemented
    assert validate_output(frame, dy.LazyFrame[Orders]) is NotImplemented


def test_validate_output_returns_the_validated_frame_and_metadata():
    out = validate_output(pl.DataFrame({"price": [1, 2]}), Prices)

    assert isinstance(out, rs.Output)
    assert_frame_equal(out.value, pl.DataFrame({"price": [1.0, 2.0]}))
    assert list(out.metadata) == ["dataframely/schema"]


def test_missing_dataframely_names_the_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "dataframely", None)
    monkeypatch.delitem(sys.modules, "rivers.integrations.dataframely")
    with pytest.raises(ImportError, match=r"pip install rivers\[dataframely\]"):
        importlib.import_module("rivers.integrations.dataframely")
