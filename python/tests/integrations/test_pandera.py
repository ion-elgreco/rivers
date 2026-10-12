"""pandera integration: assets typed with a pandera schema validate their output.

A valid frame is stored as pandera returns it, with the schema as
``pandera/schema`` metadata; an invalid frame fails the step with pandera's
report. Every case runs on both executors: the parallel executor validates a
sync step in its loky worker.
"""

import importlib
import json
import os
import sys
from dataclasses import dataclass
from typing import Annotated

import pytest

pytest.importorskip("pandera")
pd = pytest.importorskip("pandas")
pl = pytest.importorskip("polars")

import pandera.pandas as pa  # noqa: E402
from _helpers import materialize_beside_sibling  # noqa: E402
import pandera.polars as pap  # noqa: E402
from pandas.testing import assert_frame_equal as assert_pandas_equal  # noqa: E402
from pandera.errors import SchemaErrors  # noqa: E402
from pandera.typing import DataFrame, Series  # noqa: E402
from pandera.typing.polars import DataFrame as PolarsDataFrame  # noqa: E402
from pandera.typing.polars import LazyFrame  # noqa: E402
from polars.testing import assert_frame_equal as assert_polars_equal  # noqa: E402

import rivers as rs  # noqa: E402
from rivers.integrations import output_validator  # noqa: E402
from rivers.integrations.pandera import schema_metadata, validate_output  # noqa: E402


class Orders(pa.DataFrameModel):
    """Orders placed in the shop."""

    id: Series[int] = pa.Field(ge=0, unique=True, description="Order id")
    amount: Series[float] = pa.Field(gt=0, nullable=True)
    status: Series[str] = pa.Field(isin=["open", "closed"])

    @pa.dataframe_check
    def has_rows(cls, df: pd.DataFrame) -> bool:
        """At least one order."""
        return len(df) > 0


class PolarsOrders(pap.DataFrameModel):
    """Orders placed in the shop."""

    id: int = pap.Field(ge=0, unique=True, description="Order id")
    amount: float = pap.Field(gt=0, nullable=True)
    status: str = pap.Field(isin=["open", "closed"])

    @pap.dataframe_check
    def has_rows(cls, data: pap.PolarsData) -> pl.LazyFrame:
        """At least one order."""
        return data.lazyframe.select(pl.len() > 0)

    class Config:
        name = "Orders"


orders_schema = pa.DataFrameSchema(
    {
        "id": pa.Column(int, pa.Check.ge(0), unique=True, description="Order id"),
        "amount": pa.Column(float, pa.Check.gt(0), nullable=True),
        "status": pa.Column(str, pa.Check.isin(["open", "closed"])),
    },
    checks=pa.Check(lambda df: len(df) > 0, description="At least one order."),
    name="Orders",
    description="Orders placed in the shop.",
)

polars_orders_schema = pap.DataFrameSchema(
    {
        "id": pap.Column(int, pap.Check.ge(0), unique=True, description="Order id"),
        "amount": pap.Column(float, pap.Check.gt(0), nullable=True),
        "status": pap.Column(str, pap.Check.isin(["open", "closed"])),
    },
    checks=pap.Check(
        lambda data: data.lazyframe.select(pl.len() > 0),
        description="At least one order.",
    ),
    name="Orders",
    description="Orders placed in the shop.",
)


class Prices(pa.DataFrameModel):
    price: Series[float]

    class Config:
        coerce = True


class PolarsPrices(pap.DataFrameModel):
    price: float

    class Config:
        coerce = True


GOOD_ROWS = {"id": [1, 2], "amount": [9.5, None], "status": ["open", "closed"]}
BAD_ROWS = {"id": [1, -1], "amount": [9.5, -3.0], "status": ["open", "lost"]}

FRAMES = {"pandas": pd.DataFrame, "polars": pl.DataFrame}

# (backend, return hint, the schema it holds)
CASES = [
    pytest.param("pandas", DataFrame[Orders], Orders.to_schema(), id="pandas-model"),
    pytest.param(
        "pandas",
        Annotated[pd.DataFrame, Orders],
        Orders.to_schema(),
        id="pandas-annotated-model",
    ),
    pytest.param(
        "pandas",
        Annotated[pd.DataFrame, orders_schema],
        orders_schema,
        id="pandas-schema",
    ),
    pytest.param(
        "polars",
        PolarsDataFrame[PolarsOrders],
        PolarsOrders.to_schema(),
        id="polars-model",
    ),
    pytest.param(
        "polars",
        Annotated[pl.DataFrame, polars_orders_schema],
        polars_orders_schema,
        id="polars-schema",
    ),
]


def expected_schema(backend: str) -> dict:
    """The ``pandera/schema`` entry for the orders schemas above."""
    # pandas' default string dtype depends on the pandas build.
    id_type, amount_type, status_type = {
        "pandas": ("int64", "float64", str(pa.Column(str).dtype)),
        "polars": ("Int64", "Float64", "String"),
    }[backend]
    return {
        "name": "Orders",
        "description": "Orders placed in the shop.",
        "columns": [
            {
                "name": "id",
                "type": id_type,
                "nullable": False,
                "unique": True,
                "description": "Order id",
                "checks": ["greater_than_or_equal_to(0)"],
            },
            {
                "name": "amount",
                "type": amount_type,
                "nullable": True,
                "unique": False,
                "description": None,
                "checks": ["greater_than(0)"],
            },
            {
                "name": "status",
                "type": status_type,
                "nullable": False,
                "unique": False,
                "description": None,
                "checks": ["isin(['open', 'closed'])"],
            },
        ],
        "checks": ["At least one order."],
    }


@dataclass
class Env:
    storage: rs.Storage
    executor: rs.Executor
    handler: rs.BaseIOHandler

    @property
    def parallel(self) -> bool:
        return isinstance(self.executor, rs.Executor.Parallel)

    def materialize(self, *assets) -> tuple[rs.CodeRepository, rs.RunResult]:
        return materialize_beside_sibling(
            self.storage, self.executor, self.handler, *assets
        )


@pytest.fixture
def env(storage, executor_env):
    executor, io_factory = executor_env
    return Env(storage, executor, io_factory())


def orders_asset(env: Env, hint, value, *, is_async=False, pid_file=None):
    """An ``orders`` asset that returns ``value`` under the return hint ``hint``."""
    if is_async:

        @rs.Asset(name="orders", io_handler=env.handler)
        async def orders_async() -> hint:
            return value

        return orders_async

    @rs.Asset(name="orders", io_handler=env.handler)
    def orders() -> hint:
        if pid_file is not None:
            pid_file.write_text(str(os.getpid()))
        return value

    return orders


def stored_schema(env: Env, asset: str) -> dict:
    """The ``pandera/schema`` entry of ``asset``'s materialization, decoded."""
    materialization = env.storage.get_latest_materialization(asset)
    [(variant, payload)] = json.loads(
        dict(materialization.metadata)["pandera/schema"]
    ).items()
    # Metadata that a parallel worker sends back loses its type, so only the
    # value is compared there.
    if not env.parallel:
        assert variant == "Json"
    return json.loads(payload["value"])


def assert_frames_equal(left, right):
    if isinstance(right, pd.DataFrame):
        assert_pandas_equal(left, right)
    else:
        assert_polars_equal(left, right)


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(("backend", "hint", "schema"), CASES)
def test_valid_frame_is_stored_with_its_schema(
    env, tmp_path, backend, hint, schema, is_async
):
    pid_file = tmp_path / "pid"
    frame = FRAMES[backend](GOOD_ROWS)

    repo, result = env.materialize(
        orders_asset(env, hint, frame, is_async=is_async, pid_file=pid_file)
    )

    assert result.success
    if not is_async:
        ran_in_worker = int(pid_file.read_text()) != os.getpid()
        assert ran_in_worker == env.parallel
    assert_frames_equal(repo.load_node("orders"), schema.validate(frame))
    assert stored_schema(env, "orders") == expected_schema(backend)


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(("backend", "hint", "schema"), CASES)
def test_invalid_frame_fails_with_the_pandera_report(
    env, backend, hint, schema, is_async
):
    frame = FRAMES[backend](BAD_ROWS)

    _, result = env.materialize(orders_asset(env, hint, frame, is_async=is_async))

    assert not result.success
    with pytest.raises(SchemaErrors) as report:
        schema.validate(frame, lazy=True)
    error = dict(result.failed_assets)["orders"]
    assert error == (
        "AssetOutputValidationError: Asset 'orders' returned a value that does "
        f"not match '{hint}': {report.value}"
    )
    # Lazy validation: the report lists every failed check, not the first.
    for check in ("greater_than_or_equal_to(0)", "greater_than(0)", "isin("):
        assert check in error
    assert env.storage.get_latest_materialization("orders") is None


@pytest.mark.parametrize("backend", ["pandas", "polars"])
def test_coerced_frame_is_the_stored_frame(env, backend):
    hint = DataFrame[Prices] if backend == "pandas" else PolarsDataFrame[PolarsPrices]

    @rs.Asset(io_handler=env.handler)
    def prices() -> hint:
        return FRAMES[backend]({"price": [1, 2]})

    repo, result = env.materialize(prices)

    assert result.success
    expected = FRAMES[backend]({"price": [1.0, 2.0]})
    assert_frames_equal(repo.load_node("prices"), expected)


def test_lazy_frame_is_checked_against_the_schema_only(env):
    good = pl.DataFrame(GOOD_ROWS)

    @rs.Asset(io_handler=env.handler)
    def lazy_orders() -> LazyFrame[PolarsOrders]:
        return good.lazy()

    # pandera checks a LazyFrame's columns and types, not its rows, unless
    # PANDERA_VALIDATION_DEPTH says otherwise.
    @rs.Asset(io_handler=env.handler)
    def lazy_bad_rows() -> LazyFrame[PolarsOrders]:
        return pl.DataFrame(BAD_ROWS).lazy()

    @rs.Asset(io_handler=env.handler)
    def lazy_missing_column() -> LazyFrame[PolarsOrders]:
        return good.drop("status").lazy()

    repo, result = env.materialize(lazy_orders, lazy_bad_rows, lazy_missing_column)

    assert not result.success
    assert_polars_equal(repo.load_node("lazy_orders").collect(), good)
    assert_polars_equal(
        repo.load_node("lazy_bad_rows").collect(), pl.DataFrame(BAD_ROWS)
    )
    assert stored_schema(env, "lazy_orders") == expected_schema("polars")
    with pytest.raises(SchemaErrors) as report:
        PolarsOrders.validate(good.drop("status").lazy(), lazy=True)
    assert dict(result.failed_assets)["lazy_missing_column"] == (
        "AssetOutputValidationError: Asset 'lazy_missing_column' returned a value "
        f"that does not match '{LazyFrame[PolarsOrders]}': {report.value}"
    )


def test_value_that_is_not_a_frame_fails_like_any_hint(env):
    _, result = env.materialize(orders_asset(env, DataFrame[Orders], GOOD_ROWS))

    assert not result.success
    assert dict(result.failed_assets)["orders"] == (
        "AssetOutputValidationError: Asset 'orders' returned value of type 'dict' "
        f"but expected '{DataFrame[Orders]}'"
    )


def test_optional_hint_accepts_none_and_reports_a_bad_frame(env):
    hint = DataFrame[Orders] | None
    bad = pd.DataFrame(BAD_ROWS)

    @rs.Asset(io_handler=env.handler)
    def no_orders() -> hint:
        return None

    repo, result = env.materialize(no_orders, orders_asset(env, hint, bad))

    assert not result.success
    assert repo.load_node("no_orders") is None
    materialization = env.storage.get_latest_materialization("no_orders")
    assert "pandera/schema" not in dict(materialization.metadata)
    with pytest.raises(SchemaErrors) as report:
        Orders.validate(bad, lazy=True)
    assert dict(result.failed_assets)["orders"] == (
        "AssetOutputValidationError: Asset 'orders' returned a value that does "
        f"not match '{hint}': {report.value}"
    )


def test_metadata_the_asset_sets_wins_over_the_schema_entry(env):
    @rs.Asset(io_handler=env.handler)
    def orders(context: rs.AssetExecutionContext) -> DataFrame[Orders]:
        context.add_output_metadata({"pandera/schema": "see the data contract"})
        return rs.Output(pd.DataFrame(GOOD_ROWS), metadata={"rows": 2})

    _, result = env.materialize(orders)

    assert result.success
    materialization = env.storage.get_latest_materialization("orders")
    metadata = {k: json.loads(v) for k, v in materialization.metadata}
    assert metadata["pandera/schema"] == {"Text": {"value": "see the data contract"}}
    assert metadata["rows"] == {"Int": {"value": 2}}


def test_multi_asset_output_carries_schema_metadata(env):
    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("orders", io_handler=env.handler),
            rs.AssetDef("order_count", io_handler=env.handler),
        ],
    )
    def load():
        orders = Orders.validate(pd.DataFrame(GOOD_ROWS), lazy=True)
        yield rs.Output(orders, output_name="orders", metadata=schema_metadata(Orders))
        yield rs.Output(len(orders), output_name="order_count")

    repo, result = env.materialize(load)

    assert result.success
    assert repo.load_node("order_count") == 2
    assert stored_schema(env, "orders") == expected_schema("pandas")


@pytest.mark.parametrize(("backend", "hint", "schema"), CASES)
def test_schema_metadata_describes_model_and_schema(backend, hint, schema):
    [(key, value)] = schema_metadata(schema).items()

    assert key == "pandera/schema"
    assert isinstance(value, rs.MetadataValue.Json)
    assert json.loads(value.value) == expected_schema(backend)


def test_schema_metadata_takes_a_model_class():
    assert schema_metadata(Orders) == schema_metadata(Orders.to_schema())


def test_output_validator_finds_pandera_hints_only():
    pandera_hints = [
        DataFrame[Orders],
        PolarsDataFrame[PolarsOrders],
        LazyFrame[PolarsOrders],
        Orders,
        orders_schema,
        polars_orders_schema,
    ]
    other_hints = [int, pd.DataFrame, pl.DataFrame, list[int], dict[str, pd.DataFrame]]

    assert [output_validator(h) for h in pandera_hints] == [validate_output] * 6
    assert [output_validator(h) for h in other_hints] == [None] * 5


def test_validate_output_declines_hints_without_a_schema():
    frame = pd.DataFrame(GOOD_ROWS)

    assert validate_output(frame, Series[int]) is NotImplemented
    assert validate_output(frame, DataFrame) is NotImplemented
    assert validate_output(GOOD_ROWS, DataFrame[Orders]) is NotImplemented
    assert validate_output(pl.DataFrame(GOOD_ROWS), DataFrame[Orders]) is NotImplemented


def test_validate_output_returns_the_validated_frame_and_metadata():
    out = validate_output(pd.DataFrame({"price": [1, 2]}), Prices)

    assert isinstance(out, rs.Output)
    assert_pandas_equal(out.value, pd.DataFrame({"price": [1.0, 2.0]}))
    assert out.metadata == schema_metadata(Prices)


def test_missing_pandera_names_the_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "pandera.api.dataframe.container", None)
    monkeypatch.delitem(sys.modules, "rivers.integrations.pandera")
    with pytest.raises(ImportError, match=r"pip install rivers\[pandera\]"):
        importlib.import_module("rivers.integrations.pandera")
