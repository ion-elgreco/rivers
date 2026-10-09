"""DuckDB type handler for the Delta Lake IO handler: delta_scan reads, delta-rs writes."""

import json
import re

import pytest

pytest.importorskip("deltalake")
duckdb = pytest.importorskip("duckdb")

import deltalake  # noqa: E402
import polars as pl  # noqa: E402
import pyarrow as pa  # noqa: E402
from deltalake import DeltaTable  # noqa: E402
from duckdb import DuckDBPyRelation  # noqa: E402
from packaging.version import Version  # noqa: E402

import rivers as rs  # noqa: E402
from rivers.integrations.duckdb import DuckDBResource  # noqa: E402
from rivers.io_handlers.delta.duckdb import delta_secret  # noqa: E402

from .helpers import (  # noqa: E402
    make_daily_partition,
    make_multi_partition,
    make_partition,
)


def _handler(tmp_path, **kwargs):
    return rs.DeltaIOHandler(table_uri=str(tmp_path), **kwargs)


def _read(handler, name="tbl", **kwargs) -> DuckDBPyRelation:
    return handler.load_input(
        rs.InputContext(
            asset_name=name, downstream_asset="x", type_hint=DuckDBPyRelation, **kwargs
        )
    )


def _files_scanned(rel: DuckDBPyRelation) -> tuple[int, int]:
    [(_, plan)] = duckdb.connect().sql(f"EXPLAIN ANALYZE {rel.sql_query()}").fetchall()
    scanned, total = re.search(r"Scanning Files: (\d+)/(\d+)", plan).groups()
    return int(scanned), int(total)


def test_round_trip_relation(tmp_path):
    handler = _handler(tmp_path)
    rel = duckdb.sql("SELECT * FROM (VALUES (1, 'x'), (2, 'y')) v(a, b)")
    handler.handle_output(rs.OutputContext(asset_name="tbl"), rel)

    result = _read(handler)
    assert isinstance(result, DuckDBPyRelation)
    assert result.order("a").fetchall() == [(1, "x"), (2, "y")]
    assert result.columns == ["a", "b"]


def test_cross_read_with_other_handlers(tmp_path):
    handler = _handler(tmp_path)
    handler.handle_output(
        rs.OutputContext(asset_name="arrow"), pa.table({"a": [1, 2], "b": ["x", "y"]})
    )
    handler.handle_output(
        rs.OutputContext(asset_name="polars"), pl.DataFrame({"a": [3], "b": ["z"]})
    )
    handler.handle_output(
        rs.OutputContext(asset_name="rel"),
        duckdb.sql("SELECT 4::BIGINT AS a, 'w' AS b"),
    )

    assert _read(handler, "arrow").order("a").fetchall() == [(1, "x"), (2, "y")]
    assert _read(handler, "polars").fetchall() == [(3, "z")]
    from_rel = handler.load_input(
        rs.InputContext(asset_name="rel", downstream_asset="x", type_hint=pa.Table)
    )
    assert from_rel.to_pylist() == [{"a": 4, "b": "w"}]


def test_static_partition_read_prunes_files(tmp_path):
    handler = _handler(tmp_path)
    meta = {"delta/partition_expr": "region"}
    for region, val in [("us", 1), ("eu", 2), ("apac", 3)]:
        handler.handle_output(
            rs.OutputContext(
                asset_name="tbl",
                partition=make_partition(region),
                asset_metadata=meta,
            ),
            pa.table({"val": [val], "region": [region]}),
        )

    rel = _read(handler, partition=make_partition("eu"), asset_metadata=meta)
    assert rel.fetchall() == [(2, "eu")]
    assert _files_scanned(rel) == (1, 3)


def test_daily_partition_read(tmp_path):
    handler = _handler(tmp_path)
    meta = {"delta/partition_expr": "day"}
    for day, val in [("2024-01-01", 10), ("2024-01-02", 20)]:
        handler.handle_output(
            rs.OutputContext(
                asset_name="tbl",
                partition=make_daily_partition(day),
                asset_metadata=meta,
            ),
            pa.table({"day": [day], "val": [val]}),
        )

    rel = _read(
        handler, partition=make_daily_partition("2024-01-02"), asset_metadata=meta
    )
    assert rel.fetchall() == [("2024-01-02", 20)]
    assert _files_scanned(rel) == (1, 2)


def test_multi_partition_read(tmp_path):
    handler = _handler(tmp_path)
    meta = {"delta/partition_expr": json.dumps({"region": "region", "tier": "tier"})}
    for region, tier, val in [("us", "free", 1), ("us", "pro", 2), ("eu", "free", 3)]:
        handler.handle_output(
            rs.OutputContext(
                asset_name="tbl",
                partition=make_multi_partition({"region": region, "tier": tier}),
                asset_metadata=meta,
            ),
            pa.table({"region": [region], "tier": [tier], "val": [val]}),
        )

    rel = _read(
        handler,
        partition=make_multi_partition({"region": "us", "tier": "pro"}),
        asset_metadata=meta,
    )
    assert rel.fetchall() == [("us", "pro", 2)]
    assert _files_scanned(rel) == (1, 3)


def test_columns_and_version(tmp_path):
    handler = _handler(tmp_path)
    ctx = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx, pa.table({"a": [1], "b": ["old"], "c": [True]}))
    handler.handle_output(ctx, pa.table({"a": [2], "b": ["new"], "c": [False]}))

    current = _read(handler, asset_metadata={"delta/columns": json.dumps(["b", "a"])})
    assert current.columns == ["b", "a"]
    assert current.fetchall() == [("new", 2)]
    old = _read(handler, asset_metadata={"delta/version": "0"})
    assert old.fetchall() == [(1, "old", True)]


def test_streaming_write_of_many_batches(tmp_path):
    handler = _handler(tmp_path)
    rel = duckdb.sql("SELECT range AS i, range % 7 AS k FROM range(250000)")
    handler.handle_output(rs.OutputContext(asset_name="tbl"), rel)

    dt = DeltaTable(str(tmp_path / "tbl"))
    assert (
        sum(dt.get_add_actions(flatten=True).column("num_records").to_pylist())
        == 250000
    )
    totals = _read(handler).aggregate("count(*), sum(i), count(DISTINCT i)").fetchone()
    assert totals == (250000, 250000 * 249999 // 2, 250000)


column_mapping_writes = pytest.mark.skipif(
    Version(deltalake.__version__) < Version("1.6.4"),
    reason="deltalake creates column-mapped tables from 1.6.4",
)


@column_mapping_writes
def test_column_mapping_unpartitioned(tmp_path):
    handler = _handler(tmp_path, table_config={"delta.columnMapping.mode": "name"})
    handler.handle_output(
        rs.OutputContext(asset_name="tbl"), pa.table({"v": [1, 2], "p": ["a", "b"]})
    )
    assert DeltaTable(str(tmp_path / "tbl")).protocol().min_reader_version == 2
    assert _read(handler).order("v").fetchall() == [(1, "a"), (2, "b")]


@column_mapping_writes
@pytest.mark.skip(
    reason="DuckDB returns NULL partition columns with column mapping: "
    "https://github.com/duckdb/duckdb-delta/issues/343"
)
def test_column_mapping_partitioned(tmp_path):
    # NULL only when the partition column is not the first column of the schema
    handler = _handler(tmp_path, table_config={"delta.columnMapping.mode": "name"})
    meta = {"delta/partition_expr": "p"}
    for p, v in [("a", 1), ("b", 2)]:
        handler.handle_output(
            rs.OutputContext(
                asset_name="tbl", partition=make_partition(p), asset_metadata=meta
            ),
            pa.table({"v": [v], "p": [p]}),
        )
    assert _read(handler).order("v").fetchall() == [(1, "a"), (2, "b")]


def test_relations_from_two_tables_join(tmp_path):
    handler = _handler(tmp_path)
    handler.handle_output(
        rs.OutputContext(asset_name="users"),
        pa.table({"id": [1, 2], "name": ["a", "b"]}),
    )
    handler.handle_output(
        rs.OutputContext(asset_name="orders"),
        pa.table({"user": [2, 2, 1], "n": [1, 2, 3]}),
    )
    users = _read(handler, "users").set_alias("u")
    orders = _read(handler, "orders").set_alias("o")
    joined = users.join(orders, "u.id = o.user").aggregate("name, sum(n)")
    assert joined.order("name").fetchall() == [("a", 3), ("b", 3)]


def test_handler_config_resource_is_used(tmp_path):
    handler = _handler(
        tmp_path,
        handler_config={"duckdb": DuckDBResource(connection_config={"threads": 3})},
    )
    handler.handle_output(rs.OutputContext(asset_name="tbl"), pa.table({"a": [1]}))

    rel = _read(handler)
    assert rel.query("t", "SELECT current_setting('threads') FROM t").fetchall() == [
        (3,)
    ]


def test_secret_for_local_tables_is_none():
    assert delta_secret("/data/delta/t", {"aws_region": "eu-west-1"}) is None
    assert delta_secret("file:///data/delta/t", None) is None


def _params(sql: str) -> str:
    """The parameter list of a CREATE SECRET statement, keys unquoted."""
    [params] = re.fullmatch(
        r'CREATE OR REPLACE SECRET "rivers_delta_\w+" \((.*)\)', sql
    ).groups()
    return re.sub(r'"(\w+)" ', r"\1 ", params)


def test_secret_s3_keys():
    sql = delta_secret(
        "s3://bucket/delta/t",
        {
            "AWS_ACCESS_KEY_ID": "AKIA",
            "aws_secret_access_key": "it's",
            "session_token": "tok",
            "aws_region": "eu-west-1",
            "aws_endpoint_url": "http://minio:9000/",
            "aws_s3_locking_provider": "dynamodb",
        },
    )
    assert _params(sql) == (
        "TYPE 's3', PROVIDER 'config', KEY_ID 'AKIA', SECRET 'it''s', "
        "SESSION_TOKEN 'tok', REGION 'eu-west-1', ENDPOINT 'minio:9000', "
        "USE_SSL false, URL_STYLE 'path', SCOPE 's3://bucket'"
    )


def test_secret_s3_endpoint_without_scheme():
    allow_http = delta_secret(
        "s3://bucket/t",
        {"aws_endpoint": "minio:9000", "aws_allow_http": "true"},
    )
    assert "ENDPOINT 'minio:9000', USE_SSL false" in _params(allow_http)
    https = delta_secret(
        "s3://bucket/t",
        {
            "aws_endpoint": "https://s3.example.com",
            "aws_virtual_hosted_style_request": "true",
        },
    )
    assert "ENDPOINT 's3.example.com', USE_SSL true, URL_STYLE 'vhost'" in _params(
        https
    )


def test_secret_s3_credential_chain():
    sql = delta_secret("s3://bucket/t", {"aws_region": "us-east-1"})
    assert _params(sql) == (
        "TYPE 's3', PROVIDER 'credential_chain', REGION 'us-east-1', "
        "URL_STYLE 'path', SCOPE 's3://bucket'"
    )
    assert _params(delta_secret("s3://bucket/t", None)) == (
        "TYPE 's3', PROVIDER 'credential_chain', URL_STYLE 'path', SCOPE 's3://bucket'"
    )


def test_secret_azure_account_key():
    sql = delta_secret(
        "az://container/t",
        {"azure_storage_account_name": "acct", "AZURE_STORAGE_ACCOUNT_KEY": "a2V5"},
    )
    assert _params(sql) == (
        "TYPE 'azure', CONNECTION_STRING 'AccountName=acct;AccountKey=a2V5', "
        "SCOPE 'az://container'"
    )


def test_secret_azure_sas_token_from_abfss_url():
    sql = delta_secret(
        "abfss://container@acct.dfs.core.windows.net/t",
        {"azure_storage_sas_token": "?sv=2024&sig=x"},
    )
    assert _params(sql) == (
        "TYPE 'azure', "
        "CONNECTION_STRING 'AccountName=acct;SharedAccessSignature=sv=2024&sig=x', "
        "SCOPE 'abfss://container@acct.dfs.core.windows.net'"
    )


def test_secret_azure_service_principal():
    sql = delta_secret(
        "abfs://container/t",
        {
            "account_name": "acct",
            "azure_client_id": "cid",
            "azure_client_secret": "cs",
            "azure_tenant_id": "tid",
        },
    )
    assert _params(sql) == (
        "TYPE 'azure', PROVIDER 'service_principal', TENANT_ID 'tid', "
        "CLIENT_ID 'cid', CLIENT_SECRET 'cs', ACCOUNT_NAME 'acct', "
        "SCOPE 'abfs://container'"
    )


def test_secret_azure_credential_chain():
    sql = delta_secret("az://container/t", {"azure_storage_account_name": "acct"})
    assert _params(sql) == (
        "TYPE 'azure', PROVIDER 'credential_chain', ACCOUNT_NAME 'acct', "
        "SCOPE 'az://container'"
    )


def test_secret_azure_key_needs_account():
    with pytest.raises(ValueError, match="needs azure_storage_account_name"):
        delta_secret("az://container/t", {"azure_storage_account_key": "a2V5"})


def test_secret_gcs_is_rejected():
    with pytest.raises(ValueError, match="HMAC keys only"):
        delta_secret("gs://bucket/t", {"google_service_account": "/key.json"})


def test_secret_names_differ_per_store():
    a = delta_secret(
        "s3://a/t", {"aws_access_key_id": "k", "aws_secret_access_key": "s"}
    )
    b = delta_secret(
        "s3://b/t", {"aws_access_key_id": "k", "aws_secret_access_key": "s"}
    )
    again = delta_secret(
        "s3://a/t2", {"aws_access_key_id": "k", "aws_secret_access_key": "s"}
    )
    assert a.split(" (")[0] != b.split(" (")[0]
    assert a == again


def test_executors_read_delta_as_relation(storage, executor_env, tmp_path):
    executor, io_factory = executor_env
    delta = rs.DeltaIOHandler(table_uri=str(tmp_path / "delta"))

    @rs.Asset(io_handler=delta)
    def orders() -> pa.Table:
        return pa.table({"customer": ["a", "b", "a"], "amount": [10, 5, 7]})

    @rs.Asset(io_handler=io_factory())
    def totals(orders: DuckDBPyRelation) -> list:
        return (
            orders.aggregate("customer, sum(amount) AS total")
            .order("customer")
            .fetchall()
        )

    @rs.Asset(io_handler=io_factory())
    def biggest(orders: DuckDBPyRelation) -> int:
        return orders.aggregate("max(amount)").fetchone()[0]

    repo = rs.CodeRepository(
        assets=[orders, totals, biggest], default_executor=executor
    )
    repo.resolve(storage=storage)
    repo.materialize()

    assert repo.load_node("totals") == [("a", 17), ("b", 5)]
    assert repo.load_node("biggest") == 10
