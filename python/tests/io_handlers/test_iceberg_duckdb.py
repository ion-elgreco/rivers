"""IcebergIOHandler with DuckDB relations: lazy iceberg_scan reads, streamed writes, secrets."""

import json
import re
from datetime import date, datetime

import pytest

pytest.importorskip("pyiceberg")
duckdb = pytest.importorskip("duckdb")

import pyarrow as pa  # noqa: E402
from duckdb import DuckDBPyRelation  # noqa: E402

from rivers.integrations.duckdb import DuckDBResource  # noqa: E402
from rivers.io_handlers.iceberg.duckdb import iceberg_secret  # noqa: E402

from .helpers import (  # noqa: E402
    iceberg_handler,
    make_daily_partition,
    make_partition,
    read_input,
    write_output,
)


def _read(handler, name="t", **context) -> DuckDBPyRelation:
    return read_input(handler, DuckDBPyRelation, name, **context)


def _files_read(rel: DuckDBPyRelation) -> int:
    [(_, plan)] = duckdb.connect().sql(f"EXPLAIN ANALYZE {rel.sql_query()}").fetchall()
    return int(re.search(r"Total Files Read: (\d+)", plan).group(1))


def test_round_trip_relation(tmp_path):
    handler = iceberg_handler(tmp_path)
    write_output(
        handler, duckdb.sql("SELECT * FROM (VALUES (1, 'x'), (2, 'y')) v(a, b)")
    )

    result = _read(handler)
    assert isinstance(result, DuckDBPyRelation)
    assert result.columns == ["a", "b"]
    assert result.order("a").fetchall() == [(1, "x"), (2, "y")]


def test_streaming_write_of_many_batches(tmp_path):
    handler = iceberg_handler(tmp_path)
    write_output(
        handler, duckdb.sql("SELECT range AS i, range % 7 AS k FROM range(250000)")
    )

    assert (
        handler.load_table("t").current_snapshot().summary["total-records"] == "250000"
    )
    totals = _read(handler).aggregate("count(*), sum(i), count(DISTINCT i)").fetchone()
    assert totals == (250000, 250000 * 249999 // 2, 250000)


def test_relation_into_a_partitioned_table(tmp_path):
    handler = iceberg_handler(tmp_path)
    meta = {"iceberg/partition_expr": "region"}
    for region in ("us", "eu"):
        write_output(
            handler,
            duckdb.sql(f"SELECT '{region}' AS region, range AS v FROM range(3)"),
            partition=make_partition(region),
            asset_metadata=meta,
        )

    rel = _read(handler, partition=make_partition("eu"), asset_metadata=meta)
    assert rel.order("v").fetchall() == [("eu", 0), ("eu", 1), ("eu", 2)]


def test_static_partition_read_prunes_files(tmp_path):
    handler = iceberg_handler(tmp_path)
    meta = {"iceberg/partition_expr": "region"}
    for region, v in [("us", 1), ("eu", 2), ("apac", 3)]:
        write_output(
            handler,
            pa.table({"v": [v], "region": [region]}),
            partition=make_partition(region),
            asset_metadata=meta,
        )

    rel = _read(handler, partition=make_partition("eu"), asset_metadata=meta)
    assert rel.fetchall() == [(2, "eu")]
    assert _files_read(rel) == 1


def test_daily_partition_read_on_a_timestamp_column(tmp_path):
    handler = iceberg_handler(tmp_path)
    meta = {"iceberg/partition_expr": "ts"}
    for day, v in [("2024-01-01", 10), ("2024-01-02", 20)]:
        write_output(
            handler,
            pa.table({"ts": [datetime.fromisoformat(day).replace(hour=12)], "v": [v]}),
            partition=make_daily_partition(day),
            asset_metadata=meta,
        )

    rel = _read(
        handler, partition=make_daily_partition("2024-01-02"), asset_metadata=meta
    )
    assert rel.fetchall() == [(datetime(2024, 1, 2, 12), 20)]


def test_columns_and_snapshot(tmp_path):
    handler = iceberg_handler(tmp_path)
    first = write_output(handler, pa.table({"a": [1], "b": ["old"], "c": [True]}))
    write_output(handler, pa.table({"a": [2], "b": ["new"], "c": [False]}))

    current = _read(handler, asset_metadata={"iceberg/columns": json.dumps(["b", "a"])})
    assert current.columns == ["b", "a"]
    assert current.fetchall() == [("new", 2)]

    snapshot_id = first.output_metadata["iceberg/snapshot_id"].raw_value()
    old = _read(handler, asset_metadata={"iceberg/snapshot_id": str(snapshot_id)})
    assert old.fetchall() == [(1, "old", True)]


def test_relations_from_two_tables_join(tmp_path):
    handler = iceberg_handler(tmp_path)
    write_output(handler, pa.table({"id": [1, 2], "name": ["a", "b"]}), name="users")
    write_output(handler, pa.table({"user": [2, 2, 1], "n": [1, 2, 3]}), name="orders")

    users = _read(handler, "users").set_alias("u")
    orders = _read(handler, "orders").set_alias("o")
    joined = users.join(orders, "u.id = o.user").aggregate("name, sum(n)")
    assert joined.order("name").fetchall() == [("a", 3), ("b", 3)]


def test_handler_config_resource_is_used(tmp_path):
    handler = iceberg_handler(
        tmp_path,
        handler_config={"duckdb": DuckDBResource(connection_config={"threads": 3})},
    )
    write_output(handler, pa.table({"a": [1]}))

    rel = _read(handler)
    assert rel.query("t", "SELECT current_setting('threads') FROM t").fetchall() == [
        (3,)
    ]


def test_relation_upsert(tmp_path):
    handler = iceberg_handler(tmp_path, mode="upsert")
    meta = {"iceberg/join_cols": '["day"]'}
    write_output(
        handler,
        duckdb.sql("SELECT DATE '2024-01-01' AS day, 1 AS n"),
        asset_metadata=meta,
    )
    write_output(
        handler,
        duckdb.sql(
            "SELECT * FROM (VALUES (DATE '2024-01-01', 2), (DATE '2024-01-02', 3)) v(day, n)"
        ),
        asset_metadata=meta,
    )
    assert _read(handler).order("day").fetchall() == [
        (date(2024, 1, 1), 2),
        (date(2024, 1, 2), 3),
    ]


# --- secrets -------------------------------------------------------------------


def _params(sql: str) -> str:
    """The parameter list of a CREATE SECRET statement, keys unquoted."""
    [params] = re.fullmatch(
        r'CREATE OR REPLACE SECRET "rivers_iceberg_\w+" \((.*)\)', sql
    ).groups()
    return re.sub(r'"(\w+)" ', r"\1 ", params)


def test_secret_for_local_tables_is_none():
    assert iceberg_secret("file:///data/wh/ns/t/metadata/1.metadata.json", {}) is None
    assert (
        iceberg_secret("/data/wh/ns/t/metadata/1.metadata.json", {"s3.region": "x"})
        is None
    )


def test_secret_s3_keys():
    sql = iceberg_secret(
        "s3://bucket/wh/ns/t/metadata/1.metadata.json",
        {
            "s3.access-key-id": "AKIA",
            "s3.secret-access-key": "it's",
            "s3.session-token": "tok",
            "s3.region": "eu-west-1",
            "s3.endpoint": "http://minio:9000/",
            "uri": "http://catalog:8181",
        },
    )
    assert _params(sql) == (
        "TYPE 's3', PROVIDER 'config', KEY_ID 'AKIA', SECRET 'it''s', "
        "SESSION_TOKEN 'tok', REGION 'eu-west-1', ENDPOINT 'minio:9000', "
        "USE_SSL false, URL_STYLE 'path', SCOPE 's3://bucket'"
    )


def test_secret_s3_client_keys_are_a_fallback():
    sql = iceberg_secret(
        "s3a://bucket/t",
        {
            "client.access-key-id": "client-key",
            "client.secret-access-key": "client-secret",
            "client.region": "us-east-1",
            "s3.region": "eu-west-1",
            "s3.force-virtual-addressing": "true",
        },
    )
    assert _params(sql) == (
        "TYPE 's3', PROVIDER 'config', KEY_ID 'client-key', SECRET 'client-secret', "
        "REGION 'eu-west-1', URL_STYLE 'vhost', SCOPE 's3a://bucket'"
    )


def test_secret_s3_credential_chain():
    assert _params(iceberg_secret("s3://bucket/t", {})) == (
        "TYPE 's3', PROVIDER 'credential_chain', URL_STYLE 'path', SCOPE 's3://bucket'"
    )


def test_secret_azure_keys():
    location = "abfss://container@acct.dfs.core.windows.net/wh/t"
    assert _params(iceberg_secret(location, {"adls.account-key": "a2V5"})) == (
        "TYPE 'azure', CONNECTION_STRING 'AccountName=acct;AccountKey=a2V5', "
        "SCOPE 'abfss://container@acct.dfs.core.windows.net'"
    )
    service_principal = iceberg_secret(
        "abfs://container/t",
        {
            "adls.account-name": "acct",
            "adls.tenant-id": "tid",
            "adls.client-id": "cid",
            "adls.client-secret": "cs",
        },
    )
    assert _params(service_principal) == (
        "TYPE 'azure', PROVIDER 'service_principal', TENANT_ID 'tid', "
        "CLIENT_ID 'cid', CLIENT_SECRET 'cs', ACCOUNT_NAME 'acct', "
        "SCOPE 'abfs://container'"
    )


def test_secret_azure_key_needs_account():
    with pytest.raises(ValueError, match="needs adls.account-name"):
        iceberg_secret("abfs://container/t", {"adls.sas-token": "sv=1"})


def test_secret_gcs_is_rejected():
    with pytest.raises(ValueError, match="HMAC keys only"):
        iceberg_secret("gs://bucket/t", {"gcs.oauth2.token": "x"})
