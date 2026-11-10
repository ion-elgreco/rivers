"""A run records the data version of a remote input from the owner's scope.

Repositories share one store under the identities ``warehouse`` and ``shop``
(owners) and ``analytics`` (the consumer).
"""

import json
import os
import re
import subprocess
import sys
import textwrap
from pathlib import Path

import obstore
import pytest

import rivers as rs

from _helpers import EXECUTORS

pytestmark = pytest.mark.usefixtures("clean_code_location_env")

NOT_REGISTERED = (
    "code location 'dev/warehouse' is not registered; loading 'raw_orders' "
    "through the handler without data-version tracking"
)


def _pickle_handler(root: Path) -> rs.PickleIOHandler:
    return rs.PickleIOHandler(store=obstore.store.LocalStore(str(root), mkdir=True))


def _resolve_as(assets, storage, monkeypatch, identity, executor=None):
    repo = rs.CodeRepository(assets=assets, default_executor=executor)
    monkeypatch.setenv("RIVERS_CODE_LOCATION_ID", identity)
    repo.resolve(storage=storage)
    return repo


def _owner(handler, storage, monkeypatch, identity="warehouse", rows=(1, 2, 3)):
    """Materialize ``raw_orders`` as ``identity``; return its data version."""

    @rs.Asset(io_handler=handler)
    def raw_orders() -> list[int]:
        return list(rows)

    owner = _resolve_as([raw_orders], storage, monkeypatch, identity)
    assert owner.materialize().success
    record = storage._scoped(identity).get_asset_record("raw_orders")
    assert record is not None and record.last_data_version is not None
    return record.last_data_version


def _sibling(handler) -> rs.SingleAsset:
    """A second step: the parallel executor runs a lone step in-process."""

    @rs.Asset(io_handler=handler)
    def sibling() -> int:
        return 1

    return sibling


def _input_versions(storage, asset: str) -> list[tuple[str, str]]:
    record = storage._scoped("analytics").get_asset_record(asset)
    assert record is not None
    return sorted(record.last_input_data_versions)


def _logs(storage, run_id: str) -> dict[str, str]:
    """Each step's Rust log output, across its log rows."""
    logs: dict[str, str] = {}
    for log in storage._scoped("analytics").get_run_logs(run_id):
        logs[log.step_key] = logs.get(log.step_key, "") + (log.logs or "")
    return logs


@pytest.mark.parametrize("executor", EXECUTORS)
def test_remote_input_records_the_owners_data_version(
    executor, embedded_storage, tmp_path, monkeypatch
):
    handler = _pickle_handler(tmp_path / "io")
    owner_dv = _owner(handler, embedded_storage, monkeypatch)
    orders = rs.Asset.remote(
        "raw_orders", code_location="warehouse", io_handler=handler
    )

    @rs.Asset(io_handler=handler)
    def order_count(raw_orders: list[int]) -> int:
        return len(raw_orders)

    consumer = _resolve_as(
        [orders, order_count, _sibling(handler)],
        embedded_storage,
        monkeypatch,
        "analytics",
        executor,
    )
    result = consumer.materialize()

    assert result.success
    assert _input_versions(embedded_storage, "order_count") == [
        ("raw_orders", owner_dv)
    ]
    assert NOT_REGISTERED not in "".join(
        _logs(embedded_storage, result.run_id).values()
    )


def test_aliases_record_each_owners_version_under_the_local_name(
    embedded_storage, tmp_path, monkeypatch
):
    """Two aliases share one owner asset; ``ghost`` has no owner record."""
    warehouse = _pickle_handler(tmp_path / "warehouse")
    shop = _pickle_handler(tmp_path / "shop")
    warehouse_dv = _owner(warehouse, embedded_storage, monkeypatch)
    shop_dv = _owner(shop, embedded_storage, monkeypatch, identity="shop", rows=(4,))
    foo_orders = rs.Asset.remote(
        "foo_orders",
        code_location="warehouse",
        asset_key="raw_orders",
        io_handler=warehouse,
    )
    baz_orders = rs.Asset.remote(
        "baz_orders",
        code_location="warehouse",
        asset_key="raw_orders",
        io_handler=warehouse,
    )
    bar_orders = rs.Asset.remote(
        "bar_orders", code_location="shop", asset_key="raw_orders", io_handler=shop
    )
    ghost = rs.Asset.remote("ghost", code_location="warehouse")

    @rs.Asset(io_handler=warehouse, deps=[rs.AssetDef.dep("ghost")])
    def merged_orders(
        foo_orders: list[int], baz_orders: list[int], bar_orders: list[int]
    ) -> list[int]:
        return foo_orders + baz_orders + bar_orders

    consumer = _resolve_as(
        [foo_orders, baz_orders, bar_orders, ghost, merged_orders],
        embedded_storage,
        monkeypatch,
        "analytics",
    )

    assert consumer.materialize().success
    assert consumer.load_node("merged_orders") == [1, 2, 3, 1, 2, 3, 4]
    assert _input_versions(embedded_storage, "merged_orders") == [
        ("bar_orders", shop_dv),
        ("baz_orders", warehouse_dv),
        ("foo_orders", warehouse_dv),
    ]


@pytest.mark.parametrize("executor", EXECUTORS)
def test_unregistered_owner_loads_with_a_warning(
    executor, storage, embedded_storage, tmp_path, monkeypatch
):
    handler = _pickle_handler(tmp_path / "io")
    _owner(handler, storage, monkeypatch)
    orders = rs.Asset.remote(
        "raw_orders", code_location="warehouse", io_handler=handler
    )

    @rs.Asset(io_handler=handler)
    def order_count(raw_orders: list[int]) -> int:
        return len(raw_orders)

    consumer = _resolve_as(
        [orders, order_count, _sibling(handler)],
        embedded_storage,
        monkeypatch,
        "analytics",
        executor,
    )
    result = consumer.materialize()

    assert result.success
    assert consumer.load_node("order_count") == 3
    assert _input_versions(embedded_storage, "order_count") == []
    logs = _logs(embedded_storage, result.run_id)
    assert NOT_REGISTERED in logs["order_count"]
    assert NOT_REGISTERED not in logs.get("sibling", "")


# The log filter is read once, at `import rivers`, so each case runs in its
# own process.
_TWO_LOADING_STEPS = textwrap.dedent(
    """
    import json
    import os
    import sys

    import obstore
    import rivers as rs
    from rivers.testing import embedded_storage, memory_storage

    root, executor = sys.argv[1], sys.argv[2]
    handler = rs.PickleIOHandler(
        store=obstore.store.LocalStore(os.path.join(root, "io"), mkdir=True)
    )

    @rs.Asset(io_handler=handler)
    def raw_orders() -> list[int]:
        return [1, 2, 3]

    os.environ["RIVERS_CODE_LOCATION_ID"] = "warehouse"
    owner = rs.CodeRepository(assets=[raw_orders])
    owner.resolve(storage=memory_storage())
    assert owner.materialize().success

    orders = rs.Asset.remote(
        "raw_orders", code_location="warehouse", io_handler=handler
    )

    @rs.Asset(io_handler=handler)
    def order_count(raw_orders: list[int]) -> int:
        return len(raw_orders)

    @rs.Asset(io_handler=handler)
    def order_total(raw_orders: list[int]) -> int:
        return sum(raw_orders)

    @rs.Asset(io_handler=handler)
    def sibling() -> int:
        return 1

    executors = {
        "in_process": rs.Executor.in_process(),
        "parallel": rs.Executor.parallel(max_workers=2),
    }
    os.environ["RIVERS_CODE_LOCATION_ID"] = "analytics"
    storage = embedded_storage(os.path.join(root, "db"))
    consumer = rs.CodeRepository(
        assets=[orders, order_count, order_total, sibling],
        default_executor=executors[executor],
    )
    consumer.resolve(storage=storage)
    result = consumer.materialize()
    assert result.success

    logs = {}
    for log in storage._scoped("analytics").get_run_logs(result.run_id):
        logs[log.step_key] = logs.get(log.step_key, "") + (log.logs or "")
    print(json.dumps(logs))
    """
)


def _warning_lines(logs: str) -> list[str]:
    """The lines of ``logs`` with the warning, the timestamp masked."""
    return [
        re.sub(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z", "<ts>", line)
        for line in logs.splitlines()
        if NOT_REGISTERED in line
    ]


@pytest.mark.parametrize("log_filter", ["error", "rivers=info"])
@pytest.mark.parametrize("executor", ["in_process", "parallel"])
def test_unregistered_owner_warning_does_not_depend_on_the_log_filter(
    executor, log_filter, tmp_path
):
    env = {**os.environ, "RUST_LOG": log_filter}
    proc = subprocess.run(
        [sys.executable, "-c", _TWO_LOADING_STEPS, str(tmp_path), executor],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stderr

    logs = json.loads(proc.stdout.splitlines()[-1])
    expected = [f"<ts>  WARN rivers::executor: {NOT_REGISTERED}"]
    assert _warning_lines(logs.get("order_count", "")) == expected
    assert _warning_lines(logs.get("order_total", "")) == expected
    assert _warning_lines(logs.get("sibling", "")) == []
    assert proc.stderr.count(NOT_REGISTERED) == (0 if log_filter == "error" else 1)


@pytest.mark.parametrize("executor", EXECUTORS)
def test_incompatible_mirror_loads_with_a_warning(
    executor, embedded_storage, tmp_path, monkeypatch
):
    handler = _pickle_handler(tmp_path / "io")

    @rs.Asset(
        io_handler=handler, partitions_def=rs.PartitionsDefinition.static_(["a", "b"])
    )
    def raw_orders() -> list[int]:
        return [1, 2, 3]

    owner = _resolve_as([raw_orders], embedded_storage, monkeypatch, "warehouse")
    assert owner.materialize(partition_key=rs.PartitionKey.single("a")).success

    keys = rs.PartitionsDefinition.static_(["a", "b", "c"])
    orders = rs.Asset.remote(
        "raw_orders", code_location="warehouse", io_handler=handler, partitions_def=keys
    )

    @rs.Asset(io_handler=handler, partitions_def=keys)
    def order_count(raw_orders: list[int]) -> int:
        return len(raw_orders)

    @rs.Asset(io_handler=handler, partitions_def=keys)
    def sibling() -> int:
        return 1

    consumer = _resolve_as(
        [orders, order_count, sibling],
        embedded_storage,
        monkeypatch,
        "analytics",
        executor,
    )
    result = consumer.materialize(partition_key=rs.PartitionKey.single("a"))

    assert result.success
    logs = _logs(embedded_storage, result.run_id)
    warning = (
        "partition definition of 'raw_orders' differs from code location "
        "'dev/warehouse' (mirror static (3 keys), owner static (2 keys)); loading anyway"
    )
    assert warning in logs["order_count"]
    assert warning not in logs.get("sibling", "")


@pytest.mark.parametrize("executor", EXECUTORS)
@pytest.mark.parametrize(
    ("owner_keys", "incompatible"),
    [
        pytest.param(["d", "c", "b", "a"], False, id="owner_added_a_key"),
        pytest.param(["a", "b"], True, id="owner_removed_a_key"),
        pytest.param(["a", "b", "d", "e"], True, id="owner_replaced_a_key"),
    ],
)
def test_static_key_drift_of_a_mirror(
    executor, owner_keys, incompatible, embedded_storage, tmp_path, monkeypatch
):
    """The owner's keys against the mirror's ``a, b, c``. With more owner
    keys, the stored key lists of both code locations decide."""
    handler = _pickle_handler(tmp_path / "io")

    @rs.Asset(
        io_handler=handler, partitions_def=rs.PartitionsDefinition.static_(owner_keys)
    )
    def raw_orders() -> list[int]:
        return [1, 2, 3]

    owner = _resolve_as([raw_orders], embedded_storage, monkeypatch, "warehouse")
    assert owner.materialize(partition_key=rs.PartitionKey.single("a")).success
    owner_dv = (
        embedded_storage._scoped("warehouse")
        .get_asset_record("raw_orders")
        .last_data_version
    )

    keys = rs.PartitionsDefinition.static_(["a", "b", "c"])
    orders = rs.Asset.remote(
        "raw_orders", code_location="warehouse", io_handler=handler, partitions_def=keys
    )

    @rs.Asset(io_handler=handler, partitions_def=keys)
    def order_count(raw_orders: list[int]) -> int:
        return len(raw_orders)

    @rs.Asset(io_handler=handler, partitions_def=keys)
    def sibling() -> int:
        return 1

    consumer = _resolve_as(
        [orders, order_count, sibling],
        embedded_storage,
        monkeypatch,
        "analytics",
        executor,
    )
    result = consumer.materialize(partition_key=rs.PartitionKey.single("a"))

    assert result.success
    assert _input_versions(embedded_storage, "order_count") == [
        ("raw_orders", owner_dv)
    ]
    logs = _logs(embedded_storage, result.run_id)
    warning = (
        "partition definition of 'raw_orders' differs from code location "
        f"'dev/warehouse' (mirror static (3 keys), owner static ({len(owner_keys)} "
        "keys)); loading anyway"
    )
    expected = [f"WARN rivers::executor: {warning}"] if incompatible else []
    assert _warnings(logs.get("order_count", "")) == expected
    assert _warnings(logs.get("sibling", "")) == []


def _warnings(logs: str) -> list[str]:
    """Each warning line of ``logs``, from the level on."""
    return [line[line.index("WARN") :] for line in logs.splitlines() if "WARN" in line]


def test_owner_without_the_asset_loads_with_a_warning(
    embedded_storage, tmp_path, monkeypatch
):
    handler = _pickle_handler(tmp_path / "io")
    _owner(handler, embedded_storage, monkeypatch)

    @rs.Asset(io_handler=handler)
    def replacement() -> int:
        return 0

    _resolve_as([replacement], embedded_storage, monkeypatch, "warehouse")
    orders = rs.Asset.remote(
        "raw_orders", code_location="warehouse", io_handler=handler
    )

    @rs.Asset(io_handler=handler)
    def order_count(raw_orders: list[int]) -> int:
        return len(raw_orders)

    consumer = _resolve_as(
        [orders, order_count], embedded_storage, monkeypatch, "analytics"
    )
    result = consumer.materialize()

    assert result.success
    assert (
        "code location 'dev/warehouse' has no asset 'raw_orders'; loading "
        "'raw_orders' through the handler anyway"
    ) in _logs(embedded_storage, result.run_id)["order_count"]


def test_lineage_only_remote_records_the_version_without_loading(
    embedded_storage, tmp_path, monkeypatch
):
    handler = _pickle_handler(tmp_path / "io")
    owner_dv = _owner(handler, embedded_storage, monkeypatch)
    orders = rs.Asset.remote("raw_orders", code_location="warehouse")

    @rs.Asset(io_handler=handler, deps=[rs.AssetDef.dep("raw_orders")])
    def report() -> int:
        return 1

    consumer = _resolve_as([orders, report], embedded_storage, monkeypatch, "analytics")

    assert consumer.materialize().success
    assert _input_versions(embedded_storage, "report") == [("raw_orders", owner_dv)]


def test_lineage_only_remote_of_an_unregistered_owner_warns_nothing(
    embedded_storage, tmp_path, monkeypatch
):
    handler = _pickle_handler(tmp_path / "io")
    orders = rs.Asset.remote("raw_orders", code_location="warehouse")

    @rs.Asset(io_handler=handler, deps=[rs.AssetDef.dep("raw_orders")])
    def report() -> int:
        return 1

    consumer = _resolve_as([orders, report], embedded_storage, monkeypatch, "analytics")
    result = consumer.materialize()

    assert result.success
    assert _input_versions(embedded_storage, "report") == []
    assert NOT_REGISTERED not in "".join(
        _logs(embedded_storage, result.run_id).values()
    )


def test_consumer_is_stale_until_it_reads_the_owners_new_version(
    embedded_storage, tmp_path, monkeypatch
):
    handler = _pickle_handler(tmp_path / "io")
    _owner(handler, embedded_storage, monkeypatch)
    orders = rs.Asset.remote(
        "raw_orders", code_location="warehouse", io_handler=handler
    )

    @rs.Asset(io_handler=handler)
    def order_count(raw_orders: list[int]) -> int:
        return len(raw_orders)

    consumer = _resolve_as(
        [orders, order_count], embedded_storage, monkeypatch, "analytics"
    )
    scoped = embedded_storage._scoped("analytics")
    assert consumer.materialize().success
    assert scoped.compute_staleness() == {"order_count": ("UpToDate", [])}

    _owner(handler, embedded_storage, monkeypatch, rows=(1, 2, 3, 4))
    status, causes = scoped.compute_staleness()["order_count"]
    assert status == "Stale"
    assert [(c.category, c.dependency) for c in causes] == [("Data", "raw_orders")]

    assert consumer.materialize().success
    assert scoped.compute_staleness() == {"order_count": ("UpToDate", [])}
