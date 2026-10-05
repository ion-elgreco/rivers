import pytest

import rivers as rs
from _helpers import IP, event_types


# ---------------------------------------------------------------------------
# Dynamic outcome reporting (ActionResult / MayMaterialize)
# ---------------------------------------------------------------------------


class TestActionResultAccessors:
    """An ``ActionResult`` must be readable from Python.

    The ``materialized`` field shares its name with the ``materialized()``
    constructor, so a plain ``#[pyo3(get)]`` would be shadowed by the
    staticmethod and read as a truthy bound function for every result,
    including ``unchanged()``. The outcome is exposed under its own name.
    """

    def test_unchanged_reports_no_materialization(self):
        r = rs.ActionResult.unchanged(metadata={"files_scanned": 12})
        assert r.is_materialized is False
        assert r.data_version is None
        assert r.metadata == {"files_scanned": rs.MetadataValue.int(12)}

    def test_materialized_reports_materialization(self):
        r = rs.ActionResult.materialized(metadata={"rows_merged": 3}, data_version="v9")
        assert r.is_materialized is True
        assert r.data_version == "v9"
        assert r.metadata == {"rows_merged": rs.MetadataValue.int(3)}

    def test_metadata_defaults_to_empty(self):
        assert rs.ActionResult.unchanged().metadata == {}


class _MergeTable(rs.Asset):
    io_handler = rs.InMemoryIOHandler()
    late_rows = 0

    @classmethod
    def materialize(cls):
        return 1

    @rs.action(outcome=rs.Outcome.MayMaterialize)
    @classmethod
    def merge_late(cls, ctx):
        if cls.late_rows == 0:
            return rs.ActionResult.unchanged()
        return rs.ActionResult.materialized(
            metadata={"rows_merged": cls.late_rows}, data_version="merged-v2"
        )


def test_no_op_merge_does_not_cascade():
    class Table(_MergeTable):
        late_rows = 0

    repo = rs.CodeRepository(assets=[Table], default_executor=IP)
    repo.materialize()
    result = repo.run_action("merge_late")

    assert result.success
    types = event_types(repo, result.run_id)
    assert "ActionCompleted" in types
    assert "Materialization" not in types


def test_merge_with_data_emits_materialization():
    class Table(_MergeTable):
        late_rows = 7

    repo = rs.CodeRepository(assets=[Table], default_executor=IP)
    repo.materialize()
    result = repo.run_action("merge_late")

    assert result.success
    events = repo.storage.get_events_for_run(result.run_id)
    mats = [e for e in events if e.event_type == "Materialization"]
    assert len(mats) == 1
    assert mats[0].data_version == "merged-v2"
    meta = dict(mats[0].metadata)
    assert "rows_merged" in meta
    assert "ActionCompleted" not in [e.event_type for e in events]


def test_merge_preserves_upstream_provenance():
    """A merge consumes no upstream, so it must not erase what was consumed.

    Reporting `materialized()` writes an empty input-data-version list; if that
    reaches the asset row, the asset loses its provenance and reads Stale
    against a dependency nothing has touched.
    """

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def source() -> int:
        return 1

    class Derived(rs.Asset):
        io_handler = rs.InMemoryIOHandler()

        @classmethod
        def materialize(cls, source: int) -> int:
            return source + 1

        @rs.action(outcome=rs.Outcome.MayMaterialize)
        @classmethod
        def merge_late(cls, ctx):
            return rs.ActionResult.materialized(data_version="merged-v2")

    repo = rs.CodeRepository(assets=[source, Derived], default_executor=IP)
    repo.materialize()
    before = repo.storage.get_asset_record("derived").last_input_data_versions
    assert before, "precondition: derived consumed source"

    assert repo.run_action("merge_late", selection=["derived"]).success

    record = repo.storage.get_asset_record("derived")
    assert record.last_input_data_versions == before
    assert record.last_data_version == "merged-v2"
    assert repo.storage.compute_staleness()["derived"][0] == "UpToDate"


def test_unchanged_action_reporting_materialized_fails():
    class Table(rs.Asset):
        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def sneaky(cls, ctx):
            return rs.ActionResult.materialized()

    repo = rs.CodeRepository(assets=[Table], default_executor=IP)
    repo.materialize()
    result = repo.run_action("sneaky", raise_on_error=False)

    assert not result.success
    assert "declared Outcome.Unchanged" in result.failed_assets[0][1]


def test_garbage_action_return_fails():
    class Table(rs.Asset):
        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def oops(cls, ctx):
            return {"not": "an ActionResult"}

    repo = rs.CodeRepository(assets=[Table], default_executor=IP)
    repo.materialize()
    result = repo.run_action("oops", raise_on_error=False)

    assert not result.success
    assert "actions return rs.ActionResult or None" in result.failed_assets[0][1]


def test_action_inline_retry_policy_runs_the_ladder():
    attempts = []

    class Flaky(rs.Asset):
        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged, retry=rs.RetryPolicy(max_retries=2))
        @classmethod
        def wobbly(cls, ctx):
            attempts.append(1)
            if len(attempts) < 3:
                raise RuntimeError("transient")
            return None

    repo = rs.CodeRepository(assets=[Flaky], default_executor=IP)
    repo.materialize()
    result = repo.run_action("wobbly")

    assert result.success
    assert len(attempts) == 3
    types = event_types(repo, result.run_id)
    assert types.count("StepRetry") == 2


def test_action_named_retry_policy_runs_the_ladder():
    attempts = []

    class Table(rs.Asset):
        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged, retry="my_policy")
        @classmethod
        def verbed(cls, ctx):
            attempts.append(1)
            if len(attempts) < 3:
                raise RuntimeError("transient")
            return None

    repo = rs.CodeRepository(
        assets=[Table],
        default_executor=IP,
        retries={"my_policy": rs.RetryPolicy(max_retries=2)},
    )
    repo.materialize()
    result = repo.run_action("verbed")

    assert result.success
    assert len(attempts) == 3
    assert event_types(repo, result.run_id).count("StepRetry") == 2


def test_action_unknown_named_retry_rejected():
    class Table(rs.Asset):
        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged, retry="missing")
        @classmethod
        def verbed(cls, ctx):
            return None

    repo = rs.CodeRepository(assets=[Table], default_executor=IP)
    repo.materialize()
    with pytest.raises(Exception, match="unknown retry policy 'missing'"):
        repo.run_action("verbed")
