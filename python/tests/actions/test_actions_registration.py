import pytest

import rivers as rs
from rivers.exceptions import AssetDefinitionError


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def test_reserved_names_rejected():
    for verb in ("materialize", "observe", "compose"):
        with pytest.raises(AssetDefinitionError, match="reserved verb"):
            rs.AssetAction(name=verb, outcome=rs.Outcome.Unchanged)


def test_observe_outcome_reserved():
    with pytest.raises(AssetDefinitionError, match="reserved for the built-in observe"):
        rs.AssetAction(name="probe", outcome=rs.Outcome.Observe)


def test_unbound_action_rejected_at_decorator():
    with pytest.raises(AssetDefinitionError, match="has no function"):

        @rs.Asset(actions=[rs.AssetAction(name="opt", outcome=rs.Outcome.Unchanged)])
        def orders() -> int:
            return 1


def test_duplicate_action_names_rejected():
    def _body(ctx):
        return None

    a = rs.AssetAction(name="opt", outcome=rs.Outcome.Unchanged)(_body)
    b = rs.AssetAction(name="opt", outcome=rs.Outcome.Unchanged)(_body)
    with pytest.raises(AssetDefinitionError, match="duplicate action"):

        @rs.Asset(actions=[a, b])
        def orders() -> int:
            return 1


def test_rebinding_a_bound_action_rejected():
    def _body(ctx):
        return None

    bound = rs.AssetAction(name="opt", outcome=rs.Outcome.Unchanged)(_body)
    with pytest.raises(AssetDefinitionError, match="already bound"):
        bound(_body)


def test_shadowing_inherited_action_rejected():
    class Base(rs.Asset):
        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def optimize(cls, ctx):
            return None

    class Child(Base):
        optimize = "not an action"

        @classmethod
        def materialize(cls):
            return 1

    from rivers._core.assets import desugar

    with pytest.raises(AssetDefinitionError, match="shadows the inherited action"):
        desugar(Child)


def test_action_metadata_exposed():
    def _body(ctx):
        return None

    act = rs.AssetAction(
        name="vacuum",
        outcome=rs.Outcome.Unchanged,
        concurrency=rs.ActionConcurrency.Exclusive,
        description="clean up",
    )(_body)
    assert act.name == "vacuum"
    assert act.exclusive is True
    assert act.outcome == rs.Outcome.Unchanged
    assert act.description == "clean up"
