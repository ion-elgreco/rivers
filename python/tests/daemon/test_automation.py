"""Tests for declarative automation conditions."""

from typing import Any

import pytest

import rivers as rs

# ---------------------------------------------------------------------------
# Leaf condition constructors
# ---------------------------------------------------------------------------


class TestLeafConditions:
    def test_missing(self):
        cond = rs.AutomationCondition.missing()
        assert cond.description == "missing"
        assert cond.label == "missing"
        assert cond.children == []

    def test_in_progress(self):
        cond = rs.AutomationCondition.in_progress()
        assert cond.description == "in_progress"
        assert cond.label == "in_progress"

    def test_execution_failed(self):
        cond = rs.AutomationCondition.execution_failed()
        assert cond.description == "execution_failed"
        assert cond.label == "execution_failed"

    def test_newly_updated(self):
        cond = rs.AutomationCondition.newly_updated()
        assert cond.description == "newly_updated"
        assert cond.label == "newly_updated"

    def test_newly_requested(self):
        cond = rs.AutomationCondition.newly_requested()
        assert cond.description == "newly_requested"
        assert cond.label == "newly_requested"

    def test_code_version_changed(self):
        cond = rs.AutomationCondition.code_version_changed()
        assert cond.description == "code_version_changed"
        assert cond.label == "code_version_changed"

    def test_cron_tick_passed(self):
        cond = rs.AutomationCondition.cron_tick_passed("0 0 * * *")
        assert cond.description == "cron_tick_passed('0 0 * * *')"
        assert cond.label == "cron_tick_passed('0 0 * * *')"

    def test_cron_tick_passed_with_timezone(self):
        cond = rs.AutomationCondition.cron_tick_passed(
            "0 0 * * *", timezone="US/Eastern"
        )
        assert cond.description == "cron_tick_passed('0 0 * * *', tz='US/Eastern')"

    def test_in_latest_time_window(self):
        cond = rs.AutomationCondition.in_latest_time_window()
        assert cond.description == "in_latest_time_window"
        assert cond.label == "in_latest_time_window"

    def test_in_latest_time_window_with_lookback(self):
        cond = rs.AutomationCondition.in_latest_time_window(lookback_delta=3600.0)
        assert cond.description == "in_latest_time_window(lookback=3600)"

    def test_in_latest_time_window_rejects_non_finite_or_non_positive_lookback(self):
        """NaN/negative silently empty the window selection (the condition
        never fires); inf overflows the cutoff arithmetic — reject at
        construction like interval_seconds."""
        for bad in (float("nan"), float("inf"), -3600.0, 0.0):
            with pytest.raises(ValueError, match="lookback_delta must be"):
                rs.AutomationCondition.in_latest_time_window(lookback_delta=bad)

    def test_any_deps_missing(self):
        cond = rs.AutomationCondition.any_deps_missing()
        assert cond.description == "any_deps_missing"
        assert cond.label == "any_deps_missing"

    def test_any_deps_in_progress(self):
        cond = rs.AutomationCondition.any_deps_in_progress()
        assert cond.description == "any_deps_in_progress"
        assert cond.label == "any_deps_in_progress"

    def test_any_deps_updated(self):
        cond = rs.AutomationCondition.any_deps_updated()
        assert cond.description == "any_deps_updated"
        assert cond.label == "any_deps_updated"


# ---------------------------------------------------------------------------
# Dependency match conditions
# ---------------------------------------------------------------------------


class TestDepsMatch:
    def test_any_deps_match(self):
        inner = rs.AutomationCondition.newly_updated()
        cond = rs.AutomationCondition.any_deps_match(inner)
        assert cond.description == "any_deps_match(newly_updated)"
        assert cond.label == "any_deps_match(newly_updated)"
        assert len(cond.children) == 1
        assert cond.children[0].description == "newly_updated"

    def test_all_deps_match(self):
        inner = rs.AutomationCondition.missing()
        cond = rs.AutomationCondition.all_deps_match(inner)
        assert cond.description == "all_deps_match(missing)"
        assert cond.label == "all_deps_match(missing)"
        assert len(cond.children) == 1


# ---------------------------------------------------------------------------
# Boolean operators
# ---------------------------------------------------------------------------


class TestBooleanOperators:
    def test_and(self):
        a = rs.AutomationCondition.missing()
        b = rs.AutomationCondition.newly_updated()
        combined = a & b
        assert combined.description == "(missing & newly_updated)"
        assert combined.label is None
        assert len(combined.children) == 2

    def test_or(self):
        a = rs.AutomationCondition.missing()
        b = rs.AutomationCondition.newly_updated()
        combined = a | b
        assert combined.description == "(missing | newly_updated)"
        assert combined.label is None
        assert len(combined.children) == 2

    def test_not(self):
        a = rs.AutomationCondition.in_progress()
        negated = ~a
        assert negated.description == "~in_progress"
        assert negated.label is None
        assert len(negated.children) == 1

    def test_and_flattening(self):
        """Multiple & operations flatten into a single And."""
        a = rs.AutomationCondition.missing()
        b = rs.AutomationCondition.newly_updated()
        c = rs.AutomationCondition.in_progress()
        combined = a & b & c
        assert combined.description == "(missing & newly_updated & in_progress)"
        assert len(combined.children) == 3

    def test_or_flattening(self):
        """Multiple | operations flatten into a single Or."""
        a = rs.AutomationCondition.missing()
        b = rs.AutomationCondition.newly_updated()
        c = rs.AutomationCondition.in_progress()
        combined = a | b | c
        assert combined.description == "(missing | newly_updated | in_progress)"
        assert len(combined.children) == 3

    def test_complex_composition(self):
        """Complex condition: (missing | newly_updated) & ~in_progress."""
        cond = (
            rs.AutomationCondition.missing() | rs.AutomationCondition.newly_updated()
        ) & ~rs.AutomationCondition.in_progress()
        assert cond.description == "((missing | newly_updated) & ~in_progress)"
        assert len(cond.children) == 2  # (or, not)

    def test_labeled_and_not_flattened(self):
        """Labeled conditions should not be flattened."""
        a = rs.AutomationCondition.missing()
        b = rs.AutomationCondition.newly_updated()
        labeled_and = (a & b).with_label("my_group")
        c = rs.AutomationCondition.in_progress()
        combined = labeled_and & c
        # Should NOT flatten labeled_and; should have 2 children
        assert len(combined.children) == 2


# ---------------------------------------------------------------------------
# State-tracking operators
# ---------------------------------------------------------------------------


class TestStateTracking:
    def test_newly_true(self):
        cond = rs.AutomationCondition.missing().newly_true()
        assert cond.description == "missing.newly_true()"
        assert cond.label is None
        assert len(cond.children) == 1

    def test_since(self):
        trigger = rs.AutomationCondition.missing()
        reset = rs.AutomationCondition.newly_updated()
        cond = trigger.since(reset)
        assert cond.description == "missing.since(newly_updated)"
        assert cond.label is None
        assert len(cond.children) == 2

    def test_since_last_handled(self):
        cond = rs.AutomationCondition.missing().since_last_handled()
        assert cond.description == "missing.since_last_handled()"
        assert cond.label is None
        assert len(cond.children) == 1


# ---------------------------------------------------------------------------
# High-level presets
# ---------------------------------------------------------------------------


class TestPresets:
    def test_eager(self):
        cond = rs.AutomationCondition.eager()
        # eager() is a composite — description shows the expanded tree
        assert cond.label == "eager"
        assert repr(cond) == "AutomationCondition(eager)"

    def test_on_cron(self):
        cond = rs.AutomationCondition.on_cron("0 0 * * *")
        assert cond.label == "on_cron('0 0 * * *')"

    def test_on_cron_with_timezone(self):
        cond = rs.AutomationCondition.on_cron("0 0 * * *", timezone="UTC")
        assert cond.label == "on_cron('0 0 * * *', tz='UTC')"

    def test_on_missing(self):
        cond = rs.AutomationCondition.on_missing()
        assert cond.label == "on_missing"


# ---------------------------------------------------------------------------
# Labels
# ---------------------------------------------------------------------------


class TestLabels:
    def test_default_label_on_leaf(self):
        cond = rs.AutomationCondition.missing()
        assert cond.label == "missing"

    def test_no_label_on_junction(self):
        cond = rs.AutomationCondition.missing() & rs.AutomationCondition.in_progress()
        assert cond.label is None

    def test_with_label_overrides_default(self):
        cond = rs.AutomationCondition.missing().with_label("freshness_check")
        assert cond.label == "freshness_check"
        # description is unchanged by label
        assert cond.description == "missing"

    def test_label_on_junction(self):
        cond = (
            rs.AutomationCondition.missing() & rs.AutomationCondition.in_progress()
        ).with_label("my_group")
        assert cond.label == "my_group"


# ---------------------------------------------------------------------------
# repr
# ---------------------------------------------------------------------------


class TestRepr:
    def test_simple_repr(self):
        cond = rs.AutomationCondition.missing()
        assert repr(cond) == "AutomationCondition(missing)"

    def test_complex_repr(self):
        cond = rs.AutomationCondition.missing() & ~rs.AutomationCondition.in_progress()
        assert repr(cond) == "AutomationCondition((missing & ~in_progress))"


# ---------------------------------------------------------------------------
# Cron validation
# ---------------------------------------------------------------------------


class TestCronValidation:
    def test_cron_tick_passed_rejects_invalid_schedule(self):
        with pytest.raises(ValueError):
            rs.AutomationCondition.cron_tick_passed("this is not a cron")

    def test_on_cron_rejects_invalid_schedule(self):
        with pytest.raises(ValueError):
            rs.AutomationCondition.on_cron("totally bogus")

    def test_valid_schedules_accepted(self):
        # Standard 5-field and 6-field (optional seconds) both parse.
        rs.AutomationCondition.cron_tick_passed("0 0 * * *")
        rs.AutomationCondition.on_cron("*/15 * * * *")
        rs.AutomationCondition.on_cron("0 0 0 * * *")

    def test_cron_rejects_invalid_timezone(self):
        # An unknown IANA zone must fail loudly, not silently fall back to UTC.
        with pytest.raises(ValueError):
            rs.AutomationCondition.on_cron("0 0 * * *", timezone="Not/AZone")
        with pytest.raises(ValueError):
            rs.AutomationCondition.cron_tick_passed("0 0 * * *", timezone="bogus")

    def test_valid_timezones_accepted(self):
        rs.AutomationCondition.on_cron("0 0 * * *", timezone="America/New_York")
        rs.AutomationCondition.cron_tick_passed("0 0 * * *", timezone="Europe/London")
        rs.AutomationCondition.all_deps_updated_since_cron("0 0 * * *", timezone="UTC")

    def test_tag_condition_rejects_empty_filter(self):
        # An empty key+value filter vacuously matches every run; reject it.
        with pytest.raises(ValueError):
            rs.AutomationCondition.has_run_with_tags()
        with pytest.raises(ValueError):
            rs.AutomationCondition.all_runs_have_tags()
        with pytest.raises(ValueError):
            rs.AutomationCondition.last_executed_with_tags()

    def test_tag_condition_accepts_nonempty_filter(self):
        rs.AutomationCondition.has_run_with_tags(tag_keys=["env"])
        rs.AutomationCondition.all_runs_have_tags(tag_values=[("env", "prod")])

    def test_on_selected_rejects_empty_keys(self):
        # An empty asset-key set never matches → degenerate always-false subtree.
        with pytest.raises(ValueError):
            rs.AutomationCondition.newly_updated().on_selected([])


# ---------------------------------------------------------------------------
# without()
# ---------------------------------------------------------------------------


class TestWithout:
    def test_without_removing_all_operands_raises(self):
        # Removing every operand leaves an empty And, which evaluates to
        # vacuously true (fires every tick) — reject it instead.
        ac = rs.AutomationCondition
        cond = ac.in_progress() & ac.in_progress()
        with pytest.raises(ValueError, match="every operand|empty"):
            cond.without(ac.in_progress())

    def test_without_negated_guard_by_object(self):
        # The And child is Not(any_deps_in_progress); pass it as it appears (~X).
        ac = rs.AutomationCondition
        n = len(ac.eager().children)
        pruned = ac.eager().without(~ac.any_deps_in_progress())
        descs = " ".join(c.description for c in pruned.children)
        assert "any_deps_in_progress" not in descs
        assert len(pruned.children) == n - 1

    def test_without_negated_guard_by_description_string(self):
        ac = rs.AutomationCondition
        pruned = ac.eager().without("~any_deps_in_progress")
        descs = " ".join(c.description for c in pruned.children)
        assert "any_deps_in_progress" not in descs

    def test_without_bare_does_not_match_negated_guard(self):
        # X and ~X are opposites: passing the bare form must NOT drop Not(X).
        ac = rs.AutomationCondition
        n = len(ac.eager().children)
        for arg in (ac.any_deps_in_progress(), "any_deps_in_progress"):
            pruned = ac.eager().without(arg)
            assert len(pruned.children) == n, (
                f"{arg!r} must not match the Not(...) guard"
            )

    def test_without_bare_operand_structural(self):
        # A non-negated operand is removed by passing it as-is (structural ==).
        ac = rs.AutomationCondition
        cond = ac.any_deps_match(ac.missing()) & ac.in_progress()
        assert len(cond.children) == 2
        pruned = cond.without(ac.any_deps_match(ac.missing()))
        assert [c.description for c in pruned.children] == ["in_progress"]


# ---------------------------------------------------------------------------
# Asset integration
# ---------------------------------------------------------------------------


class TestAssetIntegration:
    def test_asset_with_automation_condition(self):
        @rs.Asset(automation_condition=rs.AutomationCondition.eager())
        def my_asset() -> Any:
            return 42

        assert isinstance(my_asset, rs.SingleAsset)

    def test_asset_with_complex_condition(self):
        condition = (
            rs.AutomationCondition.missing().newly_true().since_last_handled()
            & ~rs.AutomationCondition.any_deps_missing()
            & ~rs.AutomationCondition.any_deps_in_progress()
        )

        @rs.Asset(automation_condition=condition)
        def my_asset() -> Any:
            return 42

        assert isinstance(my_asset, rs.SingleAsset)

    def test_asset_no_condition_by_default(self):
        @rs.Asset
        def my_asset() -> Any:
            return 42

        assert isinstance(my_asset, rs.SingleAsset)

    def test_materialize_with_automation_condition(self):
        """Assets with automation conditions can still be materialized normally."""

        @rs.Asset(automation_condition=rs.AutomationCondition.eager())
        def my_asset() -> int:
            return 42

        repo = rs.CodeRepository(assets=[my_asset])
        result = repo.materialize()
        assert result.success
        assert repo.load_node("my_asset") == 42

    def test_newly_requested_cross_asset_signaling(self):
        """any_deps_match(newly_requested()) — trigger downstream when upstream was requested."""
        cond = rs.AutomationCondition.any_deps_match(
            rs.AutomationCondition.newly_requested()
            | rs.AutomationCondition.execution_failed()
        )
        assert (
            cond.description == "any_deps_match((newly_requested | execution_failed))"
        )

        @rs.Asset(
            automation_condition=cond,
            io_handler=rs.InMemoryIOHandler(),
        )
        def downstream(upstream: int) -> int:
            return upstream * 2

        @rs.Asset(io_handler=rs.InMemoryIOHandler())
        def upstream() -> int:
            return 1

        repo = rs.CodeRepository(assets=[upstream, downstream])
        result = repo.materialize()
        assert result.success

    def test_newly_requested_since_reset(self):
        """code_version_changed().since(newly_requested()) — detect code changes since last request."""
        cond = rs.AutomationCondition.code_version_changed().since(
            rs.AutomationCondition.newly_requested()
        )
        assert cond.description == "code_version_changed.since(newly_requested)"
        assert len(cond.children) == 2

    def test_newly_requested_in_since_last_handled(self):
        """since_last_handled uses newly_requested as debounce — verify composition."""
        trigger = rs.AutomationCondition.any_deps_updated()
        reset = (
            rs.AutomationCondition.newly_requested()
            | rs.AutomationCondition.newly_updated()
        )
        cond = trigger.since(reset)
        assert (
            cond.description
            == "any_deps_updated.since((newly_requested | newly_updated))"
        )
