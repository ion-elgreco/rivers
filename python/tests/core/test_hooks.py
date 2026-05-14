"""Tests for asset hooks (Hook.success, Hook.failure)."""

from typing import Any

import pytest
import rivers as rs
from rivers.exceptions import AssetDefinitionError


# ---------------------------------------------------------------------------
# Hook decorator tests
# ---------------------------------------------------------------------------


class TestHookDecorators:
    """Test hook creation via decorators."""

    def test_success_hook_bare_decorator(self):
        @rs.Hook.success
        def my_hook(context: Any):
            pass

        assert isinstance(my_hook, rs.Hook)
        assert isinstance(my_hook, rs.Hook.Success)
        assert my_hook.name == "my_hook"

    def test_failure_hook_bare_decorator(self):
        @rs.Hook.failure
        def on_fail(context: Any):
            pass

        assert isinstance(on_fail, rs.Hook)
        assert isinstance(on_fail, rs.Hook.Failure)
        assert on_fail.name == "on_fail"

    def test_success_hook_with_name(self):
        @rs.Hook.success(name="custom_name")
        def my_hook(context: Any):
            pass

        assert isinstance(my_hook, rs.Hook)
        assert isinstance(my_hook, rs.Hook.Success)
        assert my_hook.name == "custom_name"

    def test_failure_hook_with_name(self):
        @rs.Hook.failure(name="alert")
        def on_fail(context: Any):
            pass

        assert isinstance(on_fail, rs.Hook)
        assert isinstance(on_fail, rs.Hook.Failure)
        assert on_fail.name == "alert"

    def test_hook_repr(self):
        @rs.Hook.success
        def my_hook(context: Any):
            pass

        assert repr(my_hook) == "Hook.Success(name='my_hook')"

    def test_hook_already_bound_cannot_call_again(self):
        @rs.Hook.success
        def my_hook(context: Any):
            pass

        with pytest.raises(AssetDefinitionError, match="already bound"):
            my_hook(lambda ctx: None)

    def test_isinstance_distinguishes_types(self):
        """isinstance checks should distinguish Success from Failure hooks."""

        @rs.Hook.success
        def on_success(context: Any):
            pass

        @rs.Hook.failure
        def on_failure(context: Any):
            pass

        assert isinstance(on_success, rs.Hook.Success)
        assert not isinstance(on_success, rs.Hook.Failure)
        assert isinstance(on_failure, rs.Hook.Failure)
        assert not isinstance(on_failure, rs.Hook.Success)
        # Both are instances of the base Hook class
        assert isinstance(on_success, rs.Hook)
        assert isinstance(on_failure, rs.Hook)


# ---------------------------------------------------------------------------
# Hook integration with Asset
# ---------------------------------------------------------------------------


class TestAssetWithHooks:
    """Test attaching hooks to assets."""

    def test_asset_with_success_hook(self):
        @rs.Hook.success
        def notify(context: Any):
            pass

        @rs.Asset(hooks=[notify])
        def my_asset() -> Any:
            return 42

        assert my_asset._name == "my_asset"

    def test_asset_with_failure_hook(self):
        @rs.Hook.failure
        def on_fail(context: Any):
            pass

        @rs.Asset(hooks=[on_fail])
        def my_asset() -> Any:
            return 42

        assert my_asset._name == "my_asset"

    def test_asset_with_multiple_hooks(self):
        @rs.Hook.success
        def notify(context: Any):
            pass

        @rs.Hook.failure
        def on_fail(context: Any):
            pass

        @rs.Asset(hooks=[notify, on_fail])
        def my_asset() -> Any:
            return 42

        assert my_asset._name == "my_asset"

    def test_asset_no_hooks_by_default(self):
        @rs.Asset
        def my_asset() -> Any:
            return 42

        # Should work fine without hooks
        assert my_asset._name == "my_asset"


# ---------------------------------------------------------------------------
# Hook execution during materialization
# ---------------------------------------------------------------------------


class TestHookExecution:
    """Test that hooks are actually called during execution."""

    def test_success_hook_called_on_success(self):
        """Success hooks should fire when asset succeeds."""
        hook_calls = []

        @rs.Hook.success
        def track_success(context: rs.HookContext):
            hook_calls.append(
                {
                    "asset_name": context.asset_name,
                    "hook_type": context.hook_type,
                    "output": context.output,
                    "error": context.error,
                }
            )

        @rs.Asset(hooks=[track_success])
        def producer() -> Any:
            return 42

        repo = rs.CodeRepository(assets=[producer])
        repo.resolve()
        repo.materialize(["producer"])

        assert len(hook_calls) == 1
        assert hook_calls[0]["asset_name"] == "producer"
        assert hook_calls[0]["hook_type"] == "success"
        assert hook_calls[0]["output"] == 42
        assert hook_calls[0]["error"] is None

    def test_failure_hook_called_on_failure(self):
        """Failure hooks should fire when asset fails."""
        hook_calls = []

        @rs.Hook.failure
        def track_failure(context: rs.HookContext):
            hook_calls.append(
                {
                    "asset_name": context.asset_name,
                    "hook_type": context.hook_type,
                    "error": context.error,
                }
            )

        @rs.Asset(hooks=[track_failure])
        def failing_asset() -> Any:
            raise ValueError("intentional error")

        repo = rs.CodeRepository(assets=[failing_asset])
        repo.resolve()

        with pytest.raises(Exception):
            repo.materialize(["failing_asset"])

        assert len(hook_calls) == 1
        assert hook_calls[0]["asset_name"] == "failing_asset"
        assert hook_calls[0]["hook_type"] == "failure"
        assert "intentional error" in hook_calls[0]["error"]

    def test_success_hook_not_called_on_failure(self):
        """Success hooks should NOT fire on failure."""
        hook_calls = []

        @rs.Hook.success
        def track_success(context: rs.HookContext):
            hook_calls.append("success")

        @rs.Asset(hooks=[track_success])
        def failing_asset() -> Any:
            raise ValueError("boom")

        repo = rs.CodeRepository(assets=[failing_asset])
        repo.resolve()

        with pytest.raises(Exception):
            repo.materialize(["failing_asset"])

        assert len(hook_calls) == 0

    def test_failure_hook_not_called_on_success(self):
        """Failure hooks should NOT fire on success."""
        hook_calls = []

        @rs.Hook.failure
        def track_failure(context: rs.HookContext):
            hook_calls.append("failure")

        @rs.Asset(hooks=[track_failure])
        def producer() -> Any:
            return 42

        repo = rs.CodeRepository(assets=[producer])
        repo.resolve()
        repo.materialize(["producer"])

        assert len(hook_calls) == 0

    def test_multiple_hooks_all_called(self):
        """All matching hooks should fire."""
        calls = []

        @rs.Hook.success
        def hook_a(context: rs.HookContext):
            calls.append("a")

        @rs.Hook.success
        def hook_b(context: rs.HookContext):
            calls.append("b")

        @rs.Asset(hooks=[hook_a, hook_b])
        def my_asset() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        repo.materialize(["my_asset"])

        assert calls == ["a", "b"]

    def test_hook_error_does_not_fail_step(self):
        """Hook errors should not fail the asset execution."""

        @rs.Hook.success
        def broken_hook(context: rs.HookContext):
            raise RuntimeError("hook exploded")

        @rs.Asset(hooks=[broken_hook])
        def producer() -> Any:
            return 42

        repo = rs.CodeRepository(assets=[producer])
        repo.resolve()
        # Should NOT raise despite the hook failing
        result = repo.materialize(["producer"])
        assert result.success
        assert repo.load_node("producer") == 42

    def test_hook_context_has_metadata(self):
        """Hook context should include asset metadata."""
        seen_metadata = []

        @rs.Hook.success
        def check_meta(context: rs.HookContext):
            seen_metadata.append(context.metadata)

        @rs.Asset(hooks=[check_meta], metadata={"env": "test"})
        def my_asset() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        repo.materialize(["my_asset"])

        assert len(seen_metadata) == 1
        assert seen_metadata[0] == {"env": "test"}

    def test_hook_on_downstream_asset(self):
        """Hooks should fire for the specific asset they're attached to."""
        calls = []

        @rs.Hook.success
        def track(context: rs.HookContext):
            calls.append(context.asset_name)

        @rs.Asset
        def upstream() -> Any:
            return 10

        @rs.Asset(hooks=[track])
        def downstream(upstream: Any) -> Any:
            return upstream * 2

        repo = rs.CodeRepository(assets=[upstream, downstream])
        repo.resolve()
        repo.materialize(["upstream", "downstream"])

        # Hook should only fire for downstream, not upstream
        assert calls == ["downstream"]

    def test_both_success_and_failure_hooks_only_one_fires(self):
        """When both hook types are attached, only the matching one fires."""
        calls = []

        @rs.Hook.success
        def on_success(context: rs.HookContext):
            calls.append("success")

        @rs.Hook.failure
        def on_failure(context: rs.HookContext):
            calls.append("failure")

        @rs.Asset(hooks=[on_success, on_failure])
        def my_asset() -> Any:
            return 42

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        repo.materialize(["my_asset"])

        assert calls == ["success"]


# ---------------------------------------------------------------------------
# Hook context data
# ---------------------------------------------------------------------------


class TestHookContext:
    """Test HookContext attributes."""

    def test_success_context_fields(self):
        """Success HookContext should have correct fields."""
        captured = {}

        @rs.Hook.success
        def capture(context: rs.HookContext):
            captured["asset_name"] = context.asset_name
            captured["run_id"] = context.run_id
            captured["hook_type"] = context.hook_type
            captured["output"] = context.output
            captured["error"] = context.error

        @rs.Asset(hooks=[capture])
        def my_asset() -> Any:
            return "hello"

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        repo.materialize(["my_asset"])

        assert captured["asset_name"] == "my_asset"
        assert captured["run_id"]  # should be a non-empty string
        assert captured["hook_type"] == "success"
        assert captured["output"] == "hello"
        assert captured["error"] is None

    def test_failure_context_fields(self):
        """Failure HookContext should have correct fields."""
        captured = {}

        @rs.Hook.failure
        def capture(context: rs.HookContext):
            captured["asset_name"] = context.asset_name
            captured["hook_type"] = context.hook_type
            captured["error"] = context.error
            captured["output"] = context.output

        @rs.Asset(hooks=[capture])
        def failing() -> Any:
            raise TypeError("bad type")

        repo = rs.CodeRepository(assets=[failing])
        repo.resolve()

        with pytest.raises(Exception):
            repo.materialize(["failing"])

        assert captured["asset_name"] == "failing"
        assert captured["hook_type"] == "failure"
        assert "bad type" in captured["error"]
        assert captured["output"] is None


# ---------------------------------------------------------------------------
# Hook receives the resolved Pydantic config
# review_in_process.md bug #9: failure hook used to receive `None`; success
# hook always saw the config. After the fix, both paths get the same value.
# ---------------------------------------------------------------------------


from pydantic import BaseModel  # noqa: E402


class HookConfig(BaseModel):
    threshold: float = 0.5
    label: str = "default"


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
class TestHookContextConfig:
    """The Pydantic config resolved for the asset must reach both success and
    failure hooks. Pre-fix, the in-process and async failure paths passed
    `None` because `execute_step` only surfaced `config_instance` on
    `Ok(StepResult)`; on `Err`, the resolved config was lost.

    Parametrized over sync (`InProcessBackend::run_step_to_completion`) and
    async (`AsyncBackend::schedule_batch`) — both paths were modified.
    """

    def test_success_hook_receives_config(self, is_async):
        captured: dict[str, Any] = {}

        @rs.Hook.success
        def capture(context: rs.HookContext):
            captured["config"] = context.config

        if is_async:

            @rs.Asset(hooks=[capture])
            async def cfg_success(
                context: rs.AssetExecutionContext[HookConfig],
            ) -> int:
                return int(context.config.threshold * 10)
        else:

            @rs.Asset(hooks=[capture])
            def cfg_success(context: rs.AssetExecutionContext[HookConfig]) -> int:
                return int(context.config.threshold * 10)

        repo = rs.CodeRepository(assets=[cfg_success])
        repo.resolve()
        repo.materialize(["cfg_success"])

        assert captured["config"] is not None
        assert isinstance(captured["config"], HookConfig)
        assert captured["config"].threshold == 0.5
        assert captured["config"].label == "default"

    def test_failure_hook_receives_config(self, is_async):
        """Regression for review #9. Pre-fix the failure hook saw `None`."""
        captured: dict[str, Any] = {}

        @rs.Hook.failure
        def capture(context: rs.HookContext):
            captured["config"] = context.config

        if is_async:

            @rs.Asset(hooks=[capture])
            async def cfg_failure(
                context: rs.AssetExecutionContext[HookConfig],
            ) -> int:
                assert context.config.threshold == 0.5
                raise RuntimeError("boom")
        else:

            @rs.Asset(hooks=[capture])
            def cfg_failure(context: rs.AssetExecutionContext[HookConfig]) -> int:
                # Use the config so the resolution actually happens, then raise.
                assert context.config.threshold == 0.5
                raise RuntimeError("boom")

        repo = rs.CodeRepository(assets=[cfg_failure])
        repo.resolve()

        with pytest.raises(Exception):
            repo.materialize(["cfg_failure"])

        assert captured["config"] is not None, (
            "failure hook received `None` for config — "
            "pre-fix behavior. The fix threads `resolved_config` from "
            "`execute_step` through to `run_failure_hooks`."
        )
        assert isinstance(captured["config"], HookConfig)
        assert captured["config"].threshold == 0.5
        assert captured["config"].label == "default"


# ---------------------------------------------------------------------------
# MultiAsset hooks
# ---------------------------------------------------------------------------


class TestMultiAssetHooks:
    """Test hooks on multi-assets."""

    def test_multi_asset_success_hook_fires_per_child(self):
        """Success hooks on a multi-asset should fire for each child asset."""
        calls = []

        @rs.Hook.success
        def track(context: rs.HookContext):
            calls.append(context.asset_name)

        @rs.Asset.from_multi(
            output_defs=[rs.AssetDef("x"), rs.AssetDef("y")],
            hooks=[track],
        )
        def producer() -> dict:
            return {"x": 1, "y": 2}

        repo = rs.CodeRepository(
            assets=[producer], default_executor=rs.Executor.in_process()
        )
        repo.resolve()
        repo.materialize(["x", "y"])

        assert "x" in calls
        assert "y" in calls

    def test_multi_asset_failure_hook_fires(self):
        """Failure hooks on a multi-asset should fire when execution fails."""
        calls = []

        @rs.Hook.failure
        def on_fail(context: rs.HookContext):
            calls.append(context.asset_name)

        @rs.Asset.from_multi(
            output_defs=[rs.AssetDef("a"), rs.AssetDef("b")],
            hooks=[on_fail],
        )
        def bad_producer() -> dict:
            raise ValueError("multi boom")

        repo = rs.CodeRepository(
            assets=[bad_producer], default_executor=rs.Executor.in_process()
        )
        repo.resolve()

        with pytest.raises(Exception):
            repo.materialize(["a", "b"])

        assert len(calls) > 0

    def test_multi_asset_with_automation_condition(self):
        """Multi-asset can accept an automation condition."""
        cond = rs.AutomationCondition.any_deps_updated()

        @rs.Asset.from_multi(
            output_defs=[rs.AssetDef("p"), rs.AssetDef("q")],
            automation_condition=cond,
        )
        def producer() -> dict:
            return {"p": 10, "q": 20}

        repo = rs.CodeRepository(
            assets=[producer], default_executor=rs.Executor.in_process()
        )
        repo.resolve()
        result = repo.materialize(["p", "q"])
        assert result.success

    def test_multi_asset_hooks_and_automation_condition(self):
        """Multi-asset can have both hooks and automation condition."""
        calls = []

        @rs.Hook.success
        def track(context: rs.HookContext):
            calls.append(context.asset_name)

        cond = rs.AutomationCondition.cron_tick_passed("@daily")

        @rs.Asset.from_multi(
            output_defs=[rs.AssetDef("m"), rs.AssetDef("n")],
            hooks=[track],
            automation_condition=cond,
        )
        def producer() -> dict:
            return {"m": 1, "n": 2}

        repo = rs.CodeRepository(
            assets=[producer], default_executor=rs.Executor.in_process()
        )
        repo.resolve()
        repo.materialize(["m", "n"])

        assert "m" in calls
        assert "n" in calls
