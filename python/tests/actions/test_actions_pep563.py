"""Action config in a module with ``from __future__ import annotations``.

PEP 563 keeps every annotation as a string, so ``ActionContext[Cfg]`` reaches
the executor as text. The config type must still be read from it.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest
from pydantic import BaseModel

import rivers as rs

IP = rs.Executor.in_process()


class VacuumConfig(BaseModel):
    retention_hours: int = 168


class ProbeResource(rs.Resource):
    prefix: str = "probe"


if TYPE_CHECKING:
    # Only type checkers see this name, like an import under TYPE_CHECKING.
    Probe = ProbeResource


def _events_asset(form: str, seen: list):
    if form == "function":

        def _vacuum(ctx: rs.ActionContext[VacuumConfig]) -> None:
            seen.append(ctx.config)

    elif form == "function-async":

        async def _vacuum(ctx: rs.ActionContext[VacuumConfig]) -> None:
            await asyncio.sleep(0)
            seen.append(ctx.config)

    elif form == "classmethod":

        class Events(rs.Asset):
            @classmethod
            def materialize(cls):
                return 1

            @rs.action(outcome=rs.Outcome.Unchanged)
            @classmethod
            def vacuum(cls, ctx: rs.ActionContext[VacuumConfig]) -> None:
                seen.append(ctx.config)

        return Events
    else:

        class Events(rs.Asset):
            @classmethod
            def materialize(cls):
                return 1

            @rs.action(outcome=rs.Outcome.Unchanged)
            @classmethod
            async def vacuum(cls, ctx: rs.ActionContext[VacuumConfig]) -> None:
                await asyncio.sleep(0)
                seen.append(ctx.config)

        return Events

    vacuum = rs.AssetAction(name="vacuum", outcome=rs.Outcome.Unchanged)(_vacuum)

    @rs.Asset(actions=[vacuum])
    def events():
        return 1

    return events


@pytest.mark.parametrize(
    "form", ["function", "function-async", "classmethod", "classmethod-async"]
)
def test_action_config_under_pep563(form):
    seen = []
    repo = rs.CodeRepository(assets=[_events_asset(form, seen)], default_executor=IP)

    assert repo.run_action("vacuum").success
    assert repo.run_action(
        "vacuum", config={"events": {"retention_hours": 720}}
    ).success
    assert seen == [
        VacuumConfig(retention_hours=168),
        VacuumConfig(retention_hours=720),
    ]


def test_action_config_ignores_unresolved_resource_annotation():
    """Only the context annotation is resolved: a resource type that exists
    only for type checkers does not block the config."""
    seen = []

    def _vacuum(ctx: rs.ActionContext[VacuumConfig], probe: Probe) -> None:
        seen.append((ctx.config, probe.prefix))

    vacuum = rs.AssetAction(name="vacuum", outcome=rs.Outcome.Unchanged)(_vacuum)

    @rs.Asset(actions=[vacuum])
    def events():
        return 1

    repo = rs.CodeRepository(
        assets=[events],
        resources={"probe": ProbeResource(prefix="from-repo")},
        default_executor=IP,
    )
    assert repo.run_action(
        "vacuum", config={"events": {"retention_hours": 720}}
    ).success
    assert seen == [(VacuumConfig(retention_hours=720), "from-repo")]


def test_action_unresolved_config_type_rejects_override():
    """A config type the module globals cannot see gives no config, and an
    override for it fails the run instead of being dropped."""
    seen = []

    class LocalConfig(BaseModel):
        retention_hours: int = 168

    def _vacuum(ctx: rs.ActionContext[LocalConfig]) -> None:
        seen.append(ctx.config)

    vacuum = rs.AssetAction(name="vacuum", outcome=rs.Outcome.Unchanged)(_vacuum)

    @rs.Asset(actions=[vacuum])
    def events():
        return 1

    repo = rs.CodeRepository(assets=[events], default_executor=IP)
    assert repo.run_action("vacuum").success
    assert seen == [None]

    result = repo.run_action(
        "vacuum",
        config={"events": {"retention_hours": 720}},
        raise_on_error=False,
    )
    assert not result.success
    assert seen == [None], "the body must not run with the override dropped"
    assert result.failed_assets[0][0] == "events"
    message = result.failed_assets[0][1]
    assert "Action 'vacuum' on asset 'events'" in message
    assert "name 'LocalConfig' is not defined" in message


def test_action_without_config_type_ignores_override_under_pep563():
    """Same as without PEP 563: a plain ``ActionContext`` gets no config."""
    seen = []

    def _vacuum(ctx: rs.ActionContext) -> None:
        seen.append(ctx.config)

    vacuum = rs.AssetAction(name="vacuum", outcome=rs.Outcome.Unchanged)(_vacuum)

    @rs.Asset(actions=[vacuum])
    def events():
        return 1

    repo = rs.CodeRepository(assets=[events], default_executor=IP)
    assert repo.run_action(
        "vacuum", config={"events": {"retention_hours": 720}}
    ).success
    assert seen == [None]
