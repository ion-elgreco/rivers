"""DbtResource.cli outside dbt assets: commands, failures, and settings."""

import asyncio
import json
import os
import sys
from collections.abc import Callable

import pytest

pytest.importorskip("dbt.version")
duckdb = pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")

from rivers.integrations.dbt import (  # noqa: E402
    DbtError,
    DbtInvocation,
    DbtNode,
    DbtProject,
    DbtResource,
)
from ._shop import rows  # noqa: E402


def _dbt(shop, **kwargs) -> DbtResource:
    return DbtResource(project_dir=shop, profiles_dir=shop, **kwargs)


@pytest.fixture(params=["sync", "async"])
def wait(request) -> Callable[[DbtInvocation], None]:
    """Runs a command to the end with ``wait()``, or with ``async_wait()``."""
    if request.param == "sync":
        return DbtInvocation.wait
    return lambda invocation: asyncio.run(invocation.async_wait())


def test_a_command_runs_without_assets(shop, wait):
    wait(_dbt(shop).cli(["seed"]))

    assert rows(shop, "SELECT id, name FROM raw_customers ORDER BY id") == [
        (1, "Ann"),
        (2, "Bob"),
        (3, "Cid"),
    ]


def test_a_failed_command_raises_with_dbt_errors(shop, wait):
    invocation = _dbt(shop).cli(["run-operation", "no_such_macro"])

    with pytest.raises(DbtError) as raised:
        wait(invocation)

    command, errors = str(raised.value).split("\n", 1)
    assert command.startswith("dbt run-operation no_such_macro failed with exit code ")
    assert "no_such_macro" in errors


def test_an_invocation_runs_once(shop, wait):
    invocation = _dbt(shop).cli(["seed"])
    wait(invocation)

    with pytest.raises(RuntimeError, match="runs once"):
        wait(invocation)


@pytest.mark.skipif(os.name == "nt", reason="dbt gets no SIGINT on Windows")
@pytest.mark.parametrize("exits", [True, False], ids=["exits", "keeps running"])
def test_cancelling_an_async_run_stops_dbt(tmp_path, monkeypatch, exits):
    monkeypatch.setattr("rivers.integrations.dbt.resource._STOP_TIMEOUT_S", 0.5)
    started = tmp_path / "started"
    interrupted = tmp_path / "interrupted"
    executable = tmp_path / "dbt"
    executable.write_text(
        f"#!{sys.executable}\n"
        "import os, pathlib, signal, sys, time\n"
        "def interrupt(signum, frame):\n"
        f"    pathlib.Path({str(interrupted)!r}).touch()\n"
        f"    if {exits}:\n"
        "        sys.exit(130)\n"
        "signal.signal(signal.SIGINT, interrupt)\n"
        f"pathlib.Path({str(started)!r}).write_text(str(os.getpid()))\n"
        "time.sleep(30)\n"
    )
    executable.chmod(0o755)
    dbt = DbtResource(project_dir=tmp_path, executable=str(executable))
    invocation = dbt.cli(["build"])

    async def build() -> list:
        return [materialization async for materialization in invocation]

    async def cancel_once_started() -> None:
        task = asyncio.create_task(build())
        while not started.exists() and not task.done():
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(cancel_once_started())

    assert interrupted.exists()
    with pytest.raises(ProcessLookupError):
        os.kill(int(started.read_text()), 0)


def test_target_path_keeps_the_artifacts(shop, tmp_path, wait):
    artifacts = tmp_path / "artifacts"

    wait(_dbt(shop).cli(["seed"], target_path=artifacts))

    results = json.loads((artifacts / "run_results.json").read_text())["results"]
    assert sorted(result["unique_id"] for result in results) == [
        "seed.shop.raw_customers",
        "seed.shop.raw_orders",
    ]
    assert not (shop / "target" / "run_results.json").exists()


def test_target_picks_the_profile_output(shop, tmp_path):
    other = (tmp_path / "other.duckdb").as_posix()
    with (shop / "profiles.yml").open("a") as profile:
        profile.write(
            f"    other:\n      type: duckdb\n      path: '{other}'\n      threads: 1\n"
        )

    _dbt(shop, target="other").cli(["seed"]).wait()

    with duckdb.connect(other, read_only=True) as con:
        assert con.sql("SELECT count(*) FROM raw_orders").fetchall() == [(3,)]
    assert not (tmp_path / "warehouse.duckdb").exists()


def test_settings_come_from_dbt_environment_variables(shop, monkeypatch):
    monkeypatch.setenv("DBT_PROJECT_DIR", str(shop))
    monkeypatch.setenv("DBT_TARGET", "prod")

    resource = DbtResource()

    assert resource.project_dir == shop
    assert resource.target == "prod"


def test_a_project_gives_the_resource_its_settings(shop, tmp_path):
    project = DbtProject(
        shop,
        profiles_dir=tmp_path,
        profile="shop",
        target="prod",
        asset_def_fn=DbtNode.output_def,
        source_asset_fn=lambda source: source.name,
    )

    resource = DbtResource(project_dir=project)

    assert json.loads(resource.model_dump_json()) == {
        "project_dir": str(shop),
        "profiles_dir": str(tmp_path),
        "profile": "shop",
        "target": "prod",
        "executable": None,
    }


@pytest.mark.parametrize(
    ("given", "in_project", "target"),
    [
        ({"target": "given"}, "project", "given"),
        ({}, "project", "project"),
        ({}, None, "env"),
    ],
    ids=["given", "project", "env"],
)
def test_target_comes_from_the_resource_then_the_project_then_the_environment(
    shop, monkeypatch, given, in_project, target
):
    monkeypatch.setenv("DBT_TARGET", "env")

    resource = DbtResource(project_dir=DbtProject(shop, target=in_project), **given)

    assert resource.target == target
