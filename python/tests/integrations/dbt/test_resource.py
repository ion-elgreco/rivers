"""DbtResource.cli outside dbt assets: commands, failures, and settings."""

import json

import pytest

pytest.importorskip("dbt.version")
duckdb = pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")

from rivers.integrations.dbt import DbtError, DbtResource  # noqa: E402

from ._shop import rows  # noqa: E402


def _dbt(shop, **kwargs) -> DbtResource:
    return DbtResource(project_dir=shop, profiles_dir=shop, **kwargs)


def test_a_command_runs_without_assets(shop):
    _dbt(shop).cli(["seed"]).wait()

    assert rows(shop, "SELECT id, name FROM raw_customers ORDER BY id") == [
        (1, "Ann"),
        (2, "Bob"),
        (3, "Cid"),
    ]


def test_a_failed_command_raises_with_dbt_errors(shop):
    invocation = _dbt(shop).cli(["run-operation", "no_such_macro"])

    with pytest.raises(DbtError) as raised:
        invocation.wait()

    command, errors = str(raised.value).split("\n", 1)
    assert command.startswith("dbt run-operation no_such_macro failed with exit code ")
    assert "no_such_macro" in errors


def test_an_invocation_runs_once(shop):
    invocation = _dbt(shop).cli(["seed"])
    invocation.wait()

    with pytest.raises(RuntimeError, match="runs once"):
        invocation.wait()


def test_target_path_keeps_the_artifacts(shop, tmp_path):
    artifacts = tmp_path / "artifacts"

    _dbt(shop).cli(["seed"], target_path=artifacts).wait()

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
