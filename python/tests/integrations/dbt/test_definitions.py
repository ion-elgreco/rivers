"""DbtProject: which dbt resources become outputs, with which settings and deps."""

import importlib
import json
import shutil
import sys

import pytest

pytest.importorskip("dbt.version")
pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")

import rivers as rs  # noqa: E402
from rivers.exceptions import GraphValidationError  # noqa: E402
from rivers.integrations.dbt import (  # noqa: E402
    DbtError,
    DbtProject,
    DbtResource,
    DbtTranslator,
)

from ._shop import (  # noqa: E402
    SHOP,
    SHOP_OUTPUTS,
    daily_assets,
    payments_asset,
    shop_assets,
    write_profile,
)

STAGING = {"stg_customers", "stg_orders", "stg_payments"}


def _define(project: DbtProject, **kwargs) -> rs.MultiAsset:
    @project.assets(**kwargs)
    def shop(context: rs.AssetExecutionContext, dbt: DbtResource):
        yield from dbt.cli(["build"], context)

    return shop


def _outputs(asset: rs.MultiAsset) -> dict[str, rs.AssetDef]:
    return {output.name: output for output in asset.output_defs}


def test_outputs_are_the_models_seeds_and_snapshots(shop):
    outputs = _outputs(_define(DbtProject(shop, profiles_dir=shop)))

    # The ephemeral order_totals builds no table, and tests are not assets.
    assert set(outputs) == SHOP_OUTPUTS | {"daily_orders"}

    manifest = json.loads((shop / "target" / "manifest.json").read_text())
    customers = outputs["customers"]
    assert customers.metadata == {
        "dbt/unique_id": "model.shop.customers",
        "dbt/relation": '"warehouse"."main"."customers"',
        "dbt/materialized": "table",
        "dbt/description": "Customers and what they paid.",
        "duckdb/schema": "main",
        "duckdb/table": "customers",
    }
    assert (
        customers.code_version
        == (manifest["nodes"]["model.shop.customers"]["checksum"]["checksum"])
    )
    assert {name: output.kinds for name, output in outputs.items()} == {
        name: ["dbt"] for name in outputs
    }
    assert customers.group == "marts"
    assert outputs["stg_orders"].tags == ["staging"]
    assert outputs["customers_snapshot"].metadata["duckdb/schema"] == "snapshots"
    assert outputs["raw_orders"].metadata["dbt/materialized"] == "seed"


def test_definition_settings_fill_in_what_dbt_leaves_unset(shop):
    asset = _define(
        DbtProject(shop, profiles_dir=shop),
        name="warehouse",
        group="analytics",
        tags=["dbt"],
    )
    outputs = _outputs(asset)

    assert asset.name == "warehouse"
    assert outputs["customers"].group == "marts"
    assert outputs["raw_orders"].group == "analytics"
    assert outputs["stg_orders"].tags == ["staging"]
    assert outputs["raw_orders"].tags == ["dbt"]


def test_a_translator_renames_assets(shop):
    class Prefixed(DbtTranslator):
        def asset_name(self, node):
            return f"shop/{node['name']}"

        def group(self, node):
            return node["fqn"][1]

    outputs = _outputs(
        _define(DbtProject(shop, profiles_dir=shop), translator=Prefixed())
    )

    assert "shop/customers" in outputs
    assert outputs["shop/customers"].group == "marts"
    assert outputs["shop/stg_orders"].group == "staging"


@pytest.mark.parametrize(
    ("selection", "selected"),
    [
        ({"select": "tag:staging"}, STAGING),
        ({"selector": "staging"}, STAGING),
        (
            {"select": "+customers", "exclude": "resource_type:seed"},
            STAGING | {"customers"},
        ),
    ],
)
def test_select_exclude_and_selector_follow_dbt(shop, selection, selected):
    outputs = _outputs(_define(DbtProject(shop, profiles_dir=shop), **selection))

    assert set(outputs) == selected


def test_a_selection_is_listed_once_per_manifest(shop, monkeypatch):
    project = DbtProject(shop, profiles_dir=shop)
    cached = set((shop / "target").glob("rivers-selection-*.json"))

    _define(project, select="tag:staging")

    [cache] = set((shop / "target").glob("rivers-selection-*.json")) - cached
    assert {
        "model.shop.stg_customers",
        "model.shop.stg_orders",
        "model.shop.stg_payments",
    } <= set(json.loads(cache.read_text()))

    def no_dbt(*args, **kwargs):
        raise AssertionError("dbt ls ran again")

    monkeypatch.setattr(DbtResource, "_list", no_dbt)
    assert set(_outputs(_define(project, select="tag:staging"))) == STAGING


def test_a_selection_without_outputs_raises(shop):
    with pytest.raises(ValueError, match="matches no model, seed, or snapshot"):
        _define(DbtProject(shop, profiles_dir=shop), select="tag:missing")


def test_an_invalid_selection_raises_dbt_error(shop):
    with pytest.raises(DbtError, match="dbt ls"):
        _define(DbtProject(shop, profiles_dir=shop), select="nosuchmethod:x")


def test_selector_and_select_are_exclusive(shop):
    with pytest.raises(ValueError, match="pass selector, or select and exclude"):
        _define(
            DbtProject(shop, profiles_dir=shop),
            select="tag:staging",
            selector="staging",
        )


def _validate(shop, *assets) -> None:
    resources = {"dbt": DbtResource(project_dir=shop, profiles_dir=shop)}
    rs.CodeRepository(assets=list(assets), resources=resources).validate()


def test_sources_add_no_dep_unless_mapped(shop):
    _validate(shop, shop_assets(shop, translator=DbtTranslator()))

    mapped = shop_assets(shop)
    with pytest.raises(GraphValidationError, match="unresolved input 'payments'"):
        _validate(shop, mapped)
    _validate(shop, payments_asset(shop), mapped)


def test_a_dep_outside_the_selection_needs_its_asset(shop):
    # daily_orders refs stg_orders, which only the shop definition builds.
    daily = daily_assets(shop)
    with pytest.raises(GraphValidationError, match="unresolved input 'stg_orders'"):
        _validate(shop, daily)

    _validate(shop, payments_asset(shop), shop_assets(shop), daily)


def test_a_missing_manifest_names_the_fix(tmp_path):
    shutil.copytree(SHOP, tmp_path / "shop")

    with pytest.raises(FileNotFoundError, match=r"run `dbt parse`.*DbtProject.prepare"):
        _define(DbtProject(tmp_path / "shop"))


def test_a_directory_without_dbt_project_yml_raises(tmp_path):
    with pytest.raises(ValueError, match="no dbt_project.yml"):
        DbtProject(tmp_path)


def test_the_body_must_be_a_generator(shop):
    with pytest.raises(TypeError, match="must be a generator"):

        @DbtProject(shop, profiles_dir=shop).assets()
        def shop_assets(dbt: DbtResource):
            dbt.cli(["build"]).wait()


def test_prepare_writes_the_manifest(tmp_path):
    root = tmp_path / "shop"
    shutil.copytree(SHOP, root)
    write_profile(root)
    project = DbtProject(root, profiles_dir=root)

    project.prepare()

    manifest = json.loads(project.manifest_path.read_text())
    assert manifest["metadata"]["project_name"] == "shop"
    assert "model.shop.customers" in manifest["nodes"]
    assert sorted(path.name for path in (root / "target").iterdir()) == [
        "manifest.json",
        "partial_parse.msgpack",
    ]


def test_a_second_prepare_reuses_the_parse_in_the_target_path(shop, caplog):
    shutil.rmtree(shop / "target")
    project = DbtProject(shop, target_path="build", profiles_dir=shop)
    project.prepare()
    caplog.clear()

    with caplog.at_level("INFO"):
        project.prepare()

    messages = [record.getMessage() for record in caplog.records]
    assert any(m.startswith("Running with dbt=") for m in messages)
    assert [m for m in messages if m.startswith("Unable to do partial parsing")] == []


def test_missing_dbt_core_names_the_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "dbt.version", None)
    for name in [
        name for name in sys.modules if name.startswith("rivers.integrations.dbt")
    ]:
        monkeypatch.delitem(sys.modules, name)

    with pytest.raises(ImportError, match=r"pip install rivers\[dbt\]"):
        importlib.import_module("rivers.integrations.dbt")
