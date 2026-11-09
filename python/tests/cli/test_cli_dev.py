"""``rivers dev`` CLI tests up to the host seam.

The happy path stubs ``_serve_dev``: everything before it (flat CLI args →
nested ``RiversConfig``, the cwd module lookup) runs for real.
"""

import os

import pytest
from typer.testing import CliRunner

import rivers.cli.server
from rivers.cli import app

runner = CliRunner()

REPO_MODULE = """
import rivers as rs


@rs.Asset(name="tiny")
def tiny() -> int:
    return 1


repo = rs.CodeRepository(assets=[tiny])
"""


def test_dev_missing_module_reports_name(resolved_tmp_path):
    """A nonexistent module fails with the module name in the message."""
    result = runner.invoke(
        app,
        [
            "dev",
            "nonexistent_module_xyz",
            "--storage-path",
            str(resolved_tmp_path / "storage"),
        ],
    )
    assert result.exit_code == 1
    assert "module 'nonexistent_module_xyz' not found" in result.output


def test_dev_without_module_reports_missing_config(resolved_tmp_path):
    """No module arg and no config errors out before touching storage."""
    result = runner.invoke(app, ["dev"])
    assert result.exit_code == 1
    assert "no module configured" in result.output


def test_dev_reaches_host_seam_with_flags_applied(resolved_tmp_path, monkeypatch):
    """A valid cwd-relative module gets through config parsing and the module
    lookup, and the CLI flags arrive in the config handed to the host."""
    (resolved_tmp_path / "dev_seam_mod.py").write_text(REPO_MODULE)
    # dev() exports these; pre-set via monkeypatch so teardown restores them.
    monkeypatch.setenv("RIVERS_DEPLOYMENT", "")
    monkeypatch.setenv("RIVERS_MODULE", "")

    served = {}
    monkeypatch.setattr(
        rivers.cli.server,
        "_serve_dev",
        lambda cfg, module, ui: served.update(cfg=cfg, module=module, ui=ui),
    )

    result = runner.invoke(
        app,
        [
            "dev",
            "dev_seam_mod",
            "--host",
            "127.0.0.1",
            "--port",
            "3210",
            "--grpc-port",
            "3211",
            "--storage-path",
            str(resolved_tmp_path / "storage"),
        ],
    )
    assert result.exit_code == 0, result.output

    assert served["module"] == "dev_seam_mod"
    cfg = served["cfg"]
    assert cfg.module.path == "dev_seam_mod"
    assert cfg.module.repo_var == "repo"
    assert cfg.server.host == "127.0.0.1"
    assert cfg.server.port == 3210
    assert cfg.server.grpc_port == 3211
    assert cfg.storage.path == str(resolved_tmp_path / "storage")
    assert served["ui"] is True


def test_dev_takes_a_code_location_id_and_no_ui(resolved_tmp_path, monkeypatch):
    """A second ``rivers dev`` beside the first: its own identity, no UI."""
    (resolved_tmp_path / "dev_seam_mod.py").write_text(REPO_MODULE)
    for name in ("RIVERS_DEPLOYMENT", "RIVERS_MODULE", "RIVERS_CODE_LOCATION_ID"):
        monkeypatch.setenv(name, "")
    served = {}
    monkeypatch.setattr(
        rivers.cli.server,
        "_serve_dev",
        lambda cfg, module, ui: served.update(ui=ui),
    )

    result = runner.invoke(
        app,
        ["dev", "dev_seam_mod", "--code-location-id", "analytics", "--no-ui"],
    )

    assert result.exit_code == 0, result.output
    assert served["ui"] is False
    assert os.environ["RIVERS_CODE_LOCATION_ID"] == "analytics"


@pytest.mark.parametrize("via", ["flag", "env"])
def test_dev_reads_config_from_explicit_path(resolved_tmp_path, monkeypatch, via):
    """``--config`` and ``RIVERS_CONFIG`` both load a rivers.toml outside the walk-up."""
    config = resolved_tmp_path / "elsewhere" / "custom.toml"
    config.parent.mkdir()
    config.write_text('[module]\npath = "from_explicit_config"\n')
    (resolved_tmp_path / "rivers.toml").write_text('[module]\npath = "discovered"\n')

    args = ["dev"]
    if via == "flag":
        args += ["--config", str(config)]
    else:
        monkeypatch.setenv("RIVERS_CONFIG", str(config))

    result = runner.invoke(app, args)
    assert result.exit_code == 1
    assert "module 'from_explicit_config' not found" in result.output


def test_dev_missing_config_file_fails(resolved_tmp_path):
    result = runner.invoke(app, ["dev", "--config", "missing.toml"])
    assert isinstance(result.exception, FileNotFoundError)
