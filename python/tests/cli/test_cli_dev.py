"""``rivers dev`` CLI tests up to the server-boot seam.

The happy path is covered by stubbing ``_serve_dev`` — everything before it
(flat CLI args → nested ``RiversConfig``, cwd module import, repo resolve
against storage) runs for real.
"""

from typer.testing import CliRunner

import rivers.cli
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


def test_dev_reaches_server_seam_with_flags_applied(resolved_tmp_path, monkeypatch):
    """A valid cwd-relative module gets through config parsing, import, and
    resolve, and the CLI flags arrive in the config handed to the servers."""
    (resolved_tmp_path / "dev_seam_mod.py").write_text(REPO_MODULE)
    # dev() exports these; pre-set via monkeypatch so teardown restores them.
    monkeypatch.setenv("RIVERS_DEPLOYMENT", "")
    monkeypatch.setenv("RIVERS_MODULE", "")

    served = {}
    monkeypatch.setattr(
        rivers.cli,
        "_serve_dev",
        lambda cfg, repo_obj, storage: served.update(cfg=cfg, repo=repo_obj),
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

    cfg = served["cfg"]
    assert cfg.module.path == "dev_seam_mod"
    assert cfg.module.repo_var == "repo"
    assert cfg.server.host == "127.0.0.1"
    assert cfg.server.port == 3210
    assert cfg.server.grpc_port == 3211
    assert cfg.storage.path == str(resolved_tmp_path / "storage")
    assert served["repo"] is not None
