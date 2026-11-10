"""``rivers dev`` and ``rivers serve`` check their ``namespace/name`` at start."""

import os
import subprocess
import sys

import pytest
from _polling import free_port

from rivers._core import DevHost
from rivers._core.storage import Storage

pytestmark = pytest.mark.timeout(180)

MODULE = """
from pathlib import Path

import rivers as rs

Path("imported").touch()


@rs.Asset
def first() -> int:
    return 1


repo = rs.CodeRepository(assets=[first])
"""

# `rivers serve` has no --code-location-id flag.
ID_SETTING = {
    "dev": "--code-location-id / RIVERS_CODE_LOCATION_ID",
    "dev-no-ui": "--code-location-id / RIVERS_CODE_LOCATION_ID",
    "serve": "RIVERS_CODE_LOCATION_ID",
}
RULE = "to a value without '/', whitespace or control characters."

# source: (extra args for dev, env, expected message)
SOURCES = {
    "id-flag-slash": (
        ["--code-location-id", "team/analytics"],
        {},
        f"code location name 'team/analytics' contains '/'. Set {{id_setting}} {RULE}",
    ),
    "id-env-slash": (
        [],
        {"RIVERS_CODE_LOCATION_ID": "team/analytics"},
        f"code location name 'team/analytics' contains '/'. Set {{id_setting}} {RULE}",
    ),
    "id-env-space": (
        [],
        {"RIVERS_CODE_LOCATION_ID": "team analytics"},
        f"code location name 'team analytics' contains ' '. Set {{id_setting}} {RULE}",
    ),
    "name": (
        [],
        {
            "RIVERS_CODE_LOCATION_ID": "uuid-1",
            "RIVERS_CODE_LOCATION_NAME": "team/warehouse",
        },
        "code location name 'team/warehouse' contains '/'. "
        f"Set RIVERS_CODE_LOCATION_NAME {RULE}",
    ),
    "namespace": (
        [],
        {
            "RIVERS_CODE_LOCATION_ID": "uuid-1",
            "RIVERS_CODE_LOCATION_NAMESPACE": "team data",
        },
        "code location namespace 'team data' contains ' '. "
        f"Set RIVERS_CODE_LOCATION_NAMESPACE {RULE}",
    ),
}

CASES = [
    pytest.param(command, source, id=f"{command}-{source}")
    for command in ID_SETTING
    for source in SOURCES
    if not (command == "serve" and source.startswith("id-flag"))
]


@pytest.mark.parametrize(("command", "source"), CASES)
def test_invalid_namespace_or_name_stops_the_command_at_start(
    resolved_tmp_path, command, source
):
    (resolved_tmp_path / "serving_mod.py").write_text(MODULE)
    dev_args, source_env, message = SOURCES[source]
    message = message.format(id_setting=ID_SETTING[command])
    env = {**os.environ, **source_env}

    host = None
    if command == "serve":
        # A real store, so a check that runs late fails late, not at connect.
        host = DevHost(str(resolved_tmp_path / "serve-storage"))
        Storage.connect(host.endpoint)
        args = [
            "serve",
            "serving_mod",
            "--host",
            "127.0.0.1",
            "--grpc-port",
            str(free_port()),
            "--surreal-endpoint",
            host.endpoint,
            "--no-daemon",
        ]
    else:
        args = [
            "dev",
            "serving_mod",
            "--host",
            "127.0.0.1",
            "--port",
            str(free_port()),
            "--grpc-port",
            str(free_port()),
            "--storage-path",
            str(resolved_tmp_path / "storage"),
            "--no-daemon",
            *dev_args,
        ]
        if command == "dev-no-ui":
            args.append("--no-ui")

    try:
        result = subprocess.run(
            [sys.executable, "-c", "from rivers.cli import main; main()", *args],
            cwd=resolved_tmp_path,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
    finally:
        if host is not None:
            host.stop()

    assert (result.returncode, result.stderr.strip()) == (1, f"Error: {message}"), (
        result.stdout
    )
    assert "Storage:" not in result.stdout
    assert not (resolved_tmp_path / "imported").exists()
