"""Verify .pyi stub types don't regress by running pyright and checking reveal_type output."""

from pathlib import Path

from pyright import run as pyright_run

# Pyright runs against this directory as its own project. Without that, it
# resolves config from the *caller's* cwd: from the repo root it finds none and
# works, but from `python/` it finds `pyproject.toml`'s `exclude = ["**/tests"]`,
# skips the fixture, and emits no reveal_type output at all.
PROJECT_DIR = str(Path(__file__).parent)

EXPECTED_TYPES = {
    "ext": "ExternalAsset",
    "ext2": "ExternalAsset",
    "g": "GraphAsset",
    "g2": "GraphAsset",
    "m": "MultiAsset",
    "m2": "MultiAsset",
    "bare": "SingleAsset",
    "named": "SingleAsset",
    "sched_decorated": "Schedule",
    "sched_plain": "Schedule",
    "sens_decorated": "Sensor",
    "sens_plain": "Sensor",
    "load_any": "Any",
    "load_typed": "int",
    "load_typed_str": "str",
    "storage": "Storage",
    "storage_kind": "StorageType",
}


def _parse_reveal_types(output: str) -> dict[str, str]:
    """Parse pyright output lines like: '... Type of "ext" is "ExternalAsset"'."""
    result = {}
    for line in output.splitlines():
        if 'Type of "' not in line:
            continue
        parts = line.split('Type of "', 1)[1]
        var_name, rest = parts.split('" is "', 1)
        type_str = rest.rstrip('"')
        result[var_name] = type_str
    return result


def test_stub_types():
    result = pyright_run(
        "--project", PROJECT_DIR, capture_output=True, text=True, timeout=120
    )

    revealed = _parse_reveal_types(result.stdout)  # type: ignore

    assert result.returncode == 0, f"pyright reported errors:\n{result.stdout}"

    # A skipped fixture yields no diagnostics at all, which would otherwise read
    # as "every type is None" — a confusing failure that hides the real cause.
    assert revealed, (
        "pyright checked nothing. The fixture was excluded from analysis; "
        "check tests/core/pyrightconfig.json still exists.\n"
        f"{result.stdout}"
    )

    for var_name, expected_type in EXPECTED_TYPES.items():
        actual = revealed.get(var_name)
        assert actual == expected_type, (
            f"reveal_type({var_name}): expected {expected_type!r}, got {actual!r}"
        )
