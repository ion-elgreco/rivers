"""Verify .pyi stub types don't regress by running pyright and checking reveal_type output."""

from pathlib import Path

from pyright import run as pyright_run

STUBS_FILE = str(Path(__file__).parent / "stubs.py")
BAD_CLASS_FORM_FILE = str(Path(__file__).parent / "stubs_class_form_bad.py")
# Run pyright from the repo root: python/pyproject.toml excludes **/tests from
# package-wide checks, which would silently skip these sample files.
REPO_ROOT = Path(__file__).resolve().parents[3]

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
    "class_repo": "CodeRepository",
    "ctx.config": "TuneConfig | None",
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
        STUBS_FILE, capture_output=True, text=True, timeout=60, cwd=REPO_ROOT
    )

    revealed = _parse_reveal_types(result.stdout)  # type: ignore

    assert result.returncode == 0, f"pyright reported errors:\n{result.stdout}"

    for var_name, expected_type in EXPECTED_TYPES.items():
        actual = revealed.get(var_name)
        assert actual == expected_type, (
            f"reveal_type({var_name}): expected {expected_type!r}, got {actual!r}"
        )


def test_class_form_bad_values_flagged():
    """Wrong-typed declarable attributes must be errors — proves they aren't Any."""
    expected = {
        lineno
        for lineno, line in enumerate(
            Path(BAD_CLASS_FORM_FILE).read_text().splitlines(), start=1
        )
        if "# EXPECT-ERROR" in line
    }
    assert expected, "marker scan found no # EXPECT-ERROR lines"

    result = pyright_run(
        BAD_CLASS_FORM_FILE, capture_output=True, text=True, timeout=60, cwd=REPO_ROOT
    )
    assert result.returncode != 0, "pyright reported no errors on the bad sample"

    flagged = set()
    for line in result.stdout.splitlines():  # "  /path/file.py:11:13 - error: ..."
        location, sep, _ = line.partition(" - error:")
        if not sep:
            continue
        parts = location.strip().rsplit(":", 2)
        if len(parts) == 3 and parts[0].endswith("stubs_class_form_bad.py"):
            flagged.add(int(parts[1]))

    assert expected <= flagged, (
        f"expected errors on lines {sorted(expected)}, pyright flagged "
        f"{sorted(flagged)}:\n{result.stdout}"
    )
