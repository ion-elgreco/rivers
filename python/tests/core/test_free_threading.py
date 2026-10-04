"""On free-threaded Python, importing rivers keeps the GIL disabled."""

import subprocess
import sys
import sysconfig

import pytest

pytestmark = pytest.mark.skipif(
    not sysconfig.get_config_var("Py_GIL_DISABLED"),
    reason="needs free-threaded Python",
)


def test_import_keeps_the_gil_disabled(tmp_path):
    """A fresh interpreter, so modules imported by other tests don't count.
    Python warns with a RuntimeWarning when an import turns the GIL back on."""
    code = (
        "import sys, rivers, rivers.cli, rivers.io_handlers, rivers.testing; "
        "print(sys._is_gil_enabled())"
    )
    proc = subprocess.run(
        [sys.executable, "-W", "error::RuntimeWarning", "-c", code],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert (proc.returncode, proc.stdout.strip()) == (0, "False"), proc.stderr
