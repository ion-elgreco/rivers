from __future__ import annotations

import os
import subprocess
import sys


def test_import_with_otlp_endpoint() -> None:
    env = os.environ.copy()
    env["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://127.0.0.1:4317"

    subprocess.run(
        [sys.executable, "-c", "import rivers"],
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
