"""The result protocol every driver speaks.

A driver runs inside its framework's own virtualenv and prints whatever that
framework logs. It also prints exactly one `@@RESULT@@` line. The sweep reads
that line and ignores the rest, so framework logging can never corrupt a
measurement.
"""

from __future__ import annotations

import json
import sys
from typing import Any

RESULT_MARKER = "@@RESULT@@"


def emit(payload: dict[str, Any]) -> None:
    """Print the single machine-readable result line."""
    sys.stdout.flush()
    sys.stderr.flush()
    print(f"{RESULT_MARKER}{json.dumps(payload)}", flush=True)


def parse_result(stdout: str) -> dict[str, Any] | None:
    """Pull the result object back out of a driver's stdout."""
    for line in stdout.splitlines():
        if line.startswith(RESULT_MARKER):
            return json.loads(line[len(RESULT_MARKER) :])
    return None
