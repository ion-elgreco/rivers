"""What produced a number: the hardware, the interpreter, the versions.

Recorded with every result so a published figure can always be traced back to
the machine that produced it.
"""

from __future__ import annotations

import os
from typing import Any


def environment() -> dict[str, Any]:
    """The machine and interpreter this measurement ran on."""
    import platform

    import psutil

    return {
        "os": f"{platform.system()} {platform.release()}",
        "arch": platform.machine(),
        "cpu_count": os.cpu_count(),
        "memory_gb": round(psutil.virtual_memory().total / 1024**3, 1),
        "python": platform.python_version(),
    }


def package_version(package: str) -> str:
    """Installed version of ``package``, or "unknown"."""
    import importlib.metadata as metadata

    try:
        return metadata.version(package)
    except Exception:  # noqa: BLE001 - version reporting must never fail a run
        return "unknown"
