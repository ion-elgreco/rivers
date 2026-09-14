"""Keep one measurement from leaking into the next."""

from __future__ import annotations

import contextlib
import logging
import os
import threading
import time
from typing import Any

from .measure import rss_bytes
from .result import emit

# A workload that needs more than this much resident memory counts as `oom`.
# 8 GiB is well past what any orchestrator should need to hold a graph.
RSS_LIMIT_BYTES = 8 * 1024**3


def quiet_env(extra: dict[str, str] | None = None) -> None:
    """Silence framework logging so it cannot distort the measurement.

    ``extra`` holds the framework's own quieting variables. A driver keeps that
    dict so it can hand the same settings to a cold-start child, which does not
    inherit this call.
    """
    for key, value in (extra or {}).items():
        os.environ.setdefault(key, value)
    logging.disable(logging.INFO)


def kill_children() -> None:
    """Terminate every child process this measurement started."""
    import psutil

    try:
        children = psutil.Process().children(recursive=True)
    except psutil.Error:
        return
    for child in children:
        with contextlib.suppress(psutil.Error):
            child.kill()
    psutil.wait_procs(children, timeout=5)


def start_memory_watchdog(record: dict[str, Any], poll: float = 0.5) -> None:
    """Report `oom` and exit if the workload passes :data:`RSS_LIMIT_BYTES`.

    Waiting for the OS to kill the process gives an inconsistent signal across
    platforms. This makes the limit explicit and the data point clean.
    """

    def watch() -> None:
        while True:
            time.sleep(poll)
            try:
                used = rss_bytes()
            except Exception:  # noqa: BLE001 - the watchdog must never kill a run
                return
            if used > RSS_LIMIT_BYTES:
                record["status"] = "oom"
                record["detail"] = (
                    f"resident memory {used / 1024**3:.1f} GiB over limit"
                )
                record["rss_mb"] = used / 1024**2
                emit(record)
                os._exit(0)

    threading.Thread(target=watch, daemon=True).start()
