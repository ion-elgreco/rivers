"""Launch a fresh interpreter and time how long it needs to become useful."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable

READY_MARKER = "@@READY@@"


def time_cold_start(
    script: str, env: dict[str, str] | None = None, timeout: float = 300.0
) -> dict:
    """Time a fresh interpreter from process launch to a printed READY marker.

    This is the number that matters for Kubernetes: a pod is useless until the
    framework has imported, built its definitions, and can answer questions
    about them. ``script`` must reach that point; the marker is appended here,
    so no driver has to spell it.
    """
    child_env = dict(os.environ)
    child_env.update(env or {})
    # A real code location is a module on disk, and Prefect refuses to deploy a
    # flow defined through `python -c`. Write the script out first.
    fd, path = tempfile.mkstemp(suffix="_coldstart.py")
    with os.fdopen(fd, "w") as handle:
        handle.write(f"{script}\nprint({READY_MARKER!r}, flush=True)\n")
    t0 = time.monotonic()
    proc = subprocess.Popen(
        [sys.executable, path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=child_env,
        text=True,
    )
    ready_s = None
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            if READY_MARKER in line:
                ready_s = time.monotonic() - t0
                break
        if ready_s is None:
            err = proc.stderr.read() if proc.stderr else ""
            raise RuntimeError(f"child exited before signalling ready: {err[-800:]}")
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        proc.kill()
        raise TimeoutError(f"cold start exceeded {timeout}s") from exc
    finally:
        if proc.poll() is None:
            proc.kill()
        os.unlink(path)
    return {"cold_start_ms": ready_s * 1000, "exit_code": proc.returncode}


def cold_start_benchmark(
    script: str, env: dict[str, str] | None = None
) -> Callable[[argparse.Namespace], dict]:
    """Build the cold-start benchmark every driver registers.

    The driver supplies only the script that builds its code location. The size
    reaches that script through ``BENCH_N_ASSETS``.
    """

    def run(args: argparse.Namespace) -> dict:
        return time_cold_start(
            script,
            env={**(env or {}), "BENCH_N_ASSETS": str(args.n)},
            timeout=args.budget,
        )

    return run
