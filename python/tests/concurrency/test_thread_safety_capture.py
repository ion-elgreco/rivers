"""Thread-safety of ``rivers._capture.install()``.

Every run calls ``install()``. Runs that start together must wrap
``sys.stdout`` / ``sys.stderr`` once; each extra wrap captures every line of
every later step one more time.
"""

import gc
import json
import sys
import time

from _threads import N_THREADS, run_in_child, run_threads

import rivers._capture as cap


class SlowFalse:
    """The not-yet-installed flag. Its truth test pauses every thread, so all
    of them read the flag before the first one sets it."""

    def __bool__(self):
        gc.collect()
        time.sleep(0.05)
        return False


def layers(stream):
    """Number of capture writers around ``stream``."""
    n = 0
    while isinstance(stream, cap._Writer):
        n += 1
        stream = stream._orig
    return n


def install_from_threads():
    """Child process of ``test_threads_that_install_wrap_each_stream_once``."""
    cap._installed = SlowFalse()

    def install_then_print(i):
        cap.install()
        step = cap.StepCapture()
        step.start()
        print(f"out {i}")
        print(f"err {i}", file=sys.stderr)
        return step.finish()

    captured, errors = run_threads(install_then_print)
    print(
        json.dumps(
            {
                "errors": errors,
                "layers": [layers(sys.stdout), layers(sys.stderr)],
                "captured": captured,
            }
        )
    )


def test_threads_that_install_wrap_each_stream_once(tmp_path):
    """Threads that call ``install()`` together wrap each stream once, and
    each step captures its own lines once. Runs in a child process because
    ``install()`` replaces the streams of the whole process."""
    proc = run_in_child(
        "concurrency.test_thread_safety_capture:install_from_threads", cwd=tmp_path
    )

    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout.splitlines()[-1]) == {
        "errors": [],
        "layers": [1, 1],
        "captured": [[f"out {i}\n", f"err {i}\n"] for i in range(N_THREADS)],
    }


def install_without_stderr():
    """Child process of ``test_install_leaves_a_missing_stream_alone``."""
    sys.stderr = None
    errors = []
    for _ in range(2):
        try:
            cap.install()
        except Exception as e:
            errors.append(f"{type(e).__name__}: {e}")
    print(
        json.dumps(
            {
                "errors": errors,
                "stdout_layers": layers(sys.stdout),
                "stderr": repr(sys.stderr),
                "installed": cap._installed,
            }
        )
    )


def test_install_leaves_a_missing_stream_alone(tmp_path):
    """A process without ``sys.stderr`` (``pythonw``, closed stdio) still runs:
    ``install()`` must not raise, since every run calls it."""
    proc = run_in_child(
        "concurrency.test_thread_safety_capture:install_without_stderr", cwd=tmp_path
    )

    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout.splitlines()[-1]) == {
        "errors": [],
        "stdout_layers": 1,
        "stderr": "None",
        "installed": True,
    }
