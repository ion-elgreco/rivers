"""Per-step stdout/stderr capture for streaming user logs back to the executor.

The Rust executor wraps each step in a :class:`StepCapture` to collect everything
the user code writes to ``sys.stdout`` and ``sys.stderr``. When ``tee`` is enabled
the writes also propagate to the original streams so the developer still sees
them locally; when disabled (e.g. in K8s step pods) the parent streams are
silenced and only the buffered text is shipped back through the run event log.
"""

import contextvars
import sys

_capture = contextvars.ContextVar("_step_capture", default=None)
_get = _capture.get
_MAX = 4 * 1024 * 1024


class _Writer:
    """File-like proxy that mirrors writes into the active StepCapture buffer."""

    __slots__ = ("_orig", "_idx", "_w", "_tee")

    def __init__(self, orig, idx, tee):
        """Wrap the original stream.

        Args:
            orig: The underlying ``sys.stdout`` or ``sys.stderr`` object.
            idx: Buffer index — ``0`` for stdout, ``1`` for stderr.
            tee: When ``True`` writes also propagate to ``orig``.
        """
        self._orig = orig
        self._idx = idx
        self._w = orig.write
        self._tee = tee

    def write(self, s):
        """Write ``s``; append to the active buffer when one is set."""
        bufs = _get()
        if bufs is not None:
            bufs[self._idx].append(s)
        return self._w(s) if self._tee else len(s)

    def writelines(self, lines):
        """Write each element of ``lines``; append to the active buffer when one is set."""
        bufs = _get()
        if bufs is not None:
            bufs[self._idx].extend(lines)
        if self._tee:
            self._orig.writelines(lines)

    def flush(self):
        """Flush the underlying stream when ``tee`` is enabled."""
        if self._tee:
            self._orig.flush()

    def isatty(self):
        """Always report ``False`` — captured output is never a TTY."""
        return False

    def __getattr__(self, name):
        """Delegate any uncaptured attribute access to the original stream."""
        return getattr(self._orig, name)


class StepCapture:
    """Per-step capture context activated by the executor around user code."""

    __slots__ = ("_bufs", "_token", "_done")

    def start(self):
        """Begin capturing stdout/stderr for the current step."""
        self._bufs = [[], []]
        self._token = _capture.set(self._bufs)  # type: ignore
        self._done = False

    def finish(self):
        """Stop capturing and return ``(stdout, stderr)``.

        Returns:
            A two-tuple of strings. Each string is truncated from the front to
            ``_MAX`` bytes so a runaway logger cannot exhaust memory.
        """
        if self._done:
            return ("", "")
        self._done = True
        _capture.reset(self._token)
        out = self._join(0)
        err = self._join(1)
        return (out, err)

    def _join(self, idx):
        """Concatenate buffer ``idx`` and clear it, truncating to ``_MAX`` bytes."""
        r = "".join(self._bufs[idx])
        self._bufs[idx].clear()
        return r[-_MAX:] if len(r) > _MAX else r


_installed = False


def install(*, tee=True):
    """Install the capturing writers as ``sys.stdout`` / ``sys.stderr``.

    Idempotent: subsequent calls are no-ops.

    Args:
        tee: When ``True`` writes also pass through to the real streams so the
            developer continues to see output locally. Set to ``False`` for
            background workers where the original streams should be silenced.
    """
    global _installed
    if _installed:
        return
    _installed = True
    sys.stdout = _Writer(sys.stdout, 0, tee)
    sys.stderr = _Writer(sys.stderr, 1, tee)
