"""Step stdout capture keeps everything a step writes.

`rivers._capture.StepCapture` used to keep only the last 4 MiB and discard the
rest, silently. A benchmark step printing 100,000 lines stored 71,090 of them
and the run still reported `Success`. Three things were wrong with that cap and
each test below now pins the opposite:

1. Nothing is dropped, so a log never starts mid-stream with no explanation.
2. Non-ASCII output survives. The old slice bounded characters, not the bytes
   its docstring promised, so the budget it enforced was never the stated one.
3. What the step wrote is what the buffer holds. The old cap was applied only
   in `finish`, so it never bounded memory during the step anyway.

The three unit tests drive `_Writer` directly rather than going through
`print`. pytest reinstalls its own `sys.stdout` between fixture setup and the
test body, so a writer installed in a fixture never sees the test's output.
Each test asserts its capture was non-empty, because a capture that silently
does nothing would otherwise satisfy these assertions.
"""

import io

import rivers as rs
from rivers._capture import StepCapture, _Writer

# One line of plain ASCII, sized so byte count and character count agree.
LINE_BYTES = 64
LINE = "x" * (LINE_BYTES - 1) + "\n"
# Comfortably past the 4 MiB the old cap enforced.
OVER_OLD_CAP = 5 * 1024 * 1024


def _stdout_writer() -> _Writer:
    """A capturing stdout proxy over a throwaway stream, index 0, no tee."""
    return _Writer(io.StringIO(), 0, False)


def test_capture_keeps_output_past_the_old_cap():
    """Every line written comes back, head included."""
    writer = _stdout_writer()
    step = StepCapture()
    step.start()
    lines = OVER_OLD_CAP // LINE_BYTES
    for _ in range(lines):
        writer.write(LINE)
    out, _ = step.finish()

    assert len(out) == lines * LINE_BYTES
    assert out.count("\n") == lines
    # The head is what the old cap threw away first.
    assert out.startswith(LINE)
    assert out.endswith(LINE)


def test_capture_keeps_non_ascii_output_whole():
    """Multi-byte text survives unchanged.

    The old cap sliced a `str`, so it bounded characters while its docstring
    promised bytes. Non-ASCII output was both truncated and over budget.
    """
    writer = _stdout_writer()
    step = StepCapture()
    step.start()
    # U+65E5 is three bytes in UTF-8, so this is well past 4 MiB either way.
    chunk = "日" * 4096
    chunks = (2 * OVER_OLD_CAP) // 4096
    for _ in range(chunks):
        writer.write(chunk)
    out, _ = step.finish()

    assert len(out) == chunks * 4096
    assert len(out.encode("utf-8")) == chunks * 4096 * 3
    assert set(out) == {"日"}


def test_capture_buffer_holds_exactly_what_was_written():
    """The buffer is not trimmed behind the step's back.

    Reaching into `_bufs` is deliberate: this pins that nothing is discarded on
    the way in, which is where the old cap could not act anyway.
    """
    writer = _stdout_writer()
    step = StepCapture()
    step.start()
    lines = (4 * OVER_OLD_CAP) // LINE_BYTES
    for _ in range(lines):
        writer.write(LINE)
    held = sum(len(chunk) for chunk in step._bufs[0])
    out, _ = step.finish()

    assert held == lines * LINE_BYTES
    assert len(out) == held


def test_full_stdout_reaches_storage(monkeypatch, storage):
    """End to end: a step that writes past the old cap stores every line.

    This is the path the benchmark took. It stored 71,090 of 100,000 lines and
    still reported success.
    """
    import sys

    import rivers._capture as cap

    # `install` is idempotent through a module flag, and it wraps whatever
    # `sys.stdout` is at the moment it runs. Under pytest that has to be the
    # test body's stream, so the flag is cleared and install called here.
    # Recording both streams first lets monkeypatch unwrap them afterwards;
    # `install` assigns them directly and would otherwise outlive this test.
    monkeypatch.setattr(sys, "stdout", sys.stdout)
    monkeypatch.setattr(sys, "stderr", sys.stderr)
    monkeypatch.setattr(cap, "_installed", False)
    cap.install(tee=False)
    lines = (OVER_OLD_CAP // LINE_BYTES) + 20_000

    @rs.Asset
    def noisy_asset() -> int:
        for _ in range(lines):
            print(LINE, end="")
        return 1

    assets = [noisy_asset]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[rs.Job(name="j", assets=assets, executor=rs.Executor.in_process())],
    )
    repo.resolve(storage=storage)
    result = repo.get_job("j").execute()
    assert result.success

    logs = [
        log
        for log in storage.get_run_logs(result.run_id)
        if log.step_key == "noisy_asset"
    ]
    assert len(logs) == 1
    stored = logs[0].stdout or ""

    assert stored.count("\n") == lines
    assert len(stored) == lines * LINE_BYTES
