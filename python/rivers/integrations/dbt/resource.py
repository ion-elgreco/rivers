"""Runs dbt commands and reports each built model as a materialization."""

import asyncio
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
from collections import deque
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import PrivateAttr
from pydantic_settings import SettingsConfigDict

from rivers._core import Materialization, MetadataValue
from rivers._core.assets import AssetExecutionContext
from rivers.integrations.dbt._manifest import DbtOutputs
from rivers.resource import Resource

if TYPE_CHECKING:
    from rivers.integrations.dbt.project import DbtProject

__all__ = ["DbtError", "DbtInvocation", "DbtResource"]

_logger = logging.getLogger(__name__)

_RUN_DBT = "from dbt.cli.main import cli; cli(prog_name='dbt')"
_SELECTION_FLAGS = frozenset(
    {"--select", "-s", "--models", "--model", "-m", "--exclude", "--selector"}
)
_BUILT = frozenset({"success", "no-op", "reused"})
_FAILED = frozenset({"error", "fail", "runtime error"})
_TESTS = frozenset({"test", "unit_test"})
_LOG_LEVELS = {"info": logging.INFO, "warn": logging.WARNING, "error": logging.ERROR}
# How long dbt gets to cancel its queries before it is killed.
_STOP_TIMEOUT_S = 25
# dbt logs each event on one line, e.g. a model's SQL or every result of the
# run, so the async reader takes lines of any length, as the sync one does.
_LINE_LIMIT = sys.maxsize


class DbtError(Exception):
    """A dbt command failed."""


class DbtResource(Resource):
    """Runs dbt commands in a dbt project.

    Each command runs in a new process with its own target directory, so
    commands can run at the same time. The target directory is removed when the
    command ends, unless you pass ``target_path``. dbt's anonymous usage stats
    are off unless ``DBT_SEND_ANONYMOUS_USAGE_STATS`` is set.

    With a :class:`DbtProject` as ``project_dir``, each of ``profiles_dir``,
    ``profile``, and ``target`` that you do not give takes the project's value,
    when it is not ``None``. Fields that still have no value are read from the
    environment variables dbt reads (``DBT_PROJECT_DIR``, ``DBT_PROFILES_DIR``,
    ``DBT_PROFILE``, ``DBT_TARGET``).

    Args:
        project_dir: The directory with ``dbt_project.yml``, or a
            :class:`DbtProject`.
        profiles_dir: The directory with ``profiles.yml``. Default: dbt's search
            (the project directory, then ``~/.dbt``).
        profile: The profile to use instead of the project's.
        target: The target to use instead of the profile's default.
        executable: The dbt executable, e.g. ``"dbt"`` or a path. Default: the
            dbt-core installed with rivers.

    Example::

        project = DbtProject(Path(__file__).parent / "analytics", target="prod")
        dbt = DbtResource(project_dir=project)
    """

    model_config = SettingsConfigDict(env_prefix="DBT_")

    project_dir: Path
    profiles_dir: Path | None = None
    profile: str | None = None
    target: str | None = None
    executable: str | None = None

    _outputs: DbtOutputs | None = PrivateAttr(default=None)
    _parse_dir: Path | None = PrivateAttr(default=None)

    def __init__(
        self, *, project_dir: "str | Path | DbtProject | None" = None, **kwargs: Any
    ) -> None:
        from rivers.integrations.dbt.project import DbtProject

        # Before the environment is read: a validator sees its values as given ones.
        if isinstance(project_dir, DbtProject):
            settings = project_dir.model_dump(
                include={"profiles_dir", "profile", "target"}, exclude_none=True
            )
            kwargs = {**settings, **kwargs}
            project_dir = project_dir.project_dir
        if project_dir is not None:
            kwargs["project_dir"] = project_dir

        super().__init__(**kwargs)

    def cli(
        self,
        args: Sequence[str],
        context: AssetExecutionContext | None = None,
        *,
        target_path: str | Path | None = None,
    ) -> "DbtInvocation":
        """A dbt command, e.g. ``["build"]``. Iterate it, or call ``wait()``, to run it.

        In async code, use ``async for`` or ``await async_wait()``: they run
        dbt without blocking the event loop.

        With the context of a ``DbtProject.assets`` body, dbt runs the outputs
        the run selected, iteration yields a :class:`~rivers.Materialization` for
        each output dbt builds, and dbt's log goes to ``context.log``. In a ``build``,
        an output is reported once the tests on it pass; a failed test fails it.
        dbt's own ``--select``, ``--exclude``, or ``--selector`` in ``args``
        replace the selection.

        Args:
            args: The dbt command and its arguments.
            context: The asset context.
            target_path: Where dbt writes its artifacts (kept). Default: a
                temporary directory.

        Returns:
            The command, not started.
        """
        outputs = self._outputs if context is not None else None
        selected = set(context.output_selection) if context is not None else set()

        selection: list[str] = []
        if outputs is not None and not _selects(args):
            if selected >= set(outputs.selectors):
                selection = list(outputs.selection)
            else:
                selectors = " ".join(
                    outputs.selectors[name] for name in sorted(selected)
                )
                selection = ["--select", selectors, *outputs.exclude]

        log = context.log if context is not None else _logger
        return self._invocation(
            list(args), selection, target_path, outputs, selected, log
        )

    def _list(self, selection: Sequence[str]) -> set[str]:
        """The unique ids that ``dbt ls`` lists for a selection."""
        args = ["ls", "--output", "json", "--output-keys", "unique_id"]
        invocation = self._invocation(
            args, list(selection), None, None, set(), log=None
        )

        unique_ids = set()
        for event in invocation._events():
            message = event["info"].get("msg", "")
            if message.startswith("{"):
                unique_ids.add(json.loads(message).get("unique_id"))
        invocation._raise_on_error()

        unique_ids.discard(None)
        return unique_ids

    def _invocation(
        self,
        args: list[str],
        selection: list[str],
        target_path: str | Path | None,
        outputs: DbtOutputs | None,
        selected: set[str],
        log: logging.Logger | None,
    ) -> "DbtInvocation":
        command = [*self._executable(), *args, *selection]
        if self.profile:
            command += ["--profile", self.profile]
        if self.target:
            command += ["--target", self.target]

        parse_dir = self._parse_dir or self.project_dir / os.environ.get(
            "DBT_TARGET_PATH", "target"
        )

        return DbtInvocation(
            command=command,
            args=args,
            project_dir=self.project_dir,
            env=self._env(),
            target_path=Path(target_path) if target_path is not None else None,
            parse_dir=parse_dir,
            outputs=outputs,
            selected=selected,
            log=log,
        )

    def _bind(self, outputs: DbtOutputs, parse_dir: Path) -> "DbtResource":
        """A copy whose ``cli`` maps dbt's results to these outputs.

        Its commands start from the ``partial_parse.msgpack`` in ``parse_dir``.
        """
        bound = self.model_copy()
        bound._outputs = outputs
        bound._parse_dir = parse_dir
        return bound

    def _executable(self) -> list[str]:
        if self.executable is None:
            return [sys.executable, "-c", _RUN_DBT]
        return [shutil.which(self.executable) or self.executable]

    def _env(self) -> dict[str, str]:
        env = {
            **os.environ,
            "DBT_PROJECT_DIR": str(self.project_dir),
            "DBT_LOG_FORMAT": "json",
            "DBT_LOG_LEVEL": "debug",
            "DBT_USE_COLORS": "false",
            "PYTHONUNBUFFERED": "1",
            "PYTHONIOENCODING": "utf-8",
        }
        env.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
        if self.profiles_dir is not None:
            env["DBT_PROFILES_DIR"] = str(self.profiles_dir)
        return env


class DbtInvocation:
    """One dbt command, made by :meth:`DbtResource.cli`. It runs once.

    In async code, ``async for`` and :meth:`async_wait` run it without
    blocking the event loop.
    """

    def __init__(
        self,
        *,
        command: list[str],
        args: list[str],
        project_dir: Path,
        env: dict[str, str],
        target_path: Path | None,
        parse_dir: Path,
        outputs: DbtOutputs | None,
        selected: set[str],
        log: logging.Logger | None,
    ) -> None:
        self._args = args
        self._target_path = target_path
        self._parse_dir = parse_dir
        self._command = command
        self._project_dir = project_dir
        self._env = env
        self._results = _Results(
            outputs, selected, holds_for_tests=_subcommand(args) == "build"
        )
        self._log = log
        self._started = False
        self._returncode: int | None = None
        self._errors: list[str] = []
        self._plain: deque[str] = deque(maxlen=20)
        self._failures: list[str] = []

    def __iter__(self) -> Iterator[Materialization]:
        """Run dbt and yield a materialization for each output it builds.

        Raises:
            DbtError: When dbt fails, after the materializations of the outputs
                it built.
        """
        for event in self._events():
            yield from self._results.add(event)

        yield from self._results.release_all()
        self._raise_on_error()

    async def __aiter__(self) -> AsyncIterator[Materialization]:
        """Run dbt and yield what ``__iter__`` yields, without blocking the event loop.

        When the task is cancelled, or the iteration stops early, dbt gets an
        interrupt, so that it cancels its queries. It is killed when it still
        runs 25 seconds later.

        Raises:
            DbtError: When dbt fails, after the materializations of the outputs
                it built.
        """
        with self._run_env() as env:
            process = await asyncio.create_subprocess_exec(
                *self._command,
                cwd=self._project_dir,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                limit=_LINE_LIMIT,
            )
            try:
                assert process.stdout is not None
                async for line in process.stdout:
                    event = self._read(line.decode("utf-8", errors="replace"))
                    if event is not None:
                        for materialization in self._results.add(event):
                            yield materialization
                self._returncode = await process.wait()
            finally:
                await _async_stop(process)

        for materialization in self._results.release_all():
            yield materialization
        self._raise_on_error()

    def wait(self) -> None:
        """Run dbt to the end.

        Raises:
            DbtError: When dbt fails.
        """
        for _ in self:
            pass

    async def async_wait(self) -> None:
        """Run dbt to the end, without blocking the event loop.

        Raises:
            DbtError: When dbt fails.
        """
        async for _ in self:
            pass

    def _events(self) -> Iterator[dict[str, Any]]:
        """dbt's structured log events, logged as they arrive.

        After the last event, :meth:`_raise_on_error` raises if dbt failed.
        """
        with self._run_env() as env:
            process = subprocess.Popen(
                self._command,
                cwd=self._project_dir,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            try:
                assert process.stdout is not None
                for line in process.stdout:
                    event = self._read(line)
                    if event is not None:
                        yield event
                self._returncode = process.wait()
            finally:
                _stop(process)

    @contextmanager
    def _run_env(self) -> Iterator[dict[str, str]]:
        """The environment of the dbt process, with its own target directory.

        The directory is removed at the end, unless it is ``target_path``.
        """
        if self._started:
            raise RuntimeError("a DbtInvocation runs once; call DbtResource.cli again")
        self._started = True

        target = self._target_path or Path(tempfile.mkdtemp(prefix="rivers-dbt-"))
        target.mkdir(parents=True, exist_ok=True)
        _copy_partial_parse(self._parse_dir, target)

        try:
            yield {
                **self._env,
                "DBT_TARGET_PATH": str(target),
                "DBT_LOG_PATH": str(target),
            }
        finally:
            if self._target_path is None:
                shutil.rmtree(target, ignore_errors=True)

    def _read(self, line: str) -> dict[str, Any] | None:
        """Log a line of dbt's output, and return its structured event, if any."""
        event = _parse(line)
        if event is None:
            if line.strip():
                self._plain.append(line.rstrip())
                self._emit(logging.INFO, line.rstrip())
            return None

        info = event["info"]
        level = _LOG_LEVELS.get(info.get("level", ""))
        if level is not None and info.get("msg"):
            self._emit(level, info["msg"])
            if level == logging.ERROR:
                self._errors.append(info["msg"])
        if failure := _failure(event):
            self._failures.append(failure)
        return event

    def _raise_on_error(self) -> None:
        if self._returncode:
            lines = [
                f"dbt {' '.join(self._args)} failed with exit code {self._returncode}",
                *(self._failures or self._errors or self._plain),
            ]
            raise DbtError("\n".join(lines))

    def _emit(self, level: int, message: str) -> None:
        if self._log is not None:
            self._log.log(level, message)


class _Results:
    """Turns dbt's node results into materializations of the selected outputs.

    In a ``build``, dbt tests an output right after building it, so an output
    with tests is held until they finish: it is reported if none failed.
    """

    def __init__(
        self, outputs: DbtOutputs | None, selected: set[str], holds_for_tests: bool
    ) -> None:
        self._outputs = outputs
        self._selected = selected
        self._pending: dict[str, dict[str, Any]] = {}
        self._tests: dict[str, dict[str, str]] = {}
        self._untested: dict[str, set[str]] = {}
        if outputs is not None and holds_for_tests:
            for test, checked in outputs.tests.items():
                for name in checked:
                    self._untested.setdefault(name, set()).add(test)

    def add(self, event: dict[str, Any]) -> Iterator[Materialization]:
        if self._outputs is None or event["info"].get("name") != "NodeFinished":
            return

        node = event["data"]["node_info"]
        result = event["data"].get("run_result") or {}
        uid = node["unique_id"]
        status = result.get("status") or node.get("node_status")

        if uid in self._outputs.tests:
            for name in self._outputs.tests[uid]:
                if name not in self._selected:
                    continue
                self._tests.setdefault(name, {})[node["node_name"]] = status
                untested = self._untested.get(name, set())
                untested.discard(uid)
                if name in self._pending and not untested:
                    yield from self._release(name)
            return

        name = self._outputs.names.get(uid)
        if name not in self._selected or status not in _BUILT:
            return

        metadata: dict[str, Any] = {
            "dbt/invocation_id": event["info"]["invocation_id"],
            "dbt/execution_time": MetadataValue.duration(
                result.get("execution_time") or 0.0
            ),
        }
        if message := result.get("message"):
            metadata["dbt/message"] = message
        rows = (result.get("adapter_response") or {}).get("rows_affected")
        if rows is not None and rows >= 0:
            metadata["dbt/rows_affected"] = int(rows)

        self._pending[name] = metadata
        if not self._untested.get(name):
            yield from self._release(name)

    def release_all(self) -> Iterator[Materialization]:
        for name in list(self._pending):
            yield from self._release(name)

    def _release(self, name: str) -> Iterator[Materialization]:
        metadata = self._pending.pop(name)
        tests = self._tests.get(name, {})
        if any(status in _FAILED for status in tests.values()):
            return
        if tests:
            metadata["dbt/tests"] = MetadataValue.json(
                json.dumps(tests, sort_keys=True)
            )
        yield Materialization(output_name=name, metadata=metadata)


def _subcommand(args: Sequence[str]) -> str | None:
    return next((arg for arg in args if not arg.startswith("-")), None)


def _selects(args: Sequence[str]) -> bool:
    return any(arg.split("=", 1)[0] in _SELECTION_FLAGS for arg in args)


def _parse(line: str) -> dict[str, Any] | None:
    if not line.startswith("{"):
        return None
    try:
        event = json.loads(line)
    except ValueError:
        return None
    return event if isinstance(event, dict) and "info" in event else None


def _failure(event: dict[str, Any]) -> str | None:
    """A line for the error message when ``event`` reports a failed or skipped node."""
    if event["info"].get("name") != "NodeFinished":
        return None

    node = event["data"]["node_info"]
    result = event["data"].get("run_result") or {}
    status = result.get("status") or node.get("node_status")
    if status == "skipped" and node.get("resource_type") not in _TESTS:
        return f"{node['node_name']}: skipped"
    if status in _FAILED or status == "partial success":
        message = (result.get("message") or status).strip()
        return f"{node['node_name']}: {message}"
    return None


def _copy_partial_parse(parse_dir: Path, target: Path) -> None:
    """Start from the project's parse, so dbt only parses the files that changed."""
    saved = parse_dir / "partial_parse.msgpack"
    if saved.is_file() and not (target / saved.name).exists():
        shutil.copy2(saved, target / saved.name)


def _stop(process: subprocess.Popen[str]) -> None:
    """Stop dbt if it still runs: an interrupt lets it cancel its queries."""
    if process.poll() is not None:
        return

    _interrupt(process)
    try:
        process.wait(timeout=_STOP_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


async def _async_stop(process: asyncio.subprocess.Process) -> None:
    """:func:`_stop`, without blocking the event loop."""
    if process.returncode is not None:
        return

    _interrupt(process)
    try:
        await asyncio.wait_for(process.wait(), _STOP_TIMEOUT_S)
    except TimeoutError:
        process.kill()
        await process.wait()


def _interrupt(process: subprocess.Popen[str] | asyncio.subprocess.Process) -> None:
    if os.name == "nt":
        process.terminate()
    else:
        process.send_signal(signal.SIGINT)
