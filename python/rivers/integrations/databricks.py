"""Databricks integration: run Databricks jobs as rivers assets.

Install with ``pip install rivers[databricks]``.

Example::

    import rivers as rs
    from rivers.integrations.databricks import DatabricksResource

    databricks = DatabricksResource()  # DATABRICKS_HOST and DATABRICKS_TOKEN

    repo = rs.CodeRepository(
        assets=[
            databricks.job_asset(123, name="daily_sales"),
            databricks.job_asset(456, name="sales", tasks=["orders", "refunds"]),
        ],
        resources={"databricks": databricks},
    )

A run uses the resource registered under ``"databricks"``, so the run's
config can override its fields.
"""

import asyncio
import copy
import logging
import time
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, SecretStr
from pydantic_settings import SettingsConfigDict

from rivers._core import Materialization, MetadataValue
from rivers._core.assets import (
    Asset,
    AssetDef,
    AssetExecutionContext,
    MultiAsset,
    SingleAsset,
)
from rivers.integrations._resource_key import with_resource_key
from rivers.resource import Resource

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import jobs
except ImportError as e:
    raise ImportError(
        "rivers.integrations.databricks needs databricks-sdk: pip install rivers[databricks]"
    ) from e

__all__ = ["DatabricksResource", "DatabricksRun", "DatabricksRunError"]

_log = logging.getLogger(__name__)

_JobParameters = (
    Mapping[str, str] | Callable[[AssetExecutionContext], Mapping[str, str]]
)

# Life cycle states of a run that has ended.
_ENDED = frozenset({"TERMINATED", "SKIPPED", "INTERNAL_ERROR"})

# Result states of a task that has an error to show.
_TASK_FAILURES = frozenset({"FAILED", "TIMEDOUT", "CANCELED", "INTERNAL_ERROR"})


class DatabricksRunError(Exception):
    """A Databricks run, or a task of it, ended without ``SUCCESS``."""


class DatabricksRun(BaseModel):
    """A Databricks run that ended with ``SUCCESS``.

    Args:
        run_id: The run id.
        job_id: The job id. A one-time run also has one.
        run_page_url: The run's page in the workspace.
        duration: Seconds from the start of the run to its end.
        outputs: The value that each notebook task passed to
            ``dbutils.notebook.exit()``, by task key.
    """

    run_id: int
    job_id: int | None = None
    run_page_url: str | None = None
    duration: float | None = None
    outputs: dict[str, str] = {}


class DatabricksResource(Resource):
    """Runs Databricks jobs, and makes assets that run them.

    The Databricks SDK authenticates with one of: a personal access ``token``;
    a service principal's OAuth ``client_id`` and ``client_secret``; or an
    Azure service principal's ``azure_client_id``, ``azure_client_secret`` and
    ``azure_tenant_id``. Fields not given are read from ``DATABRICKS_*``
    environment variables. Without them, the SDK reads its own configuration:
    ``ARM_*`` and other environment variables, ``~/.databrickscfg``, or the
    Azure and Databricks CLIs.

    Args:
        host: The workspace URL, e.g. ``"https://dbc-1a2b3c4d-5e6f.cloud.databricks.com"``.
        token: Personal access token.
        client_id: OAuth client id of a service principal.
        client_secret: OAuth secret of the service principal.
        azure_client_id: Application id of an Azure service principal.
        azure_client_secret: Client secret of the Azure service principal.
        azure_tenant_id: Tenant id of the Azure service principal.
        poll_interval: Seconds between status checks of an active run.
        poll_timeout: Seconds to wait for a run to end. ``None`` waits until
            it ends.
        cancel_on_interrupt: Cancel the run when the wait stops before the run
            ends: after ``poll_timeout``, on an error, or on an interrupt such
            as Ctrl-C.

    Example::

        databricks = DatabricksResource(poll_timeout=3600)

        repo = rs.CodeRepository(
            assets=[databricks.job_asset(123, name="daily_sales")],
            resources={"databricks": databricks},
        )
    """

    model_config = SettingsConfigDict(env_prefix="DATABRICKS_")

    host: str | None = None
    token: SecretStr | None = None
    client_id: str | None = None
    client_secret: SecretStr | None = None
    azure_client_id: str | None = None
    azure_client_secret: SecretStr | None = None
    azure_tenant_id: str | None = None
    poll_interval: float = 10
    poll_timeout: float | None = None
    cancel_on_interrupt: bool = True

    def get_client(self) -> WorkspaceClient:
        """Return a ``databricks.sdk.WorkspaceClient`` for this workspace."""
        return WorkspaceClient(
            host=self.host,
            token=_reveal(self.token),
            client_id=self.client_id,
            client_secret=_reveal(self.client_secret),
            azure_client_id=self.azure_client_id,
            azure_client_secret=_reveal(self.azure_client_secret),
            azure_tenant_id=self.azure_tenant_id,
        )

    def job_asset(
        self,
        job_id: int,
        *,
        name: str,
        tasks: Sequence[str] | Mapping[str, AssetDef] | None = None,
        job_parameters: _JobParameters | None = None,
        resource_key: str = "databricks",
        **asset_options: Any,
    ) -> SingleAsset | MultiAsset:
        """An asset that runs a job of the workspace.

        Without ``tasks``, the asset has one output, and a run runs the whole
        job (see :meth:`run_job`). With ``tasks``, the asset has one output per
        task. A run of all the outputs runs the whole job; a run of some
        outputs runs only their tasks. Each task that succeeds records a
        materialization; the other selected outputs fail with
        :class:`DatabricksRunError`.

        A run uses the ``DatabricksResource`` registered under
        ``resource_key``, not the instance that this method is called on, so
        the run's config can override the resource's fields. Without that
        resource, the code location fails to load with ``unresolved input``
        and the key, e.g. ``unresolved input 'databricks'``.

        The asset's function is async, so the step waits for the run without
        holding a worker of the executor. The job is not read when the asset
        is made: a wrong job id or task key fails when the asset runs.

        Args:
            job_id: The job to run.
            name: Asset name.
            tasks: Keys of the tasks that get an output, named like the task.
                Map a task key to an ``AssetDef`` to give its output another
                name or settings; an ``AssetDef`` without a name takes the
                task key.
            job_parameters: Values for the job's parameters, or a function
                that returns them from the asset's context, e.g.
                ``lambda context: {"date": context.partition_key}``.
            resource_key: The key of the resource in
                ``CodeRepository(resources={...})``.
            **asset_options: Passed to ``Asset`` or, with ``tasks``, to
                ``Asset.from_multi``, e.g. ``group``, ``partitions_def``, or
                ``retry``. ``kinds`` defaults to ``["databricks"]``.

        Returns:
            A ``SingleAsset``, or with ``tasks`` a ``MultiAsset``, to pass to
            ``CodeRepository(assets=[...])``.

        Raises:
            ValueError: ``tasks`` is empty, or two tasks have the same output
                name.
        """
        asset_options.setdefault("kinds", ["databricks"])
        if tasks is None:

            async def run_job(
                context: AssetExecutionContext, databricks: DatabricksResource
            ) -> Materialization:
                run = await databricks.async_run_job(
                    job_id,
                    job_parameters=_parameters(job_parameters, context),
                    log=context.log,
                )
                return Materialization(metadata=_run_metadata(run))

            return Asset(name=name, **asset_options)(
                with_resource_key(run_job, resource_key)
            )

        task_keys, output_defs = _task_outputs(name, tasks)

        async def run_tasks(
            context: AssetExecutionContext, databricks: DatabricksResource
        ) -> AsyncIterator[Materialization]:
            async for materialization in databricks._run_tasks(
                job_id, task_keys, _parameters(job_parameters, context), context
            ):
                yield materialization

        return Asset.from_multi(
            with_resource_key(run_tasks, resource_key),
            output_defs=output_defs,
            name=name,
            **asset_options,
        )

    def run_job(
        self,
        job_id: int,
        *,
        job_parameters: Mapping[str, str] | None = None,
        only: Sequence[str] | None = None,
        log: logging.Logger | None = None,
    ) -> DatabricksRun:
        """Run a job of the workspace and wait until the run ends.

        Checks the run every ``poll_interval`` seconds. When the wait stops
        before the run ends, the run is canceled if ``cancel_on_interrupt`` is
        set.

        Args:
            job_id: The job to run.
            job_parameters: Values for the job's parameters.
            only: Keys of the tasks to run. ``None`` runs every task.
            log: Logger for the run's progress, e.g. ``context.log``.

        Returns:
            The run.

        Raises:
            DatabricksRunError: The run ended without ``SUCCESS``. The message
                has the error of each failed task.
            TimeoutError: The run did not end within ``poll_timeout``.
        """
        client = self.get_client()
        run_id = _run_now(client, job_id, job_parameters, only)
        return _result(client, run_id, self._wait(client, run_id, log or _log))

    async def async_run_job(
        self,
        job_id: int,
        *,
        job_parameters: Mapping[str, str] | None = None,
        only: Sequence[str] | None = None,
        log: logging.Logger | None = None,
    ) -> DatabricksRun:
        """Run a job of the workspace and wait until the run ends, without
        blocking the event loop.

        Takes the arguments of :meth:`run_job`, and returns and raises as it
        does.
        """
        client = self.get_client()
        run_id = await asyncio.to_thread(_run_now, client, job_id, job_parameters, only)
        run = await self._async_wait(client, run_id, log or _log)
        return await asyncio.to_thread(_result, client, run_id, run)

    def submit_run(
        self,
        tasks: Sequence[jobs.SubmitTask | Mapping[str, Any]],
        *,
        log: logging.Logger | None = None,
        **submit_args: Any,
    ) -> DatabricksRun:
        """Submit a one-time run of ``tasks`` and wait until it ends.

        Waits as :meth:`run_job` does.

        Args:
            tasks: ``jobs.SubmitTask`` objects, or dicts in the form of the
                Jobs API, e.g. ``{"task_key": "main", "notebook_task":
                {"notebook_path": "/Workspace/etl/sales"}}``.
            log: Logger for the run's progress, e.g. ``context.log``.
            **submit_args: Other arguments for ``WorkspaceClient.jobs.submit``,
                e.g. ``run_name``, ``timeout_seconds``, or ``environments``.

        Returns:
            The run.

        Raises:
            DatabricksRunError: The run ended without ``SUCCESS``. The message
                has the error of each failed task.
            TimeoutError: The run did not end within ``poll_timeout``.
        """
        client = self.get_client()
        run_id = _submit(client, tasks, submit_args)
        return _result(client, run_id, self._wait(client, run_id, log or _log))

    async def async_submit_run(
        self,
        tasks: Sequence[jobs.SubmitTask | Mapping[str, Any]],
        *,
        log: logging.Logger | None = None,
        **submit_args: Any,
    ) -> DatabricksRun:
        """Submit a one-time run of ``tasks`` and wait until it ends, without
        blocking the event loop.

        Takes the arguments of :meth:`submit_run`, and returns and raises as
        it does.
        """
        client = self.get_client()
        run_id = await asyncio.to_thread(_submit, client, tasks, submit_args)
        run = await self._async_wait(client, run_id, log or _log)
        return await asyncio.to_thread(_result, client, run_id, run)

    async def _run_tasks(
        self,
        job_id: int,
        task_keys: dict[str, str],
        job_parameters: Mapping[str, str] | None,
        context: AssetExecutionContext,
    ) -> AsyncIterator[Materialization]:
        selected = {output: task_keys[output] for output in context.output_selection}
        only = None if len(selected) == len(task_keys) else list(selected.values())
        client = self.get_client()
        run_id = await asyncio.to_thread(_run_now, client, job_id, job_parameters, only)
        run = await self._async_wait(client, run_id, context.log)

        tasks = {task.task_key: task for task in run.tasks or []}
        failed: list[jobs.RunTask] = []
        missing: list[str] = []
        for output, key in selected.items():
            task = tasks.get(key)
            if task is None:
                missing.append(key)
            elif _status(task.state) != "SUCCESS":
                failed.append(task)
            else:
                metadata = await asyncio.to_thread(_task_metadata, client, run, task)
                yield Materialization(output_name=output, metadata=metadata)

        if failed or missing:
            lines = [await asyncio.to_thread(_failure_message, client, run, failed)]
            lines.extend(f"Task {key} is not in the run" for key in missing)
            raise DatabricksRunError("\n".join(lines))
        if _status(run.state) != "SUCCESS":
            context.log.warning(await asyncio.to_thread(_failure_message, client, run))

    def _wait(
        self, client: WorkspaceClient, run_id: int, log: logging.Logger
    ) -> jobs.Run:
        """Poll run ``run_id`` until it ends."""
        progress = _Progress(run_id, self.poll_timeout, log)
        try:
            while not progress.ended(run := client.jobs.get_run(run_id)):
                time.sleep(self.poll_interval)
            return run
        except BaseException:
            if self.cancel_on_interrupt:
                _cancel(client, run_id, log)
            raise

    async def _async_wait(
        self, client: WorkspaceClient, run_id: int, log: logging.Logger
    ) -> jobs.Run:
        """Poll run ``run_id`` until it ends, without blocking the event loop."""
        progress = _Progress(run_id, self.poll_timeout, log)
        try:
            while not progress.ended(
                run := await asyncio.to_thread(client.jobs.get_run, run_id)
            ):
                await asyncio.sleep(self.poll_interval)
            return run
        except BaseException:
            if self.cancel_on_interrupt:
                # Not awaited, so a second cancellation cannot stop it.
                _cancel(client, run_id, log)
            raise


@dataclass
class _Progress:
    """Logs each new status of run ``run_id``, and ends the wait for it after
    ``timeout`` seconds."""

    run_id: int
    timeout: float | None
    log: logging.Logger
    started: float = field(default_factory=time.monotonic)
    last: str | None = None

    def ended(self, run: jobs.Run) -> bool:
        status = _status(run.state)
        if _ended(run):
            self.log.info("Databricks run %d ended with %s", self.run_id, status)
            return True

        if status != self.last:
            self.log.info(
                "Databricks run %d is %s: %s", self.run_id, status, run.run_page_url
            )
            self.last = status

        if self.timeout is not None and time.monotonic() - self.started >= self.timeout:
            raise TimeoutError(
                f"Databricks run {self.run_id} did not end within {self.timeout:g} s"
            )
        return False


def _task_outputs(
    name: str, tasks: Sequence[str] | Mapping[str, AssetDef]
) -> tuple[dict[str, str], list[AssetDef]]:
    """Each output's task key, by output name, and the output definitions."""
    if not tasks:
        raise ValueError(f"Databricks job asset {name!r} has no tasks")

    defs = (
        tasks if isinstance(tasks, Mapping) else {key: AssetDef(key) for key in tasks}
    )
    task_keys: dict[str, str] = {}
    output_defs = []
    for key, output_def in defs.items():
        if output_def.name is None:
            output_def = copy.copy(output_def)
            output_def.name = key
        if output_def.name in task_keys:
            raise ValueError(
                f"Databricks tasks {task_keys[output_def.name]!r} and {key!r} "
                f"both have output name {output_def.name!r}"
            )
        task_keys[output_def.name] = key
        output_defs.append(output_def)

    return task_keys, output_defs


def _run_now(
    client: WorkspaceClient,
    job_id: int,
    job_parameters: Mapping[str, str] | None,
    only: Sequence[str] | None,
) -> int:
    """Start a run of job ``job_id``, and return the run id."""
    return client.jobs.run_now(
        job_id,
        job_parameters=None if job_parameters is None else dict(job_parameters),
        only=None if only is None else list(only),
    ).run_id


def _submit(
    client: WorkspaceClient,
    tasks: Sequence[jobs.SubmitTask | Mapping[str, Any]],
    submit_args: Mapping[str, Any],
) -> int:
    """Submit a one-time run of ``tasks``, and return the run id."""
    return client.jobs.submit(
        tasks=[
            jobs.SubmitTask.from_dict(dict(task)) if isinstance(task, Mapping) else task
            for task in tasks
        ],
        **submit_args,
    ).run_id


def _parameters(
    job_parameters: _JobParameters | None, context: AssetExecutionContext
) -> Mapping[str, str] | None:
    return job_parameters(context) if callable(job_parameters) else job_parameters


def _cancel(client: WorkspaceClient, run_id: int, log: logging.Logger) -> None:
    try:
        client.jobs.cancel_run(run_id)
    except Exception as e:
        log.warning("Could not cancel Databricks run %d: %s", run_id, e)
    else:
        log.info("Canceled Databricks run %d", run_id)


def _ended(run: jobs.Run) -> bool:
    state = run.state and run.state.life_cycle_state
    return state is not None and state.value in _ENDED


def _status(state: jobs.RunState | None) -> str:
    """The result state of an ended run or task, else its life cycle state."""
    status = state and (state.result_state or state.life_cycle_state)
    return status.value if status else "UNKNOWN"


def _result(client: WorkspaceClient, run_id: int, run: jobs.Run) -> DatabricksRun:
    """The ended ``run``; raises unless it ended with ``SUCCESS``."""
    if _status(run.state) != "SUCCESS":
        raise DatabricksRunError(_failure_message(client, run))

    outputs = {}
    for task in run.tasks or []:
        output = _notebook_output(client, task)
        if output is not None:
            outputs[task.task_key] = output

    return DatabricksRun(
        run_id=run_id,
        job_id=run.job_id,
        run_page_url=run.run_page_url,
        duration=_seconds(run.start_time, run.end_time),
        outputs=outputs,
    )


def _run_metadata(run: DatabricksRun) -> dict[str, Any]:
    metadata = _metadata(run.run_id, run.job_id, run.run_page_url, run.duration)
    for task_key, output in run.outputs.items():
        metadata[f"databricks/output/{task_key}"] = output

    return metadata


def _task_metadata(
    client: WorkspaceClient, run: jobs.Run, task: jobs.RunTask
) -> dict[str, Any]:
    metadata = _metadata(
        run.run_id,
        run.job_id,
        task.run_page_url or run.run_page_url,
        _seconds(task.start_time, task.end_time),
    )
    metadata["databricks/task_key"] = task.task_key

    output = _notebook_output(client, task)
    if output is not None:
        metadata["databricks/output"] = output

    return metadata


def _metadata(
    run_id: int | None, job_id: int | None, url: str | None, duration: float | None
) -> dict[str, Any]:
    metadata: dict[str, Any] = {"databricks/run_id": run_id}
    if job_id is not None:
        metadata["databricks/job_id"] = job_id
    if url:
        metadata["databricks/run_url"] = MetadataValue.url(url)
    if duration is not None:
        metadata["databricks/duration"] = MetadataValue.duration(duration)

    return metadata


def _notebook_output(client: WorkspaceClient, task: jobs.RunTask) -> str | None:
    """The value a successful notebook task passed to ``dbutils.notebook.exit()``."""
    if not task.notebook_task or task.run_id is None:
        return None
    if _status(task.state) != "SUCCESS":
        return None

    output = client.jobs.get_run_output(task.run_id).notebook_output
    return output.result if output and output.result else None


def _seconds(start_ms: int | None, end_ms: int | None) -> float | None:
    return (end_ms - start_ms) / 1000 if start_ms and end_ms else None


def _failure_message(
    client: WorkspaceClient, run: jobs.Run, tasks: list[jobs.RunTask] | None = None
) -> str:
    """The run's state and URL, and the state and error of each of ``tasks``.

    ``tasks`` defaults to the run's tasks that have an error to show.
    """
    head = f"Databricks run {run.run_id} ended with {_status(run.state)}"
    lines = [_with_message(head, run.state and run.state.state_message)]
    if run.run_page_url:
        lines.append(run.run_page_url)

    if tasks is None:
        tasks = [t for t in run.tasks or [] if _status(t.state) in _TASK_FAILURES]
    for task in tasks:
        status = _status(task.state)
        head = f"Task {task.task_key} {status}"
        error = _task_error(client, task) if status in _TASK_FAILURES else None
        lines.append(_with_message(head, error))

    return "\n".join(lines)


def _task_error(client: WorkspaceClient, task: jobs.RunTask) -> str | None:
    """The task's error and stack trace, or its state message without them."""
    message = task.state and task.state.state_message
    if task.run_id is None:
        return message

    try:
        output = client.jobs.get_run_output(task.run_id)
    except OSError:
        return message

    return "\n".join(filter(None, [output.error or message, output.error_trace]))


def _with_message(head: str, message: str | None) -> str:
    return f"{head}: {message}" if message else head


def _reveal(secret: SecretStr | None) -> str | None:
    return secret.get_secret_value() if secret else None
