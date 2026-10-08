"""Slack integration: the resource, its hooks, and its run-failure sensor.

A ``SlackResource`` subclass returns a fake client, so nothing reaches Slack.
"""

import importlib
import sys

import pytest

pytest.importorskip("slack_sdk")

from slack_sdk import WebClient  # noqa: E402

import rivers as rs  # noqa: E402
from rivers._core import AutomationDaemon  # noqa: E402
from rivers.integrations.slack import SlackResource  # noqa: E402

from _polling import wait_for_ticks, wait_until  # noqa: E402

POSTS: list[dict] = []
UI = "https://rivers.test/locations/prod/etl"


class FakeClient:
    def chat_postMessage(self, **kwargs):
        POSTS.append(kwargs)
        return {"ok": True}


class FakeSlack(SlackResource):
    def get_client(self):  # type: ignore[override]
        return FakeClient()


@pytest.fixture(autouse=True)
def _clear_posts():
    POSTS.clear()
    yield
    POSTS.clear()


def test_resource_reads_token_from_env(monkeypatch):
    monkeypatch.setenv("SLACK_TOKEN", "xoxb-from-env")
    slack = SlackResource()
    client = slack.get_client()
    assert slack.token == "xoxb-from-env"
    assert isinstance(client, WebClient)
    assert client.token == "xoxb-from-env"


def test_missing_sdk_names_the_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "slack_sdk", None)
    monkeypatch.delitem(sys.modules, "rivers.integrations.slack")
    with pytest.raises(ImportError, match=r"pip install rivers\[slack\]"):
        importlib.import_module("rivers.integrations.slack")


def test_failure_hook_posts_once(storage, executor_env):
    executor, io_factory = executor_env
    slack = FakeSlack(token="t")

    @rs.Asset(
        name="boom", io_handler=io_factory(), hooks=[slack.failure_hook("#alerts")]
    )
    def boom() -> int:
        raise ValueError("boom went the asset")

    repo = rs.CodeRepository(assets=[boom], default_executor=executor)
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=False)

    [post] = POSTS
    assert post["channel"] == "#alerts"
    assert post["text"].startswith(
        f"Asset `boom` failed in run `{result.run_id}`:\n```"
    )
    assert "boom went the asset" in post["text"]
    assert post["text"].endswith("```")


def test_success_hook_posts_once(storage, executor_env):
    executor, io_factory = executor_env
    slack = FakeSlack(token="t")

    @rs.Asset(name="fine", io_handler=io_factory(), hooks=[slack.success_hook("#done")])
    def fine() -> int:
        return 1

    repo = rs.CodeRepository(assets=[fine], default_executor=executor)
    repo.resolve(storage=storage)
    result = repo.materialize()

    assert POSTS == [
        {
            "channel": "#done",
            "text": f"Asset `fine` succeeded in run `{result.run_id}`.",
            "blocks": None,
        }
    ]


def test_hook_text_fn_and_run_link(storage):
    slack = FakeSlack(token="t")
    hook = slack.failure_hook(
        "#alerts",
        text_fn=lambda context: f"{context.asset_name} broke",
        ui_location_url=UI + "/",
        name="page_team",
    )
    assert hook.name == "page_team"

    @rs.Asset(name="boom", hooks=[hook])
    def boom() -> int:
        raise ValueError("boom")

    repo = rs.CodeRepository(assets=[boom])
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=False)

    assert POSTS == [
        {
            "channel": "#alerts",
            "text": f"boom broke\n<{UI}/runs/{result.run_id}|View run>",
            "blocks": None,
        }
    ]


def test_run_failure_sensor_definition():
    slack = FakeSlack(token="t")
    sensor = slack.run_failure_sensor(
        "#alerts",
        name="etl_alerts",
        minimum_interval="10s",
        monitored_jobs=["nightly"],
        default_status=rs.SensorStatus.Running,
    )
    assert sensor.name == "etl_alerts"
    assert sensor.monitored_status == rs.RunStatus.Failure
    assert sensor.monitored_jobs == ["nightly"]
    assert sensor.minimum_interval == "10s"
    assert sensor.default_status == rs.SensorStatus.Running
    assert slack.run_failure_sensor("#alerts").default_status == rs.SensorStatus.Stopped


def _run_sensor_until_posted(repo, storage, sensor_name: str, fail):
    daemon = AutomationDaemon(repo=repo, storage=storage)
    daemon.start()
    try:
        assert wait_for_ticks(storage, sensor_name)
        result = fail()
        assert wait_until(lambda: POSTS, timeout=15)
        # Two more ticks: the run must not be posted again.
        seen = len(storage.get_ticks(sensor_name, limit=100))
        wait_for_ticks(storage, sensor_name, min_count=seen + 2, timeout=15)
        return result
    finally:
        daemon.stop()


def test_run_failure_sensor_posts_once_per_failed_run(storage):
    slack = FakeSlack(token="t")
    pd = rs.PartitionsDefinition.static_(["2024-01-01", "2024-01-02"])

    @rs.Asset(name="boom", partitions_def=pd)
    def boom() -> int:
        raise ValueError("boom went the asset")

    repo = rs.CodeRepository(
        assets=[boom],
        sensors=[
            slack.run_failure_sensor(
                "#alerts",
                ui_location_url=UI,
                minimum_interval="1s",
                default_status=rs.SensorStatus.Running,
            )
        ],
    )
    repo.resolve(storage=storage)
    result = _run_sensor_until_posted(
        repo,
        storage,
        "slack_run_failure_sensor",
        lambda: repo.materialize(
            partition_key=rs.PartitionKey.single("2024-01-01"), raise_on_error=False
        ),
    )

    [post] = POSTS
    assert (post["channel"], post["blocks"]) == ("#alerts", None)
    head, step, link = post["text"].split("\n")
    assert head == f"Run `{result.run_id}` failed."
    assert step.startswith("`boom[2024-01-01]`: ") and "boom went the asset" in step
    assert link == f"<{UI}/runs/{result.run_id}|View run>"


def test_run_failure_sensor_text_and_blocks_fn(storage):
    slack = FakeSlack(token="t")
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "*failed*"}}]

    @rs.Asset(name="boom")
    def boom() -> int:
        raise ValueError("boom")

    job = rs.Job(name="nightly", assets=[boom])
    repo = rs.CodeRepository(
        assets=[boom],
        jobs=[job],
        sensors=[
            slack.run_failure_sensor(
                "#alerts",
                text_fn=lambda context: (
                    f"{context.run.job_name} failed: {context.run.run_id}"
                ),
                blocks_fn=lambda context: blocks,
                monitored_jobs=["nightly"],
                minimum_interval="1s",
                default_status=rs.SensorStatus.Running,
            )
        ],
    )
    repo.resolve(storage=storage)
    result = _run_sensor_until_posted(
        repo,
        storage,
        "slack_run_failure_sensor",
        lambda: repo.get_job("nightly").execute(raise_on_error=False),
    )

    assert POSTS == [
        {
            "channel": "#alerts",
            "text": f"nightly failed: {result.run_id}",
            "blocks": blocks,
        }
    ]
