import sys

import pytest

import rivers as rs


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------


def test_task_bare_decorator():
    @rs.Task
    def my_task():
        return 42

    assert isinstance(my_task, rs.Task)
    assert my_task.name == "my_task"


def test_task_with_name():
    @rs.Task(name="custom_name", tags=["compute"])
    def my_task():
        return 42

    assert my_task.name == "custom_name"
    assert my_task.tags == ["compute"]


def test_task_direct_call():
    @rs.Task
    def add(a, b):
        return a + b

    assert add(2, 3) == 5


def test_task_name_derived_from_function():
    @rs.Task
    def some_computation():
        return 99

    assert some_computation.name == "some_computation"


# ---------------------------------------------------------------------------
# BashTask
# ---------------------------------------------------------------------------


def test_bash_task_string_command():
    task = rs.BashTask(name="greet", command="echo hello")
    assert task() == "hello"


def test_bash_task_list_command():
    task = rs.BashTask(name="greet", command=["echo", "hello"])
    assert task() == "hello"


def test_bash_task_env():
    task = rs.BashTask(
        name="env_test", command="printenv MY_VAR", env={"MY_VAR": "test_value"}
    )
    assert task() == "test_value"


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="BashTask `pwd` returns MSYS-style paths (/c/...) on Windows",
)
def test_bash_task_cwd(tmp_path):
    task = rs.BashTask(name="cwd_test", command="pwd", cwd=str(tmp_path))
    result = task()
    # On macOS /tmp -> /private/tmp, so use realpath
    assert result == str(tmp_path.resolve())


def test_bash_task_error():
    task = rs.BashTask(name="fail", command="false")
    with pytest.raises(OSError, match="Command failed"):
        task()


def test_bash_task_properties():
    task = rs.BashTask(
        name="my_task",
        command=["echo", "hi"],
        env={"KEY": "val"},
        cwd="/tmp",
        tags=["shell", "fast"],
    )
    assert task.name == "my_task"
    assert task.command == ["echo", "hi"]
    assert task.env == {"KEY": "val"}
    assert task.cwd == "/tmp"
    assert task.tags == ["shell", "fast"]


def test_bash_task_string_command_property():
    task = rs.BashTask(name="t", command="echo hi")
    assert task.command == "echo hi"


def test_bash_task_defaults():
    task = rs.BashTask(name="t", command="echo hi")
    assert task.env is None
    assert task.cwd is None
    assert task.tags is None


def test_bash_task_in_graph_asset():
    greet = rs.BashTask(name="greet", command="echo hello from bash")

    @rs.Task
    def upper(text: str):
        return text.upper()

    @rs.Asset.from_graph()
    def pipeline():
        text = greet()
        return upper(text)

    assert isinstance(pipeline, rs.GraphAsset)


def test_bash_task_stderr_in_error():
    task = rs.BashTask(name="err", command="echo oops >&2; exit 1")
    with pytest.raises(OSError, match="oops"):
        task()


def test_bash_task_multiline_output():
    task = rs.BashTask(name="multi", command="echo line1; echo line2")
    assert task() == "line1\nline2"
