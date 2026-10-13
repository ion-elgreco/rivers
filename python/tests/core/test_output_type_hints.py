"""A multi-asset output's ``AssetDef(type_hint=...)`` is checked like a return hint.

Every case runs on both executors: the parallel executor checks a sync step in
its loky worker.
"""

import json
import os

import pytest
from _helpers import materialize_beside_sibling

import rivers as rs
from rivers.exceptions import AssetDefinitionError

SHAPES = ["dict", "generator"]

BAD_TITLE = (
    "AssetOutputValidationError: Asset 'title' returned value of type 'int' "
    "but expected '<class 'str'>'"
)


class TypeHintRecorder(rs.PickleIOHandler):
    """Stores ``context.type_hint`` and the writing process as output metadata."""

    def handle_output(self, context, obj):
        context.add_output_metadata(
            {"type_hint": repr(context.type_hint), "pid": os.getpid()}
        )
        super().handle_output(context, obj)


def load_asset(handler, values, *, shape, is_async=False):
    """``load`` with outputs ``count: int``, ``rows`` (no hint) and
    ``title: str``, which get ``values`` in a returned dict or as yields."""
    output_defs = [
        rs.AssetDef("count", io_handler=handler, type_hint=int),
        rs.AssetDef("rows", io_handler=handler),
        rs.AssetDef("title", io_handler=handler, type_hint=str),
    ]

    if shape == "dict" and is_async:

        async def load():
            return dict(values)

    elif shape == "dict":

        def load():
            return dict(values)

    elif is_async:

        async def load():
            for name, value in values.items():
                yield rs.Output(value, output_name=name)

    else:

        def load():
            for name, value in values.items():
                yield rs.Output(value, output_name=name)

    return rs.Asset.from_multi(output_defs=output_defs)(load)


@pytest.mark.parametrize("shape", SHAPES)
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_values_that_match_their_type_hints_are_stored(
    storage, executor_env, shape, is_async
):
    executor, io_factory = executor_env
    handler = io_factory()
    values = {"count": 1, "rows": [{"id": 1}], "title": "one"}
    load = load_asset(handler, values, shape=shape, is_async=is_async)

    repo, result = materialize_beside_sibling(storage, executor, handler, load)

    assert result.success
    assert {name: repo.load_node(name) for name in values} == values


@pytest.mark.parametrize("shape", SHAPES)
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_value_that_does_not_match_its_type_hint_fails_its_output(
    storage, executor_env, shape, is_async
):
    executor, io_factory = executor_env
    handler = io_factory()
    # `rows` has no type hint, so its value is not checked.
    values = {"count": 1, "rows": "not rows", "title": 2}
    load = load_asset(handler, values, shape=shape, is_async=is_async)

    repo, result = materialize_beside_sibling(storage, executor, handler, load)

    assert dict(result.failed_assets) == {"title": BAD_TITLE}
    assert {name: repo.load_node(name) for name in ("count", "rows")} == {
        "count": 1,
        "rows": "not rows",
    }
    assert storage.get_latest_materialization("title") is None


@pytest.mark.parametrize("shape", SHAPES)
def test_output_without_a_value_is_not_checked(storage, executor_env, shape):
    executor, io_factory = executor_env
    handler = io_factory()
    output_defs = [
        rs.AssetDef("count", io_handler=handler, type_hint=int),
        rs.AssetDef("title", io_handler=handler, type_hint=str),
    ]

    if shape == "dict":

        def load():
            return {"count": 1, "title": rs.Materialization(metadata={"rows": 2})}

    else:

        def load():
            yield rs.Output(1, output_name="count")
            yield rs.Materialization(output_name="title", metadata={"rows": 2})

    load = rs.Asset.from_multi(output_defs=output_defs)(load)

    repo, result = materialize_beside_sibling(storage, executor, handler, load)

    assert result.success
    assert repo.load_node("count") == 1
    metadata = dict(storage.get_latest_materialization("title").metadata)
    assert json.loads(metadata["rows"]) == {"Int": {"value": 2}}


@pytest.mark.parametrize("shape", SHAPES)
def test_io_handler_gets_the_output_type_hint(storage, executor_env, shape):
    executor, io_factory = executor_env
    handler = TypeHintRecorder(store=io_factory().store)
    values = {"count": 1, "rows": [], "title": "one"}
    load = load_asset(handler, values, shape=shape)

    _, result = materialize_beside_sibling(storage, executor, handler, load)

    assert result.success
    metadata = {
        name: {
            key: json.loads(value)
            for key, value in storage.get_latest_materialization(name).metadata
        }
        for name in values
    }
    assert {name: entries["type_hint"] for name, entries in metadata.items()} == {
        "count": {"Text": {"value": "<class 'int'>"}},
        "rows": {"Text": {"value": "None"}},
        "title": {"Text": {"value": "<class 'str'>"}},
    }
    in_worker = {
        entries["pid"]["Int"]["value"] != os.getpid() for entries in metadata.values()
    }
    assert in_worker == {isinstance(executor, rs.Executor.Parallel)}


def test_class_form_output_takes_a_type_hint(storage, executor_env):
    executor, io_factory = executor_env
    handler = io_factory()

    class Load(rs.MultiAsset):
        count = rs.AssetDef(io_handler=handler, type_hint=int)
        title = rs.AssetDef(io_handler=handler, type_hint=str)

        @classmethod
        def materialize(cls):
            return {"count": 1, "title": 2}

    repo, result = materialize_beside_sibling(storage, executor, handler, Load)

    assert dict(result.failed_assets) == {"title": BAD_TITLE}
    assert repo.load_node("count") == 1


def test_type_hint_is_part_of_the_definition():
    count = rs.AssetDef("count", type_hint=list[int])

    @rs.Asset.from_multi(output_defs=[count, rs.AssetDef("title")])
    def load():
        return {"count": [1], "title": "one"}

    assert [d.type_hint for d in load.output_defs] == [list[int], None]
    assert count == rs.AssetDef("count", type_hint=list[int])
    assert count != rs.AssetDef("count", type_hint=list[str])
    assert count != rs.AssetDef("count")


def test_string_type_hint_is_rejected():
    message = "type_hint must be a type, not the string 'int'"
    with pytest.raises(AssetDefinitionError, match=message):
        rs.AssetDef("count", type_hint="int")

    count = rs.AssetDef("count", type_hint=int)
    with pytest.raises(AssetDefinitionError, match=message):
        count.type_hint = "int"
    assert count.type_hint is int
