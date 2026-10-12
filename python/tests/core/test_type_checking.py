"""Tests for strong return type checking on assets."""

from typing import Annotated, Any, Optional, Union

import pytest
from _helpers import materialize_beside_sibling

import rivers as rs
from rivers.exceptions import AssetOutputValidationError

# ---------------------------------------------------------------------------
# Basic type checking
# ---------------------------------------------------------------------------


class TestBasicTypeChecking:
    """Test that return type hints are validated at runtime."""

    def test_correct_int_return(self):
        """Asset returning int with int hint should pass."""

        @rs.Asset
        def my_asset() -> int:
            return 42

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success

    def test_correct_str_return(self):
        """Asset returning str with str hint should pass."""

        @rs.Asset
        def my_asset() -> str:
            return "hello"

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success

    def test_correct_float_return(self):
        @rs.Asset
        def my_asset() -> float:
            return 3.14

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success

    def test_correct_bool_return(self):
        @rs.Asset
        def my_asset() -> bool:
            return True

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success

    def test_correct_list_return(self):
        @rs.Asset
        def my_asset() -> list:
            return [1, 2, 3]

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success

    def test_correct_dict_return(self):
        @rs.Asset
        def my_asset() -> dict:
            return {"a": 1}

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success

    def test_wrong_type_raises_type_error(self):
        """Asset returning str but hint says int should raise TypeError."""

        @rs.Asset
        def my_asset() -> int:
            return "not an int"  # type: ignore

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        with pytest.raises(AssetOutputValidationError, match="my_asset"):
            repo.materialize(["my_asset"])

    def test_wrong_type_str_instead_of_list(self):
        @rs.Asset
        def my_asset() -> list:
            return "a string"  # type: ignore

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        with pytest.raises(AssetOutputValidationError, match="my_asset"):
            repo.materialize(["my_asset"])

    def test_none_return_when_int_expected(self):
        @rs.Asset
        def my_asset() -> int:
            return None  # type: ignore

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        with pytest.raises(AssetOutputValidationError, match="my_asset"):
            repo.materialize(["my_asset"])


# ---------------------------------------------------------------------------
# Any type (skip validation)
# ---------------------------------------------------------------------------


class TestAnyType:
    """Test that Any return hint skips validation."""

    def test_any_return_accepts_int(self):
        @rs.Asset
        def my_asset() -> Any:
            return 42

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success

    def test_any_return_accepts_none(self):
        @rs.Asset
        def my_asset() -> Any:
            return None

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success

    def test_any_return_accepts_str(self):
        @rs.Asset
        def my_asset() -> Any:
            return "anything"

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success


# ---------------------------------------------------------------------------
# No return hint (skip validation)
# ---------------------------------------------------------------------------


class TestNoReturnHint:
    """Test that assets without return type hints skip validation."""

    def test_no_hint_accepts_anything(self):
        @rs.Asset
        def my_asset():
            return 42

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success


# ---------------------------------------------------------------------------
# Optional / Union types
# ---------------------------------------------------------------------------


class TestOptionalAndUnion:
    """Test Optional and Union return type hints."""

    def test_optional_int_returns_int(self):
        @rs.Asset
        def my_asset() -> Optional[int]:
            return 42

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success

    def test_optional_int_returns_none(self):
        @rs.Asset
        def my_asset() -> Optional[int]:
            return None

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success

    def test_optional_int_returns_wrong_type(self):
        @rs.Asset
        def my_asset() -> Optional[int]:
            return "string"  # type: ignore

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        with pytest.raises(AssetOutputValidationError, match="my_asset"):
            repo.materialize(["my_asset"])

    def test_union_int_str_returns_int(self):
        @rs.Asset
        def my_asset() -> Union[int, str]:
            return 42

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success

    def test_union_int_str_returns_str(self):
        @rs.Asset
        def my_asset() -> Union[int, str]:
            return "hello"

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success

    def test_union_int_str_returns_wrong_type(self):
        @rs.Asset
        def my_asset() -> Union[int, str]:
            return [1, 2, 3]  # type: ignore

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        with pytest.raises(AssetOutputValidationError, match="my_asset"):
            repo.materialize(["my_asset"])

    def test_pipe_union_returns_correct(self):
        """Test Python 3.10+ pipe union syntax."""

        @rs.Asset
        def my_asset() -> int | str:
            return 42

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success

    def test_pipe_union_returns_none_when_optional(self):
        @rs.Asset
        def my_asset() -> int | None:
            return None

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success

    def test_union_of_generics_returns_a_member(self):
        @rs.Asset
        def my_list() -> list[int] | None:
            return [1, 2]

        @rs.Asset
        def my_dict() -> Optional[dict[str, int]]:
            return {"a": 1}

        repo = rs.CodeRepository(
            assets=[my_list, my_dict], default_executor=rs.Executor.in_process()
        )
        repo.resolve()
        result = repo.materialize(["my_list", "my_dict"])
        assert result.success
        assert repo.load_node("my_list") == [1, 2]
        assert repo.load_node("my_dict") == {"a": 1}

    def test_union_of_generics_returns_wrong_type(self):
        @rs.Asset
        def my_asset() -> list[int] | None:
            return "not a list"  # type: ignore

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        with pytest.raises(AssetOutputValidationError) as err:
            repo.materialize(["my_asset"])
        assert str(err.value) == (
            "Asset 'my_asset' returned value of type 'str' but expected 'list[int] | None'"
        )


# ---------------------------------------------------------------------------
# Annotated (the value must match the annotated type)
# ---------------------------------------------------------------------------


class TestAnnotated:
    def test_annotated_returns_base_type(self):
        @rs.Asset
        def my_asset() -> Annotated[int, "row count"]:
            return 3

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success
        assert repo.load_node("my_asset") == 3

    def test_annotated_returns_wrong_type(self):
        @rs.Asset
        def my_asset() -> Annotated[int, "row count"]:
            return "3"  # type: ignore

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        with pytest.raises(AssetOutputValidationError) as err:
            repo.materialize(["my_asset"])
        assert str(err.value) == (
            "Asset 'my_asset' returned value of type 'str' but expected "
            "'typing.Annotated[int, 'row count']'"
        )


# ---------------------------------------------------------------------------
# Generic container types
# ---------------------------------------------------------------------------


class TestGenericContainers:
    """Test generic container type hints (list[int], dict[str, int], etc.)."""

    def test_list_int_returns_list(self):
        @rs.Asset
        def my_asset() -> list[int]:
            return [1, 2, 3]

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success

    def test_list_int_returns_non_list(self):
        @rs.Asset
        def my_asset() -> list[int]:
            return "not a list"  # type: ignore

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        with pytest.raises(AssetOutputValidationError, match="my_asset"):
            repo.materialize(["my_asset"])

    def test_dict_str_int_returns_dict(self):
        @rs.Asset
        def my_asset() -> dict[str, int]:
            return {"a": 1}

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success


# ---------------------------------------------------------------------------
# Custom types
# ---------------------------------------------------------------------------


class TestCustomTypes:
    """Test type checking with custom classes."""

    def test_custom_class_correct(self):
        class MyData:
            pass

        @rs.Asset
        def my_asset() -> MyData:
            return MyData()

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        result = repo.materialize(["my_asset"])
        assert result.success

    def test_custom_class_wrong(self):
        class MyData:
            pass

        @rs.Asset
        def my_asset() -> MyData:
            return 42  # type: ignore

        repo = rs.CodeRepository(assets=[my_asset])
        repo.resolve()
        with pytest.raises(AssetOutputValidationError, match="my_asset"):
            repo.materialize(["my_asset"])


# ---------------------------------------------------------------------------
# Type checking in dependency chains
# ---------------------------------------------------------------------------


class TestDependencyChains:
    """Test type checking works correctly in multi-asset pipelines."""

    def test_upstream_type_mismatch_stops_execution(self):
        """If upstream fails type check, downstream should not run."""

        @rs.Asset
        def upstream() -> int:
            return "wrong type"  # type: ignore

        @rs.Asset
        def downstream(upstream: Any) -> str:
            return str(upstream)

        repo = rs.CodeRepository(assets=[upstream, downstream])
        repo.resolve()
        with pytest.raises(AssetOutputValidationError, match="upstream"):
            repo.materialize(["upstream", "downstream"])

    def test_downstream_type_check_with_correct_upstream(self):
        @rs.Asset
        def upstream() -> int:
            return 42

        @rs.Asset
        def downstream(upstream: Any) -> str:
            return str(upstream)

        repo = rs.CodeRepository(assets=[upstream, downstream])
        repo.resolve()
        result = repo.materialize(["upstream", "downstream"])
        assert result.success

    def test_type_error_message_contains_asset_name(self):
        """The error message should clearly identify which asset failed."""

        @rs.Asset
        def bad_asset() -> int:
            return "oops"  # type: ignore

        repo = rs.CodeRepository(assets=[bad_asset])
        repo.resolve()
        with pytest.raises(AssetOutputValidationError, match="bad_asset"):
            repo.materialize(["bad_asset"])

    def test_type_error_message_contains_type_info(self):
        """The error message should describe the expected and actual types."""

        @rs.Asset
        def bad_asset() -> int:
            return "oops"  # type: ignore

        repo = rs.CodeRepository(assets=[bad_asset])
        repo.resolve()
        with pytest.raises(AssetOutputValidationError, match="str"):
            repo.materialize(["bad_asset"])


# ---------------------------------------------------------------------------
# Every executor checks return types
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_every_executor_checks_the_return_type(storage, executor_env, is_async):
    """The parallel executor checks a sync step in its loky worker."""
    executor, io_factory = executor_env

    if is_async:

        @rs.Asset(name="bad", io_handler=io_factory())
        async def bad_async() -> int:
            return "oops"  # type: ignore

        bad = bad_async
    else:

        @rs.Asset(io_handler=io_factory())
        def bad() -> int:
            return "oops"  # type: ignore

    _, result = materialize_beside_sibling(storage, executor, io_factory(), bad)

    assert not result.success
    assert dict(result.failed_assets)["bad"] == (
        "AssetOutputValidationError: Asset 'bad' returned value of type 'str' "
        "but expected '<class 'int'>'"
    )
