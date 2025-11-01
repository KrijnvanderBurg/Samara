"""Unit tests for FunctionSpark base class used in Spark transforms."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

import pytest
from samara.workflow.jobs.models.model_transform import ArgsModel
from samara.workflow.jobs.spark.transforms.base import FunctionSpark


class _DummyArgs(ArgsModel):
    """Minimal arguments model for testing DummyFunction."""

    test_arg: str | None = None


class DummyFunction(FunctionSpark):
    """Minimal concrete Function implementation for testing purposes.

    This dummy function is used to test the FunctionSpark base class
    without requiring actual Spark transformations.
    """

    function: Literal["dummy"]
    arguments: _DummyArgs

    def transform(self) -> Callable[..., Any | None]:
        """Return a callable that passes through the input unchanged.

        Returns:
            A callable function that returns its input as-is
        """

        def _f(df: Any = None) -> Any | None:
            return df

        return _f


# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="function_config")
def fixture_function_config() -> dict:
    """Provide configuration dictionary for creating a DummyFunction.

    Returns:
        Configuration dictionary with function type and arguments
    """
    return {"function": "dummy", "arguments": {"test_arg": "example"}}


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="function_obj")
def fixture_function_obj(function_config: dict[str, Any]) -> DummyFunction:
    """Provide a DummyFunction instance for testing.

    Returns:
        Configured DummyFunction instance
    """
    return DummyFunction(**function_config)


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestFunctionSparkCreation:
    """Tests for creating FunctionSpark instances."""

    def test_function_creation__from_config__creates_valid_model(self, function_config: dict) -> None:
        """Test that FunctionSpark can be instantiated from configuration dictionary."""
        # Act
        function = DummyFunction(**function_config)

        # Assert
        assert function.function == "dummy"
        assert isinstance(function.arguments, _DummyArgs)
        assert function.arguments.test_arg == "example"

    def test_function_fixture(self, function_obj: DummyFunction) -> None:
        """Test that function fixture is properly configured."""
        # Assert
        assert function_obj.function == "dummy"
        assert isinstance(function_obj.arguments, _DummyArgs)
        assert function_obj.arguments.test_arg == "example"


class TestFunctionSparkTransform:
    """Tests for FunctionSpark transform method behavior."""

    def test_function_fixture__transform_returns_input(self, function_obj: DummyFunction) -> None:
        """Test that the transform method returns a callable that passes through input."""
        # Act
        transform_fn = function_obj.transform()

        # Assert
        assert isinstance(transform_fn, Callable)
        sample = object()
        assert transform_fn(sample) is sample
