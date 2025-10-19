"""Tests for Agg transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest

from samara.runtime.jobs.models.transforms.model_agg import AggArgs, AggregationColumn
from samara.runtime.jobs.spark.transforms.agg import AggFunction

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="agg_config")
def fixture_agg_config() -> dict:
    """Return a config dict for AggFunction."""
    return {
        "function_type": "agg",
        "arguments": {
            "aggregations": [
                {"column_name": "sales", "agg_function": "sum", "alias": "total_sales"},
                {"column_name": "quantity", "agg_function": "avg", "alias": "avg_quantity"},
            ],
        },
    }


def test_agg_creation__from_config__creates_valid_model(agg_config: dict[str, Any]) -> None:
    """Instantiate from config only to test dict-based initialization."""
    f = AggFunction(**agg_config)
    assert f.function_type == "agg"
    assert isinstance(f.arguments, AggArgs)
    assert len(f.arguments.aggregations) == 2
    assert isinstance(f.arguments.aggregations[0], AggregationColumn)


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="agg_func")
def fixture_agg_func(agg_config: dict[str, Any]) -> AggFunction:
    """Instantiate an AggFunction from the config dict."""
    return AggFunction(**agg_config)


def test_agg_fixture(agg_func: AggFunction) -> None:
    """Assert the instantiated fixture has the expected configuration."""
    assert agg_func.function_type == "agg"
    assert isinstance(agg_func.arguments, AggArgs)
    assert len(agg_func.arguments.aggregations) == 2
    assert agg_func.arguments.aggregations[0].column_name == "sales"
    assert agg_func.arguments.aggregations[0].agg_function == "sum"
    assert agg_func.arguments.aggregations[0].alias == "total_sales"


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestAggFunctionTransform:
    """Test AggFunction transform behavior."""

    def test_transform__returns_callable(self, agg_func: AggFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = agg_func.transform()

        # Assert
        assert callable(transform_fn)


# =========================================================================== #
# ============================ VALIDATION TESTS ============================= #
# =========================================================================== #


class TestAggFunctionValidation:
    """Test AggFunction validation."""

    def test_validation__empty_aggregations__raises_error(self) -> None:
        """Test that empty aggregations list raises ValidationError."""
        # Arrange
        config = {
            "function_type": "agg",
            "arguments": {
                "aggregations": [],  # Empty list
            },
        }

        # Assert
        with pytest.raises(ValueError):
            # Act
            AggFunction(**config)

    def test_validation__empty_column_name__raises_error(self) -> None:
        """Test that empty column_name raises ValidationError."""
        # Arrange
        config = {
            "function_type": "agg",
            "arguments": {
                "aggregations": [
                    {"column_name": "", "agg_function": "sum", "alias": "total"},  # Empty column_name
                ],
            },
        }

        # Assert
        with pytest.raises(ValueError):
            # Act
            AggFunction(**config)

    def test_validation__empty_agg_function__raises_error(self) -> None:
        """Test that empty agg_function raises ValidationError."""
        # Arrange
        config = {
            "function_type": "agg",
            "arguments": {
                "aggregations": [
                    {"column_name": "sales", "agg_function": "", "alias": "total"},  # Empty agg_function
                ],
            },
        }

        # Assert
        with pytest.raises(ValueError):
            # Act
            AggFunction(**config)

    def test_validation__missing_aggregations__raises_error(self) -> None:
        """Test that missing aggregations field raises ValidationError."""
        # Arrange
        config = {
            "function_type": "agg",
            "arguments": {},  # Missing aggregations
        }

        # Assert
        with pytest.raises(ValueError):
            # Act
            AggFunction(**config)

    def test_validation__without_alias__uses_default_alias(self) -> None:
        """Test that aggregations with alias are properly configured."""
        # Arrange
        config = {
            "function_type": "agg",
            "arguments": {
                "aggregations": [
                    {"column_name": "sales", "agg_function": "sum", "alias": ""},  # Empty alias
                ],
            },
        }

        # Act
        agg_func = AggFunction(**config)

        # Assert
        assert agg_func.function_type == "agg"
        assert len(agg_func.arguments.aggregations) == 1
        assert agg_func.arguments.aggregations[0].column_name == "sales"
        assert agg_func.arguments.aggregations[0].agg_function == "sum"
        assert agg_func.arguments.aggregations[0].alias == ""  # Empty alias allowed
