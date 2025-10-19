"""Tests for OrderBy transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest

from samara.runtime.jobs.models.transforms.model_orderby import OrderByArgs, OrderByColumn
from samara.runtime.jobs.spark.transforms.orderby import OrderByFunction

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="orderby_config")
def fixture_orderby_config() -> dict:
    """Return a config dict for OrderByFunction."""
    return {
        "function_type": "orderby",
        "arguments": {
            "columns": [
                {"column_name": "category", "order": "asc"},
                {"column_name": "sales", "order": "desc"},
            ],
        },
    }


def test_orderby_creation__from_config__creates_valid_model(orderby_config: dict[str, Any]) -> None:
    """Instantiate from config only to test dict-based initialization."""
    f = OrderByFunction(**orderby_config)
    assert f.function_type == "orderby"
    assert isinstance(f.arguments, OrderByArgs)
    assert len(f.arguments.columns) == 2
    assert isinstance(f.arguments.columns[0], OrderByColumn)


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="orderby_func")
def fixture_orderby_func(orderby_config: dict[str, Any]) -> OrderByFunction:
    """Instantiate an OrderByFunction from the config dict."""
    return OrderByFunction(**orderby_config)


def test_orderby_fixture(orderby_func: OrderByFunction) -> None:
    """Assert the instantiated fixture has the expected configuration."""
    assert orderby_func.function_type == "orderby"
    assert isinstance(orderby_func.arguments, OrderByArgs)
    assert len(orderby_func.arguments.columns) == 2
    assert orderby_func.arguments.columns[0].column_name == "category"
    assert orderby_func.arguments.columns[0].order == "asc"
    assert orderby_func.arguments.columns[1].column_name == "sales"
    assert orderby_func.arguments.columns[1].order == "desc"


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestOrderByFunctionTransform:
    """Test OrderByFunction transform behavior."""

    def test_transform__returns_callable(self, orderby_func: OrderByFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = orderby_func.transform()

        # Assert
        assert callable(transform_fn)


# =========================================================================== #
# ============================ VALIDATION TESTS ============================= #
# =========================================================================== #


class TestOrderByFunctionValidation:
    """Test OrderByFunction validation."""

    def test_validation__empty_columns__raises_error(self) -> None:
        """Test that empty columns list raises ValidationError."""
        # Arrange
        config = {
            "function_type": "orderby",
            "arguments": {
                "columns": [],  # Empty list
            },
        }

        # Assert
        with pytest.raises(ValueError):
            # Act
            OrderByFunction(**config)

    def test_validation__default_order_is_asc(self) -> None:
        """Test that order is mandatory - cannot omit it."""
        # Arrange
        config = {
            "function_type": "orderby",
            "arguments": {
                "columns": [
                    {"column_name": "name"},  # No order specified
                ],
            },
        }

        # Assert - should raise error since order is mandatory
        with pytest.raises(ValueError):
            # Act
            OrderByFunction(**config)

    def test_validation__invalid_order__raises_error(self) -> None:
        """Test that invalid order value raises ValidationError."""
        # Arrange
        config = {
            "function_type": "orderby",
            "arguments": {
                "columns": [
                    {"column_name": "name", "order": "invalid"},  # Invalid order
                ],
            },
        }

        # Assert
        with pytest.raises(ValueError):
            # Act
            OrderByFunction(**config)

    def test_validation__multiple_columns_with_mixed_orders(self) -> None:
        """Test that multiple columns with different orders are valid."""
        # Arrange
        config = {
            "function_type": "orderby",
            "arguments": {
                "columns": [
                    {"column_name": "category", "order": "asc"},
                    {"column_name": "sales", "order": "desc"},
                    {"column_name": "region", "order": "asc"},
                ],
            },
        }

        # Act
        f = OrderByFunction(**config)

        # Assert
        assert len(f.arguments.columns) == 3
        assert f.arguments.columns[0].order == "asc"
        assert f.arguments.columns[1].order == "desc"
        assert f.arguments.columns[2].order == "asc"

    def test_validation__empty_column_name__raises_error(self) -> None:
        """Test that empty column_name raises ValidationError."""
        # Arrange
        config = {
            "function_type": "orderby",
            "arguments": {
                "columns": [
                    {"column_name": "", "order": "asc"},  # Empty column_name
                ],
            },
        }

        # Assert
        with pytest.raises(ValueError):
            # Act
            OrderByFunction(**config)

    def test_validation__single_column_asc(self) -> None:
        """Test that single column with asc order is valid."""
        # Arrange
        config = {
            "function_type": "orderby",
            "arguments": {
                "columns": [
                    {"column_name": "name", "order": "asc"},
                ],
            },
        }

        # Act
        f = OrderByFunction(**config)

        # Assert
        assert len(f.arguments.columns) == 1
        assert f.arguments.columns[0].column_name == "name"
        assert f.arguments.columns[0].order == "asc"

    def test_validation__single_column_desc(self) -> None:
        """Test that single column with desc order is valid."""
        # Arrange
        config = {
            "function_type": "orderby",
            "arguments": {
                "columns": [
                    {"column_name": "sales", "order": "desc"},
                ],
            },
        }

        # Act
        f = OrderByFunction(**config)

        # Assert
        assert len(f.arguments.columns) == 1
        assert f.arguments.columns[0].column_name == "sales"
        assert f.arguments.columns[0].order == "desc"
