"""Tests for GroupBy transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import pytest

from samara.runtime.jobs.models.transforms.model_groupby import AggregationColumn, GroupByArgs
from samara.runtime.jobs.spark.transforms.groupby import GroupByFunction

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="groupby_config")
def fixture_groupby_config() -> dict:
    """Return a config dict for GroupByFunction."""
    return {
        "function_type": "groupby",
        "arguments": {
            "columns": ["category", "region"],
            "aggregations": [
                {"column_name": "sales", "agg_function": "sum", "alias": "total_sales"},
                {"column_name": "quantity", "agg_function": "avg", "alias": "avg_quantity"},
            ],
        },
    }


def test_groupby_creation__from_config__creates_valid_model(groupby_config: dict[str, Any]) -> None:
    """Instantiate from config only to test dict-based initialization."""
    f = GroupByFunction(**groupby_config)
    assert f.function_type == "groupby"
    assert isinstance(f.arguments, GroupByArgs)
    assert f.arguments.columns == ["category", "region"]
    assert len(f.arguments.aggregations) == 2
    assert isinstance(f.arguments.aggregations[0], AggregationColumn)


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="groupby_func")
def fixture_groupby_func(groupby_config: dict[str, Any]) -> GroupByFunction:
    """Instantiate a GroupByFunction from the config dict."""
    return GroupByFunction(**groupby_config)


def test_groupby_fixture(groupby_func: GroupByFunction) -> None:
    """Assert the instantiated fixture has the expected configuration."""
    assert groupby_func.function_type == "groupby"
    assert isinstance(groupby_func.arguments, GroupByArgs)
    assert groupby_func.arguments.columns == ["category", "region"]
    assert len(groupby_func.arguments.aggregations) == 2
    assert groupby_func.arguments.aggregations[0].column_name == "sales"
    assert groupby_func.arguments.aggregations[0].agg_function == "sum"
    assert groupby_func.arguments.aggregations[0].alias == "total_sales"


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestGroupByFunctionTransform:
    """Test GroupByFunction transform behavior."""

    def test_transform__returns_callable(self, groupby_func: GroupByFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = groupby_func.transform()

        # Assert
        assert callable(transform_fn)

    def test_transform__applies_groupby_and_aggregations(self, groupby_func: GroupByFunction) -> None:
        """Test transform applies groupBy with correct columns and aggregations."""
        # Arrange
        mock_df = Mock()
        mock_grouped = Mock()
        mock_result = Mock()

        mock_df.groupBy.return_value = mock_grouped
        mock_grouped.agg.return_value = mock_result
        mock_df.count.return_value = 100
        mock_result.count.return_value = 10
        mock_result.columns = ["category", "region", "total_sales", "avg_quantity"]

        # Act
        transform_fn = groupby_func.transform()
        result = transform_fn(mock_df)

        # Assert
        mock_df.groupBy.assert_called_once_with("category", "region")
        mock_grouped.agg.assert_called_once()
        assert result == mock_result

    def test_transform__with_single_column_groupby__applies_correctly(self) -> None:
        """Test transform with single groupBy column."""
        # Arrange
        config = {
            "function_type": "groupby",
            "arguments": {
                "columns": ["status"],
                "aggregations": [{"column_name": "id", "agg_function": "count", "alias": "count"}],
            },
        }
        func = GroupByFunction(**config)

        mock_df = Mock()
        mock_grouped = Mock()
        mock_result = Mock()

        mock_df.groupBy.return_value = mock_grouped
        mock_grouped.agg.return_value = mock_result
        mock_df.count.return_value = 50
        mock_result.count.return_value = 5
        mock_result.columns = ["status", "count"]

        # Act
        transform_fn = func.transform()
        result = transform_fn(mock_df)

        # Assert
        mock_df.groupBy.assert_called_once_with("status")
        assert result == mock_result

    def test_transform__with_default_alias__generates_correct_alias(self) -> None:
        """Test transform with empty alias works correctly."""
        # Arrange
        config = {
            "function_type": "groupby",
            "arguments": {
                "columns": ["department"],
                "aggregations": [
                    {"column_name": "salary", "agg_function": "max", "alias": ""},  # Empty alias
                ],
            },
        }
        func = GroupByFunction(**config)

        mock_df = Mock()
        mock_grouped = Mock()
        mock_result = Mock()

        mock_df.groupBy.return_value = mock_grouped
        mock_grouped.agg.return_value = mock_result
        mock_df.count.return_value = 100
        mock_result.count.return_value = 10
        mock_result.columns = ["department", "salary_max"]

        # Act
        transform_fn = func.transform()
        result = transform_fn(mock_df)

        # Assert
        mock_df.groupBy.assert_called_once_with("department")
        assert result == mock_result

    def test_transform__with_multiple_aggregations_on_same_column__applies_correctly(self) -> None:
        """Test transform with multiple aggregations on the same column."""
        # Arrange
        config = {
            "function_type": "groupby",
            "arguments": {
                "columns": ["product"],
                "aggregations": [
                    {"column_name": "price", "agg_function": "min", "alias": "min_price"},
                    {"column_name": "price", "agg_function": "max", "alias": "max_price"},
                    {"column_name": "price", "agg_function": "avg", "alias": "avg_price"},
                ],
            },
        }
        func = GroupByFunction(**config)

        mock_df = Mock()
        mock_grouped = Mock()
        mock_result = Mock()

        mock_df.groupBy.return_value = mock_grouped
        mock_grouped.agg.return_value = mock_result
        mock_df.count.return_value = 200
        mock_result.count.return_value = 20
        mock_result.columns = ["product", "min_price", "max_price", "avg_price"]

        # Act
        transform_fn = func.transform()
        transform_fn(mock_df)

        # Assert
        mock_df.groupBy.assert_called_once_with("product")
        # Verify agg was called with 3 expressions
        args = mock_grouped.agg.call_args[0]
        assert len(args) == 3

    def test_transform__with_count_aggregation__handles_correctly(self) -> None:
        """Test transform with count aggregation function."""
        # Arrange
        config = {
            "function_type": "groupby",
            "arguments": {
                "columns": ["country", "city"],
                "aggregations": [{"column_name": "*", "agg_function": "count", "alias": "record_count"}],
            },
        }
        func = GroupByFunction(**config)

        mock_df = Mock()
        mock_grouped = Mock()
        mock_result = Mock()

        mock_df.groupBy.return_value = mock_grouped
        mock_grouped.agg.return_value = mock_result
        mock_df.count.return_value = 1000
        mock_result.count.return_value = 50
        mock_result.columns = ["country", "city", "record_count"]

        # Act
        transform_fn = func.transform()
        transform_fn(mock_df)

        # Assert
        mock_df.groupBy.assert_called_once_with("country", "city")


class TestAggregationColumnModel:
    """Test AggregationColumn model validation."""

    def test_aggregation_column__with_all_fields__creates_valid_model(self) -> None:
        """Test AggregationColumn creation with all fields."""
        # Act
        agg = AggregationColumn(column_name="amount", agg_function="sum", alias="total_amount")

        # Assert
        assert agg.column_name == "amount"
        assert agg.agg_function == "sum"
        assert agg.alias == "total_amount"

    def test_aggregation_column__without_alias__creates_valid_model(self) -> None:
        """Test AggregationColumn creation with empty alias."""
        # Act
        agg = AggregationColumn(column_name="score", agg_function="avg", alias="")

        # Assert
        assert agg.column_name == "score"
        assert agg.agg_function == "avg"
        assert agg.alias == ""

    def test_aggregation_column__with_empty_column_name__raises_validation_error(self) -> None:
        """Test AggregationColumn raises error for empty column_name."""
        # Assert
        with pytest.raises(ValueError):
            # Act
            AggregationColumn(column_name="", agg_function="max")

    def test_aggregation_column__with_empty_agg_function__raises_validation_error(self) -> None:
        """Test AggregationColumn raises error for empty agg_function."""
        # Assert
        with pytest.raises(ValueError):
            # Act
            AggregationColumn(column_name="value", agg_function="")


class TestGroupByArgsModel:
    """Test GroupByArgs model validation."""

    def test_groupby_args__with_valid_data__creates_model(self) -> None:
        """Test GroupByArgs creation with valid data."""
        # Act
        args = GroupByArgs(
            columns=["region"],
            aggregations=[AggregationColumn(column_name="sales", agg_function="sum", alias="total_sales")],
        )

        # Assert
        assert len(args.columns) == 1
        assert args.columns[0] == "region"
        assert len(args.aggregations) == 1

    def test_groupby_args__with_empty_columns__raises_validation_error(self) -> None:
        """Test GroupByArgs raises error for empty columns list."""
        # Assert
        with pytest.raises(ValueError):
            # Act
            GroupByArgs(
                columns=[],
                aggregations=[AggregationColumn(column_name="sales", agg_function="sum")],
            )

    def test_groupby_args__with_empty_aggregations__raises_validation_error(self) -> None:
        """Test GroupByArgs raises error for empty aggregations list."""
        # Assert
        with pytest.raises(ValueError):
            # Act
            GroupByArgs(columns=["region"], aggregations=[])
