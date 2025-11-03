"""Tests for GroupBy transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from samara.exceptions import SamaraWorkflowError
from samara.workflow.jobs.models.transforms.model_groupby import GroupByArgs
from samara.workflow.jobs.spark.transforms.groupby import GroupByFunction

# =========================================================================== #
# ============================== TEST DATA ================================= #
# =========================================================================== #


@pytest.fixture
def employees_df(spark: SparkSession) -> DataFrame:
    """Create employees DataFrame for groupby testing."""
    data = [
        ("Engineering", "Alice", 70000),
        ("Engineering", "Bob", 80000),
        ("Sales", "Charlie", 60000),
        ("Sales", "Diana", 65000),
        ("Sales", "Eve", 55000),
        ("HR", "Frank", 50000),
    ]
    schema = StructType(
        [
            StructField("department", StringType(), True),
            StructField("name", StringType(), True),
            StructField("salary", IntegerType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="groupby_config")
def fixture_groupby_config() -> dict[str, Any]:
    """Configuration dict for GroupByFunction with all fields explicit."""
    return {
        "function_type": "groupby",
        "arguments": {
            "group_columns": ["department"],
            "aggregations": [
                {"function": "sum", "input_column": "salary", "output_column": "total_salary"},
                {"function": "count", "input_column": None, "output_column": "employee_count"},
            ],
        },
    }


def test_groupby_creation__from_config__creates_valid_model(groupby_config: dict[str, Any]) -> None:
    """Ensure the GroupByFunction can be created from a config dict with all fields."""
    f = GroupByFunction(**groupby_config)
    assert f.function_type == "groupby"
    assert isinstance(f.arguments, GroupByArgs)
    assert f.arguments.group_columns == ["department"]
    assert len(f.arguments.aggregations) == 2
    assert f.arguments.aggregations[0].function == "sum"
    assert f.arguments.aggregations[0].input_column == "salary"
    assert f.arguments.aggregations[0].output_column == "total_salary"
    assert f.arguments.aggregations[1].function == "count"
    assert f.arguments.aggregations[1].input_column is None
    assert f.arguments.aggregations[1].output_column == "employee_count"


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestGroupByFunctionValidation:
    """Test GroupByFunction model validation and instantiation."""

    def test_create_groupby_function__with_wrong_function_name__raises_validation_error(
        self, groupby_config: dict[str, Any]
    ) -> None:
        """Test GroupByFunction creation fails with wrong function name."""
        groupby_config["function_type"] = "wrong_name"

        with pytest.raises(ValidationError):
            GroupByFunction(**groupby_config)

    def test_create_groupby_function__with_missing_arguments__raises_validation_error(
        self, groupby_config: dict[str, Any]
    ) -> None:
        """Test GroupByFunction creation fails without arguments field."""
        del groupby_config["arguments"]

        with pytest.raises(ValidationError):
            GroupByFunction(**groupby_config)

    def test_create_groupby_function__with_empty_group_columns__raises_validation_error(
        self, groupby_config: dict[str, Any]
    ) -> None:
        """Test GroupByFunction creation fails with empty group_columns list."""
        groupby_config["arguments"]["group_columns"] = []

        with pytest.raises(ValidationError):
            GroupByFunction(**groupby_config)

    def test_create_groupby_function__with_empty_aggregations__raises_validation_error(
        self, groupby_config: dict[str, Any]
    ) -> None:
        """Test GroupByFunction creation fails with empty aggregations list."""
        groupby_config["arguments"]["aggregations"] = []

        with pytest.raises(ValidationError):
            GroupByFunction(**groupby_config)

    def test_create_groupby_function__with_invalid_aggregate_function__raises_validation_error(
        self, groupby_config: dict[str, Any]
    ) -> None:
        """Test GroupByFunction creation fails with invalid aggregate function."""
        groupby_config["arguments"]["aggregations"][0]["function"] = "invalid_func"

        with pytest.raises(ValidationError):
            GroupByFunction(**groupby_config)

    def test_create_groupby_function__with_missing_output_column__raises_validation_error(
        self, groupby_config: dict[str, Any]
    ) -> None:
        """Test GroupByFunction creation fails with missing output_column."""
        del groupby_config["arguments"]["aggregations"][0]["output_column"]

        with pytest.raises(ValidationError):
            GroupByFunction(**groupby_config)

    def test_create_groupby_function__with_missing_input_column_field__raises_validation_error(
        self, groupby_config: dict[str, Any]
    ) -> None:
        """Test GroupByFunction creation fails when input_column field is missing entirely."""
        del groupby_config["arguments"]["aggregations"][0]["input_column"]

        with pytest.raises(ValidationError):
            GroupByFunction(**groupby_config)

    def test_create_groupby_function__count_with_explicit_null_input_column__succeeds(self) -> None:
        """Test GroupByFunction creation succeeds for count with explicitly null input_column."""
        config = {
            "function_type": "groupby",
            "arguments": {
                "group_columns": ["category"],
                "aggregations": [{"function": "count", "input_column": None, "output_column": "count_all"}],
            },
        }

        # Act
        groupby_function = GroupByFunction(**config)

        # Assert
        assert groupby_function.arguments.aggregations[0].input_column is None

    def test_create_groupby_function__non_count_with_null_input_column__succeeds_model_validation(self) -> None:
        """Test GroupByFunction creation allows null input_column for non-count (validated at workflow)."""
        config = {
            "function_type": "groupby",
            "arguments": {
                "group_columns": ["category"],
                "aggregations": [{"function": "sum", "input_column": None, "output_column": "sum_result"}],
            },
        }

        # Act - should create successfully (validation happens at workflow)
        groupby_function = GroupByFunction(**config)

        # Assert
        assert groupby_function.arguments.aggregations[0].input_column is None

    def test_create_groupby_function__with_missing_group_columns__raises_validation_error(
        self, groupby_config: dict[str, Any]
    ) -> None:
        """Test GroupByFunction creation fails with missing group_columns field."""
        del groupby_config["arguments"]["group_columns"]

        with pytest.raises(ValidationError):
            GroupByFunction(**groupby_config)

    def test_create_groupby_function__with_missing_aggregations__raises_validation_error(
        self, groupby_config: dict[str, Any]
    ) -> None:
        """Test GroupByFunction creation fails with missing aggregations field."""
        del groupby_config["arguments"]["aggregations"]

        with pytest.raises(ValidationError):
            GroupByFunction(**groupby_config)

    def test_create_groupby_function__with_empty_output_column__raises_validation_error(
        self, groupby_config: dict[str, Any]
    ) -> None:
        """Test GroupByFunction creation fails with empty output_column string."""
        groupby_config["arguments"]["aggregations"][0]["output_column"] = ""

        with pytest.raises(ValidationError):
            GroupByFunction(**groupby_config)

    def test_create_groupby_function__with_all_supported_functions__succeeds(self) -> None:
        """Test GroupByFunction creation succeeds with all supported aggregate functions."""
        config = {
            "function_type": "groupby",
            "arguments": {
                "group_columns": ["category"],
                "aggregations": [
                    {"function": "sum", "input_column": "amount", "output_column": "total"},
                    {"function": "avg", "input_column": "score", "output_column": "avg_score"},
                    {"function": "mean", "input_column": "score", "output_column": "mean_score"},
                    {"function": "min", "input_column": "price", "output_column": "min_price"},
                    {"function": "max", "input_column": "price", "output_column": "max_price"},
                    {"function": "count", "input_column": None, "output_column": "count"},
                    {"function": "first", "input_column": "name", "output_column": "first_name"},
                    {"function": "last", "input_column": "name", "output_column": "last_name"},
                    {"function": "stddev", "input_column": "value", "output_column": "stddev_value"},
                    {"function": "variance", "input_column": "value", "output_column": "var_value"},
                ],
            },
        }

        # Act
        groupby_function = GroupByFunction(**config)

        # Assert
        assert len(groupby_function.arguments.aggregations) == 10


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="groupby_func")
def fixture_groupby_func(groupby_config: dict[str, Any]) -> GroupByFunction:
    """Instantiate a GroupByFunction from the config dict.

    The object fixture is used to assert workflow field values without
    re-creating the config.
    """
    return GroupByFunction(**groupby_config)


def test_groupby_fixture(groupby_func: GroupByFunction) -> None:
    """Sanity-check the instantiated fixture has the expected arguments."""
    assert groupby_func.function_type == "groupby"
    assert isinstance(groupby_func.arguments, GroupByArgs)
    assert groupby_func.arguments.group_columns == ["department"]
    assert len(groupby_func.arguments.aggregations) == 2
    # Verify explicit null is preserved
    assert groupby_func.arguments.aggregations[1].input_column is None


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestGroupByFunctionTransform:
    """Test GroupByFunction transform behavior with real Spark DataFrames."""

    def test_transform__returns_callable(self, groupby_func: GroupByFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = groupby_func.transform()

        # Assert
        assert callable(transform_fn)

    def test_transform__single_group_column_with_aggregations(
        self, spark: SparkSession, employees_df: DataFrame
    ) -> None:
        """Test groupby with single column and multiple aggregations using real DataFrame."""
        # Arrange
        config = {
            "function_type": "groupby",
            "arguments": {
                "group_columns": ["department"],
                "aggregations": [
                    {"function": "sum", "input_column": "salary", "output_column": "total_salary"},
                    {"function": "avg", "input_column": "salary", "output_column": "avg_salary"},
                    {"function": "count", "input_column": None, "output_column": "employee_count"},
                ],
            },
        }
        groupby_func = GroupByFunction(**config)

        # Expected result
        expected_data = [
            ("Engineering", 150000, 75000.0, 2),
            ("HR", 50000, 50000.0, 1),
            ("Sales", 180000, 60000.0, 3),
        ]
        expected_schema = StructType(
            [
                StructField("department", StringType(), True),
                StructField("total_salary", LongType(), True),
                StructField("avg_salary", DoubleType(), True),
                StructField("employee_count", LongType(), False),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = groupby_func.transform()
        result_df = transform_fn(employees_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df, checkRowOrder=False)

    def test_transform__with_min_max_aggregations(self, spark: SparkSession, employees_df: DataFrame) -> None:
        """Test groupby with min/max aggregations using real DataFrame."""
        # Arrange
        config = {
            "function_type": "groupby",
            "arguments": {
                "group_columns": ["department"],
                "aggregations": [
                    {"function": "min", "input_column": "salary", "output_column": "min_salary"},
                    {"function": "max", "input_column": "salary", "output_column": "max_salary"},
                ],
            },
        }
        groupby_func = GroupByFunction(**config)

        # Expected result
        expected_data = [
            ("Engineering", 70000, 80000),
            ("HR", 50000, 50000),
            ("Sales", 55000, 65000),
        ]
        expected_schema = StructType(
            [
                StructField("department", StringType(), True),
                StructField("min_salary", IntegerType(), True),
                StructField("max_salary", IntegerType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = groupby_func.transform()
        result_df = transform_fn(employees_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df, checkRowOrder=False)

    def test_transform__count_with_non_null_input_column__raises_workflow_error(self, employees_df: DataFrame) -> None:
        """Test transform raises error when count has non-null input_column."""
        # Arrange
        config = {
            "function_type": "groupby",
            "arguments": {
                "group_columns": ["department"],
                "aggregations": [{"function": "count", "input_column": "salary", "output_column": "total"}],
            },
        }
        groupby_func = GroupByFunction(**config)
        transform_fn = groupby_func.transform()

        # Act & Assert
        with pytest.raises(SamaraWorkflowError):
            result_df = transform_fn(employees_df)
            result_df.collect()  # Force evaluation

    def test_transform__non_count_with_null_input_column__raises_workflow_error(self, employees_df: DataFrame) -> None:
        """Test transform raises error when non-count function has null input_column."""
        # Arrange
        config = {
            "function_type": "groupby",
            "arguments": {
                "group_columns": ["department"],
                "aggregations": [{"function": "sum", "input_column": None, "output_column": "total"}],
            },
        }
        groupby_func = GroupByFunction(**config)
        transform_fn = groupby_func.transform()

        # Act & Assert
        with pytest.raises(SamaraWorkflowError):
            result_df = transform_fn(employees_df)
            result_df.collect()  # Force evaluation

    def test_transform__with_count_only(self, spark: SparkSession, employees_df: DataFrame) -> None:
        """Test groupby with count aggregation only."""
        # Arrange
        config = {
            "function_type": "groupby",
            "arguments": {
                "group_columns": ["department"],
                "aggregations": [{"function": "count", "input_column": None, "output_column": "total_count"}],
            },
        }
        groupby_func = GroupByFunction(**config)

        # Expected result
        expected_data = [
            ("Engineering", 2),
            ("HR", 1),
            ("Sales", 3),
        ]
        expected_schema = StructType(
            [
                StructField("department", StringType(), True),
                StructField("total_count", LongType(), False),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = groupby_func.transform()
        result_df = transform_fn(employees_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df, checkRowOrder=False)
