"""Tests for Select transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual
from samara.workflow.jobs.models.transforms.model_select import SelectArgs
from samara.workflow.jobs.spark.transforms.select import SelectFunction

# =========================================================================== #
# ============================== TEST DATA ================================= #
# =========================================================================== #


@pytest.fixture
def employees_df(spark: SparkSession) -> DataFrame:
    """Create a simple employees DataFrame for testing select operations."""
    data = [
        (1, "Alice", 25, "Engineering", 70000),
        (2, "Bob", 30, "Sales", 60000),
        (3, "Charlie", 35, "Engineering", 80000),
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("department", StringType(), True),
            StructField("salary", IntegerType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="select_config")
def fixture_select_config() -> dict[str, Any]:
    """Return a config dict for SelectFunction."""
    return {"function_type": "select", "arguments": {"columns": ["id", "name"]}}


def test_select_creation__from_config__creates_valid_model(select_config: dict[str, Any]) -> None:
    """Instantiate from config only to test dict-based initialization."""
    f = SelectFunction(**select_config)
    assert f.function_type == "select"
    assert isinstance(f.arguments, SelectArgs)
    assert f.arguments.columns == ["id", "name"]


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="select_func")
def select_func(select_config: dict[str, Any]) -> SelectFunction:
    """Instantiate a SelectFunction from the config dict."""
    return SelectFunction(**select_config)


def test_select_fixture(select_func: SelectFunction) -> None:
    """Assert the instantiated fixture has expected columns."""
    assert select_func.function_type == "select"
    assert isinstance(select_func.arguments, SelectArgs)
    assert select_func.arguments.columns == ["id", "name"]


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestSelectFunctionTransform:
    """Test SelectFunction transform behavior with real Spark DataFrames."""

    def test_transform__returns_callable(self, select_func: SelectFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = select_func.transform()

        # Assert
        assert callable(transform_fn)

    def test_transform__applies_select_operation(
        self, spark: SparkSession, select_func: SelectFunction, employees_df: DataFrame
    ) -> None:
        """Test transform applies select with correct columns using real DataFrame."""
        # Arrange
        expected_data = [
            (1, "Alice"),
            (2, "Bob"),
            (3, "Charlie"),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = select_func.transform()
        result_df = transform_fn(employees_df)

        # Assert using assertDataFrameEqual
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__with_different_column_order__preserves_order(
        self, spark: SparkSession, employees_df: DataFrame
    ) -> None:
        """Test that column order in select is preserved."""
        # Arrange
        config = {"function_type": "select", "arguments": {"columns": ["department", "name", "id"]}}
        select_func = SelectFunction(**config)

        expected_data = [
            ("Engineering", "Alice", 1),
            ("Sales", "Bob", 2),
            ("Engineering", "Charlie", 3),
        ]
        expected_schema = StructType(
            [
                StructField("department", StringType(), True),
                StructField("name", StringType(), True),
                StructField("id", IntegerType(), False),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = select_func.transform()
        result_df = transform_fn(employees_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__with_single_column__returns_single_column(
        self, spark: SparkSession, employees_df: DataFrame
    ) -> None:
        """Test selecting a single column."""
        # Arrange
        config = {"function_type": "select", "arguments": {"columns": ["name"]}}
        select_func = SelectFunction(**config)

        expected_data = [("Alice",), ("Bob",), ("Charlie",)]
        expected_schema = StructType([StructField("name", StringType(), True)])
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = select_func.transform()
        result_df = transform_fn(employees_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)
