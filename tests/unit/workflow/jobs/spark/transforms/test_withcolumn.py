"""Tests for WithColumn transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual
from samara.workflow.jobs.models.transforms.model_withcolumn import WithColumnArgs
from samara.workflow.jobs.spark.transforms.withcolumn import WithColumnFunction

# =========================================================================== #
# ============================== TEST DATA ================================= #
# =========================================================================== #


@pytest.fixture
def people_df(spark: SparkSession) -> DataFrame:
    """Create a people DataFrame for testing withColumn operations."""
    data = [
        ("Alice", "Smith", 25, 50000),
        ("Bob", "Jones", 30, 60000),
        ("Charlie", "Brown", 35, 70000),
    ]
    schema = StructType(
        [
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", IntegerType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="withcolumn_config")
def fixture_withcolumn_config() -> dict[str, Any]:
    """Return a config dict for WithColumnFunction."""
    return {
        "function_type": "withColumn",
        "arguments": {"col_name": "full_name", "col_expr": "concat(first_name, ' ', last_name)"},
    }


def test_withcolumn_creation__from_config__creates_valid_model(withcolumn_config: dict[str, Any]) -> None:
    """Instantiate from config only to test dict-based initialization."""
    f = WithColumnFunction(**withcolumn_config)
    assert f.function_type == "withColumn"
    assert isinstance(f.arguments, WithColumnArgs)
    assert f.arguments.col_name == "full_name"
    assert f.arguments.col_expr.startswith("concat")


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="withcolumn_func")
def fixture_withcolumn_func(withcolumn_config: dict[str, Any]) -> WithColumnFunction:
    """Instantiate a WithColumnFunction from the config dict."""
    return WithColumnFunction(**withcolumn_config)


def test_withcolumn_fixture__values(withcolumn_func: WithColumnFunction) -> None:
    """Assert the instantiated fixture has expected column name and expression."""
    assert withcolumn_func.function_type == "withColumn"
    assert isinstance(withcolumn_func.arguments, WithColumnArgs)
    assert withcolumn_func.arguments.col_name == "full_name"
    assert withcolumn_func.arguments.col_expr.startswith("concat")


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestWithColumnFunctionTransform:
    """Test WithColumnFunction transform behavior with real Spark DataFrames."""

    def test_transform__returns_callable(self, withcolumn_func: WithColumnFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = withcolumn_func.transform()

        # Assert
        assert callable(transform_fn)

    def test_transform__adds_new_column_with_expression(
        self, spark: SparkSession, withcolumn_func: WithColumnFunction, people_df: DataFrame
    ) -> None:
        """Test transform adds new column with correct expression using real DataFrame."""
        # Arrange
        expected_data = [
            ("Alice", "Smith", 25, 50000, "Alice Smith"),
            ("Bob", "Jones", 30, 60000, "Bob Jones"),
            ("Charlie", "Brown", 35, 70000, "Charlie Brown"),
        ]
        expected_schema = StructType(
            [
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("salary", IntegerType(), True),
                StructField("full_name", StringType(), False),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = withcolumn_func.transform()
        result_df = transform_fn(people_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__with_arithmetic_expression(self, spark: SparkSession, people_df: DataFrame) -> None:
        """Test withColumn with arithmetic expression."""
        # Arrange
        config = {
            "function_type": "withColumn",
            "arguments": {"col_name": "annual_bonus", "col_expr": "CAST(salary * 0.1 AS DOUBLE)"},
        }
        withcolumn_func = WithColumnFunction(**config)

        expected_data = [
            ("Alice", "Smith", 25, 50000, 5000.0),
            ("Bob", "Jones", 30, 60000, 6000.0),
            ("Charlie", "Brown", 35, 70000, 7000.0),
        ]
        expected_schema = StructType(
            [
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("salary", IntegerType(), True),
                StructField("annual_bonus", DoubleType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = withcolumn_func.transform()
        result_df = transform_fn(people_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__replaces_existing_column(self, spark: SparkSession, people_df: DataFrame) -> None:
        """Test withColumn replaces existing column."""
        # Arrange
        config = {
            "function_type": "withColumn",
            "arguments": {"col_name": "age", "col_expr": "age + 1"},
        }
        withcolumn_func = WithColumnFunction(**config)

        expected_data = [
            ("Alice", "Smith", 26, 50000),
            ("Bob", "Jones", 31, 60000),
            ("Charlie", "Brown", 36, 70000),
        ]
        expected_schema = StructType(
            [
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("salary", IntegerType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = withcolumn_func.transform()
        result_df = transform_fn(people_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__with_conditional_expression(self, spark: SparkSession, people_df: DataFrame) -> None:
        """Test withColumn with conditional (CASE WHEN) expression."""
        # Arrange
        config = {
            "function_type": "withColumn",
            "arguments": {
                "col_name": "seniority",
                "col_expr": "CASE WHEN age >= 30 THEN 'Senior' ELSE 'Junior' END",
            },
        }
        withcolumn_func = WithColumnFunction(**config)

        expected_data = [
            ("Alice", "Smith", 25, 50000, "Junior"),
            ("Bob", "Jones", 30, 60000, "Senior"),
            ("Charlie", "Brown", 35, 70000, "Senior"),
        ]
        expected_schema = StructType(
            [
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("salary", IntegerType(), True),
                StructField("seniority", StringType(), False),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = withcolumn_func.transform()
        result_df = transform_fn(people_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)
