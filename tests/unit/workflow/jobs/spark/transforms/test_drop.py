"""Tests for Drop transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual
from samara.workflow.jobs.models.transforms.model_drop import DropArgs
from samara.workflow.jobs.spark.transforms.drop import DropFunction

# =========================================================================== #
# ============================== TEST DATA ================================= #
# =========================================================================== #


@pytest.fixture
def employees_df(spark: SparkSession) -> DataFrame:
    """Create a simple employees DataFrame for testing drop operations."""
    data = [
        (1, "Alice", 25, "Engineering", "temp_data"),
        (2, "Bob", 30, "Sales", "temp_data"),
        (3, "Charlie", 35, "Engineering", "temp_data"),
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("department", StringType(), True),
            StructField("temp_col", StringType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="drop_config")
def fixture_drop_config() -> dict[str, Any]:
    """Configuration dict for DropFunction."""
    return {"function_type": "drop", "arguments": {"columns": ["temp_col"]}}


def test_drop_creation__from_config__creates_valid_model(drop_config: dict[str, Any]) -> None:
    """Ensure DropFunction can be created from a config dict."""
    f = DropFunction(**drop_config)
    assert f.function_type == "drop"
    assert isinstance(f.arguments, DropArgs)
    assert f.arguments.columns == ["temp_col"]


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="drop_func")
def fixture_drop_func(drop_config: dict[str, Any]) -> DropFunction:
    """Instantiate DropFunction from config."""
    return DropFunction(**drop_config)


def test_drop_fixture(drop_func: DropFunction) -> None:
    """Check the instantiated object fixture contains the expected columns."""
    assert drop_func.function_type == "drop"
    assert isinstance(drop_func.arguments, DropArgs)
    assert drop_func.arguments.columns == ["temp_col"]


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestDropFunctionTransform:
    """Test DropFunction transform behavior with real Spark DataFrames."""

    def test_transform__returns_callable(self, drop_func: DropFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = drop_func.transform()

        # Assert
        assert callable(transform_fn)

    def test_transform__drops_single_column(
        self, spark: SparkSession, drop_func: DropFunction, employees_df: DataFrame
    ) -> None:
        """Test transform drops a single column correctly."""
        # Arrange
        expected_data = [
            (1, "Alice", 25, "Engineering"),
            (2, "Bob", 30, "Sales"),
            (3, "Charlie", 35, "Engineering"),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("department", StringType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = drop_func.transform()
        result_df = transform_fn(employees_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__drops_multiple_columns(self, spark: SparkSession, employees_df: DataFrame) -> None:
        """Test transform drops multiple columns correctly."""
        # Arrange
        config = {"function_type": "drop", "arguments": {"columns": ["age", "temp_col"]}}
        drop_func = DropFunction(**config)

        expected_data = [
            (1, "Alice", "Engineering"),
            (2, "Bob", "Sales"),
            (3, "Charlie", "Engineering"),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("department", StringType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = drop_func.transform()
        result_df = transform_fn(employees_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)
