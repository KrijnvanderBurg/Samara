"""Tests for Filter transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from samara.runtime.jobs.models.transforms.model_filter import FilterArgs
from samara.runtime.jobs.spark.transforms.filter import FilterFunction

# =========================================================================== #
# ============================== TEST DATA ================================= #
# =========================================================================== #


@pytest.fixture
def people_df(spark: SparkSession) -> DataFrame:
    """Create a people DataFrame for testing filter operations."""
    data = [
        (1, "Alice", 17),
        (2, "Bob", 25),
        (3, "Charlie", 30),
        (4, "Diana", 16),
        (5, "Eve", 22),
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="filter_config")
def fixture_filter_config() -> dict:
    """Return a config dict for FilterFunction."""
    return {"function_type": "filter", "arguments": {"condition": "age > 18"}}


def test_filter_creation__from_config__creates_valid_model(filter_config: dict[str, Any]) -> None:
    """Instantiate from config only to test dict-based initialization."""
    f = FilterFunction(**filter_config)
    assert f.function_type == "filter"
    assert isinstance(f.arguments, FilterArgs)
    assert f.arguments.condition == "age > 18"


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="filter_func")
def fixture_filter_func(filter_config: dict[str, Any]) -> FilterFunction:
    """Instantiate a FilterFunction from the config dict."""
    return FilterFunction(**filter_config)


def test_filter_fixture(filter_func: FilterFunction) -> None:
    """Assert the instantiated fixture has the expected condition."""
    assert filter_func.function_type == "filter"
    assert isinstance(filter_func.arguments, FilterArgs)
    assert filter_func.arguments.condition == "age > 18"


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestFilterFunctionTransform:
    """Test FilterFunction transform behavior with real Spark DataFrames."""

    def test_transform__returns_callable(self, filter_func: FilterFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = filter_func.transform()

        # Assert
        assert callable(transform_fn)

    def test_transform__applies_filter_condition(
        self, spark: SparkSession, filter_func: FilterFunction, people_df: DataFrame
    ) -> None:
        """Test transform applies filter using assertDataFrameEqual."""
        # Arrange
        expected_data = [
            (2, "Bob", 25),
            (3, "Charlie", 30),
            (5, "Eve", 22),
        ]
        expected_schema = people_df.schema
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = filter_func.transform()
        result_df = transform_fn(people_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__with_complex_condition(self, spark: SparkSession, people_df: DataFrame) -> None:
        """Test filter with complex SQL condition."""
        # Arrange
        config = {"function_type": "filter", "arguments": {"condition": "age > 20 AND name LIKE 'C%'"}}
        filter_func = FilterFunction(**config)

        expected_data = [(3, "Charlie", 30)]
        expected_schema = people_df.schema
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = filter_func.transform()
        result_df = transform_fn(people_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__with_no_matching_rows__returns_empty_dataframe(
        self, spark: SparkSession, people_df: DataFrame
    ) -> None:
        """Test filter with condition that matches no rows."""
        # Arrange
        config = {"function_type": "filter", "arguments": {"condition": "age > 100"}}
        filter_func = FilterFunction(**config)

        expected_df = spark.createDataFrame([], people_df.schema)

        # Act
        transform_fn = filter_func.transform()
        result_df = transform_fn(people_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__with_all_matching_rows__returns_all_rows(
        self, spark: SparkSession, people_df: DataFrame
    ) -> None:
        """Test filter with condition that matches all rows."""
        # Arrange
        config = {"function_type": "filter", "arguments": {"condition": "age > 0"}}
        filter_func = FilterFunction(**config)

        # Act
        transform_fn = filter_func.transform()
        result_df = transform_fn(people_df)

        # Assert
        assertDataFrameEqual(result_df, people_df)
