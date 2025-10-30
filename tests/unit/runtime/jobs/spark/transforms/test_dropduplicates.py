"""Tests for DropDuplicates transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from samara.runtime.jobs.models.transforms.model_dropduplicates import DropDuplicatesArgs
from samara.runtime.jobs.spark.transforms.dropduplicates import DropDuplicatesFunction

# =========================================================================== #
# ============================== TEST DATA ================================= #
# =========================================================================== #


@pytest.fixture
def people_with_duplicates_df(spark: SparkSession) -> DataFrame:
    """Create a DataFrame with duplicate rows for testing."""
    data = [
        (1, "Alice", "Engineering"),
        (2, "Bob", "Sales"),
        (1, "Alice", "Engineering"),  # Duplicate of first row
        (3, "Charlie", "Engineering"),
        (2, "Bob", "Sales"),  # Duplicate of second row
        (4, "Diana", "Sales"),
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("department", StringType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="dropduplicates_config")
def fixture_dropduplicates_config() -> dict[str, Any]:
    """Return a config dict for DropDuplicatesFunction."""
    return {"function_type": "dropDuplicates", "arguments": {"columns": ["id"]}}


def test_dropduplicates_creation__from_config__creates_valid_model(dropduplicates_config: dict[str, Any]) -> None:
    """Ensure DropDuplicatesFunction can be created from a config dict."""
    f = DropDuplicatesFunction(**dropduplicates_config)
    assert f.function_type == "dropDuplicates"
    assert isinstance(f.arguments, DropDuplicatesArgs)
    assert f.arguments.columns == ["id"]


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="dropduplicates_func")
def fixture_dropduplicates_func(dropduplicates_config: dict[str, Any]) -> DropDuplicatesFunction:
    """Instantiate a DropDuplicatesFunction from the config dict.

    This object fixture is used by the object-based assertions below.
    """
    return DropDuplicatesFunction(**dropduplicates_config)


def test_dropduplicates_fixture(dropduplicates_func: DropDuplicatesFunction) -> None:
    """Assert the instantiated fixture has the expected columns list."""
    assert dropduplicates_func.function_type == "dropDuplicates"
    assert isinstance(dropduplicates_func.arguments, DropDuplicatesArgs)
    assert dropduplicates_func.arguments.columns == ["id"]


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestDropDuplicatesFunctionTransform:
    """Test DropDuplicatesFunction transform behavior with real Spark DataFrames."""

    def test_transform__returns_callable(self, dropduplicates_func: DropDuplicatesFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = dropduplicates_func.transform()

        # Assert
        assert callable(transform_fn)

    def test_transform__drops_duplicates_on_specific_columns(
        self, spark: SparkSession, dropduplicates_func: DropDuplicatesFunction, people_with_duplicates_df: DataFrame
    ) -> None:
        """Test transform drops duplicates based on specific columns."""
        # Arrange
        expected_data = [
            (1, "Alice", "Engineering"),
            (2, "Bob", "Sales"),
            (3, "Charlie", "Engineering"),
            (4, "Diana", "Sales"),
        ]
        expected_schema = people_with_duplicates_df.schema
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = dropduplicates_func.transform()
        result_df = transform_fn(people_with_duplicates_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df, checkRowOrder=False)

    def test_transform__with_empty_columns_list__drops_duplicates_on_all_columns(
        self, spark: SparkSession, people_with_duplicates_df: DataFrame
    ) -> None:
        """Test transform drops duplicates across all columns when columns list is empty."""
        # Arrange
        config = {"function_type": "dropDuplicates", "arguments": {"columns": []}}
        dropduplicates_func = DropDuplicatesFunction(**config)

        expected_data = [
            (1, "Alice", "Engineering"),
            (2, "Bob", "Sales"),
            (3, "Charlie", "Engineering"),
            (4, "Diana", "Sales"),
        ]
        expected_schema = people_with_duplicates_df.schema
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = dropduplicates_func.transform()
        result_df = transform_fn(people_with_duplicates_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df, checkRowOrder=False)

    def test_transform__with_multiple_columns(self, spark: SparkSession, people_with_duplicates_df: DataFrame) -> None:
        """Test transform drops duplicates based on multiple columns."""
        # Arrange
        config = {"function_type": "dropDuplicates", "arguments": {"columns": ["name", "department"]}}
        dropduplicates_func = DropDuplicatesFunction(**config)

        expected_data = [
            (1, "Alice", "Engineering"),
            (2, "Bob", "Sales"),
            (3, "Charlie", "Engineering"),
            (4, "Diana", "Sales"),
        ]
        expected_schema = people_with_duplicates_df.schema
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = dropduplicates_func.transform()
        result_df = transform_fn(people_with_duplicates_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df, checkRowOrder=False)
