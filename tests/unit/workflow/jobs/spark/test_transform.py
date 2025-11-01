"""Tests for TransformSpark functionality.

These tests verify TransformSpark creation, validation, data transformation,
and error handling scenarios.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import ExitStack
from typing import Any

import pytest
from pydantic import ValidationError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual
from samara.workflow.jobs.spark.transform import TransformSpark

# =========================================================================== #
# ============================== TEST DATA ================================= #
# =========================================================================== #


@pytest.fixture
def sample_df(spark: SparkSession) -> DataFrame:
    """Create a sample DataFrame for transformation testing."""
    data = [
        (1, "Alice", 30),
        (2, "Bob", 25),
        (3, "Charlie", 35),
        (4, "Diana", 28),
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="transform_config")
def fixture_transform_config() -> Generator[dict[str, Any], Any, None]:
    """Provide a transform configuration dict.

    Returning a Generator mirrors the `test_extract.py` layout and keeps the
    fixtures consistent across the Spark tests.
    """
    # Using ExitStack here keeps the pattern consistent even when no tempfiles
    # are required for this fixture.
    stack = ExitStack()

    data: dict[str, Any] = {
        "id": "customer_transform",
        "upstream_id": "customer_data",
        "options": {"spark.sql.shuffle.partitions": "10"},
        "functions": [],
    }

    yield data
    stack.close()


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestTransformSparkValidation:
    """Test TransformSpark model validation and instantiation."""

    def test_create_transform_spark__with_valid_config__succeeds(self, transform_config: dict[str, Any]) -> None:
        """Test TransformSpark creation with valid configuration."""
        # Act
        transform = TransformSpark(**transform_config)

        # Assert
        assert transform.id_ == "customer_transform"
        assert transform.upstream_id == "customer_data"
        assert transform.options == {"spark.sql.shuffle.partitions": "10"}
        assert isinstance(transform.functions, list)
        assert len(transform.functions) == 0

    def test_create_transform_spark__with_missing_functions__raises_validation_error(
        self, transform_config: dict[str, Any]
    ) -> None:
        """Test TransformSpark creation fails when functions field is missing."""
        # Arrange
        del transform_config["functions"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            TransformSpark(**transform_config)

    def test_create_transform_spark__with_missing_name__raises_validation_error(
        self, transform_config: dict[str, Any]
    ) -> None:
        """Test TransformSpark creation fails when name is missing."""
        # Arrange
        del transform_config["id"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            TransformSpark(**transform_config)

    def test_create_transform_spark__with_empty_name__raises_validation_error(
        self, transform_config: dict[str, Any]
    ) -> None:
        """Test TransformSpark creation fails with empty name."""
        # Arrange
        transform_config["id"] = ""

        # Assert
        with pytest.raises(ValidationError):
            # Act
            TransformSpark(**transform_config)

    def test_create_transform_spark__with_missing_upstream_id__raises_validation_error(
        self, transform_config: dict[str, Any]
    ) -> None:
        """Test TransformSpark creation fails when upstream_id is missing."""
        # Arrange
        del transform_config["upstream_id"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            TransformSpark(**transform_config)

    def test_create_transform_spark__with_missing_options__raises_validation_error(
        self, transform_config: dict[str, Any]
    ) -> None:
        """Test TransformSpark creation fails when options is missing."""
        # Arrange
        del transform_config["options"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            TransformSpark(**transform_config)

    def test_create_transform_spark__with_empty_options__succeeds(self, transform_config: dict[str, Any]) -> None:
        """Test TransformSpark creation with empty options dict succeeds."""
        # Arrange
        transform_config["options"] = {}

        # Act
        transform = TransformSpark(**transform_config)

        # Assert
        assert transform.options == {}

    def test_create_transform_spark__with_invalid_functions_type__raises_validation_error(
        self, transform_config: dict[str, Any]
    ) -> None:
        """Test TransformSpark creation fails when functions is not a list."""
        # Arrange
        transform_config["functions"] = "not-a-list"

        # Assert
        with pytest.raises(ValidationError):
            # Act
            TransformSpark(**transform_config)

    def test_create_transform_spark__with_function_list__succeeds(self, transform_config: dict[str, Any]) -> None:
        """Test TransformSpark creation with valid function list."""
        # Arrange
        transform_config["functions"] = [
            {"function_type": "select", "arguments": {"columns": ["id", "name"]}},
        ]

        # Act
        transform = TransformSpark(**transform_config)

        # Assert
        assert len(transform.functions) == 1
        assert transform.functions[0].function_type == "select"


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="transform_spark")
def fixture_transform_spark(transform_config: dict[str, Any]) -> TransformSpark:
    """Create TransformSpark instance from valid configuration."""
    return TransformSpark(**transform_config)


# =========================================================================== #
# ========================== TRANSFORM TESTS =============================== #
# =========================================================================== #


class TestTransformSparkTransform:
    """Test TransformSpark transformation functionality with real DataFrames."""

    def test_transform__with_empty_functions__completes_successfully(
        self, spark: SparkSession, transform_spark: TransformSpark, sample_df: DataFrame
    ) -> None:
        """Test transform method completes successfully with no transformation functions."""
        # Arrange - add real DataFrame to registry
        transform_spark.data_registry[transform_spark.upstream_id] = sample_df

        # Act
        transform_spark.transform()

        # Assert - dataframe should be copied to transform name
        result_df = transform_spark.data_registry[transform_spark.id_]
        assertDataFrameEqual(result_df, sample_df)

    def test_transform__with_single_function__applies_transformation(
        self, spark: SparkSession, transform_config: dict[str, Any], sample_df: DataFrame
    ) -> None:
        """Test transform method applies single transformation function with real DataFrame."""
        # Arrange - configure a select transformation
        transform_config["functions"] = [
            {"function_type": "select", "arguments": {"columns": ["id", "name"]}},
        ]
        transform = TransformSpark(**transform_config)
        transform.data_registry[transform.upstream_id] = sample_df

        # Expected result after selecting only id and name
        expected_data = [
            (1, "Alice"),
            (2, "Bob"),
            (3, "Charlie"),
            (4, "Diana"),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform.transform()

        # Assert
        result_df = transform.data_registry[transform.id_]
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__with_multiple_functions__applies_in_sequence(
        self, spark: SparkSession, transform_config: dict[str, Any], sample_df: DataFrame
    ) -> None:
        """Test transform method applies multiple transformation functions in sequence with real DataFrames."""
        # Arrange - configure select then filter transformations
        transform_config["functions"] = [
            {"function_type": "select", "arguments": {"columns": ["id", "name", "age"]}},
            {"function_type": "filter", "arguments": {"condition": "age > 27"}},
        ]
        transform = TransformSpark(**transform_config)
        transform.data_registry[transform.upstream_id] = sample_df

        # Expected result after select (all columns) then filter (age > 27)
        expected_data = [
            (1, "Alice", 30),
            (3, "Charlie", 35),
            (4, "Diana", 28),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform.transform()

        # Assert - final result should have filtered rows
        result_df = transform.data_registry[transform.id_]
        assertDataFrameEqual(result_df, expected_df, checkRowOrder=False)
