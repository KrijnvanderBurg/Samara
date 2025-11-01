"""Tests for Cast transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from samara.runtime.jobs.models.transforms.model_cast import CastArgs
from samara.runtime.jobs.spark.transforms.cast import CastFunction

# =========================================================================== #
# ============================== TEST DATA ================================= #
# =========================================================================== #


@pytest.fixture
def mixed_types_df(spark: SparkSession) -> DataFrame:
    """Create a DataFrame with mixed types for testing cast operations."""
    data = [
        ("1", "25.5", "100"),
        ("2", "30.0", "200"),
        ("3", "35.7", "300"),
    ]
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("age", StringType(), True),
            StructField("salary", StringType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="cast_config")
def fixture_cast_config() -> dict[str, Any]:
    """Configuration dict for CastFunction."""
    return {"function_type": "cast", "arguments": {"columns": [{"column_name": "age", "cast_type": "int"}]}}


def test_cast_creation__from_config__creates_valid_model(cast_config: dict[str, Any]) -> None:
    """Ensure the CastFunction can be created from a config dict."""
    f = CastFunction(**cast_config)
    assert f.function_type == "cast"
    assert isinstance(f.arguments, CastArgs)
    assert len(f.arguments.columns) == 1
    assert f.arguments.columns[0].column_name == "age"
    assert f.arguments.columns[0].cast_type == "int"


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestCastFunctionValidation:
    """Test CastFunction model validation and instantiation."""

    def test_create_cast_function__with_wrong_function_name__raises_validation_error(
        self, cast_config: dict[str, Any]
    ) -> None:
        """Test CastFunction creation fails with wrong function name."""
        cast_config["function_type"] = "wrong_name"

        with pytest.raises(ValidationError):
            CastFunction(**cast_config)

    def test_create_cast_function__with_missing_arguments__raises_validation_error(
        self, cast_config: dict[str, Any]
    ) -> None:
        """Test CastFunction creation fails without arguments field."""
        del cast_config["arguments"]

        with pytest.raises(ValidationError):
            CastFunction(**cast_config)

    def test_create_cast_function__with_empty_columns__succeeds(self, cast_config: dict[str, Any]) -> None:
        """Test CastFunction creation succeeds with empty columns list."""
        cast_config["arguments"]["columns"] = []

        # Act
        cast_function = CastFunction(**cast_config)

        # Assert
        assert cast_function.arguments.columns == []

    def test_create_cast_function__with_missing_column_name__raises_validation_error(
        self, cast_config: dict[str, Any]
    ) -> None:
        """Test CastFunction creation fails with missing column_name."""
        del cast_config["arguments"]["columns"][0]["column_name"]

        with pytest.raises(ValidationError):
            CastFunction(**cast_config)

    def test_create_cast_function__with_missing_cast_type__raises_validation_error(
        self, cast_config: dict[str, Any]
    ) -> None:
        """Test CastFunction creation fails with missing cast_type."""
        del cast_config["arguments"]["columns"][0]["cast_type"]

        with pytest.raises(ValidationError):
            CastFunction(**cast_config)


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="cast_func")
def fixture_cast_func(cast_config: dict[str, Any]) -> CastFunction:
    """Instantiate a CastFunction from the config dict.

    The object fixture is used to assert runtime field values without
    re-creating the config.
    """
    return CastFunction(**cast_config)


def test_cast_fixture(cast_func: CastFunction) -> None:
    """Sanity-check the instantiated fixture has the expected arguments."""
    assert cast_func.function_type == "cast"
    assert isinstance(cast_func.arguments, CastArgs)
    assert cast_func.arguments.columns[0].column_name == "age"
    assert cast_func.arguments.columns[0].cast_type == "int"


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestCastFunctionTransform:
    """Test CastFunction transform behavior with real Spark DataFrames."""

    def test_transform__returns_callable(self, cast_func: CastFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = cast_func.transform()

        # Assert
        assert callable(transform_fn)

    def test_transform__applies_cast_to_single_column(self, spark: SparkSession, mixed_types_df: DataFrame) -> None:
        """Test transform casts a single column correctly."""
        # Arrange
        config = {"function_type": "cast", "arguments": {"columns": [{"column_name": "age", "cast_type": "double"}]}}
        cast_func = CastFunction(**config)

        expected_data = [
            ("1", 25.5, "100"),
            ("2", 30.0, "200"),
            ("3", 35.7, "300"),
        ]
        expected_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("age", DoubleType(), True),
                StructField("salary", StringType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = cast_func.transform()
        result_df = transform_fn(mixed_types_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__applies_cast_to_multiple_columns(self, spark: SparkSession, mixed_types_df: DataFrame) -> None:
        """Test transform casts multiple columns correctly."""
        # Arrange
        config = {
            "function_type": "cast",
            "arguments": {
                "columns": [
                    {"column_name": "id", "cast_type": "int"},
                    {"column_name": "age", "cast_type": "double"},
                    {"column_name": "salary", "cast_type": "int"},
                ]
            },
        }
        cast_func = CastFunction(**config)

        expected_data = [
            (1, 25.5, 100),
            (2, 30.0, 200),
            (3, 35.7, 300),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("age", DoubleType(), True),
                StructField("salary", IntegerType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = cast_func.transform()
        result_df = transform_fn(mixed_types_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__string_to_int_cast(self, spark: SparkSession, mixed_types_df: DataFrame) -> None:
        """Test casting string to integer."""
        # Arrange
        config = {"function_type": "cast", "arguments": {"columns": [{"column_name": "salary", "cast_type": "int"}]}}
        cast_func = CastFunction(**config)

        expected_data = [
            ("1", "25.5", 100),
            ("2", "30.0", 200),
            ("3", "35.7", 300),
        ]
        expected_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("age", StringType(), True),
                StructField("salary", IntegerType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = cast_func.transform()
        result_df = transform_fn(mixed_types_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__preserves_column_order(self, spark: SparkSession, mixed_types_df: DataFrame) -> None:
        """Test that cast preserves column order."""
        # Arrange
        config = {"function_type": "cast", "arguments": {"columns": [{"column_name": "age", "cast_type": "int"}]}}
        cast_func = CastFunction(**config)

        # Act
        transform_fn = cast_func.transform()
        result_df = transform_fn(mixed_types_df)

        # Assert
        assert result_df.columns == ["id", "age", "salary"]

    def test_transform__with_empty_columns_list__returns_unchanged_df(
        self, spark: SparkSession, mixed_types_df: DataFrame
    ) -> None:
        """Test that empty columns list returns unchanged DataFrame."""
        # Arrange
        config = {"function_type": "cast", "arguments": {"columns": []}}
        cast_func = CastFunction(**config)

        # Act
        transform_fn = cast_func.transform()
        result_df = transform_fn(mixed_types_df)

        # Assert
        assertDataFrameEqual(result_df, mixed_types_df)
