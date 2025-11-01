"""Tests for Distinct transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual
from samara.workflow.jobs.models.transforms.model_distinct import DistinctArgs
from samara.workflow.jobs.spark.transforms.distinct import DistinctFunction


@pytest.fixture
def spark() -> SparkSession:
    """Create a SparkSession for testing."""
    return SparkSession.builder.master("local[1]").appName("test_distinct").getOrCreate()


@pytest.fixture(name="distinct_config")
def fixture_distinct_config() -> dict[str, Any]:
    """Configuration dict for DistinctFunction."""
    return {"function_type": "distinct", "arguments": {}}


def test_distinct_creation__from_config__creates_valid_model(distinct_config: dict[str, Any]) -> None:
    """Ensure the DistinctFunction can be created from a config dict."""
    f = DistinctFunction(**distinct_config)
    assert f.function_type == "distinct"
    assert isinstance(f.arguments, DistinctArgs)


class TestDistinctFunctionValidation:
    """Test DistinctFunction model validation and instantiation."""

    def test_create_distinct_function__with_wrong_function_name__raises_validation_error(
        self, distinct_config: dict[str, Any]
    ) -> None:
        """Test DistinctFunction creation fails with wrong function name."""
        distinct_config["function_type"] = "wrong_name"
        with pytest.raises(ValidationError):
            DistinctFunction(**distinct_config)

    def test_create_distinct_function__with_missing_function_type__raises_validation_error(
        self, distinct_config: dict[str, Any]
    ) -> None:
        """Test DistinctFunction creation fails without function_type field."""
        del distinct_config["function_type"]
        with pytest.raises(ValidationError):
            DistinctFunction(**distinct_config)

    def test_create_distinct_function__with_missing_arguments__raises_validation_error(
        self, distinct_config: dict[str, Any]
    ) -> None:
        """Test DistinctFunction creation fails without arguments field."""
        del distinct_config["arguments"]
        with pytest.raises(ValidationError):
            DistinctFunction(**distinct_config)


@pytest.fixture(name="distinct_func")
def fixture_distinct_func(distinct_config: dict[str, Any]) -> DistinctFunction:
    """Instantiate a DistinctFunction from the config dict."""
    return DistinctFunction(**distinct_config)


def test_distinct_fixture(distinct_func: DistinctFunction) -> None:
    """Sanity-check the instantiated fixture has the expected arguments."""
    assert distinct_func.function_type == "distinct"
    assert isinstance(distinct_func.arguments, DistinctArgs)


class TestDistinctFunctionTransform:
    """Test DistinctFunction transform behavior with actual Spark DataFrames."""

    def test_transform__returns_callable(self, distinct_func: DistinctFunction) -> None:
        """Test transform returns a callable function."""
        transform_fn = distinct_func.transform()
        assert callable(transform_fn)

    def test_transform__removes_all_duplicates(self, distinct_func: DistinctFunction, spark: SparkSession) -> None:
        """Test transform removes completely duplicate rows."""
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]
        )
        data = [
            ("1", "Alice"),
            ("2", "Bob"),
            ("1", "Alice"),
            ("3", "Charlie"),
            ("2", "Bob"),
        ]
        input_df = spark.createDataFrame(data, schema)
        expected_data = [("1", "Alice"), ("2", "Bob"), ("3", "Charlie")]
        expected_df = spark.createDataFrame(expected_data, schema)

        transform_fn = distinct_func.transform()
        result = transform_fn(input_df)
        assertDataFrameEqual(result, expected_df)

    def test_transform__with_no_duplicates_returns_same_data(
        self, distinct_func: DistinctFunction, spark: SparkSession
    ) -> None:
        """Test transform with no duplicates returns all rows."""
        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("date", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("amount", IntegerType(), True),
            ]
        )
        data = [
            ("U001", "2024-01-15", "P001", 100),
            ("U002", "2024-01-16", "P002", 200),
            ("U003", "2024-01-17", "P003", 300),
        ]
        input_df = spark.createDataFrame(data, schema)
        expected_df = spark.createDataFrame(data, schema)

        transform_fn = distinct_func.transform()
        result = transform_fn(input_df)
        assertDataFrameEqual(result, expected_df)

    def test_transform__preserves_schema(self, distinct_func: DistinctFunction, spark: SparkSession) -> None:
        """Test that transform preserves DataFrame schema."""
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("value", IntegerType(), True),
            ]
        )
        data = [("A", 10), ("B", 20)]
        input_df = spark.createDataFrame(data, schema)
        original_schema = input_df.schema

        transform_fn = distinct_func.transform()
        result = transform_fn(input_df)
        assert result.schema == original_schema

    def test_transform__with_null_values(self, distinct_func: DistinctFunction, spark: SparkSession) -> None:
        """Test transform handles null values correctly."""
        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("email", StringType(), True),
            ]
        )
        data = [
            ("U001", "alice@example.com"),
            (None, "bob@example.com"),
            ("U001", "alice@example.com"),
            (None, "bob@example.com"),
            ("U002", "dave@example.com"),
        ]
        input_df = spark.createDataFrame(data, schema)
        expected_data = [
            ("U001", "alice@example.com"),
            (None, "bob@example.com"),
            ("U002", "dave@example.com"),
        ]
        expected_df = spark.createDataFrame(expected_data, schema)

        transform_fn = distinct_func.transform()
        result = transform_fn(input_df)
        assertDataFrameEqual(result, expected_df)
