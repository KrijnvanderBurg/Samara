"""Tests for Distinct transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from samara.runtime.jobs.models.transforms.model_distinct import DistinctArgs
from samara.runtime.jobs.spark.transforms.distinct import DistinctFunction

# =========================================================================== #
# ============================== TEST DATA ================================= #
# =========================================================================== #


@pytest.fixture
def spark() -> SparkSession:
    """Create a SparkSession for testing."""
    return SparkSession.builder.master("local[1]").appName("test_distinct").getOrCreate()


@pytest.fixture
def sample_df(spark: SparkSession) -> DataFrame:
    """Create a sample DataFrame with duplicates."""
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
        ("U002", "2024-01-15", "P002", 200),
        ("U001", "2024-01-15", "P003", 150),  # Duplicate user_id + date
        ("U003", "2024-01-16", "P001", 300),
        ("U002", "2024-01-16", "P004", 250),
        ("U001", "2024-01-15", "P005", 175),  # Another duplicate user_id + date
    ]
    return spark.createDataFrame(data, schema)


# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="distinct_config")
def fixture_distinct_config() -> dict[str, Any]:
    """Configuration dict for DistinctFunction."""
    return {"function_type": "distinct", "arguments": {"columns": ["user_id", "date"]}}


@pytest.fixture(name="distinct_config_null_columns")
def fixture_distinct_config_null_columns() -> dict[str, Any]:
    """Configuration dict for DistinctFunction with null columns."""
    return {"function_type": "distinct", "arguments": {"columns": None}}


@pytest.fixture(name="distinct_config_empty_columns")
def fixture_distinct_config_empty_columns() -> dict[str, Any]:
    """Configuration dict for DistinctFunction with empty columns list."""
    return {"function_type": "distinct", "arguments": {"columns": []}}


def test_distinct_creation__from_config__creates_valid_model(distinct_config: dict[str, Any]) -> None:
    """Ensure the DistinctFunction can be created from a config dict."""
    f = DistinctFunction(**distinct_config)
    assert f.function_type == "distinct"
    assert isinstance(f.arguments, DistinctArgs)
    assert f.arguments.columns == ["user_id", "date"]


def test_distinct_creation__with_null_columns__creates_valid_model(
    distinct_config_null_columns: dict[str, Any],
) -> None:
    """Ensure the DistinctFunction can be created with null columns."""
    f = DistinctFunction(**distinct_config_null_columns)
    assert f.function_type == "distinct"
    assert f.arguments.columns is None


def test_distinct_creation__with_empty_columns__creates_valid_model(
    distinct_config_empty_columns: dict[str, Any],
) -> None:
    """Ensure the DistinctFunction can be created with empty columns list."""
    f = DistinctFunction(**distinct_config_empty_columns)
    assert f.function_type == "distinct"
    assert f.arguments.columns == []


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


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

    def test_create_distinct_function__with_missing_columns_field__raises_validation_error(
        self, distinct_config: dict[str, Any]
    ) -> None:
        """Test DistinctFunction creation fails without columns field."""
        del distinct_config["arguments"]["columns"]

        with pytest.raises(ValidationError):
            DistinctFunction(**distinct_config)

    def test_create_distinct_function__with_invalid_columns_type__raises_validation_error(
        self, distinct_config: dict[str, Any]
    ) -> None:
        """Test DistinctFunction creation fails with invalid columns type."""
        distinct_config["arguments"]["columns"] = "not_a_list"

        with pytest.raises(ValidationError):
            DistinctFunction(**distinct_config)

    def test_create_distinct_function__with_single_column__creates_valid_model(
        self, distinct_config: dict[str, Any]
    ) -> None:
        """Test DistinctFunction creation succeeds with single column."""
        distinct_config["arguments"]["columns"] = ["user_id"]

        f = DistinctFunction(**distinct_config)
        assert f.arguments.columns == ["user_id"]

    def test_create_distinct_function__with_many_columns__creates_valid_model(
        self, distinct_config: dict[str, Any]
    ) -> None:
        """Test DistinctFunction creation succeeds with many columns."""
        columns = ["col1", "col2", "col3", "col4", "col5"]
        distinct_config["arguments"]["columns"] = columns

        f = DistinctFunction(**distinct_config)
        assert f.arguments.columns == columns


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="distinct_func")
def fixture_distinct_func(distinct_config: dict[str, Any]) -> DistinctFunction:
    """Instantiate a DistinctFunction from the config dict."""
    return DistinctFunction(**distinct_config)


@pytest.fixture(name="distinct_func_null")
def fixture_distinct_func_null(distinct_config_null_columns: dict[str, Any]) -> DistinctFunction:
    """Instantiate a DistinctFunction with null columns."""
    return DistinctFunction(**distinct_config_null_columns)


@pytest.fixture(name="distinct_func_empty")
def fixture_distinct_func_empty(distinct_config_empty_columns: dict[str, Any]) -> DistinctFunction:
    """Instantiate a DistinctFunction with empty columns."""
    return DistinctFunction(**distinct_config_empty_columns)


def test_distinct_fixture(distinct_func: DistinctFunction) -> None:
    """Sanity-check the instantiated fixture has the expected arguments."""
    assert distinct_func.function_type == "distinct"
    assert isinstance(distinct_func.arguments, DistinctArgs)
    assert distinct_func.arguments.columns == ["user_id", "date"]


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestDistinctFunctionTransform:
    """Test DistinctFunction transform behavior with actual Spark DataFrames."""

    def test_transform__returns_callable(self, distinct_func: DistinctFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = distinct_func.transform()

        # Assert
        assert callable(transform_fn)

    def test_transform__with_columns_removes_duplicates(
        self, distinct_func: DistinctFunction, sample_df: DataFrame, spark: SparkSession
    ) -> None:
        """Test transform with columns removes duplicates based on specified columns."""
        # Arrange
        expected_schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("date", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("amount", IntegerType(), True),
            ]
        )
        expected_data = [
            ("U001", "2024-01-15", "P001", 100),  # First occurrence
            ("U002", "2024-01-15", "P002", 200),
            ("U003", "2024-01-16", "P001", 300),
            ("U002", "2024-01-16", "P004", 250),
        ]
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = distinct_func.transform()
        result = transform_fn(sample_df)

        # Assert
        assertDataFrameEqual(result, expected_df)

    def test_transform__with_null_columns_removes_all_duplicates(
        self, distinct_func_null: DistinctFunction, spark: SparkSession
    ) -> None:
        """Test transform with null columns removes completely duplicate rows."""
        # Arrange
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]
        )
        data = [
            ("1", "Alice"),
            ("2", "Bob"),
            ("1", "Alice"),  # Exact duplicate
            ("3", "Charlie"),
            ("2", "Bob"),  # Another exact duplicate
        ]
        input_df = spark.createDataFrame(data, schema)

        expected_data = [
            ("1", "Alice"),
            ("2", "Bob"),
            ("3", "Charlie"),
        ]
        expected_df = spark.createDataFrame(expected_data, schema)

        # Act
        transform_fn = distinct_func_null.transform()
        result = transform_fn(input_df)

        # Assert
        assertDataFrameEqual(result, expected_df)

    def test_transform__with_empty_columns_removes_all_duplicates(
        self, distinct_func_empty: DistinctFunction, spark: SparkSession
    ) -> None:
        """Test transform with empty columns list removes completely duplicate rows."""
        # Arrange
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("value", IntegerType(), True),
            ]
        )
        data = [
            ("A", 10),
            ("B", 20),
            ("A", 10),  # Exact duplicate
            ("C", 30),
        ]
        input_df = spark.createDataFrame(data, schema)

        expected_data = [
            ("A", 10),
            ("B", 20),
            ("C", 30),
        ]
        expected_df = spark.createDataFrame(expected_data, schema)

        # Act
        transform_fn = distinct_func_empty.transform()
        result = transform_fn(input_df)

        # Assert
        assertDataFrameEqual(result, expected_df)

    def test_transform__with_single_column(self, spark: SparkSession) -> None:
        """Test transform with single column specification."""
        # Arrange
        config = {"function_type": "distinct", "arguments": {"columns": ["id"]}}
        distinct_func = DistinctFunction(**config)

        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("score", IntegerType(), True),
            ]
        )
        data = [
            ("1", "Alice", 85),
            ("2", "Bob", 90),
            ("1", "Alice Updated", 95),  # Duplicate id, different other fields
            ("3", "Charlie", 88),
        ]
        input_df = spark.createDataFrame(data, schema)

        expected_data = [
            ("1", "Alice", 85),  # First occurrence of id=1
            ("2", "Bob", 90),
            ("3", "Charlie", 88),
        ]
        expected_df = spark.createDataFrame(expected_data, schema)

        # Act
        transform_fn = distinct_func.transform()
        result = transform_fn(input_df)

        # Assert
        assertDataFrameEqual(result, expected_df)

    def test_transform__with_no_duplicates_returns_same_data(
        self, distinct_func: DistinctFunction, spark: SparkSession
    ) -> None:
        """Test transform with no duplicates returns all rows."""
        # Arrange
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

        # Act
        transform_fn = distinct_func.transform()
        result = transform_fn(input_df)

        # Assert
        assertDataFrameEqual(result, expected_df)

    def test_transform__with_multiple_columns(self, spark: SparkSession) -> None:
        """Test transform with multiple column specifications."""
        # Arrange
        config = {"function_type": "distinct", "arguments": {"columns": ["country", "city"]}}
        distinct_func = DistinctFunction(**config)

        schema = StructType(
            [
                StructField("country", StringType(), True),
                StructField("city", StringType(), True),
                StructField("population", IntegerType(), True),
            ]
        )
        data = [
            ("USA", "New York", 8000000),
            ("USA", "Los Angeles", 4000000),
            ("USA", "New York", 8500000),  # Duplicate country+city
            ("UK", "London", 9000000),
            ("USA", "Los Angeles", 4200000),  # Another duplicate country+city
        ]
        input_df = spark.createDataFrame(data, schema)

        expected_data = [
            ("USA", "New York", 8000000),
            ("USA", "Los Angeles", 4000000),
            ("UK", "London", 9000000),
        ]
        expected_df = spark.createDataFrame(expected_data, schema)

        # Act
        transform_fn = distinct_func.transform()
        result = transform_fn(input_df)

        # Assert
        assertDataFrameEqual(result, expected_df)

    def test_transform__preserves_schema(self, distinct_func: DistinctFunction, sample_df: DataFrame) -> None:
        """Test that transform preserves DataFrame schema."""
        # Arrange
        original_schema = sample_df.schema

        # Act
        transform_fn = distinct_func.transform()
        result = transform_fn(sample_df)

        # Assert
        assert result.schema == original_schema

    def test_transform__with_null_values(self, spark: SparkSession) -> None:
        """Test transform handles null values correctly."""
        # Arrange
        config = {"function_type": "distinct", "arguments": {"columns": ["user_id"]}}
        distinct_func = DistinctFunction(**config)

        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("email", StringType(), True),
            ]
        )
        data = [
            ("U001", "alice@example.com"),
            (None, "bob@example.com"),
            ("U001", "alice.new@example.com"),  # Duplicate user_id
            (None, "charlie@example.com"),  # Duplicate None
            ("U002", "dave@example.com"),
        ]
        input_df = spark.createDataFrame(data, schema)

        expected_data = [
            ("U001", "alice@example.com"),
            (None, "bob@example.com"),
            ("U002", "dave@example.com"),
        ]
        expected_df = spark.createDataFrame(expected_data, schema)

        # Act
        transform_fn = distinct_func.transform()
        result = transform_fn(input_df)

        # Assert
        assertDataFrameEqual(result, expected_df)
