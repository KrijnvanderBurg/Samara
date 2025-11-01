"""Tests for DropNa transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual
from samara.workflow.jobs.models.transforms.model_dropna import DropNaArgs
from samara.workflow.jobs.spark.transforms.dropna import DropNaFunction

# =========================================================================== #
# ============================== TEST DATA ================================= #
# =========================================================================== #


@pytest.fixture
def data_with_nulls_df(spark: SparkSession) -> DataFrame:
    """Create a DataFrame with null values for testing dropna operations."""
    data = [
        (1, "Alice", 30, "Engineering", 75000),
        (2, "Bob", None, "Sales", 65000),
        (3, None, 35, "Marketing", 70000),
        (4, "Diana", 28, None, 80000),
        (5, "Eve", 32, "HR", None),
        (6, "Frank", 45, "Engineering", 90000),
        (7, None, None, "Operations", 60000),
        (8, "Henry", 38, "Sales", 72000),
        (9, None, None, None, None),
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
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


@pytest.fixture(name="dropna_config")
def fixture_dropna_config() -> dict[str, Any]:
    """Configuration dict for DropNaFunction."""
    return {
        "function_type": "dropna",
        "arguments": {"how": "any", "thresh": None, "subset": ["name", "age"]},
    }


def test_dropna_creation__from_config__creates_valid_model(dropna_config: dict[str, Any]) -> None:
    """Ensure the DropNaFunction can be created from a config dict."""
    f = DropNaFunction(**dropna_config)
    assert f.function_type == "dropna"
    assert isinstance(f.arguments, DropNaArgs)
    assert f.arguments.how == "any"
    assert f.arguments.thresh is None
    assert f.arguments.subset == ["name", "age"]


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestDropNaFunctionValidation:
    """Test DropNaFunction model validation and instantiation."""

    def test_create_dropna_function__with_wrong_function_name__raises_validation_error(
        self, dropna_config: dict[str, Any]
    ) -> None:
        """Test DropNaFunction creation fails with wrong function name."""
        dropna_config["function_type"] = "wrong_name"

        with pytest.raises(ValidationError):
            DropNaFunction(**dropna_config)

    def test_create_dropna_function__with_missing_arguments__raises_validation_error(
        self, dropna_config: dict[str, Any]
    ) -> None:
        """Test DropNaFunction creation fails without arguments field."""
        del dropna_config["arguments"]

        with pytest.raises(ValidationError):
            DropNaFunction(**dropna_config)

    def test_create_dropna_function__with_missing_how__raises_validation_error(
        self, dropna_config: dict[str, Any]
    ) -> None:
        """Test DropNaFunction creation fails with missing 'how' field."""
        del dropna_config["arguments"]["how"]

        with pytest.raises(ValidationError):
            DropNaFunction(**dropna_config)

    def test_create_dropna_function__with_missing_thresh__raises_validation_error(
        self, dropna_config: dict[str, Any]
    ) -> None:
        """Test DropNaFunction creation fails with missing 'thresh' field."""
        del dropna_config["arguments"]["thresh"]

        with pytest.raises(ValidationError):
            DropNaFunction(**dropna_config)

    def test_create_dropna_function__with_missing_subset__raises_validation_error(
        self, dropna_config: dict[str, Any]
    ) -> None:
        """Test DropNaFunction creation fails with missing 'subset' field."""
        del dropna_config["arguments"]["subset"]

        with pytest.raises(ValidationError):
            DropNaFunction(**dropna_config)

    def test_create_dropna_function__with_invalid_how__raises_validation_error(
        self, dropna_config: dict[str, Any]
    ) -> None:
        """Test DropNaFunction creation fails with invalid 'how' value."""
        dropna_config["arguments"]["how"] = "invalid"

        with pytest.raises(ValidationError):
            DropNaFunction(**dropna_config)

    def test_create_dropna_function__with_how_all__succeeds(self, dropna_config: dict[str, Any]) -> None:
        """Test DropNaFunction creation succeeds with 'how' set to 'all'."""
        dropna_config["arguments"]["how"] = "all"

        # Act
        dropna_function = DropNaFunction(**dropna_config)

        # Assert
        assert dropna_function.arguments.how == "all"

    def test_create_dropna_function__with_null_subset__succeeds(self, dropna_config: dict[str, Any]) -> None:
        """Test DropNaFunction creation succeeds with null subset."""
        dropna_config["arguments"]["subset"] = None

        # Act
        dropna_function = DropNaFunction(**dropna_config)

        # Assert
        assert dropna_function.arguments.subset is None

    def test_create_dropna_function__with_thresh_value__succeeds(self, dropna_config: dict[str, Any]) -> None:
        """Test DropNaFunction creation succeeds with thresh value."""
        dropna_config["arguments"]["thresh"] = 3

        # Act
        dropna_function = DropNaFunction(**dropna_config)

        # Assert
        assert dropna_function.arguments.thresh == 3

    def test_create_dropna_function__with_empty_subset__succeeds(self, dropna_config: dict[str, Any]) -> None:
        """Test DropNaFunction creation succeeds with empty subset list."""
        dropna_config["arguments"]["subset"] = []

        # Act
        dropna_function = DropNaFunction(**dropna_config)

        # Assert
        assert dropna_function.arguments.subset == []


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="dropna_func")
def fixture_dropna_func(dropna_config: dict[str, Any]) -> DropNaFunction:
    """Instantiate a DropNaFunction from the config dict.

    The object fixture is used to assert workflow field values without
    re-creating the config.
    """
    return DropNaFunction(**dropna_config)


def test_dropna_fixture(dropna_func: DropNaFunction) -> None:
    """Sanity-check the instantiated fixture has the expected arguments."""
    assert dropna_func.function_type == "dropna"
    assert isinstance(dropna_func.arguments, DropNaArgs)
    assert dropna_func.arguments.how == "any"
    assert dropna_func.arguments.thresh is None
    assert dropna_func.arguments.subset == ["name", "age"]


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestDropNaFunctionTransform:
    """Test DropNaFunction transform behavior with real Spark DataFrames."""

    def test_transform__returns_callable(self, dropna_func: DropNaFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = dropna_func.transform()

        # Assert
        assert callable(transform_fn)

    def test_transform__drops_rows_with_any_null_in_subset(
        self, spark: SparkSession, data_with_nulls_df: DataFrame
    ) -> None:
        """Test transform drops rows with any null values in specified columns."""
        # Arrange
        config = {
            "function_type": "dropna",
            "arguments": {"how": "any", "thresh": None, "subset": ["name", "age"]},
        }
        dropna_func = DropNaFunction(**config)

        expected_data = [
            (1, "Alice", 30, "Engineering", 75000),
            (4, "Diana", 28, None, 80000),
            (5, "Eve", 32, "HR", None),
            (6, "Frank", 45, "Engineering", 90000),
            (8, "Henry", 38, "Sales", 72000),
        ]
        expected_df = spark.createDataFrame(expected_data, data_with_nulls_df.schema)

        # Act
        transform_fn = dropna_func.transform()
        result_df = transform_fn(data_with_nulls_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__drops_rows_with_all_null_when_how_all(
        self, spark: SparkSession, data_with_nulls_df: DataFrame
    ) -> None:
        """Test transform drops only rows where all values are null."""
        # Arrange
        config = {
            "function_type": "dropna",
            "arguments": {"how": "all", "thresh": None, "subset": None},
        }
        dropna_func = DropNaFunction(**config)

        expected_data = [
            (1, "Alice", 30, "Engineering", 75000),
            (2, "Bob", None, "Sales", 65000),
            (3, None, 35, "Marketing", 70000),
            (4, "Diana", 28, None, 80000),
            (5, "Eve", 32, "HR", None),
            (6, "Frank", 45, "Engineering", 90000),
            (7, None, None, "Operations", 60000),
            (8, "Henry", 38, "Sales", 72000),
            (9, None, None, None, None),
        ]
        expected_df = spark.createDataFrame(expected_data, data_with_nulls_df.schema)

        # Act
        transform_fn = dropna_func.transform()
        result_df = transform_fn(data_with_nulls_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__drops_rows_with_any_null_all_columns(
        self, spark: SparkSession, data_with_nulls_df: DataFrame
    ) -> None:
        """Test transform drops rows with any null when checking all columns."""
        # Arrange
        config = {
            "function_type": "dropna",
            "arguments": {"how": "any", "thresh": None, "subset": None},
        }
        dropna_func = DropNaFunction(**config)

        # Act
        transform_fn = dropna_func.transform()
        result_df = transform_fn(data_with_nulls_df)

        # Assert - Only rows with no nulls should remain
        expected_data = [
            (1, "Alice", 30, "Engineering", 75000),
            (6, "Frank", 45, "Engineering", 90000),
            (8, "Henry", 38, "Sales", 72000),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("department", StringType(), True),
                StructField("salary", IntegerType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__uses_thresh_to_keep_rows_with_minimum_non_nulls(
        self, spark: SparkSession, data_with_nulls_df: DataFrame
    ) -> None:
        """Test transform keeps rows with at least thresh non-null values."""
        # Arrange
        config = {
            "function_type": "dropna",
            "arguments": {"how": "any", "thresh": 4, "subset": None},
        }
        dropna_func = DropNaFunction(**config)

        expected_data = [
            (1, "Alice", 30, "Engineering", 75000),
            (2, "Bob", None, "Sales", 65000),
            (3, None, 35, "Marketing", 70000),
            (4, "Diana", 28, None, 80000),
            (5, "Eve", 32, "HR", None),
            (6, "Frank", 45, "Engineering", 90000),
            (8, "Henry", 38, "Sales", 72000),
        ]
        expected_df = spark.createDataFrame(expected_data, data_with_nulls_df.schema)

        # Act
        transform_fn = dropna_func.transform()
        result_df = transform_fn(data_with_nulls_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__thresh_with_subset(self, spark: SparkSession, data_with_nulls_df: DataFrame) -> None:
        """Test transform uses both thresh and subset parameters together."""
        # Arrange
        config = {
            "function_type": "dropna",
            "arguments": {"how": "any", "thresh": 2, "subset": ["name", "age", "department"]},
        }
        dropna_func = DropNaFunction(**config)

        expected_data = [
            (1, "Alice", 30, "Engineering", 75000),
            (2, "Bob", None, "Sales", 65000),
            (3, None, 35, "Marketing", 70000),
            (4, "Diana", 28, None, 80000),
            (5, "Eve", 32, "HR", None),
            (6, "Frank", 45, "Engineering", 90000),
            (8, "Henry", 38, "Sales", 72000),
        ]
        expected_df = spark.createDataFrame(expected_data, data_with_nulls_df.schema)

        # Act
        transform_fn = dropna_func.transform()
        result_df = transform_fn(data_with_nulls_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__single_column_subset(self, spark: SparkSession, data_with_nulls_df: DataFrame) -> None:
        """Test transform with single column in subset."""
        # Arrange
        config = {
            "function_type": "dropna",
            "arguments": {"how": "any", "thresh": None, "subset": ["name"]},
        }
        dropna_func = DropNaFunction(**config)

        expected_data = [
            (1, "Alice", 30, "Engineering", 75000),
            (2, "Bob", None, "Sales", 65000),
            (4, "Diana", 28, None, 80000),
            (5, "Eve", 32, "HR", None),
            (6, "Frank", 45, "Engineering", 90000),
            (8, "Henry", 38, "Sales", 72000),
        ]
        expected_df = spark.createDataFrame(expected_data, data_with_nulls_df.schema)

        # Act
        transform_fn = dropna_func.transform()
        result_df = transform_fn(data_with_nulls_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__with_empty_subset__returns_unchanged_df(
        self, spark: SparkSession, data_with_nulls_df: DataFrame
    ) -> None:
        """Test transform with empty subset list returns unchanged DataFrame."""
        # Arrange
        config = {
            "function_type": "dropna",
            "arguments": {"how": "any", "thresh": None, "subset": []},
        }
        dropna_func = DropNaFunction(**config)

        # Act
        transform_fn = dropna_func.transform()
        result_df = transform_fn(data_with_nulls_df)

        # Assert
        assertDataFrameEqual(result_df, data_with_nulls_df)

    def test_transform__preserves_column_order(self, spark: SparkSession, data_with_nulls_df: DataFrame) -> None:
        """Test transform preserves original column order."""
        # Arrange
        config = {
            "function_type": "dropna",
            "arguments": {"how": "any", "thresh": None, "subset": ["age"]},
        }
        dropna_func = DropNaFunction(**config)

        # Act
        transform_fn = dropna_func.transform()
        result_df = transform_fn(data_with_nulls_df)

        # Assert
        assert result_df.columns == data_with_nulls_df.columns

    def test_transform__preserves_schema(self, spark: SparkSession, data_with_nulls_df: DataFrame) -> None:
        """Test transform preserves DataFrame schema."""
        # Arrange
        config = {
            "function_type": "dropna",
            "arguments": {"how": "any", "thresh": None, "subset": ["name"]},
        }
        dropna_func = DropNaFunction(**config)

        # Act
        transform_fn = dropna_func.transform()
        result_df = transform_fn(data_with_nulls_df)

        # Assert
        assert result_df.schema == data_with_nulls_df.schema

    def test_transform__with_thresh_zero__keeps_all_rows(
        self, spark: SparkSession, data_with_nulls_df: DataFrame
    ) -> None:
        """Test transform with thresh=0 keeps all rows."""
        # Arrange
        config = {
            "function_type": "dropna",
            "arguments": {"how": "any", "thresh": 0, "subset": None},
        }
        dropna_func = DropNaFunction(**config)

        # Act
        transform_fn = dropna_func.transform()
        result_df = transform_fn(data_with_nulls_df)

        # Assert
        assertDataFrameEqual(result_df, data_with_nulls_df)

    def test_transform__with_thresh_greater_than_columns__drops_all_rows(
        self, spark: SparkSession, data_with_nulls_df: DataFrame
    ) -> None:
        """Test transform with thresh > number of columns drops all rows."""
        # Arrange
        config = {
            "function_type": "dropna",
            "arguments": {"how": "any", "thresh": 10, "subset": None},
        }
        dropna_func = DropNaFunction(**config)

        expected_df = spark.createDataFrame([], data_with_nulls_df.schema)

        # Act
        transform_fn = dropna_func.transform()
        result_df = transform_fn(data_with_nulls_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__how_all_with_subset(self, spark: SparkSession, data_with_nulls_df: DataFrame) -> None:
        """Test transform with how='all' and subset drops rows where all subset columns are null."""
        # Arrange
        config = {
            "function_type": "dropna",
            "arguments": {"how": "all", "thresh": None, "subset": ["name", "age"]},
        }
        dropna_func = DropNaFunction(**config)

        expected_data = [
            (1, "Alice", 30, "Engineering", 75000),
            (2, "Bob", None, "Sales", 65000),
            (3, None, 35, "Marketing", 70000),
            (4, "Diana", 28, None, 80000),
            (5, "Eve", 32, "HR", None),
            (6, "Frank", 45, "Engineering", 90000),
            (8, "Henry", 38, "Sales", 72000),
        ]
        expected_df = spark.createDataFrame(expected_data, data_with_nulls_df.schema)

        # Act
        transform_fn = dropna_func.transform()
        result_df = transform_fn(data_with_nulls_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__on_dataframe_without_nulls__returns_unchanged(self, spark: SparkSession) -> None:
        """Test transform on DataFrame without nulls returns unchanged DataFrame."""
        # Arrange
        data = [
            (1, "Alice", 30, "Engineering", 75000),
            (2, "Bob", 25, "Sales", 65000),
            (3, "Charlie", 35, "Marketing", 70000),
        ]
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("department", StringType(), True),
                StructField("salary", IntegerType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        config = {
            "function_type": "dropna",
            "arguments": {"how": "any", "thresh": None, "subset": None},
        }
        dropna_func = DropNaFunction(**config)

        # Act
        transform_fn = dropna_func.transform()
        result_df = transform_fn(df)

        # Assert
        assertDataFrameEqual(result_df, df)
