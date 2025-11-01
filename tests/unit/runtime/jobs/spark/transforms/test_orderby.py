"""Tests for OrderBy transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from samara.runtime.jobs.models.transforms.model_orderby import OrderByArgs
from samara.runtime.jobs.spark.transforms.orderby import OrderByFunction

# =========================================================================== #
# ============================== TEST DATA ================================= #
# =========================================================================== #


@pytest.fixture(name="spark")
def fixture_spark() -> SparkSession:
    """Create a SparkSession for testing."""
    return SparkSession.builder.master("local[1]").appName("test-orderby").getOrCreate()


# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="orderby_config")
def fixture_orderby_config() -> dict[str, Any]:
    """Configuration dict for OrderByFunction."""
    return {
        "function_type": "orderby",
        "arguments": {
            "columns": [{"column_name": "age", "ascending": True}, {"column_name": "salary", "ascending": False}]
        },
    }


@pytest.fixture(name="single_column_config")
def fixture_single_column_config() -> dict[str, Any]:
    """Configuration dict with single column."""
    return {"function_type": "orderby", "arguments": {"columns": [{"column_name": "name", "ascending": True}]}}


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestOrderByFunctionValidation:
    """Test OrderByFunction model validation and instantiation."""

    def test_create_orderby_function__with_valid_config__creates_model(self, orderby_config: dict[str, Any]) -> None:
        """Test OrderByFunction creation with valid config."""
        # Act
        f = OrderByFunction(**orderby_config)

        # Assert
        assert f.function_type == "orderby"
        assert isinstance(f.arguments, OrderByArgs)
        assert len(f.arguments.columns) == 2
        assert f.arguments.columns[0].column_name == "age"
        assert f.arguments.columns[0].ascending is True
        assert f.arguments.columns[1].column_name == "salary"
        assert f.arguments.columns[1].ascending is False

    def test_create_orderby_function__with_wrong_function_name__raises_validation_error(
        self, orderby_config: dict[str, Any]
    ) -> None:
        """Test OrderByFunction creation fails with wrong function name."""
        orderby_config["function_type"] = "wrong_name"

        with pytest.raises(ValidationError):
            OrderByFunction(**orderby_config)

    def test_create_orderby_function__with_missing_arguments__raises_validation_error(
        self, orderby_config: dict[str, Any]
    ) -> None:
        """Test OrderByFunction creation fails without arguments field."""
        del orderby_config["arguments"]

        with pytest.raises(ValidationError):
            OrderByFunction(**orderby_config)

    def test_create_orderby_function__with_empty_columns__raises_validation_error(
        self, orderby_config: dict[str, Any]
    ) -> None:
        """Test OrderByFunction creation fails with empty columns list."""
        orderby_config["arguments"]["columns"] = []

        with pytest.raises(ValidationError):
            OrderByFunction(**orderby_config)

    def test_create_orderby_function__with_missing_column_name__raises_validation_error(
        self, orderby_config: dict[str, Any]
    ) -> None:
        """Test OrderByFunction creation fails with missing column_name."""
        del orderby_config["arguments"]["columns"][0]["column_name"]

        with pytest.raises(ValidationError):
            OrderByFunction(**orderby_config)

    def test_create_orderby_function__with_empty_column_name__raises_validation_error(
        self, orderby_config: dict[str, Any]
    ) -> None:
        """Test OrderByFunction creation fails with empty column_name."""
        orderby_config["arguments"]["columns"][0]["column_name"] = ""

        with pytest.raises(ValidationError):
            OrderByFunction(**orderby_config)

    def test_create_orderby_function__with_missing_ascending__raises_validation_error(
        self, orderby_config: dict[str, Any]
    ) -> None:
        """Test OrderByFunction creation fails with missing ascending field."""
        del orderby_config["arguments"]["columns"][0]["ascending"]

        with pytest.raises(ValidationError):
            OrderByFunction(**orderby_config)

    def test_create_orderby_function__with_null_ascending__raises_validation_error(
        self, orderby_config: dict[str, Any]
    ) -> None:
        """Test OrderByFunction creation fails with null ascending value."""
        orderby_config["arguments"]["columns"][0]["ascending"] = None

        with pytest.raises(ValidationError):
            OrderByFunction(**orderby_config)

    def test_create_orderby_function__with_invalid_ascending__raises_validation_error(
        self, orderby_config: dict[str, Any]
    ) -> None:
        """Test OrderByFunction creation fails with invalid ascending value."""
        orderby_config["arguments"]["columns"][0]["ascending"] = "invalid"

        with pytest.raises(ValidationError):
            OrderByFunction(**orderby_config)

    def test_create_orderby_function__with_single_column__creates_model(
        self, single_column_config: dict[str, Any]
    ) -> None:
        """Test OrderByFunction creation with single column."""
        # Act
        f = OrderByFunction(**single_column_config)

        # Assert
        assert f.function_type == "orderby"
        assert len(f.arguments.columns) == 1
        assert f.arguments.columns[0].column_name == "name"
        assert f.arguments.columns[0].ascending is True

    def test_create_orderby_function__with_three_columns__creates_model(self) -> None:
        """Test OrderByFunction creation with three columns."""
        # Arrange
        config = {
            "function_type": "orderby",
            "arguments": {
                "columns": [
                    {"column_name": "department", "ascending": True},
                    {"column_name": "salary", "ascending": False},
                    {"column_name": "hire_date", "ascending": True},
                ]
            },
        }

        # Act
        f = OrderByFunction(**config)

        # Assert
        assert len(f.arguments.columns) == 3
        assert f.arguments.columns[2].column_name == "hire_date"


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="orderby_func")
def fixture_orderby_func(orderby_config: dict[str, Any]) -> OrderByFunction:
    """Instantiate an OrderByFunction from the config dict."""
    return OrderByFunction(**orderby_config)


def test_orderby_fixture(orderby_func: OrderByFunction) -> None:
    """Sanity-check the instantiated fixture has the expected arguments."""
    assert orderby_func.function_type == "orderby"
    assert isinstance(orderby_func.arguments, OrderByArgs)
    assert orderby_func.arguments.columns[0].column_name == "age"
    assert orderby_func.arguments.columns[0].ascending is True
    assert orderby_func.arguments.columns[1].column_name == "salary"
    assert orderby_func.arguments.columns[1].ascending is False


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestOrderByFunctionTransform:
    """Test OrderByFunction transform behavior with real Spark DataFrames."""

    def test_transform__returns_callable(self, orderby_func: OrderByFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = orderby_func.transform()

        # Assert
        assert callable(transform_fn)

    def test_transform__single_column_ascending__sorts_correctly(
        self, spark: SparkSession, single_column_config: dict[str, Any]
    ) -> None:
        """Test transform with single ascending column."""
        # Arrange
        orderby_func = OrderByFunction(**single_column_config)
        schema = StructType([StructField("name", StringType(), True), StructField("age", IntegerType(), True)])
        input_df = spark.createDataFrame([("Charlie", 35), ("Alice", 28), ("Bob", 42)], schema)
        expected_df = spark.createDataFrame([("Alice", 28), ("Bob", 42), ("Charlie", 35)], schema)

        # Act
        transform_fn = orderby_func.transform()
        result_df = transform_fn(input_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__single_column_descending__sorts_correctly(self, spark: SparkSession) -> None:
        """Test transform with single descending column."""
        # Arrange
        config = {"function_type": "orderby", "arguments": {"columns": [{"column_name": "score", "ascending": False}]}}
        orderby_func = OrderByFunction(**config)
        schema = StructType([StructField("name", StringType(), True), StructField("score", IntegerType(), True)])
        input_df = spark.createDataFrame([("Alice", 85), ("Bob", 92), ("Charlie", 78)], schema)
        expected_df = spark.createDataFrame([("Bob", 92), ("Alice", 85), ("Charlie", 78)], schema)

        # Act
        transform_fn = orderby_func.transform()
        result_df = transform_fn(input_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__multiple_columns_mixed_order__sorts_correctly(
        self, spark: SparkSession, orderby_config: dict[str, Any]
    ) -> None:
        """Test transform with multiple columns and mixed sort order."""
        # Arrange - orderby_config has age ASC, salary DESC
        orderby_func = OrderByFunction(**orderby_config)
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("salary", IntegerType(), True),
            ]
        )
        input_df = spark.createDataFrame(
            [("Alice", 30, 70000), ("Bob", 25, 60000), ("Charlie", 30, 80000), ("Diana", 25, 55000)], schema
        )
        expected_df = spark.createDataFrame(
            [
                ("Bob", 25, 60000),  # age 25, salary 60000 (higher first)
                ("Diana", 25, 55000),  # age 25, salary 55000
                ("Charlie", 30, 80000),  # age 30, salary 80000 (higher first)
                ("Alice", 30, 70000),  # age 30, salary 70000
            ],
            schema,
        )

        # Act
        transform_fn = orderby_func.transform()
        result_df = transform_fn(input_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__all_columns_ascending__sorts_correctly(self, spark: SparkSession) -> None:
        """Test transform with all columns ascending."""
        # Arrange
        config = {
            "function_type": "orderby",
            "arguments": {
                "columns": [
                    {"column_name": "department", "ascending": True},
                    {"column_name": "name", "ascending": True},
                ]
            },
        }
        orderby_func = OrderByFunction(**config)
        schema = StructType([StructField("name", StringType(), True), StructField("department", StringType(), True)])
        input_df = spark.createDataFrame(
            [("Charlie", "Sales"), ("Alice", "Engineering"), ("Bob", "Engineering"), ("Diana", "Sales")], schema
        )
        expected_df = spark.createDataFrame(
            [("Alice", "Engineering"), ("Bob", "Engineering"), ("Charlie", "Sales"), ("Diana", "Sales")], schema
        )

        # Act
        transform_fn = orderby_func.transform()
        result_df = transform_fn(input_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__all_columns_descending__sorts_correctly(self, spark: SparkSession) -> None:
        """Test transform with all columns descending."""
        # Arrange
        config = {
            "function_type": "orderby",
            "arguments": {
                "columns": [{"column_name": "year", "ascending": False}, {"column_name": "score", "ascending": False}]
            },
        }
        orderby_func = OrderByFunction(**config)
        schema = StructType([StructField("year", IntegerType(), True), StructField("score", IntegerType(), True)])
        input_df = spark.createDataFrame([(2020, 85), (2021, 92), (2020, 90), (2021, 88)], schema)
        expected_df = spark.createDataFrame([(2021, 92), (2021, 88), (2020, 90), (2020, 85)], schema)

        # Act
        transform_fn = orderby_func.transform()
        result_df = transform_fn(input_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__three_columns__sorts_correctly(self, spark: SparkSession) -> None:
        """Test transform with three columns preserves sort order priority."""
        # Arrange
        config = {
            "function_type": "orderby",
            "arguments": {
                "columns": [
                    {"column_name": "department", "ascending": True},
                    {"column_name": "salary", "ascending": False},
                    {"column_name": "name", "ascending": True},
                ]
            },
        }
        orderby_func = OrderByFunction(**config)
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("department", StringType(), True),
                StructField("salary", IntegerType(), True),
            ]
        )
        input_df = spark.createDataFrame(
            [
                ("Eve", "Sales", 70000),
                ("Alice", "Engineering", 95000),
                ("Charlie", "Engineering", 105000),
                ("Bob", "Sales", 70000),
                ("Diana", "Marketing", 75000),
            ],
            schema,
        )
        expected_df = spark.createDataFrame(
            [
                ("Charlie", "Engineering", 105000),  # Engineering, highest salary
                ("Alice", "Engineering", 95000),  # Engineering, lower salary
                ("Diana", "Marketing", 75000),  # Marketing
                ("Bob", "Sales", 70000),  # Sales, 70000, "Bob" comes before "Eve"
                ("Eve", "Sales", 70000),  # Sales, 70000, "Eve"
            ],
            schema,
        )

        # Act
        transform_fn = orderby_func.transform()
        result_df = transform_fn(input_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)
