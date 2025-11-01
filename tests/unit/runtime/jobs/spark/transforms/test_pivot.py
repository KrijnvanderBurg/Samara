"""Tests for Pivot transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from samara.runtime.jobs.models.transforms.model_pivot import PivotArgs
from samara.runtime.jobs.spark.transforms.pivot import PivotFunction

# =========================================================================== #
# ============================== TEST DATA ================================= #
# =========================================================================== #


@pytest.fixture
def sales_df(spark: SparkSession) -> DataFrame:
    """Create a DataFrame with sales data for testing pivot operations."""
    data = [
        ("A", "Q1", "North", 100),
        ("A", "Q2", "North", 150),
        ("A", "Q3", "North", 200),
        ("A", "Q1", "South", 120),
        ("A", "Q2", "South", 180),
        ("B", "Q1", "North", 90),
        ("B", "Q2", "North", 110),
        ("B", "Q1", "South", 85),
        ("B", "Q3", "South", 95),
    ]
    schema = StructType(
        [
            StructField("product_id", StringType(), True),
            StructField("quarter", StringType(), True),
            StructField("region", StringType(), True),
            StructField("sales", IntegerType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="pivot_config")
def fixture_pivot_config() -> dict[str, Any]:
    """Configuration dict for PivotFunction."""
    return {
        "function_type": "pivot",
        "arguments": {
            "group_by": ["product_id", "region"],
            "pivot_column": "quarter",
            "values_column": "sales",
            "agg_func": "sum",
        },
    }


def test_pivot_creation__from_config__creates_valid_model(pivot_config: dict[str, Any]) -> None:
    """Ensure the PivotFunction can be created from a config dict."""
    f = PivotFunction(**pivot_config)
    assert f.function_type == "pivot"
    assert isinstance(f.arguments, PivotArgs)
    assert f.arguments.group_by == ["product_id", "region"]
    assert f.arguments.pivot_column == "quarter"
    assert f.arguments.values_column == "sales"
    assert f.arguments.agg_func == "sum"


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestPivotFunctionValidation:
    """Test PivotFunction model validation and instantiation."""

    def test_create_pivot_function__with_wrong_function_name__raises_validation_error(
        self, pivot_config: dict[str, Any]
    ) -> None:
        """Test PivotFunction creation fails with wrong function name."""
        pivot_config["function_type"] = "wrong_name"

        with pytest.raises(ValidationError):
            PivotFunction(**pivot_config)

    def test_create_pivot_function__with_missing_arguments__raises_validation_error(
        self, pivot_config: dict[str, Any]
    ) -> None:
        """Test PivotFunction creation fails without arguments field."""
        del pivot_config["arguments"]

        with pytest.raises(ValidationError):
            PivotFunction(**pivot_config)

    def test_create_pivot_function__with_empty_group_by__succeeds(self, pivot_config: dict[str, Any]) -> None:
        """Test PivotFunction creation succeeds with empty group_by list."""
        pivot_config["arguments"]["group_by"] = []

        # Act
        pivot_function = PivotFunction(**pivot_config)

        # Assert
        assert pivot_function.arguments.group_by == []

    def test_create_pivot_function__with_missing_group_by__raises_validation_error(
        self, pivot_config: dict[str, Any]
    ) -> None:
        """Test PivotFunction creation fails with missing group_by."""
        del pivot_config["arguments"]["group_by"]

        with pytest.raises(ValidationError):
            PivotFunction(**pivot_config)

    def test_create_pivot_function__with_missing_pivot_column__raises_validation_error(
        self, pivot_config: dict[str, Any]
    ) -> None:
        """Test PivotFunction creation fails with missing pivot_column."""
        del pivot_config["arguments"]["pivot_column"]

        with pytest.raises(ValidationError):
            PivotFunction(**pivot_config)

    def test_create_pivot_function__with_missing_values_column__raises_validation_error(
        self, pivot_config: dict[str, Any]
    ) -> None:
        """Test PivotFunction creation fails with missing values_column."""
        del pivot_config["arguments"]["values_column"]

        with pytest.raises(ValidationError):
            PivotFunction(**pivot_config)

    def test_create_pivot_function__with_missing_agg_func__raises_validation_error(
        self, pivot_config: dict[str, Any]
    ) -> None:
        """Test PivotFunction creation fails with missing agg_func."""
        del pivot_config["arguments"]["agg_func"]

        with pytest.raises(ValidationError):
            PivotFunction(**pivot_config)


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="pivot_func")
def fixture_pivot_func(pivot_config: dict[str, Any]) -> PivotFunction:
    """Instantiate a PivotFunction from the config dict.

    The object fixture is used to assert runtime field values without
    re-creating the config.
    """
    return PivotFunction(**pivot_config)


def test_pivot_fixture(pivot_func: PivotFunction) -> None:
    """Sanity-check the instantiated fixture has the expected arguments."""
    assert pivot_func.function_type == "pivot"
    assert isinstance(pivot_func.arguments, PivotArgs)
    assert pivot_func.arguments.group_by == ["product_id", "region"]
    assert pivot_func.arguments.pivot_column == "quarter"
    assert pivot_func.arguments.values_column == "sales"
    assert pivot_func.arguments.agg_func == "sum"


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestPivotFunctionTransform:
    """Test PivotFunction transform behavior with real Spark DataFrames."""

    def test_transform__returns_callable(self, pivot_func: PivotFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = pivot_func.transform()

        # Assert
        assert callable(transform_fn)

    def test_transform__pivots_single_column_with_sum(self, spark: SparkSession, sales_df: DataFrame) -> None:
        """Test transform pivots data correctly with sum aggregation."""
        # Arrange
        config = {
            "function_type": "pivot",
            "arguments": {
                "group_by": ["product_id", "region"],
                "pivot_column": "quarter",
                "values_column": "sales",
                "agg_func": "sum",
            },
        }
        pivot_func = PivotFunction(**config)

        # Act
        transform_fn = pivot_func.transform()
        result_df = transform_fn(sales_df)

        # Assert
        assert set(result_df.columns) == {"product_id", "region", "Q1", "Q2", "Q3"}
        assert result_df.count() == 4

        # Check specific values
        row_a_north = result_df.filter((result_df.product_id == "A") & (result_df.region == "North")).collect()[0]
        assert row_a_north.Q1 == 100
        assert row_a_north.Q2 == 150
        assert row_a_north.Q3 == 200

    def test_transform__pivots_with_avg_aggregation(self, spark: SparkSession) -> None:
        """Test transform pivots data with average aggregation."""
        # Arrange
        data = [
            ("A", "Jan", 100),
            ("A", "Jan", 200),
            ("A", "Feb", 150),
            ("B", "Jan", 50),
            ("B", "Feb", 75),
            ("B", "Feb", 125),
        ]
        schema = StructType(
            [
                StructField("product", StringType(), True),
                StructField("month", StringType(), True),
                StructField("value", IntegerType(), True),
            ]
        )
        input_df = spark.createDataFrame(data, schema)

        config = {
            "function_type": "pivot",
            "arguments": {
                "group_by": ["product"],
                "pivot_column": "month",
                "values_column": "value",
                "agg_func": "avg",
            },
        }
        pivot_func = PivotFunction(**config)

        # Act
        transform_fn = pivot_func.transform()
        result_df = transform_fn(input_df)

        # Assert
        assert set(result_df.columns) == {"product", "Jan", "Feb"}
        assert result_df.count() == 2

        # Check values
        row_a = result_df.filter(result_df.product == "A").collect()[0]
        assert row_a.Jan == 150.0
        assert row_a.Feb == 150.0

        row_b = result_df.filter(result_df.product == "B").collect()[0]
        assert row_b.Jan == 50.0
        assert row_b.Feb == 100.0

    def test_transform__pivots_with_max_aggregation(self, spark: SparkSession, sales_df: DataFrame) -> None:
        """Test transform pivots data with max aggregation."""
        # Arrange
        config = {
            "function_type": "pivot",
            "arguments": {
                "group_by": ["region"],
                "pivot_column": "quarter",
                "values_column": "sales",
                "agg_func": "max",
            },
        }
        pivot_func = PivotFunction(**config)

        # Act
        transform_fn = pivot_func.transform()
        result_df = transform_fn(sales_df)

        # Assert
        assert "region" in result_df.columns
        assert "Q1" in result_df.columns
        assert "Q2" in result_df.columns
        assert "Q3" in result_df.columns

    def test_transform__pivots_with_min_aggregation(self, spark: SparkSession, sales_df: DataFrame) -> None:
        """Test transform pivots data with min aggregation."""
        # Arrange
        config = {
            "function_type": "pivot",
            "arguments": {
                "group_by": ["region"],
                "pivot_column": "quarter",
                "values_column": "sales",
                "agg_func": "min",
            },
        }
        pivot_func = PivotFunction(**config)

        # Act
        transform_fn = pivot_func.transform()
        result_df = transform_fn(sales_df)

        # Assert
        assert set(result_df.columns) == {"region", "Q1", "Q2", "Q3"}
        assert result_df.count() == 2

        # Check values
        row_north = result_df.filter(result_df.region == "North").collect()[0]
        assert row_north.Q1 == 90
        assert row_north.Q2 == 110
        assert row_north.Q3 == 200

        row_south = result_df.filter(result_df.region == "South").collect()[0]
        assert row_south.Q1 == 85
        assert row_south.Q2 == 180
        assert row_south.Q3 == 95

    def test_transform__pivots_with_count_aggregation(self, spark: SparkSession, sales_df: DataFrame) -> None:
        """Test transform pivots data with count aggregation."""
        # Arrange
        config = {
            "function_type": "pivot",
            "arguments": {
                "group_by": ["product_id"],
                "pivot_column": "quarter",
                "values_column": "sales",
                "agg_func": "count",
            },
        }
        pivot_func = PivotFunction(**config)

        # Act
        transform_fn = pivot_func.transform()
        result_df = transform_fn(sales_df)

        # Assert
        assert "product_id" in result_df.columns
        assert result_df.filter(result_df.product_id == "A").count() == 1
        assert result_df.filter(result_df.product_id == "B").count() == 1

    def test_transform__with_empty_group_by__pivots_all_data(self, spark: SparkSession) -> None:
        """Test transform with empty group_by list pivots all data together."""
        # Arrange
        data = [
            ("Q1", 100),
            ("Q2", 200),
            ("Q1", 150),
            ("Q3", 300),
        ]
        schema = StructType(
            [
                StructField("quarter", StringType(), True),
                StructField("value", IntegerType(), True),
            ]
        )
        input_df = spark.createDataFrame(data, schema)

        config = {
            "function_type": "pivot",
            "arguments": {"group_by": [], "pivot_column": "quarter", "values_column": "value", "agg_func": "sum"},
        }
        pivot_func = PivotFunction(**config)

        # Act
        transform_fn = pivot_func.transform()
        result_df = transform_fn(input_df)

        # Assert
        assert result_df.count() == 1
        assert "Q1" in result_df.columns
        assert "Q2" in result_df.columns
        assert "Q3" in result_df.columns

    def test_transform__with_first_aggregation(self, spark: SparkSession, sales_df: DataFrame) -> None:
        """Test transform pivots data with first aggregation."""
        # Arrange
        config = {
            "function_type": "pivot",
            "arguments": {
                "group_by": ["product_id"],
                "pivot_column": "region",
                "values_column": "sales",
                "agg_func": "first",
            },
        }
        pivot_func = PivotFunction(**config)

        # Act
        transform_fn = pivot_func.transform()
        result_df = transform_fn(sales_df)

        # Assert
        assert "product_id" in result_df.columns
        assert "North" in result_df.columns
        assert "South" in result_df.columns

    def test_transform__with_invalid_agg_func__raises_value_error(
        self, spark: SparkSession, sales_df: DataFrame
    ) -> None:
        """Test transform raises ValueError for invalid aggregation function."""
        # Arrange
        config = {
            "function_type": "pivot",
            "arguments": {
                "group_by": ["product_id"],
                "pivot_column": "quarter",
                "values_column": "sales",
                "agg_func": "invalid_func",
            },
        }
        pivot_func = PivotFunction(**config)

        # Act & Assert
        transform_fn = pivot_func.transform()
        with pytest.raises(ValueError, match="Unsupported aggregation function: invalid_func"):
            transform_fn(sales_df)

    def test_transform__multiple_group_by_columns(self, spark: SparkSession) -> None:
        """Test transform with multiple group by columns."""
        # Arrange
        data = [
            ("Store1", "Electronics", "Jan", 1000),
            ("Store1", "Electronics", "Feb", 1200),
            ("Store1", "Clothing", "Jan", 800),
            ("Store1", "Clothing", "Feb", 900),
            ("Store2", "Electronics", "Jan", 1100),
            ("Store2", "Electronics", "Feb", 1300),
            ("Store2", "Clothing", "Jan", 700),
            ("Store2", "Clothing", "Feb", 850),
        ]
        schema = StructType(
            [
                StructField("store", StringType(), True),
                StructField("category", StringType(), True),
                StructField("month", StringType(), True),
                StructField("revenue", IntegerType(), True),
            ]
        )
        input_df = spark.createDataFrame(data, schema)

        config = {
            "function_type": "pivot",
            "arguments": {
                "group_by": ["store", "category"],
                "pivot_column": "month",
                "values_column": "revenue",
                "agg_func": "sum",
            },
        }
        pivot_func = PivotFunction(**config)

        # Act
        transform_fn = pivot_func.transform()
        result_df = transform_fn(input_df)

        # Assert
        assert set(result_df.columns) == {"store", "category", "Jan", "Feb"}
        assert result_df.count() == 4  # 2 stores × 2 categories
