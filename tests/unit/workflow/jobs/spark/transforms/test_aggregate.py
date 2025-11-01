"""Tests for Aggregate transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual
from samara.workflow.jobs.models.transforms.model_aggregate import AggregateArgs
from samara.workflow.jobs.spark.transforms.aggregate import AggregateFunction

# =========================================================================== #
# ============================== TEST DATA ================================= #
# =========================================================================== #


@pytest.fixture
def sales_df(spark: SparkSession) -> DataFrame:
    """Create sales DataFrame for aggregation testing."""
    data = [
        ("Electronics", "Laptop", 1000, 1),
        ("Electronics", "Mouse", 25, 10),
        ("Electronics", "Monitor", 400, 2),
        ("Furniture", "Desk", 300, 1),
        ("Furniture", "Chair", 150, 4),
        ("Furniture", "Chair", 200, 2),
    ]
    schema = StructType(
        [
            StructField("category", StringType(), True),
            StructField("product", StringType(), True),
            StructField("price", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="aggregate_config")
def fixture_aggregate_config() -> dict[str, Any]:
    """Configuration dict for AggregateFunction."""
    return {
        "function_type": "aggregate",
        "arguments": {
            "group_by_columns": ["department"],
            "aggregate_columns": [
                {"column_name": "salary", "function": "avg", "alias": "avg_salary"},
                {"column_name": "employee_id", "function": "count", "alias": "employee_count"},
            ],
        },
    }


@pytest.fixture(name="aggregate_config_no_grouping")
def fixture_aggregate_config_no_grouping() -> dict[str, Any]:
    """Configuration dict for AggregateFunction without grouping."""
    return {
        "function_type": "aggregate",
        "arguments": {
            "group_by_columns": None,
            "aggregate_columns": [{"column_name": "revenue", "function": "sum", "alias": "total_revenue"}],
        },
    }


def test_aggregate_creation__from_config__creates_valid_model(aggregate_config: dict[str, Any]) -> None:
    """Ensure the AggregateFunction can be created from a config dict."""
    f = AggregateFunction(**aggregate_config)
    assert f.function_type == "aggregate"
    assert isinstance(f.arguments, AggregateArgs)
    assert f.arguments.group_by_columns == ["department"]
    assert len(f.arguments.aggregate_columns) == 2
    assert f.arguments.aggregate_columns[0].column_name == "salary"
    assert f.arguments.aggregate_columns[0].function == "avg"
    assert f.arguments.aggregate_columns[0].alias == "avg_salary"


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestAggregateFunctionValidation:
    """Test AggregateFunction model validation and instantiation."""

    def test_create_aggregate_function__with_wrong_function_name__raises_validation_error(
        self, aggregate_config: dict[str, Any]
    ) -> None:
        """Test AggregateFunction creation fails with wrong function name."""
        aggregate_config["function_type"] = "wrong_name"

        with pytest.raises(ValidationError):
            AggregateFunction(**aggregate_config)

    def test_create_aggregate_function__with_missing_arguments__raises_validation_error(
        self, aggregate_config: dict[str, Any]
    ) -> None:
        """Test AggregateFunction creation fails without arguments field."""
        del aggregate_config["arguments"]

        with pytest.raises(ValidationError):
            AggregateFunction(**aggregate_config)

    def test_create_aggregate_function__with_empty_aggregate_columns__succeeds(
        self, aggregate_config: dict[str, Any]
    ) -> None:
        """Test AggregateFunction creation succeeds with empty aggregate columns list."""
        aggregate_config["arguments"]["aggregate_columns"] = []

        # Act
        aggregate_function = AggregateFunction(**aggregate_config)

        # Assert
        assert aggregate_function.arguments.aggregate_columns == []

    def test_create_aggregate_function__with_missing_group_by_columns__raises_validation_error(
        self, aggregate_config: dict[str, Any]
    ) -> None:
        """Test AggregateFunction creation fails with missing group_by_columns field."""
        del aggregate_config["arguments"]["group_by_columns"]

        with pytest.raises(ValidationError):
            AggregateFunction(**aggregate_config)

    def test_create_aggregate_function__with_null_group_by_columns__succeeds(
        self, aggregate_config_no_grouping: dict[str, Any]
    ) -> None:
        """Test AggregateFunction creation succeeds with null group_by_columns."""
        # Act
        aggregate_function = AggregateFunction(**aggregate_config_no_grouping)

        # Assert
        assert aggregate_function.arguments.group_by_columns is None

    def test_create_aggregate_function__with_missing_column_name__raises_validation_error(
        self, aggregate_config: dict[str, Any]
    ) -> None:
        """Test AggregateFunction creation fails with missing column_name."""
        del aggregate_config["arguments"]["aggregate_columns"][0]["column_name"]

        with pytest.raises(ValidationError):
            AggregateFunction(**aggregate_config)

    def test_create_aggregate_function__with_missing_function__raises_validation_error(
        self, aggregate_config: dict[str, Any]
    ) -> None:
        """Test AggregateFunction creation fails with missing function."""
        del aggregate_config["arguments"]["aggregate_columns"][0]["function"]

        with pytest.raises(ValidationError):
            AggregateFunction(**aggregate_config)

    def test_create_aggregate_function__with_missing_alias__raises_validation_error(
        self, aggregate_config: dict[str, Any]
    ) -> None:
        """Test AggregateFunction creation fails with missing alias."""
        del aggregate_config["arguments"]["aggregate_columns"][0]["alias"]

        with pytest.raises(ValidationError):
            AggregateFunction(**aggregate_config)

    def test_create_aggregate_function__with_empty_string_fields__raises_validation_error(
        self, aggregate_config: dict[str, Any]
    ) -> None:
        """Test AggregateFunction creation fails with empty string fields."""
        aggregate_config["arguments"]["aggregate_columns"][0]["column_name"] = ""

        with pytest.raises(ValidationError):
            AggregateFunction(**aggregate_config)

    def test_create_aggregate_function__with_multiple_group_columns__succeeds(
        self, aggregate_config: dict[str, Any]
    ) -> None:
        """Test AggregateFunction creation succeeds with multiple group columns."""
        aggregate_config["arguments"]["group_by_columns"] = ["department", "region", "office"]

        # Act
        aggregate_function = AggregateFunction(**aggregate_config)

        # Assert
        assert aggregate_function.arguments.group_by_columns == ["department", "region", "office"]

    def test_create_aggregate_function__with_various_agg_functions__succeeds(
        self, aggregate_config: dict[str, Any]
    ) -> None:
        """Test AggregateFunction creation succeeds with various aggregate functions."""
        aggregate_config["arguments"]["aggregate_columns"] = [
            {"column_name": "col1", "function": "sum", "alias": "sum_col"},
            {"column_name": "col2", "function": "min", "alias": "min_col"},
            {"column_name": "col3", "function": "max", "alias": "max_col"},
            {"column_name": "col4", "function": "countDistinct", "alias": "distinct_col"},
        ]

        # Act
        aggregate_function = AggregateFunction(**aggregate_config)

        # Assert
        assert len(aggregate_function.arguments.aggregate_columns) == 4
        assert aggregate_function.arguments.aggregate_columns[3].function == "countDistinct"

    def test_create_aggregate_function__with_missing_aggregate_columns__raises_validation_error(
        self, aggregate_config: dict[str, Any]
    ) -> None:
        """Test AggregateFunction creation fails with missing aggregate_columns field."""
        del aggregate_config["arguments"]["aggregate_columns"]

        with pytest.raises(ValidationError):
            AggregateFunction(**aggregate_config)

    def test_create_aggregate_function__with_empty_alias__raises_validation_error(
        self, aggregate_config: dict[str, Any]
    ) -> None:
        """Test AggregateFunction creation fails with empty alias."""
        aggregate_config["arguments"]["aggregate_columns"][0]["alias"] = ""

        with pytest.raises(ValidationError):
            AggregateFunction(**aggregate_config)

    def test_create_aggregate_function__with_empty_function_name__raises_validation_error(
        self, aggregate_config: dict[str, Any]
    ) -> None:
        """Test AggregateFunction creation fails with empty function name."""
        aggregate_config["arguments"]["aggregate_columns"][0]["function"] = ""

        with pytest.raises(ValidationError):
            AggregateFunction(**aggregate_config)


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="aggregate_func")
def fixture_aggregate_func(aggregate_config: dict[str, Any]) -> AggregateFunction:
    """Instantiate an AggregateFunction from the config dict."""
    return AggregateFunction(**aggregate_config)


@pytest.fixture(name="aggregate_func_no_grouping")
def fixture_aggregate_func_no_grouping(aggregate_config_no_grouping: dict[str, Any]) -> AggregateFunction:
    """Instantiate an AggregateFunction without grouping from the config dict."""
    return AggregateFunction(**aggregate_config_no_grouping)


def test_aggregate_fixture(aggregate_func: AggregateFunction) -> None:
    """Sanity-check the instantiated fixture has the expected arguments."""
    assert aggregate_func.function_type == "aggregate"
    assert isinstance(aggregate_func.arguments, AggregateArgs)
    assert aggregate_func.arguments.group_by_columns == ["department"]
    assert aggregate_func.arguments.aggregate_columns[0].column_name == "salary"
    assert aggregate_func.arguments.aggregate_columns[0].function == "avg"
    assert aggregate_func.arguments.aggregate_columns[0].alias == "avg_salary"


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestAggregateFunctionTransform:
    """Test AggregateFunction transform behavior with real Spark DataFrames."""

    def test_transform__returns_callable(self, aggregate_func: AggregateFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = aggregate_func.transform()

        # Assert
        assert callable(transform_fn)

    def test_transform__with_grouping__aggregates_correctly(self, spark: SparkSession, sales_df: DataFrame) -> None:
        """Test aggregation with grouping using assertDataFrameEqual."""
        # Arrange
        config = {
            "function_type": "aggregate",
            "arguments": {
                "group_by_columns": ["category"],
                "aggregate_columns": [
                    {"column_name": "price", "function": "sum", "alias": "total_price"},
                    {"column_name": "quantity", "function": "sum", "alias": "total_quantity"},
                    {"column_name": "product", "function": "count", "alias": "product_count"},
                ],
            },
        }
        agg_func = AggregateFunction(**config)

        # Expected DataFrame
        expected_data = [
            ("Electronics", 1425, 13, 3),
            ("Furniture", 650, 7, 3),
        ]
        expected_schema = StructType(
            [
                StructField("category", StringType(), True),
                StructField("total_price", LongType(), True),
                StructField("total_quantity", LongType(), True),
                StructField("product_count", LongType(), False),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = agg_func.transform()
        result_df = transform_fn(sales_df)

        # Assert - Use checkRowOrder=False since group by doesn't guarantee order
        assertDataFrameEqual(result_df, expected_df, checkRowOrder=False)

    def test_transform__without_grouping__aggregates_all_rows(self, spark: SparkSession, sales_df: DataFrame) -> None:
        """Test aggregation without grouping using assertDataFrameEqual."""
        # Arrange
        config = {
            "function_type": "aggregate",
            "arguments": {
                "group_by_columns": None,
                "aggregate_columns": [
                    {"column_name": "price", "function": "sum", "alias": "total_revenue"},
                    {"column_name": "price", "function": "max", "alias": "max_price"},
                    {"column_name": "price", "function": "min", "alias": "min_price"},
                    {"column_name": "price", "function": "avg", "alias": "avg_price"},
                ],
            },
        }
        agg_func = AggregateFunction(**config)

        # Expected DataFrame (single row with aggregates)
        expected_data = [(2075, 1000, 25, 345.8333333333333)]
        expected_schema = StructType(
            [
                StructField("total_revenue", LongType(), True),
                StructField("max_price", IntegerType(), True),
                StructField("min_price", IntegerType(), True),
                StructField("avg_price", DoubleType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = agg_func.transform()
        result_df = transform_fn(sales_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__with_multiple_group_columns(self, spark: SparkSession, sales_df: DataFrame) -> None:
        """Test transform handles multiple group-by columns."""
        # Arrange
        config = {
            "function_type": "aggregate",
            "arguments": {
                "group_by_columns": ["category", "product"],
                "aggregate_columns": [
                    {"column_name": "price", "function": "avg", "alias": "avg_price"},
                    {"column_name": "quantity", "function": "sum", "alias": "total_qty"},
                ],
            },
        }
        agg_func = AggregateFunction(**config)

        # Expected DataFrame
        expected_data = [
            ("Electronics", "Laptop", 1000.0, 1),
            ("Electronics", "Monitor", 400.0, 2),
            ("Electronics", "Mouse", 25.0, 10),
            ("Furniture", "Chair", 175.0, 6),  # Avg of 150 and 200
            ("Furniture", "Desk", 300.0, 1),
        ]
        expected_schema = StructType(
            [
                StructField("category", StringType(), True),
                StructField("product", StringType(), True),
                StructField("avg_price", DoubleType(), True),
                StructField("total_qty", LongType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = agg_func.transform()
        result_df = transform_fn(sales_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df, checkRowOrder=False)

    def test_transform__with_empty_group_columns_list__performs_global_agg(
        self, spark: SparkSession, sales_df: DataFrame
    ) -> None:
        """Test transform treats empty group columns list as global aggregation."""
        # Arrange
        config = {
            "function_type": "aggregate",
            "arguments": {
                "group_by_columns": [],
                "aggregate_columns": [
                    {"column_name": "price", "function": "count", "alias": "total_items"},
                ],
            },
        }
        agg_func = AggregateFunction(**config)

        # Expected DataFrame
        expected_data = [(6,)]  # 6 rows total
        expected_schema = StructType([StructField("total_items", LongType(), False)])
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = agg_func.transform()
        result_df = transform_fn(sales_df)

        # Assert
        assertDataFrameEqual(result_df, expected_df)

    def test_transform__with_various_agg_functions(self, spark: SparkSession) -> None:
        """Test transform with various aggregate functions."""
        # Arrange - Create a more suitable dataset for various aggregations
        data = [
            ("A", 10, 1.5),
            ("A", 20, 2.5),
            ("A", 30, 3.5),
            ("B", 15, 1.0),
            ("B", 25, 2.0),
        ]
        schema = StructType(
            [
                StructField("group", StringType(), True),
                StructField("value", IntegerType(), True),
                StructField("score", DoubleType(), True),
            ]
        )
        input_df = spark.createDataFrame(data, schema)

        config = {
            "function_type": "aggregate",
            "arguments": {
                "group_by_columns": ["group"],
                "aggregate_columns": [
                    {"column_name": "value", "function": "sum", "alias": "sum_val"},
                    {"column_name": "value", "function": "min", "alias": "min_val"},
                    {"column_name": "value", "function": "max", "alias": "max_val"},
                    {"column_name": "value", "function": "avg", "alias": "avg_val"},
                    {"column_name": "value", "function": "count", "alias": "count_val"},
                ],
            },
        }
        agg_func = AggregateFunction(**config)

        # Act
        transform_fn = agg_func.transform()
        result_df = transform_fn(input_df)

        # Assert - Check specific values
        result_a = result_df.filter("group = 'A'").collect()[0]
        assert result_a["sum_val"] == 60
        assert result_a["min_val"] == 10
        assert result_a["max_val"] == 30
        assert result_a["avg_val"] == 20.0
        assert result_a["count_val"] == 3

        result_b = result_df.filter("group = 'B'").collect()[0]
        assert result_b["sum_val"] == 40
        assert result_b["min_val"] == 15
        assert result_b["max_val"] == 25
        assert result_b["avg_val"] == 20.0
        assert result_b["count_val"] == 2
