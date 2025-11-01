"""Tests for Join transform function (Spark implementation)."""

from __future__ import annotations

from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual
from samara.workflow.jobs.models.transforms.model_join import JoinArgs
from samara.workflow.jobs.spark.transforms.join import JoinFunction

# =========================================================================== #
# ============================== TEST DATA ================================= #
# =========================================================================== #


@pytest.fixture
def users_df(spark: SparkSession) -> DataFrame:
    """Create users DataFrame for join testing."""
    data = [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Diana")]
    schema = StructType(
        [
            StructField("user_id", IntegerType(), False),
            StructField("name", StringType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


@pytest.fixture
def orders_df(spark: SparkSession) -> DataFrame:
    """Create orders DataFrame for join testing."""
    data = [
        (101, 1, 100.0),
        (102, 2, 200.0),
        (103, 1, 150.0),
        (104, 5, 300.0),  # user_id 5 doesn't exist in users
    ]
    schema = StructType(
        [
            StructField("order_id", IntegerType(), False),
            StructField("user_id", IntegerType(), False),
            StructField("amount", FloatType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="join_config")
def fixture_join_config() -> dict[str, Any]:
    """Return a config dict for JoinFunction."""
    return {"function_type": "join", "arguments": {"other_upstream_id": "other_df", "on": "id", "how": "inner"}}


def test_join_creation__from_config__creates_valid_model(join_config: dict[str, Any]) -> None:
    """Instantiate from config only to test dict-based initialization."""
    f = JoinFunction(**join_config)
    assert f.function_type == "join"
    assert isinstance(f.arguments, JoinArgs)
    assert f.arguments.other_upstream_id == "other_df"
    assert f.arguments.on == "id"
    assert f.arguments.how == "inner"


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="join_func")
def fixture_join_func(join_config: dict[str, Any]) -> JoinFunction:
    """Instantiate a JoinFunction from the config dict."""
    return JoinFunction(**join_config)


def test_join_fixture__args(join_func: JoinFunction) -> None:
    """Assert the instantiated fixture has expected join arguments."""
    assert join_func.function_type == "join"
    assert isinstance(join_func.arguments, JoinArgs)
    assert join_func.arguments.other_upstream_id == "other_df"
    assert join_func.arguments.on == "id"
    assert join_func.arguments.how == "inner"


# =========================================================================== #
# ================================== TESTS ================================== #
# =========================================================================== #


class TestJoinFunctionTransform:
    """Test JoinFunction transform behavior with real Spark DataFrames."""

    def test_transform__returns_callable(self, join_func: JoinFunction) -> None:
        """Test transform returns a callable function."""
        # Act
        transform_fn = join_func.transform()

        # Assert
        assert callable(transform_fn)

    def test_transform__inner_join(self, spark: SparkSession, users_df: DataFrame, orders_df: DataFrame) -> None:
        """Test inner join with real DataFrames using assertDataFrameEqual."""
        # Setup
        config = {
            "function_type": "join",
            "arguments": {"other_upstream_id": "orders", "on": "user_id", "how": "inner"},
        }
        join_func = JoinFunction(**config)

        # Set up registry with real DataFrame
        join_func.data_registry["orders"] = orders_df

        # Expected result - only matching rows
        expected_data = [
            (1, "Alice", 101, 100.0),
            (1, "Alice", 103, 150.0),
            (2, "Bob", 102, 200.0),
        ]
        expected_schema = StructType(
            [
                StructField("user_id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("order_id", IntegerType(), False),
                StructField("amount", FloatType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Act
        transform_fn = join_func.transform()
        result_df = transform_fn(users_df)

        # Assert - Use checkRowOrder=False since join order may vary
        assertDataFrameEqual(result_df, expected_df, checkRowOrder=False)

        # Clean up
        join_func.data_registry.clear()

    def test_transform__left_join(self, spark: SparkSession, users_df: DataFrame, orders_df: DataFrame) -> None:
        """Test left join includes all users even without orders."""
        # Setup
        config = {
            "function_type": "join",
            "arguments": {"other_upstream_id": "orders", "on": "user_id", "how": "left"},
        }
        join_func = JoinFunction(**config)
        join_func.data_registry["orders"] = orders_df

        # Act
        transform_fn = join_func.transform()
        result_df = transform_fn(users_df)

        # Assert
        assert result_df.count() == 5  # Alice appears twice (2 orders) + Bob + Charlie + Diana

        # Check that users without orders have null values
        no_orders = result_df.filter("name IN ('Charlie', 'Diana')").select("order_id", "amount").collect()
        for row in no_orders:
            assert row["order_id"] is None
            assert row["amount"] is None

        # Clean up
        join_func.data_registry.clear()

    def test_transform__right_join(self, spark: SparkSession, users_df: DataFrame, orders_df: DataFrame) -> None:
        """Test right join includes all orders even for non-existent users."""
        # Setup
        config = {
            "function_type": "join",
            "arguments": {"other_upstream_id": "orders", "on": "user_id", "how": "right"},
        }
        join_func = JoinFunction(**config)
        join_func.data_registry["orders"] = orders_df

        # Act
        transform_fn = join_func.transform()
        result_df = transform_fn(users_df)

        # Assert
        assert result_df.count() == 4  # All 4 orders

        # Check that order 104 (user_id=5) has null name
        orphan_order = result_df.filter("order_id = 104").select("name").collect()
        assert orphan_order[0]["name"] is None

        # Clean up
        join_func.data_registry.clear()

    def test_transform__outer_join(self, spark: SparkSession, users_df: DataFrame, orders_df: DataFrame) -> None:
        """Test outer join includes all users and all orders."""
        # Setup
        config = {
            "function_type": "join",
            "arguments": {"other_upstream_id": "orders", "on": "user_id", "how": "outer"},
        }
        join_func = JoinFunction(**config)
        join_func.data_registry["orders"] = orders_df

        # Act
        transform_fn = join_func.transform()
        result_df = transform_fn(users_df)

        # Assert
        assert result_df.count() == 6  # Alice(2 orders) + Bob + Charlie + Diana + orphan order(104)

        # Clean up
        join_func.data_registry.clear()
