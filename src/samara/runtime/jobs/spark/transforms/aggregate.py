"""Aggregation transform - Perform group-by and aggregate operations on data.

This module provides a transform function for performing aggregation operations
on a DataFrame. It enables group-by operations with various aggregate functions,
allowing you to summarize and analyze data patterns in your pipeline.

The AggregateFunction is registered with the TransformFunctionRegistry under the name
'aggregate', making it available for use in configuration files and pipeline definitions.
"""

from collections.abc import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from samara.runtime.jobs.models.transforms.model_aggregate import AggregateFunctionModel
from samara.runtime.jobs.spark.transforms.base import FunctionSpark


class AggregateFunction(AggregateFunctionModel, FunctionSpark):
    """Perform aggregation operations on DataFrame columns.

    This transform enables grouping data by specified columns and applying
    aggregate functions (sum, avg, min, max, count, etc.) to compute summary
    statistics. Use it to analyze patterns, create summaries, or reduce data
    dimensionality through aggregation.

    Attributes:
        function_type: The name of the function (always "aggregate")
        arguments: Container for the aggregation parameters including group-by
            columns and aggregate function specifications

    Example:
        **Configuration in JSON (full transform step):**
        ```
        "functions": [
            {
                "function_type": "aggregate",
                "arguments": {
                    "group_by_columns": ["store_id", "product_category"],
                    "aggregate_columns": [
                        {
                            "column_name": "sales_amount",
                            "function": "sum",
                            "alias": "total_sales"
                        },
                        {
                            "column_name": "quantity",
                            "function": "sum",
                            "alias": "total_quantity"
                        },
                        {
                            "column_name": "transaction_id",
                            "function": "count",
                            "alias": "transaction_count"
                        },
                        {
                            "column_name": "sales_amount",
                            "function": "avg",
                            "alias": "avg_sale_amount"
                        }
                    ]
                }
            }
        ]
        ```

        **Configuration in YAML (full transform step):**
        ```
        functions:
          - function_type: aggregate
            arguments:
              group_by_columns:
                - store_id
                - product_category
              aggregate_columns:
                - column_name: sales_amount
                  function: sum
                  alias: total_sales
                - column_name: quantity
                  function: sum
                  alias: total_quantity
                - column_name: transaction_id
                  function: count
                  alias: transaction_count
                - column_name: sales_amount
                  function: avg
                  alias: avg_sale_amount
        ```

    Note:
        Supported aggregate functions include: sum, avg, mean, min, max, count,
        countDistinct, stddev, variance, collect_list, collect_set, first, last.
        When group_by_columns is null or empty, performs global aggregation.
    """

    def transform(self) -> Callable:
        """Return a callable function that applies aggregation operations to a DataFrame.

        This method creates and returns a transformation function that groups data
        by specified columns and applies aggregate functions. The returned function
        can be applied to any DataFrame with the configured columns.

        Returns:
            A callable function that accepts a DataFrame and returns a new DataFrame
            with the aggregation results.

        Example:
            >>> aggregate_transform = aggregate_fn.transform()
            >>> result_df = aggregate_transform(input_df)
        """

        def __f(df: DataFrame) -> DataFrame:
            """Apply aggregation operations to the DataFrame.

            Args:
                df: Input DataFrame containing columns to aggregate

            Returns:
                DataFrame with aggregation results based on group-by columns
            """
            # Build list of aggregation expressions
            agg_exprs = []
            for agg_col in self.arguments.aggregate_columns:
                # Get the appropriate Spark aggregation function
                agg_func = getattr(F, agg_col.function)
                agg_exprs.append(agg_func(F.col(agg_col.column_name)).alias(agg_col.alias))

            # Apply aggregation with or without grouping
            if self.arguments.group_by_columns:
                # Group by specified columns then aggregate
                result = df.groupBy(*self.arguments.group_by_columns).agg(*agg_exprs)
            else:
                # Global aggregation (no grouping)
                result = df.agg(*agg_exprs)

            return result

        return __f
