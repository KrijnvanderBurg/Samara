"""GroupBy aggregation transform - Group rows and apply aggregate functions.

This module provides a transform function for grouping rows by specified columns
and applying aggregate functions to the grouped data. It enables SQL-like GROUP BY
operations in the pipeline, essential for data summarization and analysis.

The GroupByFunction is registered with the TransformFunctionRegistry under the name
'groupby', making it available for use in configuration files and pipeline definitions.
"""

from collections.abc import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from samara.runtime.jobs.models.transforms.model_groupby import GroupByFunctionModel
from samara.runtime.jobs.spark.transforms.base import FunctionSpark


class GroupByFunction(GroupByFunctionModel, FunctionSpark):
    """Group rows and apply aggregate functions to DataFrame columns.

    This transform groups DataFrame rows by specified columns and applies aggregate
    functions like sum, avg, count, etc. to the grouped data. It's equivalent to
    SQL's GROUP BY clause with aggregate functions.

    All configuration fields must be explicitly present. For count operations,
    input_column must be explicitly set to null. For all other aggregate functions,
    input_column must reference a valid column name.

    Attributes:
        function_type: The name of the function (always "groupby")
        arguments: Container for the grouping columns and aggregate functions

    Example:
        **Configuration in JSON:**
        ```
        "functions": [
            {
                "function_type": "groupby",
                "arguments": {
                    "group_columns": ["department", "location"],
                    "aggregations": [
                        {
                            "function": "sum",
                            "input_column": "sales_amount",
                            "output_column": "total_sales"
                        },
                        {
                            "function": "avg",
                            "input_column": "rating",
                            "output_column": "avg_rating"
                        },
                        {
                            "function": "count",
                            "input_column": null,
                            "output_column": "record_count"
                        },
                        {
                            "function": "max",
                            "input_column": "transaction_date",
                            "output_column": "latest_transaction"
                        }
                    ]
                }
            }
        ]
        ```

        **Configuration in YAML:**
        ```
        functions:
          - function_type: groupby
            arguments:
              group_columns:
                - department
                - location
              aggregations:
                - function: sum
                  input_column: sales_amount
                  output_column: total_sales
                - function: avg
                  input_column: rating
                  output_column: avg_rating
                - function: count
                  input_column: null
                  output_column: record_count
                - function: max
                  input_column: transaction_date
                  output_column: latest_transaction
        ```

    Note:
        - The count function requires input_column to be explicitly set to null
        - All other aggregate functions require a valid input_column value
        - Multiple aggregations can be applied in a single groupby operation
        - The 'mean' function is an alias for 'avg' for compatibility
        - Supported aggregate functions: sum, avg, mean, min, max, count, first,
          last, stddev, variance
    """

    def transform(self) -> Callable:
        """Return a callable function that groups and aggregates DataFrame data.

        This method creates and returns a transformation function that groups rows
        by specified columns and applies configured aggregate functions. The returned
        function can be applied to any DataFrame with the required columns.

        Returns:
            A callable function that accepts a DataFrame and returns a new DataFrame
            with grouped and aggregated data.

        Raises:
            RuntimeError: If a count function has a non-null input_column, or if
                a non-count function has a null input_column.
        """

        def __f(df: DataFrame) -> DataFrame:
            """Apply grouping and aggregation to the DataFrame.

            Args:
                df: Input DataFrame containing columns to group and aggregate

            Returns:
                DataFrame with rows grouped by specified columns and aggregate
                functions applied, containing group columns and aggregate result columns

            Raises:
                RuntimeError: If aggregate function configuration is invalid
            """
            # Start with groupBy operation
            grouped = df.groupBy(*self.arguments.group_columns)

            # Build aggregation expressions
            agg_exprs = []
            for agg in self.arguments.aggregations:
                # Map function names to PySpark functions
                func_name = "mean" if agg.function == "avg" else agg.function

                if agg.function == "count":
                    # Count requires input_column to be explicitly null
                    if agg.input_column is not None:
                        raise RuntimeError(f"Count function requires input_column to be null, got: {agg.input_column}")
                    agg_exprs.append(F.count("*").alias(agg.output_column))
                else:
                    # All other functions require a valid input_column
                    if agg.input_column is None:
                        raise RuntimeError(
                            f"Aggregate function '{agg.function}' requires a valid input_column, got null"
                        )
                    # Get the appropriate PySpark function
                    spark_func = getattr(F, func_name)
                    agg_exprs.append(spark_func(agg.input_column).alias(agg.output_column))

            # Apply all aggregations
            result = grouped.agg(*agg_exprs)

            return result

        return __f
