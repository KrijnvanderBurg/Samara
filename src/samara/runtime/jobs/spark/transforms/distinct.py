"""Distinct transform - Remove duplicate rows from DataFrame.

This module provides a transform function for selecting distinct rows from a
DataFrame. It enables deduplication operations in the pipeline, allowing you
to remove duplicate records based on all columns or a specific subset.

The DistinctFunction is registered with the TransformFunctionRegistry under the name
'distinct', making it available for use in configuration files and pipeline definitions.
"""

from collections.abc import Callable

from pyspark.sql import DataFrame

from samara.runtime.jobs.models.transforms.model_distinct import DistinctFunctionModel
from samara.runtime.jobs.spark.transforms.base import FunctionSpark


class DistinctFunction(DistinctFunctionModel, FunctionSpark):
    """Remove duplicate rows from a DataFrame.

    This transform enables selecting only unique rows from a DataFrame, removing
    duplicates based on all columns or a specified subset. Use it to deduplicate
    data after joins, aggregations, or when cleaning raw input data.

    Attributes:
        function_type: The name of the function (always "distinct")
        arguments: Container for the distinct operation parameters with optional
            column list

    Example:
        >>> distinct_fn = DistinctFunction(
        ...     function_type="distinct",
        ...     arguments=DistinctArgs(columns=["user_id", "date"])
        ... )
        >>> transformed_df = distinct_fn.transform()(df)

        **Configuration in JSON:**
        ```
        {
            "id": "transform-deduplicate",
            "type": "transform",
            "upstream_id": "extract-data",
            "functions": [
                {
                    "function_type": "distinct",
                    "arguments": {
                        "columns": ["customer_id", "order_date"]
                    }
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        id: transform-deduplicate
        type: transform
        upstream_id: extract-data
        functions:
          - function_type: distinct
            arguments:
              columns:
                - customer_id
                - order_date
        ```

    Note:
        When columns is null or empty, all columns are considered for uniqueness.
        The distinct operation keeps the first occurrence of each unique combination.
        For large datasets, consider the performance implications of distinct operations.
    """

    def transform(self) -> Callable:
        """Return a callable function that removes duplicate rows from a DataFrame.

        This method creates and returns a transformation function that performs
        deduplication based on the configured columns. The returned function can be
        applied to any DataFrame with the specified columns.

        Returns:
            A callable function that accepts a DataFrame and returns a new DataFrame
            with duplicate rows removed based on the configured criteria.

        Example:
            >>> distinct_transform = distinct_fn.transform()
            >>> result_df = distinct_transform(input_df)
        """

        def __f(df: DataFrame) -> DataFrame:
            """Remove duplicate rows from the DataFrame.

            Args:
                df: Input DataFrame potentially containing duplicate rows

            Returns:
                DataFrame with duplicate rows removed based on configured columns
            """
            if self.arguments.columns is None or len(self.arguments.columns) == 0:
                # Consider all columns for uniqueness
                return df.distinct()
            else:
                # Consider only specified columns for uniqueness
                return df.dropDuplicates(self.arguments.columns)

        return __f
