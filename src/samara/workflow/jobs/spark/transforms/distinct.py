"""Distinct transform - Remove duplicate rows from DataFrame.

This module provides a transform function for selecting distinct rows from a
DataFrame. It enables deduplication operations in the pipeline, allowing you
to remove duplicate records based on all columns or a specific subset.

The DistinctFunction is registered with the TransformFunctionRegistry under the name
'distinct', making it available for use in configuration files and pipeline definitions.
"""

from collections.abc import Callable

from pyspark.sql import DataFrame

from samara.workflow.jobs.models.transforms.model_distinct import DistinctFunctionModel
from samara.workflow.jobs.spark.transforms.base import FunctionSpark


class DistinctFunction(DistinctFunctionModel, FunctionSpark):
    """Remove duplicate rows from a DataFrame.

    This transform enables selecting only unique rows from a DataFrame, removing
    duplicates based on all columns. Use it to deduplicate data after joins,
    aggregations, or when cleaning raw input data.

    Attributes:
        function_type: The name of the function (always "distinct")
        arguments: Container for the distinct operation parameters

    Example:
        >>> distinct_fn = DistinctFunction(
        ...     function_type="distinct",
        ...     arguments=DistinctArgs()
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
                    "arguments": {}
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
            arguments: {}
        ```

    Note:
        All columns are considered for uniqueness. The distinct operation keeps
        the first occurrence of each unique combination. For large datasets,
        consider the performance implications of distinct operations.
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
            return df.distinct()

        return __f
