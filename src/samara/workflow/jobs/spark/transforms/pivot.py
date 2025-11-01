"""Pivot transform - Convert rows to columns based on unique values.

This module provides a transform function for pivoting data in a DataFrame,
converting unique row values into separate columns. This is useful for reshaping
data from long format to wide format.

The PivotFunction is registered with the TransformFunctionRegistry under the name
'pivot', making it available for use in configuration files and pipeline definitions.
"""

from collections.abc import Callable

from pyspark.sql import DataFrame

from samara.workflow.jobs.models.transforms.model_pivot import PivotFunctionModel
from samara.workflow.jobs.spark.transforms.base import FunctionSpark


class PivotFunction(PivotFunctionModel, FunctionSpark):
    """Pivot rows to columns in a DataFrame based on unique values.

    This transform reshapes data by converting unique values from a specified column
    into separate columns, with values aggregated according to the specified function.
    Use it to transform data from long format to wide format for analysis or reporting.

    Attributes:
        function_type: The name of the function (always "pivot")
        arguments: Container for the pivot parameters including group_by columns,
            pivot column, values column, and aggregation function

    Example:
        >>> pivot_fn = PivotFunction(
        ...     function_type="pivot",
        ...     arguments=PivotArgs(
        ...         group_by=["product_id"],
        ...         pivot_column="quarter",
        ...         values_column="sales",
        ...         agg_func="sum"
        ...     )
        ... )
        >>> transformed_df = pivot_fn.transform()(df)

        **Configuration in JSON:**
        ```
        {
            "type": "transform",
            "id": "transform-pivot-sales",
            "upstream_id": "extract-sales-data",
            "functions": [
                {
                    "function_type": "pivot",
                    "arguments": {
                        "group_by": ["product_id", "region"],
                        "pivot_column": "quarter",
                        "values_column": "sales",
                        "agg_func": "sum"
                    }
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        type: transform
        id: transform-pivot-sales
        upstream_id: extract-sales-data
        functions:
          - function_type: pivot
            arguments:
              group_by:
                - product_id
                - region
              pivot_column: quarter
              values_column: sales
              agg_func: sum
        ```

    Note:
        The pivot operation creates a new column for each unique value in the pivot_column.
        Be cautious with columns containing many unique values as this can result in a very
        wide DataFrame. The aggregation function is applied when multiple values exist for
        the same group and pivot value combination.
    """

    def transform(self) -> Callable:
        """Return a callable function that applies pivot transformation to a DataFrame.

        This method creates and returns a transformation function that pivots the DataFrame,
        converting unique values from the pivot column into separate columns with aggregated values.

        Returns:
            A callable function that accepts a DataFrame and returns a new DataFrame
            with the pivot transformation applied.

        Example:
            >>> pivot_transform = pivot_fn.transform()
            >>> result_df = pivot_transform(input_df)
        """

        def __f(df: DataFrame) -> DataFrame:
            """Apply pivot transformation to the DataFrame.

            Args:
                df: Input DataFrame to be pivoted

            Returns:
                DataFrame with pivot transformation applied, where unique values
                from pivot_column become new columns
            """
            # Validate supported aggregation functions
            supported_agg_funcs = {"sum", "avg", "max", "min", "count", "first"}
            if self.arguments.agg_func not in supported_agg_funcs:
                raise ValueError(f"Unsupported aggregation function: {self.arguments.agg_func}")

            # Perform the pivot operation
            result = (
                df.groupBy(*self.arguments.group_by)
                .pivot(self.arguments.pivot_column)
                .agg({self.arguments.values_column: self.arguments.agg_func})
            )

            return result

        return __f
