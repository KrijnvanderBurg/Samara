"""Row ordering transform - Sort DataFrame rows by specified columns.

This module provides a transform function for ordering rows in a DataFrame by
one or more columns with configurable sort directions. It enables sorting operations
in the pipeline, allowing you to arrange data for reporting, analysis, or downstream
processing requirements.

The OrderByFunction is registered with the TransformFunctionRegistry under the name
'orderby', making it available for use in configuration files and pipeline definitions.
"""

from collections.abc import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from samara.workflow.jobs.models.transforms.model_orderby import OrderByFunctionModel
from samara.workflow.jobs.spark.transforms.base import FunctionSpark


class OrderByFunction(OrderByFunctionModel, FunctionSpark):
    """Sort DataFrame rows by specified columns and directions.

    This transform enables ordering rows in a DataFrame by one or more columns,
    similar to the ORDER BY clause in SQL. Use it to sort data for reports,
    prepare data for window functions, or ensure consistent output ordering.

    Attributes:
        function_type: The name of the function (always "orderby")
        arguments: Container for the column ordering parameters with list of columns
            and their sort directions

    Example:
        >>> orderby_fn = OrderByFunction(
        ...     function_type="orderby",
        ...     arguments=OrderByArgs(columns=[
        ...         OrderByColumn(column_name="department", ascending=True),
        ...         OrderByColumn(column_name="salary", ascending=False)
        ...     ])
        ... )
        >>> transformed_df = orderby_fn.transform()(df)

        **Configuration in JSON:**
        ```
        {
            "id": "transform-order-data",
            "type": "transform",
            "upstream_id": "extract-data",
            "functions": [
                {
                    "function_type": "orderby",
                    "arguments": {
                        "columns": [
                            {
                                "column_name": "department",
                                "ascending": true
                            },
                            {
                                "column_name": "salary",
                                "ascending": false
                            },
                            {
                                "column_name": "hire_date",
                                "ascending": true
                            }
                        ]
                    }
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        id: transform-order-data
        type: transform
        upstream_id: extract-data
        functions:
          - function_type: orderby
            arguments:
              columns:
                - column_name: department
                  ascending: true
                - column_name: salary
                  ascending: false
                - column_name: hire_date
                  ascending: true
        ```

    Note:
        Ordering is applied in the sequence columns are specified. The first column
        is the primary sort key, subsequent columns break ties. Large datasets may
        require significant resources for global ordering operations.
    """

    def transform(self) -> Callable:
        """Return a callable function that applies row ordering to a DataFrame.

        This method creates and returns a transformation function that sorts
        DataFrame rows according to specified columns and directions. The returned
        function can be applied to any DataFrame with the configured columns.

        Returns:
            A callable function that accepts a DataFrame and returns a new DataFrame
            with rows sorted according to the specified columns and directions.

        Example:
            >>> orderby_transform = orderby_fn.transform()
            >>> result_df = orderby_transform(input_df)
        """

        def __f(df: DataFrame) -> DataFrame:
            """Apply row ordering to the DataFrame.

            Args:
                df: Input DataFrame containing columns to be sorted by

            Returns:
                DataFrame with rows sorted according to specified columns and directions
            """
            # Build list of column expressions with sort directions
            sort_cols = []
            for column in self.arguments.columns:
                if column.ascending:
                    sort_cols.append(col(column.column_name).asc())
                else:
                    sort_cols.append(col(column.column_name).desc())

            # Apply ordering with all columns at once
            return df.orderBy(*sort_cols)

        return __f
