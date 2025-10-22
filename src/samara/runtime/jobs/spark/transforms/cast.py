"""Column casting transform - Convert columns to specified data types.

This module provides a transform function for casting columns to specific data
types in a DataFrame. It enables type conversion operations in the pipeline,
allowing you to ensure data types match expected formats for downstream processing.

The CastFunction is registered with the TransformFunctionRegistry under the name
'cast', making it available for use in configuration files and pipeline definitions.
"""

from collections.abc import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from samara.runtime.jobs.models.transforms.model_cast import CastFunctionModel
from samara.runtime.jobs.spark.transforms.base import FunctionSpark


class CastFunction(CastFunctionModel, FunctionSpark):
    """Cast columns to specified data types in a DataFrame.

    This transform enables changing the data type of specific columns in a DataFrame,
    similar to the CAST statement in SQL. Use it to ensure data types match expected
    formats for downstream processing or to correct data type issues after import.

    Attributes:
        function_type: The name of the function (always "cast")
        arguments: Container for the column casting parameters with list of columns
            and their target types

    Example:
        >>> cast_fn = CastFunction(
        ...     function_type="cast",
        ...     arguments=CastArgs(columns=[
        ...         CastColumn(column_name="age", cast_type="int"),
        ...         CastColumn(column_name="salary", cast_type="decimal(10, 2)")
        ...     ])
        ... )
        >>> transformed_df = cast_fn.transform()(df)

        **Configuration in JSON:**
        ```
        {
            "id": "transform-cast-types",
            "upstream_id": "extract-data",
            "functions": [
                {
                    "function_type": "cast",
                    "arguments": {
                        "columns": [
                            {
                                "column_name": "age",
                                "cast_type": "int"
                            },
                            {
                                "column_name": "salary",
                                "cast_type": "decimal(10, 2)"
                            },
                            {
                                "column_name": "hire_date",
                                "cast_type": "date"
                            }
                        ]
                    }
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        id: transform-cast-types
        upstream_id: extract-data
        functions:
          - function_type: cast
            arguments:
              columns:
                - column_name: age
                  cast_type: int
                - column_name: salary
                  cast_type: decimal(10, 2)
                - column_name: hire_date
                  cast_type: date
        ```

    Note:
        Supported cast types depend on the underlying engine (Spark, Pandas, etc.).
        Common types include: int, long, double, string, date, timestamp, boolean.
        Ensure the source data can be safely cast to the target type to avoid
        null values or casting errors.
    """

    def transform(self) -> Callable:
        """Return a callable function that applies column type conversions to a DataFrame.

        This method creates and returns a transformation function that converts
        specified columns to their target data types. The returned function can be
        applied to any DataFrame with the configured columns.

        Returns:
            A callable function that accepts a DataFrame and returns a new DataFrame
            with the specified column type conversions applied.

        Example:
            >>> cast_transform = cast_fn.transform()
            >>> result_df = cast_transform(input_df)
        """

        def __f(df: DataFrame) -> DataFrame:
            """Apply column type conversions to the DataFrame.

            Args:
                df: Input DataFrame containing columns to be cast to new types

            Returns:
                DataFrame with all specified column type conversions applied
            """
            result = df
            for column in self.arguments.columns:
                # Cast each column to its specified data type
                result = result.withColumn(column.column_name, col(column.column_name).cast(column.cast_type))
            return result

        return __f
