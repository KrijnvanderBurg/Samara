"""WithColumn transform - Add or replace DataFrame columns with expressions.

This module provides the WithColumn transform, enabling column manipulation
in configuration-driven ETL pipelines. Supports adding new columns or replacing
existing ones using PySpark SQL expressions.
"""

from collections.abc import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from samara.runtime.jobs.models.transforms.model_withcolumn import WithColumnFunctionModel
from samara.runtime.jobs.spark.transforms.base import FunctionSpark


class WithColumnFunction(WithColumnFunctionModel, FunctionSpark):
    """Add or replace a column in a DataFrame using an expression.

    This transform enables dynamic column addition and modification.
    Specify a column name and a PySpark SQL expression to create derived
    columns, perform data transformations, or add calculated fields.

    Attributes:
        function: The name of the function (always "withColumn").
        arguments: Configuration specifying col_name and col_expr.
        data_registry: Shared registry for accessing and storing DataFrames.

    Example:
        **Configuration in JSON:**
        ```
        {
            "function": "withColumn",
            "arguments": {
                "col_name": "full_name",
                "col_expr": "concat(first_name, ' ', last_name)"
            }
        }
        ```

        **Configuration in YAML:**
        ```
        function: withColumn
        arguments:
          col_name: full_name
          col_expr: "concat(first_name, ' ', last_name)"
        ```

        **Input DataFrame:**
        ```
        +----------+---------+---+
        |first_name|last_name|age|
        +----------+---------+---+
        |John      |Doe      |25 |
        |Jane      |Smith    |30 |
        +----------+---------+---+
        ```

        **Output DataFrame:**
        ```
        +----------+---------+---+---------+
        |first_name|last_name|age|full_name|
        +----------+---------+---+---------+
        |John      |Doe      |25 |John Doe |
        |Jane      |Smith    |30 |Jane Smith|
        +----------+---------+---+---------+
        ```

    Note:
        The col_expr parameter accepts any valid PySpark SQL expression
        that can be used with DataFrame.withColumn(). Expressions can reference
        existing columns and PySpark functions.
    """

    def transform(self) -> Callable:
        """Return a callable that adds or replaces a column in a DataFrame.

        Extracts the column name and expression from the configuration model
        and returns a function that applies the withColumn operation. The
        returned function is designed to be applied to a DataFrame in the
        transform pipeline.

        Returns:
            A callable that accepts a DataFrame and returns a DataFrame with
            the specified column added or replaced based on the configured
            expression.

        Example:
            >>> transform_func = WithColumnFunction(args).transform()
            >>> result_df = transform_func(input_df)

            The above applies the column transformation using the configured
            column name and SQL expression.
        """

        def __f(df: DataFrame) -> DataFrame:
            return df.withColumn(self.arguments.col_name, expr(self.arguments.col_expr))

        return __f
