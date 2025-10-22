"""Drop transform - Remove columns from a DataFrame.

This module provides a transform function for removing unwanted columns from
a DataFrame. The drop transform is registered with the TransformFunctionRegistry,
enabling column pruning through configuration in data pipelines."""

from collections.abc import Callable

from pyspark.sql import DataFrame

from samara.runtime.jobs.models.transforms.model_drop import DropFunctionModel
from samara.runtime.jobs.spark.transforms.base import FunctionSpark


class DropFunction(DropFunctionModel, FunctionSpark):
    """Remove specified columns from a DataFrame.

    This transform function removes unwanted columns from a DataFrame,
    enabling memory optimization and data focus in configuration-driven
    pipelines. Columns to drop are specified through configuration.

    Attributes:
        function: The name of the function (always "drop").
        arguments: Container for drop parameters including the columns list.

    Example:
        **Configuration in JSON:**
        ```
        {
            "transforms": [
                {
                    "function": "drop",
                    "arguments": {
                        "columns": ["temp_col", "unused_field"]
                    }
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        transforms:
          - function: drop
            arguments:
              columns:
                - temp_col
                - unused_field
        ```

    Note:
        Attempting to drop columns that do not exist will raise an error.
        Specify only the columns you want to remove; the drop operation
        is executed when the transform is applied to the DataFrame.
    """

    def transform(self) -> Callable:
        """Return a callable that removes specified columns from a DataFrame.

        Extracts the columns to drop from the model configuration and returns
        a function that applies the drop operation to any DataFrame.

        Returns:
            A callable function that accepts a DataFrame and returns a new
            DataFrame with the specified columns removed.

        Example:
            Input DataFrame:

            ```
            +----+-------+---+--------+
            |id  |name   |age|temp_col|
            +----+-------+---+--------+
            |1   |John   |25 |xyz     |
            |2   |Jane   |30 |abc     |
            +----+-------+---+--------+
            ```

            With configuration:

            ```
            {
                "function": "drop",
                "arguments": {
                    "columns": ["temp_col"]
                }
            }
            ```

            Output DataFrame:

            ```
            +----+-------+---+
            |id  |name   |age|
            +----+-------+---+
            |1   |John   |25 |
            |2   |Jane   |30 |
            +----+-------+---+
            ```
        """

        def __f(df: DataFrame) -> DataFrame:
            return df.drop(*self.arguments.columns)

        return __f
