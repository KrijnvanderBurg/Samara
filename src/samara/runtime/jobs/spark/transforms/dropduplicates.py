"""Drop duplicates transform function.

This module provides a transform function for removing duplicate rows from a DataFrame,
enabling data deduplication in the ETL pipeline.

The DropDuplicatesFunction is registered with the TransformFunctionRegistry under
the name 'dropduplicates', making it available for use in configuration files.
"""

from collections.abc import Callable

from pyspark.sql import DataFrame

from samara.runtime.jobs.models.transforms.model_dropduplicates import (
    DropDuplicatesFunctionModel,
)
from samara.runtime.jobs.spark.transforms.base import FunctionSpark


class DropDuplicatesFunction(DropDuplicatesFunctionModel, FunctionSpark):
    """Function that removes duplicate rows from a DataFrame.

    This transform function allows for removing duplicate rows based on all columns
    or a subset of columns, helping to clean and deduplicate data.

    Attributes:
        function: The name of the function (always "dropduplicates")
        arguments: Container for the dropDuplicates parameters
    """

    def transform(self) -> Callable:
        """Apply the duplicate removal transformation to the DataFrame.

        This method extracts the columns to consider from the model
        and applies the dropDuplicates operation to the DataFrame, removing duplicate rows.
        If no columns are specified, all columns are considered when identifying duplicates.

        Returns:
            A callable function that performs the duplicate removal when applied
            to a DataFrame

        Examples:
            Consider the following DataFrame:

            ```
            +----+-------+---+
            |id  |name   |age|
            +----+-------+---+
            |1   |John   |25 |
            |2   |Jane   |30 |
            |3   |John   |25 |
            |4   |Bob    |40 |
            +----+-------+---+
            ```

            Applying the dropDuplicates transform with columns ["name", "age"]:

            ```
            {
              "transforms": [
                {
                  "function": "dropduplicates",
                  "arguments": {
                    "columns": ["name", "age"]
                  }
                }
              ]
            }
            ```

            The resulting DataFrame will be:

            ```
            +----+-------+---+
            |id  |name   |age|
            +----+-------+---+
            |1   |John   |25 |
            |2   |Jane   |30 |
            |4   |Bob    |40 |
            +----+-------+---+
            ```
        """

        def __f(df: DataFrame) -> DataFrame:
            columns: list[str] = self.arguments.columns
            if columns:
                return df.dropDuplicates(columns)
            return df.dropDuplicates()

        return __f
