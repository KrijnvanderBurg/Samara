"""Select transform - Project specific columns from a DataFrame.

This module provides column projection capabilities for data pipelines,
enabling users to select only the columns needed for downstream processing.
It focuses on configuration-driven column filtering, making pipeline logic
accessible without code modifications.
"""

import logging
from collections.abc import Callable

from pyspark.sql import DataFrame

from samara.runtime.jobs.models.transforms.model_select import SelectFunctionModel
from samara.runtime.jobs.spark.transforms.base import FunctionSpark
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class SelectFunction(SelectFunctionModel, FunctionSpark):
    """Project specific columns from a DataFrame.

    This transform selects and returns only the specified columns from
    a DataFrame, similar to SQL SELECT projections. It enables efficient
    data filtering by removing unnecessary columns early in the pipeline,
    improving performance and clarity of data flow.

    Attributes:
        function: The transform name (always "select").
        arguments: Configuration specifying which columns to project.
        data_registry: Shared registry for accessing and storing DataFrames.

    Example:
        **Configuration in JSON:**
        ```
        {
            "transforms": [
                {
                    "function": "select",
                    "arguments": {
                        "columns": ["id", "name", "email"]
                    }
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        transforms:
          - function: select
            arguments:
              columns:
                - id
                - name
                - email
        ```

    Note:
        The columns list must contain only columns that exist in the input
        DataFrame. Requesting non-existent columns will raise an error.
    """

    def transform(self) -> Callable:
        """Return a function that projects specified columns from a DataFrame.

        Builds a transformation function that filters the input DataFrame
        to include only the columns specified in the configuration. This is
        a common operation in data pipelines to reduce data volume and focus
        on relevant fields.

        Returns:
            A callable that accepts a DataFrame and returns a new DataFrame
            containing only the selected columns in the specified order.

        Example:
            Input DataFrame schema:
            ```
            root
            |-- id: long (nullable = true)
            |-- name: string (nullable = true)
            |-- age: integer (nullable = true)
            |-- department: string (nullable = true)
            ```

            Configuration:
            ```
            {
                "function": "select",
                "arguments": {
                    "columns": ["name", "age"]
                }
            }
            ```

            Output DataFrame schema:
            ```
            root
            |-- name: string (nullable = true)
            |-- age: integer (nullable = true)
            ```

            The output contains only the projected columns in the order specified.
        """
        logger.debug("Creating select transform for columns: %s", self.arguments.columns)

        def __f(df: DataFrame) -> DataFrame:
            logger.debug("Applying select transform - input columns: %s", df.columns)
            logger.debug("Selecting columns: %s", self.arguments.columns)

            result_df = df.select(*self.arguments.columns)
            logger.info(
                "Select transform completed - selected %d columns from %d", len(result_df.columns), len(df.columns)
            )
            logger.debug("Selected columns: %s", result_df.columns)
            return result_df

        return __f
