"""Filter transform implementation.

This module provides row filtering capabilities for data pipelines, enabling
users to retain only rows matching specified conditions through configuration-
driven filtering."""

import logging
from collections.abc import Callable

from pyspark.sql import DataFrame

from samara.runtime.jobs.models.transforms.model_filter import FilterFunctionModel
from samara.runtime.jobs.spark.transforms.base import FunctionSpark
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class FilterFunction(FilterFunctionModel, FunctionSpark):
    """Filter rows from a DataFrame based on a condition.

    Applies a boolean condition to filter rows from the input DataFrame,
    similar to the WHERE clause in SQL. This enables efficient data reduction
    by removing unwanted rows early in the pipeline, improving downstream
    performance and focusing data flow on relevant records.

    Attributes:
        function: The transform name (always "filter").
        arguments: Configuration specifying the filter condition.
        data_registry: Shared registry for accessing and storing DataFrames.

    Example:
        **Configuration in JSON:**
        ```
        {
            "transforms": [
                {
                    "id": "filter-adults",
                    "upstream_id": "extract-users",
                    "options": {},
                    "functions": [
                        {
                            "function_type": "filter",
                            "arguments": {
                                "condition": "age > 18"
                            }
                        }
                    ]
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        transforms:
          - id: filter-adults
            upstream_id: extract-users
            options: {}
            functions:
              - function_type: filter
                arguments:
                  condition: age > 18
        ```

    Note:
        The condition must be a valid PySpark SQL expression that evaluates
        to a boolean. Refer to PySpark DataFrame API documentation for
        supported functions and syntax.
    """

    def transform(self) -> Callable:
        """Build and return a filter transformation function.

        Returns a callable that applies the configured condition to filter
        rows from an input DataFrame. Rows matching the condition are retained,
        while non-matching rows are removed.

        Returns:
            A callable that accepts a DataFrame and returns a new DataFrame
            containing only rows where the condition evaluates to true.

        Example:
            Input DataFrame:
            ```
            +----+-------+---+
            | id | name  |age|
            +----+-------+---+
            | 1  | John  |25 |
            | 2  | Jane  |17 |
            | 3  | Bob   |42 |
            | 4  | Alice |15 |
            +----+-------+---+
            ```

            Configuration:
            ```
            {
                "id": "filter-adults",
                "upstream_id": "extract-users",
                "options": {},
                "functions": [
                    {
                        "function_type": "filter",
                        "arguments": {
                            "condition": "age > 18"
                        }
                    }
                ]
            }
            ```

            Output DataFrame:
            ```
            +----+-------+---+
            | id | name  |age|
            +----+-------+---+
            | 1  | John  |25 |
            | 3  | Bob   |42 |
            +----+-------+---+
            ```
        """

        def __f(df: DataFrame) -> DataFrame:
            logger.debug("Applying filter transform with condition: %s", self.arguments.condition)
            original_count = df.count()
            logger.debug("Input DataFrame has %d rows", original_count)

            result_df = df.filter(self.arguments.condition)
            filtered_count = result_df.count()
            filtered_out = original_count - filtered_count

            logger.info("Filter transform completed - kept %d rows, filtered out %d rows", filtered_count, filtered_out)
            return result_df

        return __f
