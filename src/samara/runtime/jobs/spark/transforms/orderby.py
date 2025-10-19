"""OrderBy (sorting) transform function.

This module provides a transform function for sorting DataFrames by one or
more columns in ascending or descending order.

The OrderByFunction is registered with the TransformFunctionRegistry under
the name 'orderby', making it available for use in configuration files.
"""

import logging
from collections.abc import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import asc, desc

from samara.runtime.jobs.models.transforms.model_orderby import OrderByFunctionModel
from samara.runtime.jobs.spark.transforms.base import FunctionSpark
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class OrderByFunction(OrderByFunctionModel, FunctionSpark):
    """Function that sorts a DataFrame by specified columns.

    This transform function allows for sorting DataFrames by one or more
    columns in ascending or descending order, similar to the ORDER BY clause
    in SQL. Multiple columns can be specified, with sort priority determined
    by their order in the configuration.

    The function is configured using an OrderByFunctionModel that specifies
    which columns to sort by and their sort directions.

    Attributes:
        function_type: The name of the function (always "orderby")
        arguments: Container for the sorting parameters

    Example:
        ```json
        {
            "function_type": "orderby",
            "arguments": {
                "columns": [
                    {"column_name": "category", "order": "asc"},
                    {"column_name": "sales", "order": "desc"}
                ]
            }
        }
        ```
    """

    def transform(self) -> Callable:
        """Apply the sorting transformation to the DataFrame.

        This method extracts the sorting configuration from the model
        and applies it to the DataFrame, returning the sorted result.

        Returns:
            A callable function that performs the sorting when applied
            to a DataFrame

        Examples:
            Consider the following DataFrame:

            ```
            +----------+-------+-------+
            |category  |region |sales  |
            +----------+-------+-------+
            |A         |North  |1000   |
            |B         |South  |1500   |
            |A         |South  |2000   |
            |B         |North  |800    |
            |A         |North  |1200   |
            +----------+-------+-------+
            ```

            Applying the orderBy function to sort by category (asc) then sales (desc):

            ```json
            {
                "function_type": "orderby",
                "arguments": {
                    "columns": [
                        {"column_name": "category", "order": "asc"},
                        {"column_name": "sales", "order": "desc"}
                    ]
                }
            }
            ```

            The resulting DataFrame will be:

            ```
            +----------+-------+-------+
            |category  |region |sales  |
            +----------+-------+-------+
            |A         |South  |2000   |
            |A         |North  |1200   |
            |A         |North  |1000   |
            |B         |South  |1500   |
            |B         |North  |800    |
            +----------+-------+-------+
            ```
        """
        logger.debug("Creating orderBy transform with %d sort columns", len(self.arguments.columns))
        logger.debug("Sort columns configuration: %s", self.arguments.columns)

        def __f(df: DataFrame) -> DataFrame:
            logger.debug("Applying orderBy transform - input columns: %s", df.columns)
            original_count = df.count()
            logger.debug("Input DataFrame has %d rows", original_count)

            # Build list of column expressions with sort order
            sort_exprs = []
            for col_spec in self.arguments.columns:
                if col_spec.order == "desc":
                    sort_exprs.append(desc(col_spec.column_name))
                else:
                    sort_exprs.append(asc(col_spec.column_name))

            logger.debug("Sorting by columns: %s", [col.column_name for col in self.arguments.columns])

            # Apply sorting
            result_df = df.orderBy(*sort_exprs)

            result_count = result_df.count()
            logger.debug("Output DataFrame has %d rows", result_count)
            logger.debug("Output columns: %s", result_df.columns)

            return result_df

        return __f
