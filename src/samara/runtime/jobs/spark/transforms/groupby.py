"""GroupBy transform function with aggregation.

This module provides a transform function for grouping data by specified columns
and applying aggregation operations in the ETL pipeline.

The GroupByFunction is registered with the TransformFunctionRegistry under
the name 'groupby', making it available for use in configuration files.
"""

import logging
from collections.abc import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from samara.runtime.jobs.models.transforms.model_groupby import GroupByFunctionModel
from samara.runtime.jobs.spark.transforms.base import FunctionSpark
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class GroupByFunction(GroupByFunctionModel, FunctionSpark):
    """Function that groups data by specified columns and applies aggregations.

    This transform function allows for grouping a DataFrame by one or more columns
    and applying aggregation operations, similar to the GROUP BY clause in SQL.

    The function is configured using a GroupByFunctionModel that specifies
    which columns to group by and what aggregation operations to apply.

    Attributes:
        function_type: The name of the function (always "groupby")
        arguments: Container for the groupBy and aggregation parameters

    Example:
        ```json
        {
            "function_type": "groupby",
            "arguments": {
                "columns": ["category", "region"],
                "aggregations": [
                    {"column_name": "sales", "agg_function": "sum", "alias": "total_sales"},
                    {"column_name": "quantity", "agg_function": "avg", "alias": "avg_quantity"},
                    {"column_name": "order_id", "agg_function": "count", "alias": "order_count"}
                ]
            }
        }
        ```
    """

    def transform(self) -> Callable:
        """Apply the groupBy and aggregation transformation to the DataFrame.

        This method extracts the groupBy and aggregation configuration from the model
        and applies it to the DataFrame, returning the aggregated results.

        Returns:
            A callable function that performs the groupBy and aggregation when applied
            to a DataFrame

        Examples:
            Consider the following DataFrame:

            ```
            +----------+--------+-------+--------+
            |category  |region  |sales  |quantity|
            +----------+--------+-------+--------+
            |Electronics|North  |1000   |10      |
            |Electronics|South  |1500   |15      |
            |Electronics|North  |2000   |20      |
            |Furniture |North  |800    |8       |
            |Furniture |South  |1200   |12      |
            +----------+--------+-------+--------+
            ```

            Applying the groupBy function:

            ```json
            {
                "function_type": "groupby",
                "arguments": {
                    "columns": ["category", "region"],
                    "aggregations": [
                        {"column_name": "sales", "agg_function": "sum", "alias": "total_sales"},
                        {"column_name": "quantity", "agg_function": "avg", "alias": "avg_quantity"}
                    ]
                }
            }
            ```

            The resulting DataFrame will be:

            ```
            +----------+--------+-----------+------------+
            |category  |region  |total_sales|avg_quantity|
            +----------+--------+-----------+------------+
            |Electronics|North  |3000       |15.0        |
            |Electronics|South  |1500       |15.0        |
            |Furniture |North  |800        |8.0         |
            |Furniture |South  |1200       |12.0        |
            +----------+--------+-----------+------------+
            ```
        """
        logger.debug("Creating groupBy transform for columns: %s", self.arguments.columns)
        logger.debug("Aggregations to apply: %s", self.arguments.aggregations)

        def __f(df: DataFrame) -> DataFrame:
            logger.debug("Applying groupBy transform - input columns: %s", df.columns)
            original_count = df.count()
            logger.debug("Input DataFrame has %d rows", original_count)

            # Group by specified columns
            grouped_df = df.groupBy(*self.arguments.columns)
            logger.debug("Grouped by columns: %s", self.arguments.columns)

            # Build aggregation expressions
            agg_exprs = []
            for agg in self.arguments.aggregations:
                # Get the aggregation function from pyspark.sql.functions
                agg_func = getattr(F, agg.agg_function)

                # Apply aggregation and optionally alias
                if agg.alias:
                    expr = agg_func(agg.column_name).alias(agg.alias)
                    logger.debug(
                        "Applying %s on column '%s' with alias '%s'", agg.agg_function, agg.column_name, agg.alias
                    )
                else:
                    # Default alias: column_name_agg_function
                    default_alias = f"{agg.column_name}_{agg.agg_function}"
                    expr = agg_func(agg.column_name).alias(default_alias)
                    logger.debug(
                        "Applying %s on column '%s' with default alias '%s'",
                        agg.agg_function,
                        agg.column_name,
                        default_alias,
                    )

                agg_exprs.append(expr)

            # Apply aggregations
            result_df = grouped_df.agg(*agg_exprs)

            result_count = result_df.count()
            logger.info(
                "GroupBy transform completed - reduced from %d to %d rows (%d groups)",
                original_count,
                result_count,
                result_count,
            )
            logger.debug("Result columns: %s", result_df.columns)

            return result_df

        return __f
