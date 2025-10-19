"""Agg (aggregation) transform function.

This module provides a transform function for applying aggregation operations
across the entire DataFrame without grouping.

The AggFunction is registered with the TransformFunctionRegistry under
the name 'agg', making it available for use in configuration files.
"""

import logging
from collections.abc import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from samara.runtime.jobs.models.transforms.model_agg import AggFunctionModel
from samara.runtime.jobs.spark.transforms.base import FunctionSpark
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class AggFunction(AggFunctionModel, FunctionSpark):
    """Function that applies aggregation operations to the entire DataFrame.

    This transform function allows for applying aggregation operations
    across all rows of a DataFrame without grouping, similar to SQL aggregate
    functions without GROUP BY.

    The function is configured using an AggFunctionModel that specifies
    what aggregation operations to apply.

    Attributes:
        function_type: The name of the function (always "agg")
        arguments: Container for the aggregation parameters

    Example:
        ```json
        {
            "function_type": "agg",
            "arguments": {
                "aggregations": [
                    {"column_name": "sales", "agg_function": "sum", "alias": "total_sales"},
                    {"column_name": "quantity", "agg_function": "avg", "alias": "avg_quantity"},
                    {"column_name": "order_id", "agg_function": "count", "alias": "total_orders"}
                ]
            }
        }
        ```
    """

    def transform(self) -> Callable:
        """Apply the aggregation transformation to the DataFrame.

        This method extracts the aggregation configuration from the model
        and applies it to the DataFrame, returning a single row with the
        aggregated results.

        Returns:
            A callable function that performs the aggregation when applied
            to a DataFrame

        Examples:
            Consider the following DataFrame:

            ```
            +----------+-------+--------+
            |order_id  |sales  |quantity|
            +----------+-------+--------+
            |1         |1000   |10      |
            |2         |1500   |15      |
            |3         |2000   |20      |
            |4         |800    |8       |
            |5         |1200   |12      |
            +----------+-------+--------+
            ```

            Applying the agg function:

            ```json
            {
                "function_type": "agg",
                "arguments": {
                    "aggregations": [
                        {"column_name": "sales", "agg_function": "sum", "alias": "total_sales"},
                        {"column_name": "sales", "agg_function": "avg", "alias": "avg_sales"},
                        {"column_name": "quantity", "agg_function": "max", "alias": "max_quantity"},
                        {"column_name": "order_id", "agg_function": "count", "alias": "order_count"}
                    ]
                }
            }
            ```

            The resulting DataFrame will be:

            ```
            +-----------+---------+------------+-----------+
            |total_sales|avg_sales|max_quantity|order_count|
            +-----------+---------+------------+-----------+
            |6500       |1300.0   |20          |5          |
            +-----------+---------+------------+-----------+
            ```
        """
        logger.debug("Creating agg transform with %d aggregations", len(self.arguments.aggregations))
        logger.debug("Aggregations to apply: %s", self.arguments.aggregations)

        def __f(df: DataFrame) -> DataFrame:
            logger.debug("Applying agg transform - input columns: %s", df.columns)
            original_count = df.count()
            logger.debug("Input DataFrame has %d rows", original_count)

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

            # Apply aggregations to the entire DataFrame
            result_df = df.agg(*agg_exprs)

            result_count = result_df.count()
            logger.info(
                "Agg transform completed - aggregated from %d rows to %d row(s)",
                original_count,
                result_count,
            )
            logger.debug("Result columns: %s", result_df.columns)

            return result_df

        return __f
