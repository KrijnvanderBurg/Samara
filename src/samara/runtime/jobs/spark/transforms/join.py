"""Join transform - Combine DataFrames from multiple sources.

This module provides the join transform function enabling users to combine
DataFrames based on specified columns and join strategies. The JoinFunction
is registered as 'join' in the transform function registry.
"""

import logging
from collections.abc import Callable

from pyspark.sql import DataFrame

from samara.runtime.jobs.models.transforms.model_join import JoinFunctionModel
from samara.runtime.jobs.spark.transforms.base import FunctionSpark
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class JoinFunction(JoinFunctionModel, FunctionSpark):
    """Combine two DataFrames based on join keys and strategy.

    This transform function joins the current DataFrame with another
    DataFrame from the data registry using specified join columns and
    join type. Supports all Spark join strategies (inner, outer, left,
    right, cross, etc.) enabling flexible data combinations.

    Attributes:
        function: The name of the function (always "join")
        arguments: Container for the join parameters with join configuration

    Example:
        **Configuration in JSON:**
        ```
        {
            "transforms": [
                {
                    "function": "join",
                    "arguments": {
                        "other_upstream_id": "orders_data",
                        "on": "customer_id",
                        "how": "inner"
                    }
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        transforms:
          - function: join
            arguments:
              other_upstream_id: orders_data
              on: customer_id
              how: inner
        ```

        **Multiple join columns example (JSON):**
        ```
        {
            "function": "join",
            "arguments": {
                "other_upstream_id": "reference_table",
                "on": ["country", "product_id"],
                "how": "left"
            }
        }
        ```

    Note:
        The `other_upstream_id` must refer to a DataFrame already in the
        data registry. The join columns must exist in both DataFrames.
        Performance may vary depending on DataFrame sizes and cluster resources.
    """

    def transform(self) -> Callable:
        """Return a callable that applies the join transformation to a DataFrame.

        Extracts join configuration from the model and builds a transformation
        function that retrieves the right DataFrame from the registry and
        performs the join operation with the specified columns and strategy.

        Returns:
            A callable that takes a DataFrame and returns the joined result.
            The callable performs the join with the DataFrame specified by
            `other_upstream_id` using the configured join columns and type.

        Note:
            The join is performed in-memory on the Spark cluster. Large joins
            may require appropriate cluster resources and shuffle operations.
        """
        logger.debug(
            "Creating join transform - other: %s, on: %s, how: %s",
            self.arguments.other_upstream_id,
            self.arguments.on,
            self.arguments.how,
        )

        def __f(df: DataFrame) -> DataFrame:
            logger.debug("Applying join transform")

            # Get the right DataFrame from the registry
            right_df = self.data_registry[self.arguments.other_upstream_id]
            logger.debug(
                "Retrieved right DataFrame: %s (columns: %s)",
                self.arguments.other_upstream_id,
                right_df.columns,
            )

            # Get the join type
            join_type = self.arguments.how
            # Get the join columns
            join_on = self.arguments.on

            logger.debug("Performing join - left: %d rows, right: %d rows", df.count(), right_df.count())
            logger.debug("Join parameters - on: %s, how: %s", join_on, join_type)

            # Perform the join operation
            result_df = df.join(right_df, on=join_on, how=join_type)
            result_count = result_df.count()

            logger.info("Join transform completed - result: %d rows, join type: %s", result_count, join_type)

            return result_df

        return __f
