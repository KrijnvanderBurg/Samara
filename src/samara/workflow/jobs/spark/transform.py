"""PySpark transformation implementation - Execute configuration-driven data transformations.

This module provides concrete implementations for applying transformation chains to data
using Apache PySpark as the processing engine. It focuses on configuration-driven
transformations enabling pipeline authors to define complex data manipulation sequences
through structured configuration rather than code.
"""

import logging
from typing import Any

from pydantic import Field

from samara.types import DataFrameRegistry
from samara.utils.logger import get_logger
from samara.workflow.jobs.models.model_transform import TransformModel
from samara.workflow.jobs.spark.session import SparkHandler
from samara.workflow.jobs.spark.transforms import TransformFunctionSparkUnion

logger: logging.Logger = get_logger(__name__)


class TransformSpark(TransformModel[TransformFunctionSparkUnion]):
    """Apply PySpark-based transformations to dataframes in the pipeline.

    This class executes a sequence of transformation functions configured in the pipeline
    definition. It manages the transformation chain by copying upstream data and applying
    each function sequentially, tracking row counts through each step for debugging and
    monitoring purposes.

    Attributes:
        options: Transformation options passed to Spark as key-value pairs for configuring
            Spark behavior and tuning parameters.

    Example:
        >>> from samara.workflow.jobs.spark.transform import TransformSpark
        >>> transform = TransformSpark(
        ...     id_="customer_transform",
        ...     upstream_id="customer_extract",
        ...     functions=[...],
        ...     options={"spark.sql.adaptive.enabled": "true"}
        ... )
        >>> transform.transform()

        **Configuration in JSON:**
        ```
        {
            "type": "transform",
            "id": "customer_transform",
            "upstream_id": "customer_extract",
            "functions": [
                {
                    "functionType": "select",
                    "params": ["id", "name", "email"]
                },
                {
                    "functionType": "filter",
                    "params": ["age > 18"]
                }
            ],
            "options": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.shuffle.partitions": "200"
            }
        }
        ```

        **Configuration in YAML:**
        ```
        type: transform
        id: customer_transform
        upstream_id: customer_extract
        functions:
          - functionType: select
            params:
              - id
              - name
              - email
          - functionType: filter
            params:
              - "age > 18"
        options:
          spark.sql.adaptive.enabled: "true"
          spark.sql.shuffle.partitions: "200"
        ```

    Note:
        Spark configurations are applied before data transformations. Each transformation
        function modifies the dataframe in place within the registry, so order matters.
    """

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    options: dict[str, Any] = Field(..., description="Transformation options as key-value pairs")

    def __init__(self, **data: Any) -> None:
        """Initialize TransformSpark with configuration data.

        Creates a Pydantic model instance from the provided configuration data and then
        initializes workflow attributes for managing dataframes and Spark sessions. This
        two-stage initialization separates Pydantic model validation from workflow setup.

        Args:
            **data: Configuration data for initializing the Pydantic model. Should include
                `id_`, `upstream_id`, `functions`, and `options` keys at minimum.

        Returns:
            None
        """
        super().__init__(**data)
        # Set up non-Pydantic attributes that shouldn't be in schema
        self.data_registry: DataFrameRegistry = DataFrameRegistry()
        self.spark: SparkHandler = SparkHandler()

    def transform(self) -> None:
        """Execute the complete transformation chain on the upstream dataframe.

        Perform sequential transformation operations on the input data by:
        1. Applying configured Spark options to optimize execution
        2. Copying the dataframe from the upstream stage to this transform's namespace
        3. Applying each transformation function in order, with row count tracking
        4. Logging metrics and results for each transformation step

        Args:
            None

        Note:
            Functions execute sequentially in the order specified in the configuration.
            Each transformation modifies the dataframe registry in place. Row count changes
            are logged to help identify unexpected data loss or multiplication during
            transformation.
        """
        logger.info("Starting transformation for: %s from upstream: %s", self.id_, self.upstream_id)

        logger.debug("Adding Spark configurations: %s", self.options)
        self.spark.add_configs(options=self.options)

        # Copy the dataframe from upstream to current id
        logger.debug("Copying dataframe from %s to %s", self.upstream_id, self.id_)
        self.data_registry[self.id_] = self.data_registry[self.upstream_id]

        # Apply transformations
        logger.debug("Applying %d transformation functions", len(self.functions))
        for i, function in enumerate(self.functions):
            logger.debug("Applying function %d/%d: %s", i, len(self.functions), function.function_type)

            original_count = self.data_registry[self.id_].count()
            callable_ = function.transform()
            self.data_registry[self.id_] = callable_(df=self.data_registry[self.id_])

            new_count = self.data_registry[self.id_].count()

            logger.info(
                "Function %s applied - rows changed from %d to %d", function.function_type, original_count, new_count
            )

        logger.info("Transformation completed successfully for: %s", self.id_)


TransformSparkUnion = TransformSpark
