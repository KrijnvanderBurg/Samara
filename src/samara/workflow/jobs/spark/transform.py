"""PySpark transformation implementation - Execute configuration-driven data transformations.

This module provides concrete implementations for applying transformation chains to data
using Apache PySpark as the processing engine. It focuses on configuration-driven
transformations enabling pipeline authors to define complex data manipulation sequences
through structured configuration rather than code.
"""

import logging
from typing import Any

from pydantic import Field

from samara.telemetry import (
    ATTR_COLUMN_COUNT,
    ATTR_COMPONENT_ID,
    ATTR_COMPONENT_TYPE,
    ATTR_ROW_COUNT,
    ATTR_ROW_COUNT_AFTER,
    ATTR_ROW_COUNT_BEFORE,
    ATTR_TRANSFORM_FUNCTION_INDEX,
    ATTR_TRANSFORM_FUNCTION_TOTAL,
    ATTR_TRANSFORM_FUNCTION_TYPE,
    add_span_event,
    get_current_span,
    set_span_attributes,
)
from samara.types import DataFrameRegistry
from samara.utils.logger import get_logger
from samara.workflow.jobs.models.model_transform import TransformModel
from samara.workflow.jobs.spark.session import SparkHandler
from samara.workflow.jobs.spark.transforms import transform_function_spark_union

logger: logging.Logger = get_logger(__name__)


class TransformSpark(TransformModel[transform_function_spark_union]):
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

        # Add span attributes for transform observability
        span = get_current_span()
        if span.is_recording():
            set_span_attributes({
                ATTR_COMPONENT_ID: self.id_,
                ATTR_COMPONENT_TYPE: "transform",
                ATTR_TRANSFORM_FUNCTION_TOTAL: len(self.functions),
                "samara.transform.upstream_id": self.upstream_id,
            })

        add_span_event("transform.config.applied", {
            ATTR_COMPONENT_ID: self.id_,
            "samara.transform.options_count": len(self.options),
        })

        logger.debug("Adding Spark configurations: %s", self.options)
        self.spark.add_configs(options=self.options)

        # Copy the dataframe from upstream to current id
        logger.debug("Copying dataframe from %s to %s", self.upstream_id, self.id_)
        self.data_registry[self.id_] = self.data_registry[self.upstream_id]

        # Record initial data stats
        initial_row_count = self.data_registry[self.id_].count()
        initial_column_count = len(self.data_registry[self.id_].columns)
        add_span_event("transform.data.received", {
            ATTR_COMPONENT_ID: self.id_,
            ATTR_ROW_COUNT: initial_row_count,
            ATTR_COLUMN_COUNT: initial_column_count,
            "samara.transform.upstream_id": self.upstream_id,
        })

        # Apply transformations
        logger.debug("Applying %d transformation functions", len(self.functions))
        for i, function in enumerate(self.functions):
            function_index = i + 1
            logger.debug("Applying function %d/%d: %s", function_index, len(self.functions), function.function_type)

            original_count = self.data_registry[self.id_].count()
            add_span_event("transform.function.started", {
                ATTR_COMPONENT_ID: self.id_,
                ATTR_TRANSFORM_FUNCTION_TYPE: function.function_type,
                ATTR_TRANSFORM_FUNCTION_INDEX: function_index,
                ATTR_TRANSFORM_FUNCTION_TOTAL: len(self.functions),
                ATTR_ROW_COUNT_BEFORE: original_count,
            })

            callable_ = function.transform()
            self.data_registry[self.id_] = callable_(df=self.data_registry[self.id_])

            new_count = self.data_registry[self.id_].count()
            new_column_count = len(self.data_registry[self.id_].columns)
            row_delta = new_count - original_count

            add_span_event("transform.function.completed", {
                ATTR_COMPONENT_ID: self.id_,
                ATTR_TRANSFORM_FUNCTION_TYPE: function.function_type,
                ATTR_TRANSFORM_FUNCTION_INDEX: function_index,
                ATTR_ROW_COUNT_BEFORE: original_count,
                ATTR_ROW_COUNT_AFTER: new_count,
                "samara.transform.row_delta": row_delta,
                ATTR_COLUMN_COUNT: new_column_count,
            })

            logger.info(
                "Function %s applied - rows changed from %d to %d", function.function_type, original_count, new_count
            )

        # Record final data stats
        final_row_count = self.data_registry[self.id_].count()
        final_column_count = len(self.data_registry[self.id_].columns)
        set_span_attributes({
            ATTR_ROW_COUNT: final_row_count,
            ATTR_COLUMN_COUNT: final_column_count,
            "samara.transform.row_delta_total": final_row_count - initial_row_count,
        })
        add_span_event("transform.completed", {
            ATTR_COMPONENT_ID: self.id_,
            ATTR_ROW_COUNT: final_row_count,
            ATTR_COLUMN_COUNT: final_column_count,
            "samara.transform.functions_applied": len(self.functions),
        })

        logger.info("Transformation completed successfully for: %s", self.id_)


TransformSparkUnion = TransformSpark
