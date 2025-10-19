"""PySpark implementation for data transformation operations.

This module provides concrete implementations for transforming data using Apache PySpark.
It includes:

- Abstract base classes defining the transformation interface
- Function-based transformation support with configurable arguments
- Registry mechanisms for dynamically selecting transformation functions
- Configuration-driven transformation functionality

The Transform components represent the middle phase in the ETL pipeline, responsible
for manipulating data between extraction and loading.
"""

import logging
from typing import Any

from pydantic import Field

from samara.runtime.jobs.models.model_transform import TransformModel
from samara.runtime.jobs.spark.session import SparkHandler
from samara.runtime.jobs.spark.transforms import transform_function_spark_union
from samara.types import DataFrameRegistry
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class TransformSpark(TransformModel[transform_function_spark_union]):
    """
    Concrete implementation for DataFrame transformation.

    This class provides functionality for transforming data.

    Attributes:
        options: Transformation options as key-value pairs
    """

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    options: dict[str, Any] = Field(..., description="Transformation options as key-value pairs")

    def __init__(self, **data: Any) -> None:
        """Initialize TransformSpark with data and set up runtime instances.

        Creates the Pydantic model with provided data and then initializes
        non-Pydantic instance attributes for DataFrameRegistry and SparkHandler.

        Args:
            **data: Pydantic model initialization data
        """
        super().__init__(**data)
        # Set up non-Pydantic attributes that shouldn't be in schema
        self.data_registry: DataFrameRegistry = DataFrameRegistry()
        self.spark: SparkHandler = SparkHandler()

    def transform(self) -> None:
        """
        Apply all transformation functions to the data source.

        This method performs the following steps:
        1. Copies the dataframe from the upstream source to current transform's id
        2. Sequentially applies each transformation function to the dataframe
        3. Each function updates the registry with its results

        Note:
            Functions are applied in the order they were defined in the configuration.
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
