"""
PySpark implementation for data extraction operations.

This module provides concrete implementations for extracting data using PySpark.
It includes:
    - Abstract base classes for extraction
    - Concrete file-based extractors
    - A registry for selecting extraction strategies
    - Support for both batch and streaming extraction
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import Field, model_validator
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from samara.runtime.jobs.models.model_extract import ExtractFileModel, ExtractMethod, ExtractModel
from samara.runtime.jobs.spark.schema import SchemaFilepathHandler, SchemaStringHandler
from samara.runtime.jobs.spark.session import SparkHandler
from samara.types import DataFrameRegistry
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class ExtractSpark(ExtractModel, ABC):
    """Abstract base class for data extraction operations.

    Defines the interface for all extraction implementations, supporting both
    batch and streaming extractions. Manages a data registry for extracted DataFrames.

    Attributes:
        options: PySpark reader options
    """

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    options: dict[str, Any] = Field(..., description="PySpark reader options as key-value pairs")

    def __init__(self, **data: Any) -> None:
        """Initialize ExtractSpark with data and set up runtime instances.

        Creates the Pydantic model with provided data and then initializes
        non-Pydantic instance attributes for DataFrameRegistry and SparkHandler.

        Args:
            **data: Pydantic model initialization data
        """
        super().__init__(**data)
        # Set up non-Pydantic attributes that shouldn't be in schema
        self.data_registry: DataFrameRegistry = DataFrameRegistry()
        self.spark: SparkHandler = SparkHandler()
        self._schema_parsed: StructType | None = None

    @model_validator(mode="after")
    def parse_schema(self) -> Self:
        """Parse schema_ field into _schema_parsed after model creation.

        This validator automatically converts the schema_ field value into a
        PySpark StructType based on the input type:
        - File path ending in .json: Uses SchemaFilepathHandler
        - JSON string: Uses SchemaStringHandler

        Returns:
            Self: The model instance with _schema_parsed populated
        """
        if not self.schema_:
            return self

        # Convert to string for processing
        schema_str = str(self.schema_).strip()

        # Detect if it's a file path or JSON string
        if schema_str.endswith(".json"):
            # File path - use FilepathHandler
            self._schema_parsed = SchemaFilepathHandler.parse(schema=Path(schema_str))
        else:
            # JSON string - use StringHandler
            self._schema_parsed = SchemaStringHandler.parse(schema=schema_str)

        return self

    def extract(self) -> None:
        """Main extraction method.

        Selects batch or streaming extraction based on the model configuration
        and stores the result in the data registry.
        """
        logger.info("Starting extraction for source: %s using method: %s", self.id_, self.method.value)

        logger.debug("Adding Spark configurations: %s", self.options)
        self.spark.add_configs(options=self.options)

        if self.method == ExtractMethod.BATCH:
            logger.debug("Performing batch extraction for: %s", self.id_)
            self.data_registry[self.id_] = self._extract_batch()
            logger.info("Batch extraction completed successfully for: %s", self.id_)
        elif self.method == ExtractMethod.STREAMING:
            logger.debug("Performing streaming extraction for: %s", self.id_)
            self.data_registry[self.id_] = self._extract_streaming()
            logger.info("Streaming extraction completed successfully for: %s", self.id_)
        else:
            raise ValueError(f"Extraction method {self.method} is not supported for PySpark")

    @abstractmethod
    def _extract_batch(self) -> DataFrame:
        """Extract data in batch mode.

        Returns:
            DataFrame: The extracted data as a DataFrame.
        """

    @abstractmethod
    def _extract_streaming(self) -> DataFrame:
        """Extract data in streaming mode.

        Returns:
            DataFrame: The extracted data as a streaming DataFrame.
        """


class ExtractFileSpark(ExtractSpark, ExtractFileModel):
    """Concrete extractor for file-based sources (CSV, JSON, Parquet).

    Supports both batch and streaming extraction using PySpark's DataFrame API.
    """

    extract_type: Literal["file"]

    def _extract_batch(self) -> DataFrame:
        """Read from file in batch mode using PySpark.

        Returns:
            DataFrame: The extracted data as a DataFrame.
        """
        logger.debug("Reading files in batch mode - path: %s, format: %s", self.location, self.data_format)

        dataframe = self.spark.session.read.load(
            path=self.location,
            format=self.data_format,
            schema=self._schema_parsed,
            **self.options,
        )
        row_count = dataframe.count()
        logger.info("Batch extraction successful - loaded %d rows from %s", row_count, self.location)
        return dataframe

    def _extract_streaming(self) -> DataFrame:
        """Read from file in streaming mode using PySpark.

        Returns:
            DataFrame: The extracted data as a streaming DataFrame.
        """
        logger.debug("Reading files in streaming mode - path: %s, format: %s", self.location, self.data_format)

        dataframe = self.spark.session.readStream.load(
            path=self.location,
            format=self.data_format,
            schema=self._schema_parsed,
            **self.options,
        )
        logger.info("Streaming extraction successful for %s", self.location)
        return dataframe


# When more extract types are added, use a discriminated union:
# from typing import Annotated, Union
# from pydantic import Discriminator
# ExtractSparkUnion = Annotated[
#     Union[ExtractFileSpark, ExtractDatabaseSpark, ...],
#     Discriminator("extract_type"),
# ]
# For now, with only one type, just use it directly:
ExtractSparkUnion = ExtractFileSpark
