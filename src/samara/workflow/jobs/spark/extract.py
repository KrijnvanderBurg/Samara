"""PySpark data extraction - Batch and streaming extraction operations.

This module provides extraction implementations for the Samara framework, enabling
pipeline authors to configure data sources in their pipeline definitions. It supports
both batch and streaming extraction modes from file-based sources, with automatic
schema parsing and PySpark configuration management.
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import Field, model_validator
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from samara.telemetry import trace_span
from samara.types import DataFrameRegistry
from samara.utils.logger import get_logger
from samara.workflow.jobs.models.model_extract import ExtractFileModel, ExtractMethod, ExtractModel
from samara.workflow.jobs.spark.schema import SchemaFilepathHandler, SchemaStringHandler
from samara.workflow.jobs.spark.session import SparkHandler

logger: logging.Logger = get_logger(__name__)


class ExtractSpark(ExtractModel, ABC):
    """Base class for data extraction operations.

    Defines the extraction interface for all PySpark-based extractors, supporting
    both batch and streaming modes. Manages automatic schema parsing and maintains
    a registry of extracted DataFrames for use in downstream transforms.

    Attributes:
        options: PySpark reader configuration as key-value pairs (e.g., delimiter,
            header options for CSV).
        _schema_parsed: Parsed PySpark StructType schema, populated automatically
            from schema_ field during model validation.

    Example:
        **Configuration in JSON:**
        ```
        {
            "extract": {
                "id": "source_data",
                "method": "batch",
                "schema_": "path/to/schema.json",
                "options": {"delimiter": ",", "header": "true"}
            }
        }
        ```

        **Configuration in YAML:**
        ```
        extract:
          id: source_data
          method: batch
          schema_: path/to/schema.json
          options:
            delimiter: ","
            header: "true"
        ```

    Note:
        Subclasses must implement both _extract_batch() and _extract_streaming()
        methods. The extract() method automatically selects the appropriate
        extraction mode based on the configured method.
    """

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    _schema_parsed: StructType
    options: dict[str, Any] = Field(..., description="PySpark reader options as key-value pairs")

    def __init__(self, **data: Any) -> None:
        """Initialize the extractor with configuration data and workflow components.

        Sets up the Pydantic model with provided configuration data and initializes
        workflow components (DataFrameRegistry for storing extracted data and
        SparkHandler for managing the Spark session).

        Args:
            **data: Configuration data for model initialization. Should include
                required fields like id_, method, schema_, and extract-specific fields.
        """
        super().__init__(**data)
        # Set up non-Pydantic attributes that shouldn't be in schema
        self.data_registry: DataFrameRegistry = DataFrameRegistry()
        self.spark: SparkHandler = SparkHandler()

    @model_validator(mode="after")
    @trace_span("extract_spark.parse_schema")
    def parse_schema(self) -> Self:
        """Parse schema configuration into a PySpark StructType.

        Automatically converts the schema_ field into a parsed PySpark StructType
        after model validation. Supports both schema file paths (.json) and inline
        JSON schema strings. File paths are parsed via SchemaFilepathHandler while
        JSON strings use SchemaStringHandler.

        Returns:
            Self: The validated model instance with _schema_parsed populated.

        Note:
            If schema_ is empty or None, returns early without parsing.
            File path detection relies on the .json extension.
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

    @trace_span("extract_spark.extract")
    def extract(self) -> None:
        """Execute the extraction process based on configured method.

        Routes to batch or streaming extraction based on the configured method,
        applies PySpark reader options, and stores the resulting DataFrame in the
        data registry for subsequent pipeline operations. Logs extraction progress
        and validates that the method is supported.

        Raises:
            ValueError: If the extraction method is not BATCH or STREAMING.

        Note:
            The extracted DataFrame is stored in the registry with the configured
            source id_ as the key, making it available to downstream transforms.
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
        """Extract data using batch mode processing.

        Subclasses implement this to load all data from the configured source
        using PySpark's batch read operations.

        Returns:
            DataFrame: The complete extracted dataset.
        """

    @abstractmethod
    def _extract_streaming(self) -> DataFrame:
        """Extract data using streaming mode processing.

        Subclasses implement this to set up a streaming read from the configured
        source using PySpark's streaming read operations.

        Returns:
            DataFrame: A streaming DataFrame that continuously receives data.
        """


class ExtractFileSpark(ExtractSpark, ExtractFileModel):
    """Extract data from file-based sources (CSV, JSON, Parquet, etc.).

    Concrete extractor implementation for file sources, supporting both batch
    and streaming extraction modes. Automatically handles schema application and
    reader option configuration for the selected file format.

    Attributes:
        extract_type: Discriminator field set to "file" for configuration routing.
        location: File path or pattern for the source data.
        data_format: File format (csv, json, parquet, etc.).

    Example:
        **Configuration in JSON:**
        ```
        {
            "extract": {
                "id": "raw_data",
                "method": "batch",
                "extract_type": "file",
                "location": "/data/input/transactions.csv",
                "data_format": "csv",
                "schema_": "schemas/transactions_schema.json",
                "options": {
                    "delimiter": ",",
                    "header": "true",
                    "inferSchema": "false"
                }
            }
        }
        ```

        **Configuration in YAML:**
        ```
        extract:
          id: raw_data
          method: batch
          extract_type: file
          location: /data/input/transactions.csv
          data_format: csv
          schema_: schemas/transactions_schema.json
          options:
            delimiter: ","
            header: "true"
            inferSchema: "false"
        ```

    Note:
        Inherits schema parsing and option management from ExtractSpark base class.
    """

    extract_type: Literal["file"]

    @trace_span("extract_file_spark._extract_batch")
    def _extract_batch(self) -> DataFrame:
        """Read data from file in batch mode using PySpark.

        Loads all data from the configured file location using the specified format
        and applies the parsed schema and reader options. Logs the row count on
        successful completion.

        Returns:
            DataFrame: The complete extracted dataset with schema applied.

        Note:
            Row count is logged for monitoring and debugging purposes.
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

    @trace_span("extract_file_spark._extract_streaming")
    def _extract_streaming(self) -> DataFrame:
        """Set up streaming read from file using PySpark.

        Configures PySpark to continuously read from the file location as new data
        arrives, applying the specified format, schema, and reader options.

        Returns:
            DataFrame: A streaming DataFrame connected to the file source.

        Note:
            Streaming reads are suitable for sources that continuously append new
            files. The schema and options from configuration are applied to the
            stream reader.
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
