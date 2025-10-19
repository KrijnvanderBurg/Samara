"""Load interface and implementations for various data formats.

This module provides abstract classes and implementations for loading data to
various destinations and formats using Apache PySpark. It includes:

- Abstract base classes defining the loading interface
- Concrete implementations for different output formats (CSV, JSON, etc.)
- Support for both batch and streaming writes
- Registry mechanism for dynamically selecting appropriate loaders
- Configuration-driven loading functionality

The Load components represent the final phase in the ETL pipeline, responsible
for writing processed data to target destinations.
"""

import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Literal

from pydantic import Field
from pyspark.sql.streaming.query import StreamingQuery

from samara.runtime.jobs.models.model_load import LoadMethod, LoadModel, LoadModelFile
from samara.runtime.jobs.spark.session import SparkHandler
from samara.types import DataFrameRegistry, StreamingQueryRegistry
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class LoadSpark(LoadModel, ABC):
    """
    Abstract base class for data loading operations.

    This class defines the interface for all loading implementations,
    supporting both batch and streaming loads to various destinations.

    Attributes:
        options: Options for the sink input
    """

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    options: dict[str, Any] = Field(..., description="Options for the sink input.")

    def __init__(self, **data: Any) -> None:
        """Initialize LoadSpark with data and set up runtime instances.

        Creates the Pydantic model with provided data and then initializes
        non-Pydantic instance attributes for registries and SparkHandler.

        Args:
            **data: Pydantic model initialization data
        """
        super().__init__(**data)
        # Set up non-Pydantic attributes that shouldn't be in schema
        self.data_registry: DataFrameRegistry = DataFrameRegistry()
        self.streaming_query_registry: StreamingQueryRegistry = StreamingQueryRegistry()
        self.spark: SparkHandler = SparkHandler()

    @abstractmethod
    def _load_batch(self) -> None:
        """
        Perform batch loading of data to the destination.
        """

    @abstractmethod
    def _load_streaming(self) -> StreamingQuery:
        """
        Perform streaming loading of data to the destination.

        Returns:
            A streaming query object that can be used to monitor the stream
        """

    def _export_schema(self, schema_json: str, schema_path: str) -> None:
        """
        Export DataFrame schema to a JSON file.

        Args:
            schema_json: JSON representation of the DataFrame schema
            schema_path: File path where schema should be written
        """
        logger.debug("Exporting schema for %s to: %s", self.id_, schema_path)

        with open(schema_path, mode="w", encoding="utf-8") as f:
            f.write(schema_json)

        logger.info("Schema exported successfully for %s to: %s", self.id_, schema_path)

    def load(self) -> None:
        """
        Load data with PySpark.
        """
        logger.info(
            "Starting load operation for: %s from upstream: %s using method: %s",
            self.id_,
            self.upstream_id,
            self.method.value,
        )

        logger.debug("Adding Spark configurations: %s", self.options)
        self.spark.add_configs(options=self.options)

        logger.debug("Copying dataframe from %s to %s", self.upstream_id, self.id_)
        self.data_registry[self.id_] = self.data_registry[self.upstream_id]

        if self.method == LoadMethod.BATCH:
            logger.debug("Performing batch load for: %s", self.id_)
            self._load_batch()
            logger.info("Batch load completed successfully for: %s", self.id_)
        elif self.method == LoadMethod.STREAMING:
            logger.debug("Performing streaming load for: %s", self.id_)
            self.streaming_query_registry[self.id_] = self._load_streaming()
            logger.info("Streaming load started successfully for: %s", self.id_)
        else:
            raise ValueError(f"Loading method {self.method} is not supported for PySpark")

        # Export schema if location is specified
        if self.schema_export:
            schema_json = json.dumps(self.data_registry[self.id_].schema.jsonValue())
            self._export_schema(schema_json, self.schema_export)

        logger.info("Load operation completed successfully for: %s", self.id_)


class LoadFileSpark(LoadSpark, LoadModelFile):
    """
    Concrete class for file loading using PySpark DataFrame.
    """

    load_type: Literal["file"]

    def _load_batch(self) -> None:
        """
        Write to file in batch mode.
        """
        logger.debug(
            "Writing file in batch mode - path: %s, format: %s, mode: %s",
            self.location,
            self.data_format,
            self.mode,
        )

        row_count = self.data_registry[self.id_].count()
        logger.debug("Writing %d rows to %s", row_count, self.location)

        self.data_registry[self.id_].write.save(
            path=self.location,
            format=self.data_format,
            mode=self.mode,
            **self.options,
        )

        logger.info("Batch write successful - wrote %d rows to %s", row_count, self.location)

    def _load_streaming(self) -> StreamingQuery:
        """
        Write to file in streaming mode.

        Returns:
            StreamingQuery: Represents the ongoing streaming query.
        """
        logger.debug(
            "Writing file in streaming mode - path: %s, format: %s, mode: %s",
            self.location,
            self.data_format,
            self.mode,
        )

        streaming_query = self.data_registry[self.id_].writeStream.start(
            path=self.location,
            format=self.data_format,
            outputMode=self.mode,
            **self.options,
        )

        logger.info("Streaming write started successfully for %s, query ID: %s", self.location, streaming_query.id)
        return streaming_query


# When more load types are added, use a discriminated union:
# from typing import Annotated, Union
# from pydantic import Discriminator
# LoadSparkUnion = Annotated[
#     Union[LoadFileSpark, LoadDatabaseSpark, ...],
#     Discriminator("load_type"),
# ]
# For now, with only one type, just use it directly:
LoadSparkUnion = LoadFileSpark
