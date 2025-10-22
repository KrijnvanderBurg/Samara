"""Load framework - Write DataFrames to destinations via configuration.

This module provides load implementations for writing data to various destinations
and formats in the final phase of a data pipeline. Enables configuration-driven
loading to multiple output formats with batch and streaming support, flexible
write modes, and optional schema export capabilities."""

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
    """Write DataFrames to destinations from configured pipeline stages.

    Abstract base class defining the interface for all load implementations.
    Orchestrates the complete loading operation: DataFrame registry management,
    Spark configuration, batch or streaming writes, and optional schema export.
    Subclasses implement specific destination types (file, database, etc.)
    by implementing abstract write methods.

    Attributes:
        options: Engine-specific writer options passed to PySpark write
            operations (e.g., {"header": "true"} for CSV output).
        data_registry: Registry storing DataFrames keyed by component ID,
            enabling data passing between pipeline stages.
        streaming_query_registry: Registry for active streaming queries,
            used for monitoring and managing long-running streams.
        spark: SparkHandler instance managing Spark session configuration.

    Example:
        Configure load stages in the pipeline definition (parent key: `loads`):

        **Configuration in JSON:**
        ```
        {
            "loads": [
                {
                    "id": "output_load",
                    "type": "load",
                    "upstreamId": "transform_step",
                    "loadType": "file",
                    "location": "/data/output",
                    "format": "csv",
                    "mode": "overwrite",
                    "options": {
                        "header": "true",
                        "delimiter": ","
                    },
                    "schemaExport": "/data/schemas/output_schema.json"
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        loads:
          - id: output_load
            type: load
            upstreamId: transform_step
            loadType: file
            location: /data/output
            format: csv
            mode: overwrite
            options:
              header: "true"
              delimiter: ","
            schemaExport: /data/schemas/output_schema.json
        ```

    See Also:
        LoadFileSpark: Concrete implementation for file-based destinations.

    Note:
        This is an abstract base class. Use concrete implementations for
        actual data loading. Schema export paths must be writable by the
        Spark application.
    """

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    options: dict[str, Any] = Field(..., description="Options for the sink input.")

    def __init__(self, **data: Any) -> None:
        """Initialize the load component from pipeline configuration.

        Creates a Pydantic model instance from provided configuration and
        initializes runtime registries and Spark handler. Establishes the
        component's ability to manage DataFrames, streaming queries, and
        Spark session settings during the load operation.

        Args:
            **data: Configuration fields from pipeline definition. Expected
                fields include: id_, upstream_id, method, location,
                data_format, mode, options, and optionally schema_export.
                See class Example for full configuration structure.
        """
        super().__init__(**data)
        # Set up non-Pydantic attributes that shouldn't be in schema
        self.data_registry: DataFrameRegistry = DataFrameRegistry()
        self.streaming_query_registry: StreamingQueryRegistry = StreamingQueryRegistry()
        self.spark: SparkHandler = SparkHandler()

    @abstractmethod
    def _load_batch(self) -> None:
        """Write the DataFrame to destination in batch mode.

        Implement this method in subclasses to execute a single-pass write
        of the complete DataFrame to the configured destination using the
        specified format and write mode.
        """

    @abstractmethod
    def _load_streaming(self) -> StreamingQuery:
        """Write the DataFrame to destination in streaming mode.

        Implement this method in subclasses to initiate continuous streaming
        write to the configured destination. Return an active StreamingQuery
        allowing the caller to monitor and control the ongoing stream.

        Returns:
            StreamingQuery: Active streaming write operation. Monitor with
                query.status property or terminate with query.stop() method.
        """

    def _export_schema(self, schema_json: str, schema_path: str) -> None:
        """Export the DataFrame schema to a JSON file.

        Write the schema of the loaded DataFrame in JSON format to the
        specified path. Enables data governance, schema validation, and
        documentation of output structure for downstream processes.

        Args:
            schema_json: JSON string representation of the DataFrame schema,
                typically obtained from DataFrame.schema.jsonValue().
            schema_path: File system path where schema JSON is written.
                Parent directories must exist or be creatable by the Spark
                application with appropriate permissions.
        """
        logger.debug("Exporting schema for %s to: %s", self.id_, schema_path)

        with open(schema_path, mode="w", encoding="utf-8") as f:
            f.write(schema_json)

        logger.info("Schema exported successfully for %s to: %s", self.id_, schema_path)

    def load(self) -> None:
        """Execute the load operation on the DataFrame.

        Orchestrate the complete load workflow: copy upstream DataFrame to
        this component's registry, apply Spark configurations, execute the
        write operation (batch or streaming), and optionally export schema.
        Logs detailed information at each stage for monitoring and debugging.

        Raises:
            ValueError: When the load method is neither BATCH nor STREAMING.

        Note:
            The actual write operation is delegated to _load_batch() or
            _load_streaming() in subclasses. Schema export occurs after the
            write is initiated. Upstream DataFrame is required in registry.
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
    """Write DataFrames to file destinations using PySpark.

    Concrete implementation for writing DataFrames to file-based destinations
    including local filesystem, HDFS, and cloud storage (S3, GCS, etc.).
    Supports multiple formats (CSV, JSON, Parquet, etc.) with configurable
    write modes (overwrite, append, ignore, error) for batch and streaming.

    Attributes:
        load_type: Fixed to "file" for this loader type.
        location: Output path for the data (directory or file path).
        data_format: Output format string (e.g., "csv", "json", "parquet").
        mode: Write mode controlling behavior when path exists:
            "overwrite" (replace), "append" (add rows), "ignore" (skip),
            "error" (raise exception).
        options: Format-specific writer options (e.g., {"header": "true",
            "delimiter": ","} for CSV).

    Example:
        Configure in the pipeline definition (parent key: `loads`):

        **Configuration in JSON:**
        ```
        {
            "loads": [
                {
                    "id": "file_output",
                    "type": "load",
                    "upstreamId": "previous_transform",
                    "loadType": "file",
                    "location": "/data/output/results",
                    "format": "csv",
                    "mode": "overwrite",
                    "options": {
                        "header": "true",
                        "delimiter": ","
                    },
                    "schemaExport": "/data/schemas/output_schema.json"
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        loads:
          - id: file_output
            type: load
            upstreamId: previous_transform
            loadType: file
            location: /data/output/results
            format: csv
            mode: overwrite
            options:
              header: "true"
              delimiter: ","
            schemaExport: /data/schemas/output_schema.json
        ```

    Note:
        Batch writes log row count before and after. Streaming writes return
        immediately with active StreamingQuery; caller must manage stream
        lifecycle (monitoring, error handling, stopping). Output location
        must be writable by Spark application.
    """

    load_type: Literal["file"]

    def _load_batch(self) -> None:
        """Write the DataFrame to file in batch mode.

        Execute a single-pass write of the complete DataFrame to the
        specified location with the configured format and mode. Logs row
        count before writing and confirms successful completion after.
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
        """Write the DataFrame to file in streaming mode.

        Initiate a continuous streaming write to the specified location with
        the configured format and output mode. Return immediately with an
        active StreamingQuery that the caller can monitor or terminate.

        Returns:
            StreamingQuery: Active streaming write operation. Monitor with
                query.status property or terminate with query.stop() method.
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
