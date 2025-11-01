"""Data models for extraction configuration.

This module provides data models and Pydantic schemas for defining extraction
operations in configuration files. It enables users to declaratively specify
how data should be read from various sources with type validation.
"""

import logging
from enum import Enum
from typing import Literal

from pydantic import Field, FilePath

from samara import BaseModel
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class ExtractMethod(Enum):
    """Define supported data extraction methods.

    Specifies the extraction approach used when reading data from sources.
    Configure this value in pipeline definitions to control whether data is
    read in a single batch or continuously streamed.

    Attributes:
        BATCH: Read all data at once in a single operation.
        STREAMING: Read data continuously as it becomes available.
    """

    BATCH = "batch"
    STREAMING = "streaming"


class ExtractModel(BaseModel):
    """Base model for extraction configurations.

    Provides the core schema for defining data extraction operations in
    configuration files. All extraction sources inherit from this model to
    ensure consistent structure and validation across different extract types.

    Attributes:
        id_: Unique identifier for this extraction operation. Used to reference
            this extract in transform chains and other pipeline components.
        method: Extraction approach - batch or streaming.
        data_format: Format of the source data (parquet, json, csv, etc.).
        schema_: Schema definition as a file path or JSON/YAML string. Defines
            the structure and types of columns in the extracted data.

    Example:
        **Configuration in JSON (inside extracts array):**
        ```
        {
            "id": "customers_extract",
            "method": "batch",
            "data_format": "json",
            "schema": "schemas/customers_schema.json"
        }
        ```

        **Configuration in YAML (under extracts):**
        ```
        - id: customers_extract
          method: batch
          data_format: json
          schema: schemas/customers_schema.json
        ```

    Note:
        This is a base class. Use specific extract types like ExtractFileModel
        for file-based extraction or ExtractDatabaseModel for database sources.
    """

    id_: str = Field(..., alias="id", description="Identifier for this extraction operation", min_length=1)
    method: ExtractMethod = Field(..., description="Method of extraction (batch or streaming)")
    data_format: str = Field(..., description="Format of the data to extract (parquet, json, csv, etc.)")
    schema_: str | FilePath = Field(..., alias="schema", description="Schema definition - can be a file path or string")


class ExtractFileModel(ExtractModel):
    """Configure extraction from file-based data sources.

    Specifies how to read data from files stored in local or remote locations.
    Supports multiple formats and includes schema validation. This model is
    used when declaring file-based extraction operations in pipeline configs.

    Attributes:
        extract_type: Type discriminator - always "file" for file extraction.
        id_: Unique identifier for this extraction operation.
        method: Extraction approach (batch or streaming).
        data_format: File format (parquet, json, csv, etc.).
        location: URI or path where source files are located.
        schema_: Schema definition for validating and typing extracted columns.

    Example:
        **Configuration in JSON (inside extracts array):**
        ```
        {
            "type": "file",
            "id": "orders_extract",
            "method": "batch",
            "data_format": "json",
            "location": "s3://my-bucket/orders/",
            "schema": "schemas/orders_schema.json"
        }
        ```

        **Configuration in YAML (under extracts):**
        ```
        - type: file
          id: orders_extract
          method: batch
          data_format: json
          location: s3://my-bucket/orders/
          schema: schemas/orders_schema.json
        ```

    Note:
        The location can be a local path, S3 URI, HDFS path, or any URI
        supported by the configured execution engine.
    """

    extract_type: Literal["file"] = Field(..., description="Extract type discriminator")
    location: str = Field(..., description="URI where the files are located")
