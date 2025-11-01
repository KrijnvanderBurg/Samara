"""Load models - Configuration structures for data loading operations.

This module provides the data models and configuration schemas for load operations
in data pipelines. It defines type-safe structures that enable users to specify
where and how to write processed data to destinations through configuration files.
"""

import logging
from abc import ABC
from enum import Enum
from typing import Literal

from pydantic import Field

from samara import BaseModel
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class LoadMethod(Enum):
    """Enumeration of supported data loading methods.

    Specifies the loading strategies available for writing data to destinations.
    These methods determine how data is delivered: all at once or in a streaming
    fashion.

    Attributes:
        BATCH: Write all data to destination in a single batch operation
        STREAMING: Write data continuously as it becomes available

    Example:
        **Configuration in JSON:**
        ```
            {
                "load": {
                    "id": "load_customers",
                    "method": "batch",
                    "upstream_id": "transform_data",
                    "location": "s3://bucket/output/customers",
                    "schema_export": "s3://bucket/schema/customers.json"
                }
            }
        ```

        **Configuration in YAML:**
        ```
            load:
              id: load_customers
              method: batch
              upstream_id: transform_data
              location: s3://bucket/output/customers
              schema_export: s3://bucket/schema/customers.json
        ```
    """

    BATCH = "batch"
    STREAMING = "streaming"


class LoadModel(BaseModel, ABC):
    """Configure how to write processed data to a destination.

    Defines the core configuration for load operations, specifying the source
    of data (upstream component), destination location, and loading method.
    This abstract class establishes the common interface for all load types
    and is extended by specific implementations (file-based, database, etc.).

    Attributes:
        id_: Unique identifier for this load operation within the pipeline.
            Used to reference this load in transforms and alerts.
        upstream_id: Identifier of the upstream component providing data to load.
            Must reference an extract or transform operation in the pipeline.
        method: Loading method controlling how data is written (batch or streaming).
        location: URI specifying the destination where data will be written.
            Format depends on the load type (file paths, database connection strings, etc.).
        schema_export: URI specifying where the schema information will be exported.
            Typically a JSON file containing the data structure definition.

    Example:
        **Configuration in JSON:**
        ```
            {
                "load": {
                    "id": "load_final_data",
                    "method": "batch",
                    "upstream_id": "transform_step",
                    "location": "/data/output/final",
                    "schema_export": "/data/schemas/final_schema.json"
                }
            }
        ```

        **Configuration in YAML:**
        ```
            load:
              id: load_final_data
              method: batch
              upstream_id: transform_step
              location: /data/output/final
              schema_export: /data/schemas/final_schema.json
        ```

    Note:
        This is an abstract base class. Use concrete implementations like
        LoadModelFile for actual pipeline configurations.
    """

    id_: str = Field(..., alias="id", description="Identifier for this load operation", min_length=1)
    upstream_id: str = Field(..., description="Identifier of the upstream component providing data", min_length=1)
    method: LoadMethod = Field(..., description="Loading method (batch or streaming)")
    location: str = Field(
        ..., description="URI that identifies where to load data in the modelified format.", min_length=1
    )
    schema_export: str = Field(..., description="URI that identifies where to load schema.")


class LoadModelFile(LoadModel):
    """Configure loading data to file-based destinations.

    Extends the base load configuration to specify file-specific parameters
    such as write mode and output format. Used when writing data to local
    file systems or cloud storage services.

    Attributes:
        load_type: Type discriminator set to "file" for file-based loads.
            Used by the framework to select the appropriate load handler.
        mode: Write mode controlling how data is written to the destination.
            Common values: "overwrite", "append", "ignore", "error".
        data_format: Format of the output files determining serialization.
            Supported formats: "csv", "json", "parquet", "orc", etc.

    Example:
        **Configuration in JSON:**
        ```
            {
                "load": {
                    "id": "load_customers",
                    "type": "file",
                    "method": "batch",
                    "upstream_id": "select_customer_fields",
                    "location": "/warehouse/customers",
                    "schema_export": "/schemas/customers.json",
                    "mode": "overwrite",
                    "format": "parquet"
                }
            }
        ```

        **Configuration in YAML:**
        ```
            load:
              id: load_customers
              type: file
              method: batch
              upstream_id: select_customer_fields
              location: /warehouse/customers
              schema_export: /schemas/customers.json
              mode: overwrite
              format: parquet
        ```

    Note:
        The load_type field uses a literal value of "file" to enable
        Pydantic discriminator-based polymorphism for automatic load type detection.
    """

    load_type: Literal["file"] = Field(..., description="Load type discriminator")
    mode: str = Field(..., description="Write mode for the load operation")
    data_format: str = Field(..., description="Format of the output files")
