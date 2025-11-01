"""Schema handling for PySpark data processing.

This module provides a unified interface for creating PySpark StructType schemas
from multiple source formats. It enables configuration-driven schema definition
by accepting schemas as dictionaries, JSON strings, or file paths, converting
them seamlessly into PySpark's schema format for data extraction and loading
operations.
"""

import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from pyspark.sql.types import StructType

from samara.utils.file import FileHandler, FileHandlerContext
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class SchemaHandler(ABC):
    """Abstract base for schema format handlers.

    Defines the common interface for converting schemas from various source
    formats into PySpark StructType objects. Concrete implementations handle
    specific source formats (dictionary, JSON string, file), enabling
    configuration-driven schema specification in pipeline definitions.

    All concrete schema handlers inherit from this class and must implement
    the parse() method to support their specific source format.
    """

    @staticmethod
    @abstractmethod
    def parse(schema: Any) -> StructType:
        """Convert a schema definition to a PySpark StructType.

        Transform the schema from its source format into a PySpark StructType
        that can be used for data extraction and loading operations.

        Args:
            schema: The schema definition in a format specific to the handler
                implementation. May be a dictionary, JSON string, file path,
                or other format depending on the concrete handler.

        Returns:
            StructType: A PySpark StructType representing the schema with all
                fields and types properly defined.

        Raises:
            NotImplementedError: If not implemented by a subclass.
        """


class SchemaDictHandler(SchemaHandler):
    """Convert dictionary schemas to PySpark StructType.

    Handles schema definitions provided as Python dictionaries following
    PySpark's schema JSON format. Enables pipeline authors to define schemas
    inline as configuration dictionaries, supporting both direct schema
    definitions and those loaded from configuration files.

    Example:
        ```python
        schema_dict = {
            "fields": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "name", "type": "string", "nullable": True}
            ]
        }
        schema = SchemaDictHandler.parse(schema_dict)
        ```
    """

    @staticmethod
    def parse(schema: dict) -> StructType:
        """Convert a dictionary to a PySpark StructType schema.

        Parse a Python dictionary representation following PySpark's schema
        JSON format and convert it into a ready-to-use StructType schema.

        Args:
            schema: Dictionary containing the schema definition. Must follow
                PySpark's schema JSON format with a "fields" array containing
                field definitions (name, type, nullable, etc.).

        Returns:
            StructType: A fully configured PySpark StructType schema ready for
                use in data operations.

        Raises:
            ValueError: If the dictionary format is invalid, missing required
                fields, or contains unsupported type definitions.
            TypeError: If the dictionary structure doesn't match PySpark's
                expected schema format.
            KeyError: If required schema keys are missing from the dictionary.
        """
        logger.debug("Parsing schema from dictionary with keys: %s", list(schema.keys()))

        try:
            struct_type = StructType.fromJson(json=schema)
            field_count = len(struct_type.fields)
            logger.info("Successfully parsed schema from dictionary - %d fields", field_count)
            logger.debug("Schema fields: %s", [f.name for f in struct_type.fields])
            return struct_type
        except (ValueError, TypeError, KeyError) as e:
            raise ValueError(f"Failed to convert dictionary to schema: {e}") from e


class SchemaStringHandler(SchemaHandler):
    """Convert JSON string schemas to PySpark StructType.

    Handles schema definitions provided as JSON strings. Enables flexible
    schema specification where pipeline definitions contain serialized schema
    representations that need conversion to PySpark's native format.

    Example:
        ```python
        schema_str = '''
        {
            "fields": [
                {"name": "id", "type": "integer", "nullable": false},
                {"name": "name", "type": "string", "nullable": true}
            ]
        }
        '''
        schema = SchemaStringHandler.parse(schema_str)
        ```
    """

    @staticmethod
    def parse(schema: str) -> StructType:
        """Convert a JSON string to a PySpark StructType schema.

        Parse a JSON string representation of a schema and convert it into
        a PySpark StructType. Internally deserializes the JSON string and
        delegates to the dictionary handler for conversion.

        Args:
            schema: String containing a valid JSON schema definition following
                PySpark's schema JSON format.

        Returns:
            StructType: A fully configured PySpark StructType schema ready for
                use in data operations.

        Raises:
            ValueError: If the JSON is invalid or cannot be converted to a
                valid schema structure.
            json.JSONDecodeError: If the string is not valid JSON.
        """
        logger.debug("Parsing schema from JSON string (length: %d)", len(schema))

        try:
            logger.debug("Parsing JSON string to dictionary")
            parsed_json = json.loads(s=schema)
            logger.debug("Successfully parsed JSON string")

            result = SchemaDictHandler.parse(schema=parsed_json)
            logger.info("Successfully parsed schema from JSON string")
            return result

        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON schema format: {e}"
            logger.error("JSON parsing failed for schema string: %s", error_msg)
            raise ValueError(error_msg) from e


class SchemaFilepathHandler(SchemaHandler):
    """Convert file-based schemas to PySpark StructType.

    Handles schema definitions stored in files (typically JSON). Enables
    pipeline authors to separate schema definitions from configuration,
    improving maintainability and reusability of schema definitions across
    multiple pipeline configurations.

    Example:
        ```python
        from pathlib import Path
        schema = SchemaFilepathHandler.parse(Path("schema.json"))
        ```
    """

    @staticmethod
    def parse(schema: Path) -> StructType:
        """Convert a schema file to a PySpark StructType.

        Read a schema definition from a file and convert it into a PySpark
        StructType. Automatically selects the appropriate file handler based
        on the file extension to support multiple file formats.

        Args:
            schema: Path object pointing to the schema definition file. File
                format is determined by extension (typically .json).

        Returns:
            StructType: A fully configured PySpark StructType schema ready for
                use in data operations.

        Raises:
            FileNotFoundError: If the schema file doesn't exist at the
                specified path.
            PermissionError: If insufficient permissions exist to read the file.
            ValueError: If the file content is invalid or cannot be converted
                to a valid schema structure.
            NotImplementedError: If the file format is not supported by any
                available file handler.
        """
        logger.info("Parsing schema from file: %s", schema)

        try:
            logger.debug("Creating file handler for schema file: %s", schema)
            file_handler: FileHandler = FileHandlerContext.from_filepath(filepath=schema)

            logger.debug("Reading schema file content")
            file_content = file_handler.read()

            logger.debug("Converting file content to schema")
            result = SchemaDictHandler.parse(schema=file_content)

            logger.info("Successfully parsed schema from file: %s", schema)
            return result

        except (FileNotFoundError, PermissionError, NotImplementedError) as e:
            logger.error("File access error reading schema file %s: %s", schema, str(e))
            raise e
