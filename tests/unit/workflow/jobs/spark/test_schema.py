"""Unit tests for the schema handling utility module.

This module contains tests for the schema handling utilities that are responsible
for creating and managing PySpark schemas from different source formats including
dictionaries, JSON strings, and files.

The tests verify that:
- Schemas can be correctly created from various source formats
- Error conditions are properly handled
- The conversion between formats preserves schema structure and field definitions
"""

import json
import tempfile
from pathlib import Path

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from samara.workflow.jobs.spark.schema import SchemaDictHandler, SchemaFilepathHandler, SchemaStringHandler


class TestSchemaHandlers:
    """Unit tests for schema handler classes.

    Tests cover:
        - Creating PySpark schemas from dict, string, and file
        - Error handling for invalid schema input
        - Edge cases for missing or malformed schema definitions
    """

    @pytest.fixture(name="schema_struct")
    def fixture_schema_struct(self) -> StructType:
        """
        Return a sample PySpark StructType schema for testing.

        Returns:
            StructType: A simple schema with name, age, and job_title fields.
        """
        return StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("job_title", StringType(), True),
            ]
        )

    @pytest.fixture(name="schema_json_file")
    def fixture_schema_json_file(self, schema_struct: StructType) -> Path:
        """
        Create a temporary named JSON file containing the schema definition.

        Args:
            schema_struct: The StructType schema from the schema_struct fixture.

        Returns:
            Path: Path to a temporary named JSON file containing schema definition.
        """

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(schema_struct.json())
            f.flush()
            return Path(f.name)

    @pytest.fixture(name="schema_dict")
    def fixture_schema_dict(self, schema_struct: StructType) -> dict:
        """
        Convert the sample schema to a dictionary representation.

        Args:
            schema_struct: The StructType schema from the schema_struct fixture.

        Returns:
            dict: Dictionary representation of the schema.
        """
        return schema_struct.jsonValue()

    @pytest.fixture(name="schema_json_str")
    def fixture_schema_json_str(self, schema_struct: StructType) -> str:
        """
        Convert the sample schema to a JSON string representation.

        Args:
            schema_struct: The StructType schema from the schema_struct fixture.

        Returns:
            str: JSON string representation of the schema.
        """
        return schema_struct.json()

    def test_schema_dict_handler_parse(self, schema_dict: dict) -> None:
        """
        Test SchemaDictHandler parses a dictionary into a schema.

        Args:
            schema_dict: Dictionary representation of a schema.
        """
        # Act
        schema = SchemaDictHandler.parse(schema=schema_dict)

        # Assert
        assert schema.jsonValue() == schema_dict

    def test_schema_dict_handler_invalid_dict(self) -> None:
        """
        Test SchemaDictHandler raises ValueError when given an invalid schema dict.

        Verifies that proper validation happens when an invalid schema is provided.
        """
        # Act & Assert
        with pytest.raises(ValueError):
            SchemaDictHandler.parse(schema={"invalid": "schema"})

    def test_schema_string_handler_parse(self, schema_json_str: str, schema_struct: StructType) -> None:
        """
        Test SchemaStringHandler parses a JSON string into a schema.

        Args:
            schema_json_str: JSON string representation of a schema.
            schema_struct: Expected StructType schema.
        """
        # Act
        schema = SchemaStringHandler.parse(schema=schema_json_str)

        # Act
        assert schema.json() == schema_struct.json()

    def test_schema_string_handler_invalid_json(self) -> None:
        """
        Test SchemaStringHandler raises ValueError when given invalid JSON.

        Verifies that proper JSON validation happens when a malformed string is provided.
        """
        # Arrange
        invalid_json = '{"id": "John" "age": 30}'  # Missing comma makes it invalid JSON

        # Act & Assert
        with pytest.raises(ValueError):
            SchemaStringHandler.parse(schema=invalid_json)

    def test_schema_filepath_handler_parse(self, schema_json_file: Path) -> None:
        """
        Test SchemaFilepathHandler loads schema from a file.

        Args:
            schema_json_file: Path to the JSON file containing the schema.
        """
        # Arrange - Read the JSON file content to get expected schema
        with open(file=schema_json_file, mode="r", encoding="utf-8") as f:
            schema_json_str = f.read()
        expected_schema_dict = json.loads(schema_json_str)

        # Act
        schema = SchemaFilepathHandler.parse(schema=schema_json_file)

        # Assert
        assert schema.jsonValue() == expected_schema_dict

    def test_schema_filepath_handler_file_not_found(self) -> None:
        """
        Test SchemaFilepathHandler propagates FileNotFoundError.

        Verifies that exceptions from file handling are properly propagated.
        """
        # Act & Assert
        with pytest.raises(FileNotFoundError):
            ap = SchemaFilepathHandler.parse(schema=Path("nonexistent.json"))
            print(ap)

    def test_schema_filepath_handler_permission_error(self, schema_json_file: Path) -> None:
        """
        Test SchemaFilepathHandler propagates PermissionError.

        Verifies that permission errors during file access are properly propagated.

        Args:
            schema_json_file: Path to the JSON file containing the schema.
        """
        # Remove read permissions to trigger PermissionError
        schema_json_file.chmod(0o000)

        # Act & Assert
        with pytest.raises(PermissionError):
            SchemaFilepathHandler.parse(schema=schema_json_file)

    def test_schema_filepath_handler_invalid_json(self) -> None:
        """
        Test SchemaFilepathHandler raises ValueError when file contains invalid schema.

        Verifies that schema validation occurs after successfully reading a file.
        """
        # Arrange
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('{"invalid": "schema"}')
            file_path = Path(f.name)

        # Act & Assert
        with pytest.raises(ValueError):
            SchemaFilepathHandler.parse(schema=file_path)
