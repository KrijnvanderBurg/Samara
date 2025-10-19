"""Tests for LoadFileSpark functionality.

These tests verify LoadFileSpark creation, validation, data loading,
and error handling scenarios.
"""

import json
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
from pydantic import ValidationError

from samara.runtime.jobs.models.model_load import LoadMethod
from samara.runtime.jobs.spark.load import LoadFileSpark

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="valid_load_config")
def fixture_valid_load_config(tmp_path: Path) -> Generator[dict[str, Any], Any, None]:
    """Provide a valid load configuration with real test files using tmp_path.

    Args:
        tmp_path: pytest temporary directory fixture.

    Yields:
        dict: configuration dictionary pointing to files under tmp_path.
    """

    # Create output location file under tmp_path
    output_file = Path(tmp_path) / "output.json"

    # Create schema location file under tmp_path
    schema_file = Path(tmp_path) / "schema.json"

    config = {
        "id": "customer_data_output",
        "upstream_id": "customer_transform",
        "load_type": "file",
        "method": "batch",
        "location": str(output_file),
        "schema_export": str(schema_file),
        "options": {
            "header": True,
        },
        "mode": "overwrite",
        "data_format": "json",
    }

    yield config


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestLoadFileSparkValidation:
    """Test LoadFileSpark model validation and instantiation."""

    def test_create_load_file_spark__with_valid_config__succeeds(self, valid_load_config: dict[str, Any]) -> None:
        """Test LoadFileSpark creation with valid configuration."""
        # Act
        load = LoadFileSpark(**valid_load_config)

        # Assert
        assert load.id_ == "customer_data_output"
        assert load.upstream_id == "customer_transform"
        assert load.method == LoadMethod.BATCH
        assert load.data_format == "json"
        assert load.mode == "overwrite"
        assert load.options == {"header": True}
        assert isinstance(load.location, str) and load.location.endswith(".json")
        assert isinstance(load.schema_export, str) and load.schema_export.endswith(".json")

    def test_create_load_file_spark__with_missing_name__raises_validation_error(
        self, valid_load_config: dict[str, Any]
    ) -> None:
        """Test LoadFileSpark creation fails when name is missing."""
        # Arrange
        del valid_load_config["id"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            LoadFileSpark(**valid_load_config)

    def test_create_load_file_spark__with_empty_name__raises_validation_error(
        self, valid_load_config: dict[str, Any]
    ) -> None:
        """Test LoadFileSpark creation fails with empty name."""
        # Arrange
        valid_load_config["id"] = ""

        # Assert
        with pytest.raises(ValidationError):
            # Act
            LoadFileSpark(**valid_load_config)

    def test_create_load_file_spark__with_invalid_method__raises_validation_error(
        self, valid_load_config: dict[str, Any]
    ) -> None:
        """Test LoadFileSpark creation fails with invalid load method."""
        # Arrange
        valid_load_config["method"] = "invalid_method"

        # Assert
        with pytest.raises(ValidationError):
            # Act
            LoadFileSpark(**valid_load_config)

    def test_create_load_file_spark__with_missing_upstream_id__raises_validation_error(
        self, valid_load_config: dict[str, Any]
    ) -> None:
        """Test LoadFileSpark creation fails when upstream_id is missing."""
        # Arrange
        del valid_load_config["upstream_id"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            LoadFileSpark(**valid_load_config)

    def test_create_load_file_spark__with_missing_location__raises_validation_error(
        self, valid_load_config: dict[str, Any]
    ) -> None:
        """Test LoadFileSpark creation fails when location is missing."""
        # Arrange
        del valid_load_config["location"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            LoadFileSpark(**valid_load_config)

    def test_create_load_file_spark__with_streaming_method__succeeds(self, valid_load_config: dict[str, Any]) -> None:
        """Test LoadFileSpark creation with streaming method."""
        # Arrange
        valid_load_config["method"] = "streaming"

        # Act
        load = LoadFileSpark(**valid_load_config)

        # Assert
        assert load.method == LoadMethod.STREAMING

    def test_create_load_file_spark__with_csv_format__succeeds(self, valid_load_config: dict[str, Any]) -> None:
        """Test LoadFileSpark creation with CSV data format."""
        # Arrange
        valid_load_config["data_format"] = "csv"

        # Act
        load = LoadFileSpark(**valid_load_config)

        # Assert
        assert load.data_format == "csv"

    def test_create_load_file_spark__with_parquet_format__succeeds(self, valid_load_config: dict[str, Any]) -> None:
        """Test LoadFileSpark creation with Parquet data format."""
        # Arrange
        valid_load_config["data_format"] = "parquet"

        # Act
        load = LoadFileSpark(**valid_load_config)

        # Assert
        assert load.data_format == "parquet"

    def test_create_load_file_spark__with_empty_schema_export__succeeds(
        self, valid_load_config: dict[str, Any]
    ) -> None:
        """Test LoadFileSpark creation with empty schema location."""
        # Arrange
        valid_load_config["schema_export"] = ""

        # Act
        load = LoadFileSpark(**valid_load_config)

        # Assert
        assert load.schema_export == ""

    def test_create_load_file_spark__with_empty_options__succeeds(self, valid_load_config: dict[str, Any]) -> None:
        """Test LoadFileSpark creation with empty options."""
        # Arrange
        valid_load_config["options"] = {}

        # Act
        load = LoadFileSpark(**valid_load_config)

        # Assert
        assert load.options == {}


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="load_file_spark")
def fixture_load_file_spark(valid_load_config: dict[str, Any]) -> LoadFileSpark:
    """Create LoadFileSpark instance from valid configuration."""
    return LoadFileSpark(**valid_load_config)


# =========================================================================== #
# ============================ LOAD TESTS ================================== #
# =========================================================================== #


class TestLoadFileSparkLoad:
    """Test LoadFileSpark load functionality."""

    def test_load__with_batch_method__completes_successfully(self, load_file_spark: LoadFileSpark) -> None:
        """Test load method completes successfully for batch loading."""
        # Arrange
        mock_dataframe = Mock()
        mock_dataframe.count.return_value = 5
        mock_dataframe.schema.jsonValue.return_value = {"type": "struct", "fields": []}
        mock_dataframe.write = Mock()
        mock_dataframe.write.save = Mock()

        load_file_spark.data_registry[load_file_spark.upstream_id] = mock_dataframe

        # Act & Assert - should complete without exception
        load_file_spark.load()

        # Verify the dataframe was copied to the load step
        assert load_file_spark.data_registry[load_file_spark.id_] == mock_dataframe

    def test_load__with_streaming_method__completes_successfully(self, valid_load_config: dict[str, Any]) -> None:
        """Test load method completes successfully for streaming loading."""
        # Arrange
        valid_load_config["method"] = "streaming"
        load_streaming = LoadFileSpark(**valid_load_config)

        mock_dataframe = Mock()
        mock_dataframe.schema.jsonValue.return_value = {"type": "struct", "fields": []}
        mock_streaming_query = Mock()
        mock_streaming_query.id = "test-query-id"
        mock_write_stream = Mock()
        mock_write_stream.start.return_value = mock_streaming_query
        mock_dataframe.writeStream = mock_write_stream

        load_streaming.data_registry[load_streaming.upstream_id] = mock_dataframe

        # Act & Assert - should complete without exception
        load_streaming.load()

        # Verify the streaming query was registered
        assert load_streaming.streaming_query_registry[load_streaming.id_] == mock_streaming_query

    def test_load__with_invalid_method__raises_value_error(self, load_file_spark: LoadFileSpark) -> None:
        """Test load method raises ValueError for unsupported loading method."""
        # Arrange
        mock_method = Mock()
        mock_method.value = "invalid_method"
        mock_dataframe = Mock()

        load_file_spark.data_registry[load_file_spark.upstream_id] = mock_dataframe

        with patch.object(load_file_spark, "method", mock_method):
            # Assert
            with pytest.raises(ValueError):
                # Act
                load_file_spark.load()

    def test_load__with_empty_schema_export__skips_schema_export(self, load_file_spark: LoadFileSpark) -> None:
        """Test schema export is skipped when schema_export is empty."""
        # Arrange
        load_file_spark.schema_export = ""  # No schema export

        mock_dataframe = Mock()
        mock_dataframe.count.return_value = 5
        mock_dataframe.write = Mock()
        mock_dataframe.write.save = Mock()

        load_file_spark.data_registry[load_file_spark.upstream_id] = mock_dataframe

        # Act
        load_file_spark.load()

        # Assert - no exception should be raised, load should complete

    def test_load__with_valid_schema_export__writes_schema_to_file(
        self, tmp_path: Path, load_file_spark: LoadFileSpark
    ) -> None:
        """Test schema export writes schema to file when schema_export is provided."""
        # Arrange
        schema_file = tmp_path / "test_schema.json"
        load_file_spark.schema_export = str(schema_file)

        mock_dataframe = Mock()
        mock_dataframe.count.return_value = 5
        test_schema = {"type": "struct", "fields": [{"id": "id", "type": "integer"}]}
        mock_dataframe.schema.jsonValue.return_value = test_schema
        mock_dataframe.write = Mock()
        mock_dataframe.write.save = Mock()

        load_file_spark.data_registry[load_file_spark.upstream_id] = mock_dataframe

        # Act
        load_file_spark.load()

        # Assert - schema file should exist and contain the expected schema
        assert schema_file.exists()
        written_schema = json.loads(schema_file.read_text(encoding="utf-8"))
        assert written_schema == test_schema
