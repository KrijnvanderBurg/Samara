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
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual
from samara.workflow.jobs.models.model_load import LoadMethod
from samara.workflow.jobs.spark.load import LoadFileSpark

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
    """Test LoadFileSpark load functionality with real DataFrames."""

    def test_load__with_batch_method__completes_successfully(
        self, spark: SparkSession, load_file_spark: LoadFileSpark
    ) -> None:
        """Test load method completes successfully for batch loading with real DataFrame."""
        # Arrange - create real DataFrame
        test_data = [(1, "Alice", 30), (2, "Bob", 25)]
        test_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        test_df = spark.createDataFrame(test_data, test_schema)

        # Mock only the write I/O
        mock_write = Mock()
        mock_write.save = Mock()

        load_file_spark.data_registry[load_file_spark.upstream_id] = test_df

        # Act - patch the write property
        with patch.object(type(test_df), "write", mock_write):
            load_file_spark.load()

        # Assert - verify the dataframe was copied to the load step
        result_df = load_file_spark.data_registry[load_file_spark.id_]
        assertDataFrameEqual(result_df, test_df)
        mock_write.save.assert_called_once()

    def test_load__with_streaming_method__completes_successfully(
        self, spark: SparkSession, valid_load_config: dict[str, Any]
    ) -> None:
        """Test load method completes successfully for streaming loading with real DataFrame."""
        # Arrange
        valid_load_config["method"] = "streaming"
        load_streaming = LoadFileSpark(**valid_load_config)

        test_data = [(1, "Alice"), (2, "Bob")]
        test_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )
        test_df = spark.createDataFrame(test_data, test_schema)

        # Mock streaming write
        mock_streaming_query = Mock()
        mock_streaming_query.id = "test-query-id"
        mock_write_stream = Mock()
        mock_write_stream.start.return_value = mock_streaming_query

        load_streaming.data_registry[load_streaming.upstream_id] = test_df

        # Act - patch the writeStream property
        with patch.object(type(test_df), "writeStream", mock_write_stream):
            load_streaming.load()

        # Assert - verify the streaming query was registered
        assert load_streaming.streaming_query_registry[load_streaming.id_] == mock_streaming_query
        mock_write_stream.start.assert_called_once()

    def test_load__with_invalid_method__raises_value_error(
        self, spark: SparkSession, load_file_spark: LoadFileSpark
    ) -> None:
        """Test load method raises ValueError for unsupported loading method."""
        # Arrange
        mock_method = Mock()
        mock_method.value = "invalid_method"

        test_data = [(1, "Alice")]
        test_schema = StructType([StructField("id", IntegerType(), True), StructField("name", StringType(), True)])
        test_df = spark.createDataFrame(test_data, test_schema)

        load_file_spark.data_registry[load_file_spark.upstream_id] = test_df

        with patch.object(load_file_spark, "method", mock_method):
            # Assert
            with pytest.raises(ValueError):
                # Act
                load_file_spark.load()

    def test_load__with_empty_schema_export__skips_schema_export(
        self, spark: SparkSession, load_file_spark: LoadFileSpark
    ) -> None:
        """Test schema export is skipped when schema_export is empty."""
        # Arrange
        load_file_spark.schema_export = ""  # No schema export

        test_data = [(1, "Alice", 30), (2, "Bob", 25)]
        test_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        test_df = spark.createDataFrame(test_data, test_schema)

        # Mock write
        mock_write = Mock()
        mock_write.save = Mock()

        load_file_spark.data_registry[load_file_spark.upstream_id] = test_df

        # Act - should complete without exception
        with patch.object(type(test_df), "write", mock_write):
            load_file_spark.load()

        # Assert
        mock_write.save.assert_called_once()

    def test_load__with_valid_schema_export__writes_schema_to_file(
        self, spark: SparkSession, tmp_path: Path, load_file_spark: LoadFileSpark
    ) -> None:
        """Test schema export writes schema to file when schema_export is provided with real DataFrame."""
        # Arrange
        schema_file = tmp_path / "test_schema.json"
        load_file_spark.schema_export = str(schema_file)

        test_data = [(1, "Alice", 30), (2, "Bob", 25)]
        test_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        test_df = spark.createDataFrame(test_data, test_schema)

        # Mock write
        mock_write = Mock()
        mock_write.save = Mock()

        load_file_spark.data_registry[load_file_spark.upstream_id] = test_df

        # Act
        with patch.object(type(test_df), "write", mock_write):
            load_file_spark.load()

        # Assert - schema file should exist and contain the expected schema
        assert schema_file.exists()
        written_schema = json.loads(schema_file.read_text(encoding="utf-8"))
        assert written_schema == test_schema.jsonValue()
