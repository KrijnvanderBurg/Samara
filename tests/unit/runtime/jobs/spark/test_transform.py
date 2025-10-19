"""Tests for TransformSpark functionality.

These tests verify TransformSpark creation, validation, data transformation,
and error handling scenarios.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import ExitStack
from typing import Any
from unittest.mock import Mock, patch

import pytest
from pydantic import ValidationError

from samara.runtime.jobs.spark.transform import TransformSpark

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="transform_config")
def fixture_transform_config() -> Generator[dict[str, Any], Any, None]:
    """Provide a transform configuration dict.

    Returning a Generator mirrors the `test_extract.py` layout and keeps the
    fixtures consistent across the Spark tests.
    """
    # Using ExitStack here keeps the pattern consistent even when no tempfiles
    # are required for this fixture.
    stack = ExitStack()

    data: dict[str, Any] = {
        "id": "customer_transform",
        "upstream_id": "customer_data",
        "options": {"spark.sql.shuffle.partitions": "10"},
        "functions": [],
    }

    yield data
    stack.close()


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestTransformSparkValidation:
    """Test TransformSpark model validation and instantiation."""

    def test_create_transform_spark__with_valid_config__succeeds(self, transform_config: dict[str, Any]) -> None:
        """Test TransformSpark creation with valid configuration."""
        # Act
        transform = TransformSpark(**transform_config)

        # Assert
        assert transform.id_ == "customer_transform"
        assert transform.upstream_id == "customer_data"
        assert transform.options == {"spark.sql.shuffle.partitions": "10"}
        assert isinstance(transform.functions, list)
        assert len(transform.functions) == 0

    def test_create_transform_spark__with_missing_functions__raises_validation_error(
        self, transform_config: dict[str, Any]
    ) -> None:
        """Test TransformSpark creation fails when functions field is missing."""
        # Arrange
        del transform_config["functions"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            TransformSpark(**transform_config)

    def test_create_transform_spark__with_missing_name__raises_validation_error(
        self, transform_config: dict[str, Any]
    ) -> None:
        """Test TransformSpark creation fails when name is missing."""
        # Arrange
        del transform_config["id"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            TransformSpark(**transform_config)

    def test_create_transform_spark__with_empty_name__raises_validation_error(
        self, transform_config: dict[str, Any]
    ) -> None:
        """Test TransformSpark creation fails with empty name."""
        # Arrange
        transform_config["id"] = ""

        # Assert
        with pytest.raises(ValidationError):
            # Act
            TransformSpark(**transform_config)

    def test_create_transform_spark__with_missing_upstream_id__raises_validation_error(
        self, transform_config: dict[str, Any]
    ) -> None:
        """Test TransformSpark creation fails when upstream_id is missing."""
        # Arrange
        del transform_config["upstream_id"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            TransformSpark(**transform_config)

    def test_create_transform_spark__with_missing_options__raises_validation_error(
        self, transform_config: dict[str, Any]
    ) -> None:
        """Test TransformSpark creation fails when options is missing."""
        # Arrange
        del transform_config["options"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            TransformSpark(**transform_config)

    def test_create_transform_spark__with_empty_options__succeeds(self, transform_config: dict[str, Any]) -> None:
        """Test TransformSpark creation with empty options dict succeeds."""
        # Arrange
        transform_config["options"] = {}

        # Act
        transform = TransformSpark(**transform_config)

        # Assert
        assert transform.options == {}

    def test_create_transform_spark__with_invalid_functions_type__raises_validation_error(
        self, transform_config: dict[str, Any]
    ) -> None:
        """Test TransformSpark creation fails when functions is not a list."""
        # Arrange
        transform_config["functions"] = "not-a-list"

        # Assert
        with pytest.raises(ValidationError):
            # Act
            TransformSpark(**transform_config)

    def test_create_transform_spark__with_function_list__succeeds(self, transform_config: dict[str, Any]) -> None:
        """Test TransformSpark creation with valid function list."""
        # Arrange
        transform_config["functions"] = [
            {"function_type": "select", "arguments": {"columns": ["id", "name"]}},
        ]

        # Act
        transform = TransformSpark(**transform_config)

        # Assert
        assert len(transform.functions) == 1
        assert transform.functions[0].function_type == "select"


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="transform_spark")
def fixture_transform_spark(transform_config: dict[str, Any]) -> TransformSpark:
    """Create TransformSpark instance from valid configuration."""
    return TransformSpark(**transform_config)


# =========================================================================== #
# ========================== TRANSFORM TESTS =============================== #
# =========================================================================== #


class TestTransformSparkTransform:
    """Test TransformSpark transformation functionality."""

    def test_transform__with_empty_functions__completes_successfully(self, transform_spark: TransformSpark) -> None:
        """Test transform method completes successfully with no transformation functions."""
        # Arrange
        mock_dataframe = Mock()
        mock_dataframe.count.return_value = 10

        transform_spark.data_registry[transform_spark.upstream_id] = mock_dataframe

        # Act
        transform_spark.transform()

        # Assert - dataframe should be copied to transform name
        assert transform_spark.data_registry[transform_spark.id_] == mock_dataframe

    def test_transform__with_single_function__applies_transformation(self, transform_config: dict[str, Any]) -> None:
        """Test transform method applies single transformation function."""
        # Arrange
        transform_config["functions"] = [
            {"function_type": "select", "arguments": {"columns": ["id", "name"]}},
        ]
        transform = TransformSpark(**transform_config)

        mock_dataframe = Mock()
        mock_dataframe.count.return_value = 10
        mock_transformed_df = Mock()
        mock_transformed_df.count.return_value = 10

        mock_callable = Mock(return_value=mock_transformed_df)

        transform.data_registry[transform.upstream_id] = mock_dataframe

        with patch(
            "samara.runtime.jobs.spark.transforms.select.SelectFunction.transform",
            return_value=mock_callable,
        ) as mock_transform_func:
            # Act
            transform.transform()

            # Assert
            mock_transform_func.assert_called_once()
            mock_callable.assert_called_once_with(df=mock_dataframe)
            assert transform.data_registry[transform.id_] == mock_transformed_df

    def test_transform__with_multiple_functions__applies_in_sequence(self, transform_config: dict[str, Any]) -> None:
        """Test transform method applies multiple transformation functions in sequence."""
        # Arrange
        transform_config["functions"] = [
            {"function_type": "select", "arguments": {"columns": ["id", "name", "age"]}},
            {"function_type": "filter", "arguments": {"condition": "age > 18"}},
        ]
        transform = TransformSpark(**transform_config)

        mock_df_original = Mock()
        mock_df_original.count.return_value = 100

        mock_df_after_select = Mock()
        mock_df_after_select.count.return_value = 100

        mock_df_after_filter = Mock()
        mock_df_after_filter.count.return_value = 80

        mock_select_callable = Mock(return_value=mock_df_after_select)
        mock_filter_callable = Mock(return_value=mock_df_after_filter)

        transform.data_registry[transform.upstream_id] = mock_df_original

        with (
            patch(
                "samara.runtime.jobs.spark.transforms.select.SelectFunction.transform",
                return_value=mock_select_callable,
            ),
            patch(
                "samara.runtime.jobs.spark.transforms.filter.FilterFunction.transform",
                return_value=mock_filter_callable,
            ),
        ):
            # Act
            transform.transform()

            # Assert
            # First function should receive original dataframe
            mock_select_callable.assert_called_once_with(df=mock_df_original)
            # Second function should receive result from first function
            mock_filter_callable.assert_called_once_with(df=mock_df_after_select)
            # Final result should be in registry
            assert transform.data_registry[transform.id_] == mock_df_after_filter
