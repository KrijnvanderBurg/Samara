"""Configure dropDuplicates transformations for duplicate row removal.

This module provides models for defining dropDuplicates operations,
enabling you to remove duplicate rows based on specified columns
or across all columns in a data pipeline configuration.
"""

from typing import Literal

from pydantic import Field

from samara.runtime.jobs.models.model_transform import ArgsModel, FunctionModel


class DropDuplicatesArgs(ArgsModel):
    """Arguments for the dropDuplicates transformation.

    Specifies which columns to consider when identifying and removing
    duplicate rows. When columns is empty, all columns are considered
    for duplicate detection.

    Attributes:
        columns: List of column names to compare for duplicate detection.
            An empty list removes duplicates based on all columns.

    Example:
        **Configuration in JSON:**
        ```
        {
            "function": "dropDuplicates",
            "arguments": {
                "columns": ["customer_id", "email"]
            }
        }
        ```

        **Configuration in YAML:**
        ```
        function: dropDuplicates
        arguments:
            columns:
              - customer_id
              - email
        ```
    """

    columns: list[str] = Field(
        ...,
        description=(
            "List of column names to consider when dropping duplicates. If empty list, all columns are considered."
        ),
    )


class DropDuplicatesFunctionModel(FunctionModel[DropDuplicatesArgs]):
    """Configure a dropDuplicates transformation in a data pipeline.

    Remove duplicate rows from a dataset based on specified columns.
    This transformation identifies and retains only the first occurrence
    of each unique row combination.

    Attributes:
        function_type: The transform operation identifier (always "dropDuplicates").
        arguments: Parameters for the dropDuplicates operation.

    Example:
        **Configuration in JSON:**
        ```
        {
            "transforms": [
                {
                    "function": "dropDuplicates",
                    "arguments": {
                        "columns": ["user_id"]
                    }
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        transforms:
          - function: dropDuplicates
            arguments:
                columns:
                  - user_id
        ```

    Note:
        When columns is empty, the transformation considers all columns
        to detect duplicates. This operation is engine-agnostic and works
        with both Pandas and Polars backends.
    """

    function_type: Literal["dropDuplicates"] = "dropDuplicates"
    arguments: DropDuplicatesArgs = Field(..., description="Container for the dropDuplicates parameters")
