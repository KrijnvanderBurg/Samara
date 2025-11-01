"""Distinct row selection transform configuration models.

This module provides data models for configuring distinct row operations
in data pipelines. It enables declarative specification of deduplication
logic, allowing users to remove duplicate rows through configuration rather
than code.
"""

from typing import Literal

from pydantic import Field

from samara.runtime.jobs.models.model_transform import ArgsModel, FunctionModel


class DistinctArgs(ArgsModel):
    """Container for distinct operation parameters.

    Attributes:
        columns: List of column names to consider for determining uniqueness.
            When specified, rows are considered duplicates if they have the same
            values in all specified columns. When null or empty, all columns
            are considered.

    Example:
        **Configuration in JSON:**
        ```
        {
            "columns": ["user_id", "timestamp"]
        }
        ```

        **Configuration in YAML:**
        ```
        columns:
          - user_id
          - timestamp
        ```
    """

    columns: list[str] | None = Field(..., description="Column names to consider for uniqueness, null for all columns")


class DistinctFunctionModel(FunctionModel[DistinctArgs]):
    """Configure distinct row selection transformations.

    This model defines the complete configuration for a distinct transform operation,
    allowing you to specify which columns should be considered when determining
    row uniqueness within your pipeline definition.

    Attributes:
        function_type: Identifies this transform as a distinct operation. Always
            set to "distinct".
        arguments: Container holding the list of columns to consider for uniqueness.

    Example:
        **Configuration in JSON:**
        ```
        {
            "function_type": "distinct",
            "arguments": {
                "columns": ["customer_id", "order_date"]
            }
        }
        ```

        **Configuration in YAML:**
        ```
        function_type: distinct
        arguments:
          columns:
            - customer_id
            - order_date
        ```

    Note:
        The distinct operation returns the first occurrence of each unique
        combination of values. Column order in the result matches the input.
        Performance may vary with large datasets depending on your configured
        processing engine.
    """

    function_type: Literal["distinct"] = Field(..., description="Transform type identifier")
    arguments: DistinctArgs = Field(..., description="Container for the distinct operation parameters")
