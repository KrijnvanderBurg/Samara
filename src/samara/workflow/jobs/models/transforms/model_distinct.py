"""Distinct row selection transform configuration models.

This module provides data models for configuring distinct row operations
in data pipelines. It enables declarative specification of deduplication
logic, allowing users to remove duplicate rows through configuration rather
than code.
"""

from typing import Literal

from pydantic import Field

from samara.workflow.jobs.models.model_transform import ArgsModel, FunctionModel


class DistinctArgs(ArgsModel):
    """Container for distinct operation parameters.

    The distinct operation considers all columns when determining uniqueness.

    Example:
        **Configuration in JSON:**
        ```
        {}
        ```

        **Configuration in YAML:**
        ```
        {}
        ```
    """


class DistinctFunctionModel(FunctionModel[DistinctArgs]):
    """Configure distinct row selection transformations.

    This model defines the complete configuration for a distinct transform operation.
    The distinct operation considers all columns when determining row uniqueness.

    Attributes:
        function_type: Identifies this transform as a distinct operation. Always
            set to "distinct".
        arguments: Container for the distinct operation parameters.

    Example:
        **Configuration in JSON:**
        ```
        {
            "function_type": "distinct",
            "arguments": {}
        }
        ```

        **Configuration in YAML:**
        ```
        function_type: distinct
        arguments: {}
        ```

    Note:
        The distinct operation returns the first occurrence of each unique
        combination of values. Column order in the result matches the input.
        Performance may vary with large datasets depending on your configured
        processing engine.
    """

    function_type: Literal["distinct"] = Field(..., description="Transform type identifier")
    arguments: DistinctArgs = Field(..., description="Container for the distinct operation parameters")
