"""Column removal transform configuration models.

This module defines the data models for configuring column removal
transformations in data pipelines, enabling users to specify which columns
to drop from the output DataFrame.
"""

from typing import Literal

from pydantic import Field

from samara.workflow.jobs.models.model_transform import ArgsModel, FunctionModel


class DropArgs(ArgsModel):
    """Arguments for column removal transform operations.

    Specifies which columns to remove from the input DataFrame.

    Attributes:
        columns: List of column names to drop. Must include at least one
            column. Columns are removed in a single operation.

    Example:
        >>> args = DropArgs(columns=["temp_id", "internal_flag"])
        >>> print(args.columns)
        ["temp_id", "internal_flag"]

    Note:
        Column names must exist in the input DataFrame. Attempting to drop
        non-existent columns will cause the transform to fail during execution.
    """

    columns: list[str] = Field(..., description="List of column names to drop from the DataFrame", min_length=1)


class DropFunctionModel(FunctionModel[DropArgs]):
    """Configuration model for column removal transform operations.

    Defines the structure for removing specific columns from the input
    DataFrame. This transform narrows the output by excluding the requested
    columns while preserving all others and their order.

    Attributes:
        function_type: The transform type identifier (always "drop").
        arguments: Parameters specifying which columns to remove.

    Example:
        >>> config = {
        ...     "function_type": "drop",
        ...     "arguments": {"columns": ["temp_id", "debug_flag"]}
        ... }
        >>> model = DropFunctionModel(**config)

        **Configuration in JSON:**
        ```
        {
            "transforms": [
                {
                    "function_type": "drop",
                    "arguments": {
                        "columns": ["temp_id", "internal_flag", "debug_mode"]
                    }
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        transforms:
          - function_type: drop
            arguments:
              columns:
                - temp_id
                - internal_flag
                - debug_mode
        ```

    See Also:
        SelectFunctionModel: For selecting specific columns instead of dropping.
        DropDuplicatesFunctionModel: For removing duplicate rows.
    """

    function_type: Literal["drop"] = "drop"
    arguments: DropArgs = Field(..., description="Container for the drop parameters")
