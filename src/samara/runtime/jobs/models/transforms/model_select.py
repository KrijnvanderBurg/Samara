"""Column selection transform configuration models.

This module defines the data models for configuring column selection
transformations in data pipelines, enabling users to specify which columns
to include in the output DataFrame.
"""

import logging
from typing import Literal

from pydantic import Field

from samara.runtime.jobs.models.model_transform import ArgsModel
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class SelectArgs(ArgsModel):
    """Arguments for column selection transform operations.

    Specifies which columns to select from the input DataFrame.

    Attributes:
        columns: List of column names to select. Must include at least one
            column. Order is preserved in the output DataFrame.

    Example:
        >>> args = SelectArgs(columns=["id", "name", "email"])
        >>> print(args.columns)
        ["id", "name", "email"]

    Note:
        Column names must exist in the input DataFrame. Missing columns
        will cause the transform to fail during execution.
    """

    columns: list[str] = Field(..., description="List of column names to select from the DataFrame", min_length=1)


class SelectFunctionModel:
    """Configuration model for column selection transform operations.

    Defines the structure for selecting specific columns from the input
    DataFrame. This transform narrows the output to only the requested
    columns while preserving their order.

    Attributes:
        function_type: The transform type identifier (always "select").
        arguments: Parameters specifying which columns to select.

    Example:
        >>> config = {
        ...     "function_type": "select",
        ...     "arguments": {"columns": ["user_id", "name"]}
        ... }
        >>> model = SelectFunctionModel(**config)

        **Configuration in JSON:**
        ```
        {
            "transforms": [
                {
                    "function_type": "select",
                    "arguments": {
                        "columns": ["user_id", "name", "email"]
                    }
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        transforms:
          - function_type: select
            arguments:
              columns:
                - user_id
                - name
                - email
        ```

    See Also:
        FilterFunctionModel: For filtering rows based on conditions.
        DropFunctionModel: For removing specific columns instead of selecting.
    """

    function_type: Literal["select"] = "select"
    arguments: SelectArgs = Field(..., description="Container for the column selection parameters")
