"""Column type casting transform configuration models.

This module provides data models for configuring column type casting operations
in data pipelines. It enables declarative specification of which columns should
be cast to which data types, allowing users to transform column types through
configuration rather than code.
"""

from typing import Literal

from pydantic import Field

from samara import BaseModel
from samara.runtime.jobs.models.model_transform import ArgsModel, FunctionModel


class CastColumn(BaseModel):
    """Specify a column and its target data type for casting.

    Attributes:
        column_name: Name of the column to cast. Must be a valid column present
            in the input dataset.
        cast_type: Target data type for the column. Valid types depend on the
            processing engine (e.g., "int", "string", "double", "boolean").

    Example:
        **Configuration in JSON:**
        ```
        {
            "column_name": "user_id",
            "cast_type": "int"
        }
        ```

        **Configuration in YAML:**
        ```
        column_name: user_id
        cast_type: int
        ```
    """

    column_name: str = Field(..., description="Name of the column to cast", min_length=1)
    cast_type: str = Field(..., description="Target data type to cast the column to", min_length=1)


class CastArgs(ArgsModel):
    """Container for column casting operation parameters.

    Attributes:
        columns: List of column definitions specifying which columns to cast
            and their target data types.

    Example:
        **Configuration in JSON:**
        ```
        {
            "columns": [
                {
                    "column_name": "user_id",
                    "cast_type": "int"
                },
                {
                    "column_name": "amount",
                    "cast_type": "double"
                },
                {
                    "column_name": "is_active",
                    "cast_type": "boolean"
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        columns:
          - column_name: user_id
            cast_type: int
          - column_name: amount
            cast_type: double
          - column_name: is_active
            cast_type: boolean
        ```
    """

    columns: list[CastColumn] = Field(..., description="List of column casting definitions")


class CastFunctionModel(FunctionModel[CastArgs]):
    """Configure column type casting transformations.

    This model defines the complete configuration for a cast transform operation,
    allowing you to specify which columns should be cast to which data types
    within your pipeline definition.

    Attributes:
        function_type: Identifies this transform as a cast operation. Always
            set to "cast".
        arguments: Container holding the list of columns and their target types.

    Example:
        **Configuration in JSON:**
        ```
        {
            "function_type": "cast",
            "arguments": {
                "columns": [
                    {
                        "column_name": "order_id",
                        "cast_type": "int"
                    },
                    {
                        "column_name": "total_price",
                        "cast_type": "double"
                    }
                ]
            }
        }
        ```

        **Configuration in YAML:**
        ```
        function_type: cast
        arguments:
          columns:
            - column_name: order_id
              cast_type: int
            - column_name: total_price
              cast_type: double
        ```

    Note:
        The cast operation validates that target columns exist in the input
        dataset before execution. Data type compatibility depends on your
        configured processing engine (Pandas, Polars, etc.).
    """

    function_type: Literal["cast"] = "cast"
    arguments: CastArgs = Field(..., description="Container for the column casting parameters")
