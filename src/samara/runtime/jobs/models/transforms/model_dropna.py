"""Drop null values transform configuration models.

This module provides data models for configuring operations to drop rows with
null values from DataFrames. It enables declarative specification of how to
handle missing data through configuration rather than code.
"""

from typing import Literal

from pydantic import Field

from samara.runtime.jobs.models.model_transform import ArgsModel, FunctionModel


class DropNaArgs(ArgsModel):
    """Container for drop null values operation parameters.

    Attributes:
        how: Determines how to drop rows with nulls. "any" drops rows with
            any null values, "all" drops only rows where all values are null.
        thresh: Minimum number of non-null values required to keep a row.
            If specified (> 0), overrides the 'how' parameter. Use 0 to disable.
        subset: List of column names to consider for null checking. Use empty
            list to check all columns.

    Example:
        **Configuration in JSON:**
        ```
        {
            "how": "any",
            "thresh": 0,
            "subset": ["column1", "column2"]
        }
        ```

        **Configuration in YAML:**
        ```
        how: any
        thresh: 0
        subset:
          - column1
          - column2
        ```
    """

    how: Literal["any", "all"] = Field(..., description="How to determine if row should be dropped")
    thresh: int = Field(..., description="Minimum number of non-null values to keep row (0 to disable)")
    subset: list[str] = Field(..., description="Column names to check for null values (empty list for all columns)")


class DropNaFunctionModel(FunctionModel[DropNaArgs]):
    """Configure drop null values transformations.

    This model defines the complete configuration for a dropna transform operation,
    allowing you to specify how to handle rows with missing values within your
    pipeline definition.

    Attributes:
        function_type: Identifies this transform as a dropna operation. Always
            set to "dropna".
        arguments: Container holding the parameters for dropping null values.

    Example:
        **Configuration in JSON:**
        ```
        {
            "function_type": "dropna",
            "arguments": {
                "how": "any",
                "thresh": 0,
                "subset": ["user_id", "transaction_amount"]
            }
        }
        ```

        **Configuration in YAML:**
        ```
        function_type: dropna
        arguments:
          how: any
          thresh: 0
          subset:
            - user_id
            - transaction_amount
        ```

    Note:
        The dropna operation validates that specified columns in 'subset' exist
        in the input dataset before execution. The behavior differs between
        engines but aims to be consistent across implementations.
    """

    function_type: Literal["dropna"] = "dropna"
    arguments: DropNaArgs = Field(..., description="Container for the drop null values parameters")
