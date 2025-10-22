"""Define withColumn transform configurations.

This module provides configuration models enabling pipeline authors to add,
replace, or transform columns in data processing pipelines through
configuration-driven specifications.
"""

from typing import Literal

from pydantic import Field

from samara.runtime.jobs.models.model_transform import ArgsModel, FunctionModel


class WithColumnArgs(ArgsModel):
    """Specify the column name and expression for a withColumn operation.

    Attributes:
        col_name: Name of the column to add or replace. Used as the target
            column identifier in the DataFrame.
        col_expr: Column expression representing the value. Accepts SQL-like
            expressions that will be evaluated during transformation.

    Example:
        **Configuration in JSON:**
        ```
        {
            "function": "withColumn",
            "arguments": {
                "colName": "customer_id",
                "colExpr": "CAST(id AS STRING)"
            }
        }
        ```

        **Configuration in YAML:**
        ```
        function: withColumn
        arguments:
          colName: customer_id
          colExpr: CAST(id AS STRING)
        ```
    """

    col_name: str = Field(..., description="Name of the column to add or replace", min_length=1)
    col_expr: str = Field(..., description="Column expression representing the value", min_length=1)


class WithColumnFunctionModel(FunctionModel[WithColumnArgs]):
    """Configure a withColumn transformation operation.

    This model defines the structure for specifying a column addition or
    replacement transformation within a pipeline. It combines the function
    type identifier with the withColumn-specific arguments.

    Attributes:
        function_type: The transform function identifier (always "withColumn").
        arguments: Parameters controlling the column operation, including the
            target column name and the expression to apply.

    Example:
        **Configuration in JSON:**
        ```
        {
            "function": "withColumn",
            "arguments": {
                "colName": "total_amount",
                "colExpr": "quantity * unit_price"
            }
        }
        ```

        **Configuration in YAML:**
        ```
        function: withColumn
        arguments:
          colName: total_amount
          colExpr: quantity * unit_price
        ```

    Note:
        The withColumn operation adds a new column if it doesn't exist or
        replaces the column if it already exists in the DataFrame.
    """

    function_type: Literal["withColumn"] = "withColumn"
    arguments: WithColumnArgs = Field(..., description="Container for the withColumn parameters")
