"""Order by transform configuration models.

This module provides data models for configuring row ordering operations
in data pipelines. It enables declarative specification of which columns
to sort by and their sort direction, allowing users to order data through
configuration rather than code.
"""

from typing import Literal

from pydantic import Field

from samara import BaseModel
from samara.runtime.jobs.models.model_transform import ArgsModel, FunctionModel


class OrderByColumn(BaseModel):
    """Specify a column and its sort direction for ordering.

    Attributes:
        column_name: Name of the column to sort by. Must be a valid column present
            in the input dataset.
        ascending: Sort direction for the column. True for ascending order,
            False for descending order.

    Example:
        **Configuration in JSON:**
        ```
        {
            "column_name": "timestamp",
            "ascending": false
        }
        ```

        **Configuration in YAML:**
        ```
        column_name: timestamp
        ascending: false
        ```
    """

    column_name: str = Field(..., description="Name of the column to sort by", min_length=1)
    ascending: bool = Field(..., description="Sort direction: true for ascending, false for descending")


class OrderByArgs(ArgsModel):
    """Container for order by operation parameters.

    Attributes:
        columns: List of column definitions specifying which columns to sort by
            and their sort directions. Columns are sorted in the order specified.

    Example:
        **Configuration in JSON:**
        ```
        {
            "columns": [
                {
                    "column_name": "department",
                    "ascending": true
                },
                {
                    "column_name": "salary",
                    "ascending": false
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        columns:
          - column_name: department
            ascending: true
          - column_name: salary
            ascending: false
        ```
    """

    columns: list[OrderByColumn] = Field(..., description="List of column ordering definitions", min_length=1)


class OrderByFunctionModel(FunctionModel[OrderByArgs]):
    """Configure row ordering transformations.

    This model defines the complete configuration for an order by transform operation,
    allowing you to specify which columns to sort by and their sort directions
    within your pipeline definition.

    Attributes:
        function_type: Identifies this transform as an orderby operation. Always
            set to "orderby".
        arguments: Container holding the list of columns and their sort directions.

    Example:
        **Configuration in JSON:**
        ```
        {
            "function_type": "orderby",
            "arguments": {
                "columns": [
                    {
                        "column_name": "created_date",
                        "ascending": false
                    },
                    {
                        "column_name": "priority",
                        "ascending": true
                    }
                ]
            }
        }
        ```

        **Configuration in YAML:**
        ```
        function_type: orderby
        arguments:
          columns:
            - column_name: created_date
              ascending: false
            - column_name: priority
              ascending: true
        ```

    Note:
        The orderby operation validates that target columns exist in the input
        dataset before execution. Multi-column sorting is applied in the order
        columns are specified in the configuration.
    """

    function_type: Literal["orderby"] = "orderby"
    arguments: OrderByArgs = Field(..., description="Container for the column ordering parameters")
