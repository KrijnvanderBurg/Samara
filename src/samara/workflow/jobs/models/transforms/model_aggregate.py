"""Aggregation transform configuration models.

This module provides data models for configuring aggregation operations
in data pipelines. It enables declarative specification of group-by operations
with various aggregate functions, allowing users to perform data summarization
through configuration rather than code.
"""

from typing import Literal

from pydantic import Field

from samara import BaseModel
from samara.workflow.jobs.models.model_transform import ArgsModel, FunctionModel


class AggregateColumn(BaseModel):
    """Specify a column and its aggregation function.

    Attributes:
        column_name: Name of the column to aggregate. Must be a valid column present
            in the input dataset.
        function: Aggregation function to apply (sum, avg, min, max, count, etc.)
        alias: Output column name for the aggregated result.

    Example:
        **Configuration in JSON:**
        ```
        {
            "column_name": "amount",
            "function": "sum",
            "alias": "total_amount"
        }
        ```

        **Configuration in YAML:**
        ```
        column_name: amount
        function: sum
        alias: total_amount
        ```
    """

    column_name: str = Field(..., description="Name of the column to aggregate", min_length=1)
    function: str = Field(..., description="Aggregation function to apply", min_length=1)
    alias: str = Field(..., description="Output column name for aggregated result", min_length=1)


class AggregateArgs(ArgsModel):
    """Container for aggregation operation parameters.

    Attributes:
        group_by_columns: List of column names to group by. Use null for no grouping.
        aggregate_columns: List of columns with their aggregation functions.

    Example:
        **Configuration in JSON:**
        ```
        {
            "group_by_columns": ["department", "region"],
            "aggregate_columns": [
                {
                    "column_name": "salary",
                    "function": "avg",
                    "alias": "avg_salary"
                },
                {
                    "column_name": "employee_id",
                    "function": "count",
                    "alias": "employee_count"
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        group_by_columns:
          - department
          - region
        aggregate_columns:
          - column_name: salary
            function: avg
            alias: avg_salary
          - column_name: employee_id
            function: count
            alias: employee_count
        ```
    """

    group_by_columns: list[str] | None = Field(..., description="Columns to group by, or null for no grouping")
    aggregate_columns: list[AggregateColumn] = Field(..., description="List of aggregation definitions")


class AggregateFunctionModel(FunctionModel[AggregateArgs]):
    """Configure data aggregation transformations.

    This model defines the complete configuration for an aggregate transform operation,
    allowing you to specify group-by columns and aggregation functions to apply
    within your pipeline definition.

    Attributes:
        function_type: Identifies this transform as an aggregate operation. Always
            set to "aggregate".
        arguments: Container holding the group-by columns and aggregation definitions.

    Example:
        **Configuration in JSON:**
        ```
        "functions": [
            {
                "function_type": "aggregate",
                "arguments": {
                    "group_by_columns": ["product_category"],
                    "aggregate_columns": [
                        {
                            "column_name": "quantity",
                            "function": "sum",
                            "alias": "total_quantity"
                        },
                        {
                            "column_name": "price",
                            "function": "avg",
                            "alias": "average_price"
                        }
                    ]
                }
            }
        ]
        ```

        **Configuration in YAML:**
        ```
        functions:
          - function_type: aggregate
            arguments:
              group_by_columns:
                  - product_category
              aggregate_columns:
                - column_name: quantity
                  function: sum
                  alias: total_quantity
                - column_name: price
                  function: avg
                  alias: average_price
        ```

    Note:
        The aggregate operation supports standard SQL aggregation functions.
        Available functions depend on your configured processing engine.
    """

    function_type: Literal["aggregate"] = "aggregate"
    arguments: AggregateArgs = Field(..., description="Container for the aggregation parameters")
