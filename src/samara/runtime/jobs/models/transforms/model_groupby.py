"""GroupBy aggregation transform configuration models.

This module provides data models for configuring group-by and aggregation operations
in data pipelines. It enables declarative specification of grouping columns and
aggregate functions, allowing users to perform data summarization through
configuration rather than code.
"""

from typing import Literal

from pydantic import Field

from samara import BaseModel
from samara.runtime.jobs.models.model_transform import ArgsModel, FunctionModel


class AggregateFunction(BaseModel):
    """Specify an aggregate function to apply to grouped data.

    All fields must be explicitly provided in configuration. For count operations,
    input_column must be explicitly set to null.

    Attributes:
        function: The aggregate function name to apply. Supported functions include
            sum, avg, mean (alias for avg), min, max, count, first, last, stddev,
            and variance.
        input_column: Name of the column to aggregate. Must be explicitly null for
            count function, required for all other functions.
        output_column: Name for the resulting aggregated column in the output.

    Example:
        **Configuration in JSON:**
        ```
        {
            "aggregations": [
                {
                    "function": "sum",
                    "input_column": "sales_amount",
                    "output_column": "total_sales"
                }
            ]
        }
        ```

        **Configuration in JSON (count function):**
        ```
        {
            "aggregations": [
                {
                    "function": "count",
                    "input_column": null,
                    "output_column": "record_count"
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        aggregations:
          - function: avg
            input_column: rating
            output_column: avg_rating
        ```

        **Configuration in YAML (count function):**
        ```
        aggregations:
          - function: count
            input_column: null
            output_column: total_records
        ```
    """

    function: Literal["sum", "avg", "mean", "min", "max", "count", "first", "last", "stddev", "variance"] = Field(
        ..., description="Aggregate function to apply"
    )
    input_column: str | None = Field(
        ..., description="Column to aggregate (must be null for count, required for others)"
    )
    output_column: str = Field(..., description="Name for the aggregated result column", min_length=1)


class GroupByArgs(ArgsModel):
    """Container for group-by and aggregation operation parameters.

    All fields must be explicitly provided in configuration.

    Attributes:
        group_columns: List of column names to group by. Cannot be empty.
        aggregations: List of aggregate functions to apply to the grouped data.
            Cannot be empty.

    Example:
        **Configuration in JSON:**
        ```
        {
            "arguments": {
                "group_columns": ["department", "location"],
                "aggregations": [
                    {
                        "function": "sum",
                        "input_column": "sales_amount",
                        "output_column": "total_sales"
                    },
                    {
                        "function": "avg",
                        "input_column": "discount_percent",
                        "output_column": "avg_discount"
                    },
                    {
                        "function": "count",
                        "input_column": null,
                        "output_column": "transaction_count"
                    }
                ]
            }
        }
        ```

        **Configuration in YAML:**
        ```
        arguments:
          group_columns:
            - department
            - location
          aggregations:
            - function: sum
              input_column: sales_amount
              output_column: total_sales
            - function: avg
              input_column: discount_percent
              output_column: avg_discount
            - function: count
              input_column: null
              output_column: transaction_count
        ```
    """

    group_columns: list[str] = Field(..., description="List of column names to group by", min_length=1)
    aggregations: list[AggregateFunction] = Field(..., description="List of aggregate functions to apply", min_length=1)


class GroupByFunctionModel(FunctionModel[GroupByArgs]):
    """Configure group-by and aggregation transformations.

    This model defines the complete configuration for a groupby transform operation,
    allowing you to specify which columns to group by and which aggregate functions
    to apply within your pipeline definition. All fields must be explicitly provided.

    Attributes:
        function_type: Identifies this transform as a groupby operation. Always
            set to "groupby".
        arguments: Container holding the grouping columns and aggregate functions.

    Example:
        **Configuration in JSON:**
        ```
        "functions": [
            {
                "function_type": "groupby",
                "arguments": {
                    "group_columns": ["region", "product_category"],
                    "aggregations": [
                        {
                            "function": "sum",
                            "input_column": "revenue",
                            "output_column": "total_revenue"
                        },
                        {
                            "function": "max",
                            "input_column": "sale_date",
                            "output_column": "latest_sale"
                        },
                        {
                            "function": "count",
                            "input_column": null,
                            "output_column": "sale_count"
                        }
                    ]
                }
            }
        ]
        ```

        **Configuration in YAML:**
        ```
        functions:
          - function_type: groupby
            arguments:
                group_columns:
                    - region
                    - product_category
                aggregations:
                    - function: sum
                    input_column: revenue
                    output_column: total_revenue
                    - function: max
                    input_column: sale_date
                    output_column: latest_sale
                    - function: count
                    input_column: null
                    output_column: sale_count
        ```
    """

    function_type: Literal["groupby"]
    arguments: GroupByArgs
