"""Pivot transform configuration models.

This module provides data models for configuring pivot operations in data pipelines.
It enables declarative specification of pivot transformations, converting rows to
columns based on unique values in a pivot column.
"""

from typing import Literal

from pydantic import Field

from samara.runtime.jobs.models.model_transform import ArgsModel, FunctionModel


class PivotArgs(ArgsModel):
    """Container for pivot operation parameters.

    Attributes:
        group_by: List of columns to group by before pivoting.
        pivot_column: Column whose unique values will become new column names.
        values_column: Column containing the values to populate the pivoted columns.
        agg_func: Aggregation function to apply (sum, avg, max, min, count, first).

    Example:
        **Configuration in JSON:**
        ```
        {
            "group_by": ["product_id", "region"],
            "pivot_column": "quarter",
            "values_column": "sales",
            "agg_func": "sum"
        }
        ```

        **Configuration in YAML:**
        ```
        group_by:
          - product_id
          - region
        pivot_column: quarter
        values_column: sales
        agg_func: sum
        ```
    """

    group_by: list[str] = Field(..., description="Columns to group by before pivoting")
    pivot_column: str = Field(..., description="Column to pivot (values become column names)")
    values_column: str = Field(..., description="Column containing values to aggregate")
    agg_func: str = Field(..., description="Aggregation function (sum, avg, max, min, count, first)")


class PivotFunctionModel(FunctionModel[PivotArgs]):
    """Configure pivot transformations.

    This model defines the complete configuration for a pivot transform operation,
    allowing you to convert rows to columns based on unique values in a specified column.

    Attributes:
        function_type: Identifies this transform as a pivot operation. Always set to "pivot".
        arguments: Container holding the pivot parameters.

    Example:
        **Configuration in JSON:**
        ```
        {
            "function_type": "pivot",
            "arguments": {
                "group_by": ["store_id", "product_category"],
                "pivot_column": "month",
                "values_column": "revenue",
                "agg_func": "sum"
            }
        }
        ```

        **Configuration in YAML:**
        ```
        function_type: pivot
        arguments:
          group_by:
            - store_id
            - product_category
          pivot_column: month
          values_column: revenue
          agg_func: sum
        ```

    Note:
        The pivot operation will create new columns for each unique value found in
        the pivot_column. Ensure the number of unique values is reasonable to avoid
        creating too many columns.
    """

    function_type: Literal["pivot"] = "pivot"
    arguments: PivotArgs = Field(..., description="Container for the pivot parameters")
