"""Configuration model for the groupBy transform function.

This module defines the data models used to configure groupBy
transformations in the ingestion framework. It includes:

- GroupByFunctionModel: Main configuration model for groupBy operations
- GroupByArgs: Container for the grouping and aggregation parameters
- AggregationColumn: Configuration for a single aggregation operation

These models provide a type-safe interface for configuring groupBy operations
from configuration files or dictionaries.
"""

from typing import Literal

from pydantic import Field

from samara import BaseModel
from samara.runtime.jobs.models.model_transform import ArgsModel, FunctionModel


class AggregationColumn(BaseModel):
    """Configuration for a single aggregation operation.

    Attributes:
        column_name: Name of the column to aggregate
        agg_function: Aggregation function to apply (sum, avg, count, min, max, etc.)
        alias: Optional alias for the aggregated column (defaults to {column_name}_{agg_function} if empty)
    """

    column_name: str = Field(..., description="Name of the column to aggregate", min_length=1)
    agg_function: str = Field(
        ...,
        description=("Aggregation function (sum, avg, count, min, max, first, last, etc.)"),
        min_length=1,
    )
    alias: str = Field(
        ...,
        description="Optional alias for the aggregated column (defaults to {column_name}_{agg_function} if empty)",
    )


class GroupByArgs(ArgsModel):
    """Arguments for groupBy transform operations.

    Attributes:
        columns: List of column names to group by
        aggregations: List of aggregation operations to apply on grouped data
    """

    columns: list[str] = Field(..., description="List of column names to group by", min_length=1)
    aggregations: list[AggregationColumn] = Field(
        ..., description="List of aggregation operations to apply on grouped data", min_length=1
    )


class GroupByFunctionModel(FunctionModel[GroupByArgs]):
    """Configuration model for groupBy transform operations.

    This model defines the structure for configuring a groupBy
    transformation, specifying which columns to use for grouping
    and what aggregation operations to apply.

    Attributes:
        function_type: The name of the function to be used (always "groupby")
        arguments: Container for the groupBy and aggregation parameters
    """

    function_type: Literal["groupby"] = "groupby"
    arguments: GroupByArgs = Field(..., description="Container for the groupBy and aggregation parameters")
