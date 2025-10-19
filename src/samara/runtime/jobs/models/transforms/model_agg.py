"""Configuration model for the agg (aggregation) transform function.

This module defines the data models used to configure aggregation
transformations in the ingestion framework. It includes:

- AggFunctionModel: Main configuration model for agg operations
- AggArgs: Container for the aggregation parameters
- AggregationColumn: Configuration for a single aggregation operation

These models provide a type-safe interface for configuring aggregations
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
        description="Aggregation function to apply (sum, avg, count, min, max, first, last, collect_list, collect_set, etc.)",
        min_length=1,
    )
    alias: str = Field(
        ...,
        description="Optional alias for the aggregated column (defaults to {column_name}_{agg_function} if empty)",
    )


class AggArgs(ArgsModel):
    """Arguments for agg (aggregation) transform operations.

    Attributes:
        aggregations: List of aggregation operations to apply on the entire DataFrame
    """

    aggregations: list[AggregationColumn] = Field(
        ..., description="List of aggregation operations to apply on the entire DataFrame", min_length=1
    )


class AggFunctionModel(FunctionModel[AggArgs]):
    """Configuration model for agg (aggregation) transform operations.

    This model defines the structure for configuring an aggregation
    transformation without grouping, applying aggregation functions
    across the entire DataFrame.

    Attributes:
        function_type: The name of the function to be used (always "agg")
        arguments: Container for the aggregation parameters
    """

    function_type: Literal["agg"] = "agg"
    arguments: AggArgs = Field(..., description="Container for the aggregation parameters")
