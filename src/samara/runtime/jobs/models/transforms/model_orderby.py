"""Configuration model for the orderBy (sorting) transform function.

This module defines the data models used to configure sorting
transformations in the ingestion framework. It includes:

- OrderByFunctionModel: Main configuration model for orderBy operations
- OrderByArgs: Container for the sorting parameters
- OrderByColumn: Configuration for a single column sort specification

These models provide a type-safe interface for configuring sorting operations
from configuration files or dictionaries.
"""

from typing import Literal

from pydantic import Field

from samara import BaseModel
from samara.runtime.jobs.models.model_transform import ArgsModel, FunctionModel


class OrderByColumn(BaseModel):
    """Configuration for a single column sort specification.

    Attributes:
        column_name: Name of the column to sort by
        order: Sort order - "asc" for ascending or "desc" for descending (default: "asc")
    """

    column_name: str = Field(..., description="Name of the column to sort by", min_length=1)
    order: Literal["asc", "desc"] = Field(
        ...,
        description='Sort order - "asc" for ascending or "desc" for descending',
    )


class OrderByArgs(ArgsModel):
    """Arguments for orderBy (sorting) transform operations.

    Attributes:
        columns: List of columns to sort by, in order of priority
    """

    columns: list[OrderByColumn] = Field(
        ..., description="List of columns to sort by, in order of priority", min_length=1
    )


class OrderByFunctionModel(FunctionModel[OrderByArgs]):
    """Configuration model for orderBy (sorting) transform operations.

    This model defines the structure for configuring a sorting
    transformation, specifying which columns to sort by and in what order.

    Attributes:
        function_type: The name of the function to be used (always "orderby")
        arguments: Container for the sorting parameters
    """

    function_type: Literal["orderby"] = "orderby"
    arguments: OrderByArgs = Field(..., description="Container for the sorting parameters")
