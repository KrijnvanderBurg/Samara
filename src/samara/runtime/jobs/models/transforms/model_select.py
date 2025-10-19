"""Configuration model for the column selection transform function.

This module defines the data models used to configure column selection
transformations in the ingestion framework. It includes:

- SelectFunctionModel: Main configuration model for select operations
- SelectArgs: Container for the selection parameters

These models provide a type-safe interface for configuring column selections
from configuration files or dictionaries.
"""

import logging
from typing import Literal

from pydantic import Field

from samara.runtime.jobs.models.model_transform import ArgsModel, FunctionModel
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class SelectArgs(ArgsModel):
    """Arguments for column selection transform operations.

    Attributes:
        columns: List of column names to select from the DataFrame
    """

    columns: list[str] = Field(..., description="List of column names to select from the DataFrame", min_length=1)


class SelectFunctionModel(FunctionModel[SelectArgs]):
    """Configuration model for column selection transform operations.

    This model defines the structure for configuring a column selection
    transformation, specifying which columns should be included in the output.

    Attributes:
        function_type: The name of the function to be used (always "select")
        arguments: Container for the column selection parameters
    """

    function_type: Literal["select"] = "select"
    arguments: SelectArgs = Field(..., description="Container for the column selection parameters")
