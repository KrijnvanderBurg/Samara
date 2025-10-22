"""Join transform configuration models.

This module provides data models for configuring join operations
in the configuration-driven pipeline, enabling users to combine
datasets from different sources through declarative configuration.
"""

import logging
from typing import Literal

from pydantic import Field

from samara.runtime.jobs.models.model_transform import ArgsModel, FunctionModel
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class JoinArgs(ArgsModel):
    """Container for join transform parameters.

    Specifies how to join two dataframes, including the upstream identifier
    of the dataframe to join with, the columns to join on, and the join type.

    Attributes:
        other_upstream_id: Identifier of the dataframe to join with the current
            dataframe. Must reference an upstream transform or extract.
        on: Column(s) to join on. Provide a string for a single column or a list
            of strings for multiple columns. Columns must exist in both dataframes.
        how: Type of join to perform (inner, outer, left, right, cross, etc.).
            Defaults to "inner".

    Example:
        **Configuration in JSON:**
        ```
        {
            "function": "join",
            "arguments": {
                "other_upstream_id": "orders",
                "on": ["customer_id"],
                "how": "left"
            }
        }
        ```

        **Configuration in YAML:**
        ```
        function: join
        arguments:
          other_upstream_id: orders
          on:
            - customer_id
          how: left
        ```

    Note:
        The join operation requires both dataframes to be available from
        upstream transforms or extracts. Column names are case-sensitive.
    """

    other_upstream_id: str = Field(
        ..., description="Identifier of the dataframe to join with the current dataframe", min_length=1
    )
    on: str | list[str] = Field(
        ...,
        description=(
            "Column(s) to join on. Can be a string for a single column or a list of strings for multiple columns"
        ),
    )
    how: str = Field(default="inner", description="Type of join to perform (inner, outer, left, right, etc.)")


class JoinFunctionModel(FunctionModel[JoinArgs]):
    """Configuration model for join transform operations.

    Defines the structure for configuring a join transformation within
    a pipeline, specifying the dataframes to combine and how to combine them.
    This is part of the transform chain that enables users to merge datasets
    declaratively through configuration.

    Attributes:
        function_type: The transform operation name (always "join").
        arguments: Container for the join parameters as defined in JoinArgs.

    Example:
        **Configuration in JSON:**
        ```
        {
            "function": "join",
            "arguments": {
                "other_upstream_id": "customers",
                "on": "customer_id",
                "how": "inner"
            }
        }
        ```

        **Configuration in YAML:**
        ```
        function: join
        arguments:
          other_upstream_id: customers
          on: customer_id
          how: inner
        ```

    Note:
        The join operation is part of the transform chain. Ensure all referenced
        upstream dataframe identifiers exist and columns to join on are compatible.
    """

    function_type: Literal["join"] = "join"
    arguments: JoinArgs = Field(..., description="Container for the join parameters")
