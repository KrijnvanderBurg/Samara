"""Filter transform - Configuration models for row filtering operations.

This module provides data models for configuring filter/where transformations,
enabling users to apply conditional row filtering in data pipelines.
"""

from typing import Literal

from pydantic import Field

from samara.workflow.jobs.models.model_transform import ArgsModel, FunctionModel


class FilterArgs(ArgsModel):
    """Container for filter transform parameters.

    Holds the filtering parameters for a filter/where transformation,
    enabling users to specify conditions for row-level filtering operations.

    Attributes:
        condition: SQL-like expression used to filter rows. The condition
            should evaluate to a boolean value for each row (e.g., "age > 18",
            "department == 'Sales'").

    Example:
        **Configuration in JSON:**
        ```
        {
            "transforms": [
                {
                    "function": "filter",
                    "arguments": {
                        "condition": "age > 18"
                    }
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        transforms:
          - function: filter
            arguments:
              condition: age > 18
        ```
    """

    condition: str = Field(..., description="String expression representing the filter condition", min_length=1)


class FilterFunctionModel(FunctionModel[FilterArgs]):
    """Configuration model for filter/where transform operations.

    Defines the structure for configuring a filter transformation that applies
    conditional row filtering to data, enabling users to keep only rows matching
    specified criteria in their pipelines.

    Attributes:
        function_type: The transform function identifier (always "filter").
        arguments: Filter parameters including the condition expression.

    Example:
        **Configuration in JSON:**
        ```
        {
            "transforms": [
                {
                    "function": "filter",
                    "arguments": {
                        "condition": "status == 'active'"
                    }
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        transforms:
          - function: filter
            arguments:
              condition: status == 'active'
        ```

    Note:
        The condition expression syntax may vary depending on the execution
        engine (Pandas vs Polars). Ensure expressions are compatible with
        your target engine.
    """

    function_type: Literal["filter"] = "filter"
    arguments: FilterArgs = Field(..., description="Container for the filter parameters")
