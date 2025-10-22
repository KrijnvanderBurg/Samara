"""Transformation models - Define and structure transformation operations.

This module provides type-safe models for configuring transformation operations
within data pipelines. It focuses on the configuration-driven approach,
enabling pipeline authors to define transformation functions and their
parameters through structured configuration rather than code."""

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Generic, TypeVar

from pydantic import Field

from samara import BaseModel
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class ArgsModel(BaseModel, ABC):
    """Define arguments for transformation functions.

    Abstract base class that serves as the foundation for all argument
    containers used by transformation functions. Each concrete subclass
    should implement type-specific argument handling for different
    transformation operations.

    All transformation argument models should inherit from this class to
    ensure consistent interface and validation throughout the framework.

    See Also:
        FunctionModel: Uses ArgsModel subclasses to configure transformations.
    """


ArgsT = TypeVar("ArgsT", bound=ArgsModel)
FunctionNameT = TypeVar("FunctionNameT", bound=str)


class FunctionModel(BaseModel, Generic[ArgsT], ABC):
    """Specify a transformation function with its configuration.

    Represents the configuration for a single transformation function,
    including its arguments. This model bridges configuration files and
    transformation execution by validating and structuring function arguments.

    Attributes:
        arguments: Arguments model specific to the transformation function.
            Must be a subclass of ArgsModel with fields matching the function's
            parameters.

    Example:
        >>> # Concrete implementation for a "cast" transformation
        >>> class CastArgs(ArgsModel):
        ...     columns: dict[str, str] = {"age": "StringType"}
        >>> class CastFunctionModel(FunctionModel[CastArgs]):
        ...     def transform(self) -> Callable:
        ...         return self._create_cast_function()

    See Also:
        ArgsModel: Base class for all transformation arguments.
        TransformModel: Chains multiple FunctionModel instances together.

    Note:
        Concrete implementations must define the transform() method
        to create the actual callable that processes data.
    """

    arguments: ArgsT

    @abstractmethod
    def transform(self) -> Callable:
        """Create a callable transformation function based on the model.

        This method should implement the logic to create a function that
        can be called to transform data according to the model configuration.

        Returns:
            A callable function that applies the transformation to data.
        """


FunctionModelT = TypeVar("FunctionModelT", bound=FunctionModel)


class TransformModel(BaseModel, Generic[FunctionModelT], ABC):
    """Configure a transformation operation within a pipeline.

    This model configures transformation operations for data processing,
    including the operation identifier, upstream data source, and a sequence
    of transformation functions to apply. It enables pipeline authors to
    define complex transformation chains through configuration.

    Attributes:
        id_: Identifier for this transformation operation (minimum 1 character).
            Used to reference this transform in other pipeline components.
        upstream_id: Identifier of the upstream component providing input data
            (minimum 1 character). References a data source or previous transform.
        functions: List of transformation functions to apply in sequence.
            Each function receives the output of the previous function.

    Example:
        >>> transform_config = {
        ...     "transforms": [
        ...         {
        ...             "id": "transform_1",
        ...             "upstream_id": "source_data",
        ...             "functions": [
        ...                 {
        ...                     "function_type": "cast",
        ...                     "arguments": {"columns": {"age": "StringType"}}
        ...                 }
        ...             ]
        ...         }
        ...     ]
        ... }

        **Configuration in JSON:**
        ```
            {
                "transforms": [
                    {
                        "id": "transform_1",
                        "upstream_id": "source_data",
                        "functions": [
                            {
                                "function_type": "cast",
                                "arguments": {
                                    "columns": {"age": "StringType"}
                                }
                            }
                        ]
                    }
                ]
            }
        ```

        **Configuration in YAML:**
        ```
            transforms:
              - id: transform_1
                upstream_id: source_data
                functions:
                  - function_type: cast
                    arguments:
                      columns:
                        age: StringType
        ```

    Note:
        Functions are applied in the order specified. Each function's output
        becomes the input to the next function in the sequence.
    """

    id_: str = Field(..., alias="id", description="Identifier for this transformation operation", min_length=1)
    upstream_id: str = Field(..., description="Identifier(s) of the upstream component(s) providing data", min_length=1)
    functions: list[FunctionModelT] = Field(..., description="List of transformation functions to apply")
