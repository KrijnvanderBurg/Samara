"""Base class for Spark transformation functions.

This module provides the base class for all Spark-specific transformation functions,
enabling shared access to the DataFrame registry across transformation operations.
"""

from typing import ClassVar

from samara.types import DataFrameRegistry
from samara.workflow.jobs.models.model_transform import ArgsT, FunctionModel


class FunctionSpark(FunctionModel[ArgsT]):
    """Extend transformation functions with Spark-specific capabilities.

    This class extends FunctionModel with Spark-specific functionality, including
    access to the shared DataFrame registry for operations that need to reference
    other DataFrames (such as joins across multiple upstream sources). Used with
    multiple inheritance alongside concrete FunctionModel subclasses to provide
    registry access throughout the transformation execution.

    Attributes:
        data_registry: Shared class-level registry for accessing processed
            DataFrames by their identifier within the pipeline execution context.

    Note:
        The data_registry is a class-level attribute shared across all instances
        within a pipeline execution, enabling cross-reference between DataFrames
        created by different transformation steps. This is essential for operations
        that operate on multiple DataFrames such as joins and unions.
    """

    data_registry: ClassVar[DataFrameRegistry] = DataFrameRegistry()
