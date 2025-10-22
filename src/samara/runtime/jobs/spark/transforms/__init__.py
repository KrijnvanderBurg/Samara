"""Spark transform operations - Configurable data transformation functions.

This module provides all available transform operations for Spark-based data
processing pipelines. It enables automatic registration of transform functions
through a union discriminator pattern, allowing pipeline configurations to
specify transformations declaratively without requiring code changes.
"""

from typing import Annotated

from pydantic import Discriminator

from .cast import CastFunction
from .drop import DropFunction
from .dropduplicates import DropDuplicatesFunction
from .filter import FilterFunction
from .join import JoinFunction
from .select import SelectFunction
from .withcolumn import WithColumnFunction

__all__ = [
    "CastFunction",
    "DropFunction",
    "DropDuplicatesFunction",
    "FilterFunction",
    "JoinFunction",
    "SelectFunction",
    "WithColumnFunction",
]

transform_function_spark_union = Annotated[
    CastFunction
    | DropFunction
    | DropDuplicatesFunction
    | FilterFunction
    | JoinFunction
    | SelectFunction
    | WithColumnFunction,
    Discriminator("function_type"),
]
