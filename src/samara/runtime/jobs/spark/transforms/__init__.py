"""Spark transform operations - Configurable data transformation functions.

This module provides all available transform operations for Spark-based data
processing pipelines. It enables automatic registration of transform functions
through a union discriminator pattern, allowing pipeline configurations to
specify transformations declaratively without requiring code changes.
"""

from typing import Annotated

from pydantic import Discriminator

from .aggregate import AggregateFunction
from .cast import CastFunction
from .drop import DropFunction
from .dropduplicates import DropDuplicatesFunction
from .filter import FilterFunction
from .groupby import GroupByFunction
from .join import JoinFunction
from .select import SelectFunction
from .withcolumn import WithColumnFunction

__all__ = [
    "AggregateFunction",
    "CastFunction",
    "DropFunction",
    "DropDuplicatesFunction",
    "FilterFunction",
    "GroupByFunction",
    "JoinFunction",
    "SelectFunction",
    "WithColumnFunction",
]

transform_function_spark_union = Annotated[
    AggregateFunction
    | CastFunction
    | DropFunction
    | DropDuplicatesFunction
    | FilterFunction
    | GroupByFunction
    | JoinFunction
    | SelectFunction
    | WithColumnFunction,
    Discriminator("function_type"),
]
