"""Transform function implementations for Spark.

This module imports all available transform functions to register them with the
TransformFunctionRegistry. Each transform function is automatically registered
when imported.
"""

from typing import Annotated

from pydantic import Discriminator

from .agg import AggFunction
from .cast import CastFunction
from .drop import DropFunction
from .dropduplicates import DropDuplicatesFunction
from .filter import FilterFunction
from .groupby import GroupByFunction
from .join import JoinFunction
from .orderby import OrderByFunction
from .select import SelectFunction
from .withcolumn import WithColumnFunction

__all__ = [
    "AggFunction",
    "CastFunction",
    "DropFunction",
    "DropDuplicatesFunction",
    "FilterFunction",
    "GroupByFunction",
    "JoinFunction",
    "OrderByFunction",
    "SelectFunction",
    "WithColumnFunction",
]

transform_function_spark_union = Annotated[
    AggFunction
    | CastFunction
    | DropFunction
    | DropDuplicatesFunction
    | FilterFunction
    | GroupByFunction
    | JoinFunction
    | OrderByFunction
    | SelectFunction
    | WithColumnFunction,
    Discriminator("function_type"),
]
