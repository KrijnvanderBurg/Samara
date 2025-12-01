"""Configuration-driven data processing framework.

Define complete data pipelines through JSON configuration rather than code.
This module provides a scalable, modular framework for building data extraction,
transformation, and loading (ETL) pipelines with support for multiple processing
engines and extensible components.

Key capabilities:
    - Define pipelines via configuration (sources, transforms, destinations)
    - Multi-engine architecture (Pandas, Polars, and more)
    - Configurable alert system with multiple notification channels
    - Event-triggered custom actions at pipeline stages
    - Engine-agnostic configuration supporting different backends

Example:
    Define and execute a pipeline from configuration:

    >>> from pathlib import Path
    >>> from samara.workflow.controller import WorkflowController
    >>> controller = WorkflowController.from_config_file(Path("pipeline.json"))
    >>> controller.execute()

See Also:
    - Configuration format documentation in docs/
    - Alert system setup in docs/alert/
    - Available transforms and operations documentation
"""

__author__ = "Krijn van der Burg"
__copyright__ = "Krijn van der Burg"
__credits__ = [""]
__license__ = "Creative Commons BY-NC-ND 4.0 DEED Attribution-NonCommercial-NoDerivs 4.0 International License"
__maintainer__ = "Krijn van der Burg"
__email__ = ""
__status__ = "Prototype"

import uuid
from abc import ABC
from datetime import datetime, timezone

from pydantic import BaseModel as PydanticBaseModel

# Generate a run identifier as early as possible so the entire application
# can reference the same run id. This is created at import time and is
# stable for the lifetime of the process (or test run).
RUN_ID: str = str(uuid.uuid4())
RUN_DATETIME: datetime = datetime.now(timezone.utc)


def get_run_id() -> str:
    """Return the globally generated run identifier.

    The run id is created on module import and is intended to uniquely
    identify a single execution of the application. This can be used in
    logs and traces to correlate data.
    """
    return RUN_ID


def get_run_datetime() -> datetime:
    """Return the globally generated run datetime.

    The run datetime is created on module import and is intended to
    represent the start time of a single execution of the application.
    This can be used in logs and traces to correlate data.
    """
    return RUN_DATETIME


class BaseModel(PydanticBaseModel, ABC):
    """Abstract base class for all configuration models.

    Defines the common interface that all model classes must implement using
    Pydantic v2 for configuration validation and serialization. This base class
    ensures type safety and consistency when converting dictionary-based
    configuration into strongly-typed objects used throughout the framework.

    All configuration models inherit from this class to provide:
        - Configuration validation and error handling
        - Automatic type conversion and coercion
        - Clear error messages for invalid configurations
        - Consistent serialization/deserialization behavior

    Example:
        >>> from samara import BaseModel
        >>> class CustomTransform(BaseModel):
        ...     name: str
        ...     parameters: dict
        >>> config = {"name": "my_transform", "parameters": {"key": "value"}}
        >>> transform = CustomTransform(**config)
        >>> transform.name
        'my_transform'

    Note:
        Subclasses should define their specific configuration schema using
        Pydantic field annotations. All configuration models are immutable
        by default to prevent accidental modifications during pipeline execution.

    See Also:
        pydantic.BaseModel: For configuration validation framework details
    """
