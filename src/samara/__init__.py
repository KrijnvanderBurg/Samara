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
    >>> from samara.runtime.controller import RuntimeController
    >>> controller = RuntimeController.from_config_file(Path("pipeline.json"))
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


from abc import ABC

from pydantic import BaseModel as PydanticBaseModel

from samara.utils.logger import get_logger

logger = get_logger(__name__)


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
