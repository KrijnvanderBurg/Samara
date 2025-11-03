"""Application settings - Centralized configuration management.

This module provides a singleton-based settings management system using Pydantic
BaseSettings. Settings can be loaded from environment variables and accessed
globally throughout the application with automatic caching and validation.
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """Application-wide settings loaded from environment variables.

    Provides centralized configuration management for the Samara framework.
    Settings are automatically loaded from environment variables with the
    SAMARA_ prefix and cached for efficient access throughout the application.

    Attributes:
        log_level: Logging level for the application (DEBUG, INFO, WARNING,
            ERROR, CRITICAL). Defaults to INFO if not specified via
            SAMARA_LOG_LEVEL environment variable.

    Example:
        Access settings anywhere in the application:

        >>> from samara.settings import get_settings
        >>> settings = get_settings()
        >>> settings.log_level
        'INFO'

        Override via environment variable:

        >>> import os
        >>> os.environ['SAMARA_LOG_LEVEL'] = 'DEBUG'
        >>> settings = get_settings()
        >>> settings.log_level
        'DEBUG'

    Note:
        Settings are cached using lru_cache, so the singleton instance is
        created once and reused. To reload settings after environment changes,
        call `get_settings.cache_clear()` first.
    """

    model_config = SettingsConfigDict(
        case_sensitive=False,
    )

    log_level: str = Field(default="INFO", description="Logging level for the application")


@lru_cache
def get_settings() -> AppSettings:
    """Retrieve the singleton application settings instance.

    Returns a cached AppSettings instance, creating it on first call and
    reusing the same instance for subsequent calls. This ensures consistent
    configuration access throughout the application lifecycle.

    Returns:
        The singleton AppSettings instance with all configuration loaded
        from environment variables and validated.

    Example:
        >>> from samara.settings import get_settings
        >>> settings = get_settings()
        >>> print(settings.log_level)
        INFO

        Clear cache to reload settings after environment changes:

        >>> get_settings.cache_clear()
        >>> settings = get_settings()  # Reloads from environment

    Note:
        The lru_cache decorator with no maxsize creates a singleton pattern,
        ensuring only one settings instance exists during the application
        lifecycle. This is thread-safe and efficient.
    """
    return AppSettings()
