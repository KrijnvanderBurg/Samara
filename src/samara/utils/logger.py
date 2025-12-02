"""Logging utilities for the Samara framework.

Uses Python's standard logging which integrates natively with OpenTelemetry.
When OTLP logs endpoint is configured, logs are automatically exported with
full structured attributes for querying in Loki/Grafana.
"""

import logging


def set_logger(level: str = "INFO") -> None:
    """Configure Python's standard logging system.

    Sets up logging format and level for the entire application. Should be called
    once at application startup before any logging occurs.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
            Defaults to INFO if not specified.

    Example:
        >>> from samara.utils.logger import set_logger, get_logger
        >>> set_logger(level="DEBUG")
        >>> logger = get_logger(__name__)
        >>> logger.debug("Debug message")
    """
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=level,
        force=True,  # Reconfigure even if already configured
    )


def get_logger(name: str) -> logging.Logger:
    """Return a standard Python logger instance by name.

    Args:
        name: Logger name, typically the module name.

    Returns:
        A standard Python Logger instance. When OTLP logs endpoint is configured,
        logs will be automatically exported to the configured backend.
    """
    return logging.getLogger(name)
