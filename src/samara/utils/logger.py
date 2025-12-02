"""Logging utilities for the Samara framework.

This module provides a centralized logging configuration for the Samara framework,
building on Python's standard logging library. It integrates seamlessly with
OpenTelemetry for distributed tracing and observability.

When an OTLP (OpenTelemetry Protocol) logs endpoint is configured via environment
variables or settings, all logs are automatically exported with full structured
attributes, enabling powerful querying and analysis in backends like Loki/Grafana,
Datadog, or any OpenTelemetry-compatible observability platform.

Key Features:
    - Centralized logging configuration with consistent formatting
    - Automatic OpenTelemetry integration for distributed tracing
    - Structured logging attributes for enhanced observability
    - Standard Python logging interface for familiarity

Typical Usage:
    >>> from samara.utils.logger import set_logger, get_logger
    >>> set_logger(level="INFO")
    >>> logger = get_logger(__name__)
    >>> logger.info("Application started", extra={"version": "1.0.0"})
"""

import logging


def set_logger(level: str = "INFO") -> None:
    """Configure Python's standard logging system for the application.

    This function initializes the root logger with a consistent format and logging
    level that applies to all loggers in the application. It should be called once
    at application startup, before any logging operations are performed.

    The logging format includes timestamp, logger name, log level, and message,
    providing essential context for debugging and monitoring. The configuration
    uses `force=True` to allow reconfiguration even if logging was previously
    initialized, which is useful for testing or dynamic configuration changes.

    Args:
        level: The logging level threshold as a string. Valid values are:
            - "DEBUG": Detailed diagnostic information (most verbose)
            - "INFO": Confirmation that things are working as expected
            - "WARNING": Indication of potential issues
            - "ERROR": Serious problems that prevented a function from completing
            - "CRITICAL": Very serious errors that may prevent program execution
            Defaults to "INFO" if not specified.

    Note:
        - This function configures the root logger, affecting all loggers in the
          application unless they have explicitly set handlers or levels.
        - When OpenTelemetry is configured, all logs will automatically include
          trace context (trace_id, span_id) for correlation with distributed traces.
        - The `force=True` parameter ensures the configuration is applied even if
          logging has been previously configured elsewhere.

    Examples:
        Basic usage with INFO level:
            >>> from samara.utils.logger import set_logger, get_logger
            >>> set_logger(level="INFO")
            >>> logger = get_logger(__name__)
            >>> logger.info("Application initialized successfully")

        Enable debug logging for development:
            >>> set_logger(level="DEBUG")
            >>> logger = get_logger(__name__)
            >>> logger.debug("Detailed variable state: x=%s", x)

        Configure from environment variable:
            >>> import os
            >>> log_level = os.getenv("LOG_LEVEL", "INFO")
            >>> set_logger(level=log_level)

    See Also:
        get_logger: Create logger instances for specific modules
    """
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=level,
        force=True,  # Reconfigure even if already configured
    )


def get_logger(name: str) -> logging.Logger:
    """Create and return a logger instance for the specified module or component.

    This function creates a standard Python logger with the given name, following
    Python's hierarchical logging convention. The logger inherits configuration
    from the root logger set by `set_logger()`, including log level and formatting.

    When OpenTelemetry is properly configured with an OTLP logs endpoint, all logs
    emitted through this logger are automatically enriched with trace context and
    exported to the configured observability backend (e.g., Grafana Loki, Datadog,
    Jaeger, or any OpenTelemetry-compatible system).

    Args:
        name: The name of the logger, which should follow Python's hierarchical
            naming convention. Typically, use `__name__` to automatically use the
            module's fully qualified name (e.g., "samara.workflow.controller").
            This enables fine-grained control over logging configuration by module.

    Returns:
        logging.Logger: A configured Python Logger instance that:
            - Inherits the log level and format from the root logger
            - Automatically includes OpenTelemetry trace context when available
            - Supports all standard logging methods (debug, info, warning, error, critical)
            - Exports logs to OTLP endpoint when configured

    Note:
        - Always call `set_logger()` before using `get_logger()` to ensure proper
          configuration of log formatting and levels.
        - Logger names are hierarchical: "samara.workflow" is the parent of
          "samara.workflow.controller", allowing granular level control.
        - The returned logger is cached by Python's logging system, so multiple
          calls with the same name return the same logger instance.
        - When OpenTelemetry tracing is active, logs automatically include
          trace_id and span_id fields for correlation with distributed traces.

    Examples:
        Standard usage with module name:
            >>> from samara.utils.logger import get_logger
            >>> logger = get_logger(__name__)
            >>> logger.info("Processing started")

        Using different log levels:
            >>> logger = get_logger(__name__)
            >>> logger.debug("Variable state: count=%d", count)
            >>> logger.info("Operation completed successfully")
            >>> logger.warning("Deprecated feature used")
            >>> logger.error("Failed to process record", exc_info=True)

        Adding structured context with extra fields:
            >>> logger = get_logger(__name__)
            >>> logger.info(
            ...     "User action performed",
            ...     extra={"user_id": 123, "action": "login", "ip": "192.168.1.1"}
            ... )

        Creating logger for specific component:
            >>> logger = get_logger("samara.workflow.spark_executor")
            >>> logger.info("Spark job submitted", extra={"job_id": "job-001"})

    See Also:
        set_logger: Configure the root logger before creating loggers
        logging.Logger: Python's standard Logger class documentation
    """
    return logging.getLogger(name)
