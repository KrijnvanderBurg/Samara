"""Structured logging utilities for the Samara framework."""

import logging
from typing import Any

import structlog

from samara import get_run_id


def set_logger(name: str = "Samara", level: str = "INFO") -> structlog.BoundLogger:
    """Configure and return a structured logger with console output.

    Args:
        name: Optional logger name.
        level: Logging level (e.g., "INFO", "DEBUG"). If not provided,
            uses the log level from application settings.

    Returns:
        A structlog BoundLogger instance configured with the specified level.
    """
    # get log level from settings if not provided via CLI

    # Configure structlog only once
    if not structlog.is_configured():
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.processors.TimeStamper(fmt="ISO"),
                structlog.processors.add_log_level,
                structlog.dev.ConsoleRenderer(colors=True),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(level),
            logger_factory=structlog.PrintLoggerFactory(),
            cache_logger_on_first_use=True,
        )

    structlog.contextvars.bind_contextvars(run_id=get_run_id())

    return structlog.get_logger(name)


def get_logger(name: str) -> logging.Logger:
    """Return a structured logger instance by name.

    Args:
        name: Logger name, typically the module name.

    Returns:
        A structlog logger instance bound to the specified name.
    """
    return structlog.get_logger(name)


def bind_context(**context: Any) -> None:
    """Bind context variables to all subsequent log messages.

    Args:
        **context: Key-value pairs to include in log context. Examples: job_id,
            pipeline_name, user_id.
    """
    structlog.contextvars.bind_contextvars(**context)


def clear_context() -> None:
    """Clear all bound context variables."""
    structlog.contextvars.clear_contextvars()
