"""Custom exceptions for the data pipeline framework.

This module defines specialized exception classes used throughout the framework
to provide detailed error information and enable granular error handling.

Benefits of using custom exceptions:
- Associate exceptions with semantic exit codes for CLI integration
- Enable specific error handling for different failure scenarios
- Provide context-rich information for debugging and logging
- Follow Unix exit code conventions for better system integration

Each exception type maps to a specific exit code, allowing the CLI to report
meaningful error states to the operating system.
"""

import enum
from typing import TypeVar

K = TypeVar("K")  # Key type


class ExitCode(enum.IntEnum):
    """Define standardized exit codes for application termination.

    Map semantic error conditions to Unix/Linux exit code conventions, enabling
    scripts and tools to programmatically handle different failure scenarios.
    """

    SUCCESS = 0
    USAGE_ERROR = 2
    INVALID_ARGUMENTS = 10
    IO_ERROR = 20
    CONFIGURATION_ERROR = 30
    ALERT_CONFIGURATION_ERROR = 31
    RUNTIME_CONFIGURATION_ERROR = 32
    VALIDATION_ERROR = 40
    ALERT_TEST_ERROR = 41
    JOB_ERROR = 50
    KEYBOARD_INTERRUPT = 98
    UNEXPECTED_ERROR = 99


class SamaraError(Exception):
    """Base exception for all framework-specific errors.

    Associates exceptions with exit codes for CLI integration and provides
    a foundation for granular error handling throughout the pipeline.

    Attributes:
        exit_code: The exit code associated with this exception

    Example:
        >>> try:
        ...     process_pipeline(config)
        ... except SamaraError as e:
        ...     sys.exit(e.exit_code)
    """

    def __init__(self, message: str, exit_code: ExitCode) -> None:
        """Initialize the exception with a message and exit code.

        Args:
            message: Description of the error condition
            exit_code: The exit code to report on termination
        """
        self.exit_code = exit_code
        super().__init__(message)


class SamaraIOError(SamaraError):
    """Raise when file system or I/O operations fail.

    Covers file access, read/write errors, and resource unavailability.
    """

    def __init__(self, message: str) -> None:
        """Initialize the exception.

        Args:
            message: Description of the I/O error
        """
        super().__init__(message=message, exit_code=ExitCode.IO_ERROR)


class SamaraAlertConfigurationError(SamaraError):
    """Raise when alert configuration is invalid.

    Indicates issues with alert definition JSON/YAML including invalid
    channels, triggers, or template configuration.
    """

    def __init__(self, message: str) -> None:
        """Initialize the exception.

        Args:
            message: Description of the configuration error
        """
        super().__init__(message=message, exit_code=ExitCode.CONFIGURATION_ERROR)


class SamaraRuntimeConfigurationError(SamaraError):
    """Raise when runtime configuration is invalid.

    Indicates issues with job, extract, transform, or load configuration
    including missing required fields or incompatible settings.
    """

    def __init__(self, message: str) -> None:
        """Initialize the exception.

        Args:
            message: Description of the configuration error
        """
        super().__init__(message=message, exit_code=ExitCode.CONFIGURATION_ERROR)


class SamaraValidationError(SamaraError):
    """Raise when data or schema validation fails.

    Occurs during pipeline execution when input data doesn't conform to
    expected schemas or validation rules fail.
    """

    def __init__(self, message: str) -> None:
        """Initialize the exception.

        Args:
            message: Description of the validation error
        """
        super().__init__(message=message, exit_code=ExitCode.VALIDATION_ERROR)


class SamaraAlertTestError(SamaraError):
    """Raise when alert system testing fails.

    Indicates failure during alert channel validation or test execution,
    including delivery failures or notification errors.
    """

    def __init__(self, message: str) -> None:
        """Initialize the exception.

        Args:
            message: Description of the alert test error
        """
        super().__init__(message=message, exit_code=ExitCode.ALERT_TEST_ERROR)


class SamaraJobError(SamaraError):
    """Raise when ETL job execution fails.

    Covers errors during data extraction, transformation, or loading phases
    including engine failures or transformation logic errors.
    """

    def __init__(self, message: str) -> None:
        """Initialize the exception.

        Args:
            message: Description of the job execution error
        """
        super().__init__(message=message, exit_code=ExitCode.JOB_ERROR)
