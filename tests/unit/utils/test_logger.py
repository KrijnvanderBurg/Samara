"""Unit tests for logging utility functions."""

import logging

from samara.utils.logger import get_logger, set_logger


class TestSetLogger:
    """Test set_logger function."""

    def test_configures_logging_system(self) -> None:
        """Test that set_logger configures the logging system."""
        # Act
        set_logger(level="INFO")
        logger = get_logger("test_logger")

        # Assert
        assert logger is not None
        assert isinstance(logger, logging.Logger)
        logger.info("test message")

    def test_accepts_custom_log_level(self) -> None:
        """Test set_logger accepts custom log level parameter."""
        # Act
        set_logger(level="DEBUG")
        logger = get_logger("test_custom_level")

        # Assert
        assert logger is not None
        assert logger.isEnabledFor(logging.DEBUG)
        logger.debug("debug message")

    def test_handles_lowercase_level(self) -> None:
        """Test that set_logger handles lowercase log level strings."""
        # Act
        set_logger(level="warning")
        logger = get_logger("test_lowercase")

        # Assert
        assert logger is not None
        assert logger.isEnabledFor(logging.WARNING)

    def test_defaults_to_info_level(self) -> None:
        """Test that logger defaults to INFO level when not specified."""
        # Act
        set_logger()  # No level specified
        logger = get_logger("test_default")

        # Assert
        assert logger is not None
        assert logger.isEnabledFor(logging.INFO)
        logger.info("info message with default level")

    def test_can_reconfigure_logging(self) -> None:
        """Test that set_logger can be called multiple times to reconfigure."""
        # Act
        set_logger(level="ERROR")
        logger1 = get_logger("test_reconfig")
        assert logger1.isEnabledFor(logging.ERROR)

        set_logger(level="DEBUG")
        logger2 = get_logger("test_reconfig")

        # Assert - same logger instance but reconfigured
        assert logger1 is logger2
        assert logger2.isEnabledFor(logging.DEBUG)


class TestGetLogger:
    """Test get_logger function."""

    def test_returns_logger_instance(self) -> None:
        """Test get_logger returns a standard Python logger."""
        # Act
        logger = get_logger("test_logger")

        # Assert
        assert logger is not None
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_logger"

    def test_returns_different_loggers_for_different_names(self) -> None:
        """Test that different names return different logger instances."""
        # Act
        logger1 = get_logger("logger_one")
        logger2 = get_logger("logger_two")

        # Assert
        assert logger1 is not logger2
        assert logger1.name == "logger_one"
        assert logger2.name == "logger_two"

    def test_returns_same_logger_for_same_name(self) -> None:
        """Test that same name returns same logger instance (cached)."""
        # Act
        logger1 = get_logger("same_logger")
        logger2 = get_logger("same_logger")

        # Assert
        assert logger1 is logger2
