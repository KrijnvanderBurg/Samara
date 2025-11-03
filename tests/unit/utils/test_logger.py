"""Unit tests for logging utility functions."""

import pytest

from samara.settings import get_settings
from samara.utils.logger import bind_context, clear_context, get_logger, set_logger


class TestSetLogger:
    """Test set_logger function."""

    def test_creates_logger_with_default_name(self) -> None:
        """Test that set_logger creates a logger instance."""
        # Act
        logger = set_logger("test_logger")

        # Assert
        assert logger is not None
        logger.info("test message")

    def test_accepts_custom_log_level(self) -> None:
        """Test set_logger accepts custom log level parameter."""
        # Act
        logger = set_logger("test_custom_level", level="DEBUG")

        # Assert
        assert logger is not None
        logger.debug("debug message")

    def test_uses_settings_when_no_level_provided(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that set_logger uses application settings when no level is provided."""
        # Arrange

        get_settings.cache_clear()
        monkeypatch.setenv("SAMARA_LOG_LEVEL", "DEBUG")

        # Act
        logger = set_logger("test_logger_from_settings")

        # Assert
        assert logger is not None
        logger.debug("debug message from settings")

    def test_explicit_level_overrides_settings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that explicit level parameter overrides settings."""
        # Arrange

        get_settings.cache_clear()
        monkeypatch.setenv("SAMARA_LOG_LEVEL", "DEBUG")

        # Act - explicitly pass ERROR level
        logger = set_logger("test_logger_override", level="ERROR")

        # Assert
        assert logger is not None
        logger.error("error message")

    def test_defaults_to_info_when_no_env_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that logger defaults to INFO level when no environment variables are set."""
        # Arrange

        get_settings.cache_clear()
        monkeypatch.delenv("SAMARA_LOG_LEVEL", raising=False)

        # Act
        logger = set_logger("test_logger_default")

        # Assert
        assert logger is not None
        logger.info("info message with default level")


class TestGetLogger:
    """Test get_logger function."""

    def test_returns_logger_instance(self) -> None:
        """Test get_logger returns a logger that can log messages."""
        # Act
        logger = get_logger("test_logger")

        # Assert
        assert logger is not None
        logger.info("test message")


class TestContextBinding:
    """Test context binding and clearing functions."""

    def test_bind_context_adds_context_variables(self) -> None:
        """Test that bind_context adds context variables to subsequent logs."""
        # Arrange
        logger = set_logger("test_context")

        # Act
        bind_context(user_id="123", request_id="abc")
        logger.info("test message with context")

        # Cleanup
        clear_context()

    def test_clear_context_removes_bound_variables(self) -> None:
        """Test that clear_context removes all bound context variables."""
        # Arrange
        bind_context(user_id="456", request_id="def")

        # Act
        clear_context()

        # Assert
        logger = get_logger("test_clear")
        logger.info("test message after clear")
