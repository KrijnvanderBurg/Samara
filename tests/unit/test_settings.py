"""Tests for application settings module.

These tests verify AppSettings validation, singleton behavior, environment
variable loading, caching mechanism, and settings access patterns.
"""

import os
from unittest.mock import patch

from samara.settings import AppSettings, get_settings


class TestAppSettingsValidation:
    """Test AppSettings model validation and instantiation."""

    def test_default_log_level(self) -> None:
        """Verify default log level is None when no environment variable is set."""
        with patch.dict(os.environ, {}, clear=True):
            settings = AppSettings()
            assert settings.log_level is None

    def test_log_level_from_env_uppercase(self) -> None:
        """Verify log level is loaded from SAMARA_LOG_LEVEL environment variable."""
        with patch.dict(os.environ, {"SAMARA_LOG_LEVEL": "DEBUG"}, clear=True):
            settings = AppSettings()
            assert settings.log_level == "DEBUG"

    def test_log_level_from_env_lowercase(self) -> None:
        """Verify log level is loaded from environment as-is."""
        with patch.dict(os.environ, {"SAMARA_LOG_LEVEL": "debug"}, clear=True):
            settings = AppSettings()
            assert settings.log_level == "debug"

    def test_log_level_all_valid_values(self) -> None:
        """Verify all valid log levels are accepted."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        for level in valid_levels:
            with patch.dict(os.environ, {"SAMARA_LOG_LEVEL": level}, clear=True):
                settings = AppSettings()
                assert settings.log_level == level

    def test_invalid_log_level_accepted(self) -> None:
        """Verify settings accepts any log level string (validation happens in logger)."""
        with patch.dict(os.environ, {"SAMARA_LOG_LEVEL": "INVALID"}, clear=True):
            settings = AppSettings()
            assert settings.log_level == "INVALID"

    def test_extra_env_vars_ignored(self) -> None:
        """Verify extra environment variables with SAMARA_ prefix are ignored."""
        with patch.dict(os.environ, {"SAMARA_LOG_LEVEL": "INFO", "SAMARA_UNKNOWN_SETTING": "value"}, clear=True):
            settings = AppSettings()
            assert settings.log_level == "INFO"
            assert not hasattr(settings, "unknown_setting")


class TestGetSettingsSingleton:
    """Test singleton behavior of get_settings function."""

    def test_get_settings_returns_same_instance(self) -> None:
        """Verify get_settings returns the same instance on multiple calls."""
        get_settings.cache_clear()
        settings1 = get_settings()
        settings2 = get_settings()
        assert settings1 is settings2

    def test_cache_clear_creates_new_instance(self) -> None:
        """Verify cache_clear allows creating a new settings instance."""
        get_settings.cache_clear()
        with patch.dict(os.environ, {"SAMARA_LOG_LEVEL": "DEBUG"}, clear=True):
            settings1 = get_settings()
            assert settings1.log_level == "DEBUG"

        get_settings.cache_clear()
        with patch.dict(os.environ, {"SAMARA_LOG_LEVEL": "ERROR"}, clear=True):
            settings2 = get_settings()
            assert settings2.log_level == "ERROR"
            assert settings1 is not settings2

    def test_get_settings_with_default_values(self) -> None:
        """Verify get_settings returns None for log_level when no env vars set."""
        get_settings.cache_clear()
        with patch.dict(os.environ, {}, clear=True):
            settings = get_settings()
            assert settings.log_level is None


class TestSettingsIntegration:
    """Test settings integration with application components."""

    def test_settings_accessible_after_import(self) -> None:
        """Verify settings can be imported and accessed from any module."""

        get_settings.cache_clear()
        settings = get_settings()
        # log_level can be None or a valid level string
        assert settings.log_level is None or settings.log_level in [
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
            "CRITICAL",
        ]

    def test_settings_reflect_environment_changes_after_cache_clear(self) -> None:
        """Verify settings reflect environment changes after cache clear."""
        get_settings.cache_clear()

        # Set initial environment
        with patch.dict(os.environ, {"SAMARA_LOG_LEVEL": "INFO"}, clear=True):
            settings1 = get_settings()
            assert settings1.log_level == "INFO"

        # Change environment and clear cache
        get_settings.cache_clear()
        with patch.dict(os.environ, {"SAMARA_LOG_LEVEL": "DEBUG"}, clear=True):
            settings2 = get_settings()
            assert settings2.log_level == "DEBUG"


class TestSettingsEnvironmentVariables:
    """Test environment variable loading and precedence."""

    def test_samara_prefix_required(self) -> None:
        """Verify settings only load from SAMARA_ prefixed environment variables."""
        get_settings.cache_clear()
        with patch.dict(os.environ, {"LOG_LEVEL": "DEBUG", "SAMARA_LOG_LEVEL": "INFO"}, clear=True):
            settings = AppSettings()
            # SAMARA_LOG_LEVEL takes precedence
            assert settings.log_level == "INFO"

    def test_env_file_not_required(self) -> None:
        """Verify settings work without .env file present."""
        get_settings.cache_clear()
        with patch.dict(os.environ, {"SAMARA_LOG_LEVEL": "WARNING"}, clear=True):
            settings = AppSettings()
            assert settings.log_level == "WARNING"
