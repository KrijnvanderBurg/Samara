"""Tests for AlertTrigger model.

These tests verify AlertTrigger creation, validation, trigger evaluation,
and error handling scenarios.
"""

import os
from typing import Any
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from samara.alert.trigger import AlertTrigger
from samara.exceptions import SamaraWorkflowError

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="valid_trigger_config")
def fixture_valid_trigger_config() -> dict[str, Any]:
    """Provide a valid alert trigger configuration."""
    return {
        "id": "production-errors",
        "enabled": True,
        "description": "Production error notifications",
        "channel_ids": ["email", "file"],
        "template": {
            "prepend_title": "[ALERT]",
            "append_title": "[/ALERT]",
            "prepend_body": ">>> ",
            "append_body": " <<<",
        },
        "rules": [],
    }


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestAlertTriggerValidation:
    """Test AlertTrigger model validation and instantiation."""

    def test_create_alert_trigger__with_valid_config__succeeds(self, valid_trigger_config: dict[str, Any]) -> None:
        """Test AlertTrigger creation with valid configuration."""
        # Act
        trigger = AlertTrigger(**valid_trigger_config)

        # Assert
        assert trigger.id_ == "production-errors"
        assert trigger.enabled is True
        assert trigger.description == "Production error notifications"
        assert trigger.channel_ids == ["email", "file"]
        assert trigger.template.prepend_title == "[ALERT]"
        assert trigger.template.append_title == "[/ALERT]"
        assert trigger.template.prepend_body == ">>> "
        assert trigger.template.append_body == " <<<"
        assert trigger.rules == []

    def test_create_alert_trigger__with_missing_id__raises_validation_error(
        self, valid_trigger_config: dict[str, Any]
    ) -> None:
        """Test AlertTrigger creation fails when id is missing."""
        # Arrange
        del valid_trigger_config["id"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            AlertTrigger(**valid_trigger_config)

    def test_create_alert_trigger__with_empty_id__raises_validation_error(
        self, valid_trigger_config: dict[str, Any]
    ) -> None:
        """Test AlertTrigger creation fails when id is empty string."""
        # Arrange
        valid_trigger_config["id"] = ""

        # Act & Assert
        with pytest.raises(ValidationError):
            AlertTrigger(**valid_trigger_config)

    def test_create_alert_trigger__with_missing_enabled__raises_validation_error(
        self, valid_trigger_config: dict[str, Any]
    ) -> None:
        """Test AlertTrigger creation fails when enabled is missing."""
        # Arrange
        del valid_trigger_config["enabled"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            AlertTrigger(**valid_trigger_config)

    def test_create_alert_trigger__with_missing_description__raises_validation_error(
        self, valid_trigger_config: dict[str, Any]
    ) -> None:
        """Test AlertTrigger creation fails when description is missing."""
        # Arrange
        del valid_trigger_config["description"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            AlertTrigger(**valid_trigger_config)

    def test_create_alert_trigger__with_missing_channel_ids__raises_validation_error(
        self, valid_trigger_config: dict[str, Any]
    ) -> None:
        """Test AlertTrigger creation fails when channel_ids is missing."""
        # Arrange
        del valid_trigger_config["channel_ids"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            AlertTrigger(**valid_trigger_config)

    def test_create_alert_trigger__with_missing_template__raises_validation_error(
        self, valid_trigger_config: dict[str, Any]
    ) -> None:
        """Test AlertTrigger creation fails when template is missing."""
        # Arrange
        del valid_trigger_config["template"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            AlertTrigger(**valid_trigger_config)

    def test_create_alert_trigger__with_missing_rules__raises_validation_error(
        self, valid_trigger_config: dict[str, Any]
    ) -> None:
        """Test AlertTrigger creation fails when rules is missing."""
        # Arrange
        del valid_trigger_config["rules"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            AlertTrigger(**valid_trigger_config)

    def test_create_alert_trigger__with_empty_channel_ids__succeeds(self, valid_trigger_config: dict[str, Any]) -> None:
        """Test AlertTrigger creation with empty channel ids list."""
        # Arrange
        valid_trigger_config["channel_ids"] = []

        # Act
        trigger = AlertTrigger(**valid_trigger_config)

        # Assert
        assert trigger.channel_ids == []

    def test_create_alert_trigger__with_empty_rules__succeeds(self, valid_trigger_config: dict[str, Any]) -> None:
        """Test AlertTrigger creation with empty rules list."""
        # Arrange
        valid_trigger_config["rules"] = []

        # Act
        trigger = AlertTrigger(**valid_trigger_config)

        # Assert
        assert trigger.rules == []

    def test_create_alert_trigger__with_regex_rule__succeeds(self, valid_trigger_config: dict[str, Any]) -> None:
        """Test AlertTrigger creation with exception regex rule."""
        # Arrange
        valid_trigger_config["rules"] = [{"rule_type": "exception_regex", "pattern": r"database.*connection.*failed"}]

        # Act
        trigger = AlertTrigger(**valid_trigger_config)

        # Assert
        assert len(trigger.rules) == 1
        assert trigger.rules[0].rule_type == "exception_regex"
        assert trigger.rules[0].pattern == r"database.*connection.*failed"

    def test_create_alert_trigger__with_env_vars_rule__succeeds(self, valid_trigger_config: dict[str, Any]) -> None:
        """Test AlertTrigger creation with environment variables rule."""
        # Arrange
        valid_trigger_config["rules"] = [
            {
                "rule_type": "env_vars_matches",
                "env_var_name": "ENVIRONMENT",
                "env_var_values": ["production", "staging"],
            }
        ]

        # Act
        trigger = AlertTrigger(**valid_trigger_config)

        # Assert
        assert len(trigger.rules) == 1
        assert trigger.rules[0].rule_type == "env_vars_matches"
        assert trigger.rules[0].env_var_name == "ENVIRONMENT"
        assert trigger.rules[0].env_var_values == ["production", "staging"]

    def test_create_alert_trigger__with_multiple_rules__succeeds(self, valid_trigger_config: dict[str, Any]) -> None:
        """Test AlertTrigger creation with multiple rules."""
        # Arrange
        valid_trigger_config["rules"] = [
            {"rule_type": "exception_regex", "pattern": r"database.*error"},
            {"rule_type": "env_vars_matches", "env_var_name": "ENVIRONMENT", "env_var_values": ["production"]},
        ]

        # Act
        trigger = AlertTrigger(**valid_trigger_config)

        # Assert
        assert len(trigger.rules) == 2
        assert trigger.rules[0].rule_type == "exception_regex"
        assert trigger.rules[1].rule_type == "env_vars_matches"


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="alert_trigger")
def fixture_alert_trigger(valid_trigger_config: dict[str, Any]) -> AlertTrigger:
    """Create AlertTrigger instance from valid configuration."""
    return AlertTrigger(**valid_trigger_config)


# =========================================================================== #
# ======================== SHOULD FIRE TESTS =============================== #
# =========================================================================== #


class TestAlertTriggerShouldFire:
    """Test AlertTrigger should_fire functionality."""

    def test_should_fire__when_enabled_no_rules__returns_true(self, alert_trigger: AlertTrigger) -> None:
        """Test trigger fires when enabled with no rules configured."""
        # Act
        result = alert_trigger.should_fire(Exception("test error"))

        # Assert
        assert result is True

    def test_should_fire__when_disabled_no_rules__returns_false(self, valid_trigger_config: dict[str, Any]) -> None:
        """Test trigger does not fire when disabled regardless of rules."""
        # Arrange
        valid_trigger_config["enabled"] = False
        trigger = AlertTrigger(**valid_trigger_config)

        # Act
        result = trigger.should_fire(Exception("test error"))

        # Assert
        assert result is False

    def test_should_fire__when_disabled_with_matching_rules__returns_false(
        self, valid_trigger_config: dict[str, Any]
    ) -> None:
        """Test trigger does not fire when disabled even if rules would match."""
        # Arrange
        valid_trigger_config["enabled"] = False
        valid_trigger_config["rules"] = [{"rule_type": "exception_regex", "pattern": "test"}]
        trigger = AlertTrigger(**valid_trigger_config)

        # Act
        result = trigger.should_fire(Exception("test error"))

        # Assert
        assert result is False

    def test_should_fire__with_matching_regex_rule__returns_true(self, valid_trigger_config: dict[str, Any]) -> None:
        """Test trigger fires when regex rule matches exception message."""
        # Arrange
        valid_trigger_config["rules"] = [{"rule_type": "exception_regex", "pattern": r"database.*connection"}]
        trigger = AlertTrigger(**valid_trigger_config)

        # Act
        result = trigger.should_fire(Exception("database connection failed"))

        # Assert
        assert result is True

    def test_should_fire__with_non_matching_regex_rule__returns_false(
        self, valid_trigger_config: dict[str, Any]
    ) -> None:
        """Test trigger does not fire when regex rule does not match."""
        # Arrange
        valid_trigger_config["rules"] = [{"rule_type": "exception_regex", "pattern": r"database.*connection"}]
        trigger = AlertTrigger(**valid_trigger_config)

        # Act
        result = trigger.should_fire(Exception("network timeout"))

        # Assert
        assert result is False

    def test_should_fire__with_matching_env_vars_rule__returns_true(self, valid_trigger_config: dict[str, Any]) -> None:
        """Test trigger fires when environment variable rule matches."""
        # Arrange
        valid_trigger_config["rules"] = [
            {
                "rule_type": "env_vars_matches",
                "env_var_name": "ENVIRONMENT",
                "env_var_values": ["production", "staging"],
            }
        ]
        trigger = AlertTrigger(**valid_trigger_config)

        # Mock environment variable to simulate production environment
        with patch.dict(os.environ, {"ENVIRONMENT": "production"}):
            # Act
            result = trigger.should_fire(Exception("test error"))

        # Assert
        assert result is True

    def test_should_fire__with_non_matching_env_vars_rule__returns_false(
        self, valid_trigger_config: dict[str, Any]
    ) -> None:
        """Test trigger does not fire when environment variable rule does not match."""
        # Arrange
        valid_trigger_config["rules"] = [
            {
                "rule_type": "env_vars_matches",
                "env_var_name": "ENVIRONMENT",
                "env_var_values": ["production", "staging"],
            }
        ]
        trigger = AlertTrigger(**valid_trigger_config)

        # Mock environment variable to simulate development environment
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}):
            # Act
            result = trigger.should_fire(Exception("test error"))

        # Assert
        assert result is False

    def test_should_fire__with_missing_env_var__returns_false(self, valid_trigger_config: dict[str, Any]) -> None:
        """Test trigger does not fire when environment variable is not set."""
        # Arrange
        valid_trigger_config["rules"] = [
            {"rule_type": "env_vars_matches", "env_var_name": "NONEXISTENT_VAR", "env_var_values": ["any_value"]}
        ]
        trigger = AlertTrigger(**valid_trigger_config)

        # Act
        result = trigger.should_fire(Exception("test error"))

        # Assert
        assert result is False

    def test_should_fire__with_all_rules_matching__returns_true(self, valid_trigger_config: dict[str, Any]) -> None:
        """Test trigger fires when all rules evaluate to True (AND logic)."""
        # Arrange
        valid_trigger_config["rules"] = [
            {"rule_type": "exception_regex", "pattern": "database"},
            {"rule_type": "env_vars_matches", "env_var_name": "ENVIRONMENT", "env_var_values": ["production"]},
        ]
        trigger = AlertTrigger(**valid_trigger_config)

        # Mock environment variable to match rule
        with patch.dict(os.environ, {"ENVIRONMENT": "production"}):
            # Act
            result = trigger.should_fire(Exception("database connection failed"))

        # Assert
        assert result is True

    def test_should_fire__with_one_rule_failing__returns_false(self, valid_trigger_config: dict[str, Any]) -> None:
        """Test trigger does not fire when any rule evaluates to False (AND logic)."""
        # Arrange
        valid_trigger_config["rules"] = [
            {"rule_type": "exception_regex", "pattern": "database"},
            {"rule_type": "env_vars_matches", "env_var_name": "ENVIRONMENT", "env_var_values": ["production"]},
        ]
        trigger = AlertTrigger(**valid_trigger_config)

        # Mock environment variable to not match rule
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}):
            # Act
            result = trigger.should_fire(Exception("database connection failed"))

        # Assert
        assert result is False

    def test_should_fire__with_complex_regex_pattern__handles_correctly(
        self, valid_trigger_config: dict[str, Any]
    ) -> None:
        """Test trigger with complex regex patterns."""
        # Arrange
        valid_trigger_config["rules"] = [
            {"rule_type": "exception_regex", "pattern": r"(?i)(error|exception|failed).*\d{3,4}"}
        ]
        trigger = AlertTrigger(**valid_trigger_config)

        # Act & Assert - matching cases
        assert trigger.should_fire(Exception("Error code 404")) is True
        assert trigger.should_fire(Exception("EXCEPTION STATUS 500")) is True
        assert trigger.should_fire(Exception("failed with code 1234")) is True

        # Act & Assert - non-matching cases
        assert trigger.should_fire(Exception("warning code 50")) is False
        assert trigger.should_fire(Exception("error without code")) is False

    def test_should_fire__with_empty_regex_pattern__returns_true(self, valid_trigger_config: dict[str, Any]) -> None:
        """Test trigger with empty regex pattern defaults to True."""
        # Arrange
        valid_trigger_config["rules"] = [{"rule_type": "exception_regex", "pattern": ""}]
        trigger = AlertTrigger(**valid_trigger_config)

        # Act
        result = trigger.should_fire(Exception("any error message"))

        # Assert
        assert result is True

    def test_should_fire__with_empty_env_var_name__returns_true(self, valid_trigger_config: dict[str, Any]) -> None:
        """Test trigger with empty env var name defaults to True."""
        # Arrange
        valid_trigger_config["rules"] = [
            {"rule_type": "env_vars_matches", "env_var_name": "", "env_var_values": ["production"]}
        ]
        trigger = AlertTrigger(**valid_trigger_config)

        # Act
        result = trigger.should_fire(Exception("test error"))

        # Assert
        assert result is True

    def test_should_fire__with_various_exception_types__handles_correctly(
        self, valid_trigger_config: dict[str, Any]
    ) -> None:
        """Test trigger handles different exception types correctly."""
        # Arrange
        valid_trigger_config["rules"] = [{"rule_type": "exception_regex", "pattern": "connection"}]
        trigger = AlertTrigger(**valid_trigger_config)

        # Act & Assert - different exception types
        assert trigger.should_fire(ValueError("connection error")) is True
        assert trigger.should_fire(SamaraWorkflowError("database connection failed")) is True
        assert trigger.should_fire(ConnectionError("connection timeout")) is True
        assert trigger.should_fire(Exception("no connection available")) is True

        # Non-matching case
        assert trigger.should_fire(ValueError("validation failed")) is False
