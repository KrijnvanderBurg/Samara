"""Tests for EnvVarsMatchesRule model.

These tests verify that EnvVarsMatchesRule instances can be correctly created
from configuration and function as expected.
"""

import os
from typing import Any
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from samara.alert.rules.env_vars_matches import EnvVarsMatchesRule

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="env_rule_config")
def fixture_env_rule_config() -> dict[str, Any]:
    """Provide a representative environment variable matching rule configuration."""
    return {
        "rule_type": "env_vars_matches",
        "env_var_name": "TEST_ENV",
        "env_var_values": ["1", "true"],
    }


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestEnvVarsMatchesRuleValidation:
    """Test EnvVarsMatchesRule model validation and instantiation."""

    def test_create_env_vars_rule__with_valid_config__succeeds(self, env_rule_config: dict[str, Any]) -> None:
        """Test EnvVarsMatchesRule creation with valid configuration."""
        # Act
        rule = EnvVarsMatchesRule(**env_rule_config)

        # Assert
        assert rule.rule_type == "env_vars_matches"
        assert rule.env_var_name == "TEST_ENV"
        assert rule.env_var_values == ["1", "true"]

    def test_create_env_vars_rule__with_missing_rule_type__raises_validation_error(
        self, env_rule_config: dict[str, Any]
    ) -> None:
        """Test EnvVarsMatchesRule creation fails when rule_type is missing."""
        # Arrange
        del env_rule_config["rule_type"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            EnvVarsMatchesRule(**env_rule_config)

    def test_create_env_vars_rule__with_missing_env_var_name__raises_validation_error(
        self, env_rule_config: dict[str, Any]
    ) -> None:
        """Test EnvVarsMatchesRule creation fails when env_var_name is missing."""
        # Arrange
        del env_rule_config["env_var_name"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            EnvVarsMatchesRule(**env_rule_config)

    def test_create_env_vars_rule__with_missing_env_var_values__raises_validation_error(
        self, env_rule_config: dict[str, Any]
    ) -> None:
        """Test EnvVarsMatchesRule creation fails when env_var_values is missing."""
        # Arrange
        del env_rule_config["env_var_values"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            EnvVarsMatchesRule(**env_rule_config)

    def test_create_env_vars_rule__with_wrong_rule_type__raises_validation_error(
        self, env_rule_config: dict[str, Any]
    ) -> None:
        """Test EnvVarsMatchesRule creation fails when rule type is incorrect."""
        # Arrange
        env_rule_config["rule_type"] = "wrong_rule_type"

        # Assert
        with pytest.raises(ValidationError):
            # Act
            EnvVarsMatchesRule(**env_rule_config)

    def test_create_env_vars_rule__with_empty_env_var_name__succeeds(self, env_rule_config: dict[str, Any]) -> None:
        """Test EnvVarsMatchesRule creation succeeds with empty env_var_name."""
        # Arrange
        env_rule_config["env_var_name"] = ""

        # Act
        rule = EnvVarsMatchesRule(**env_rule_config)

        # Assert
        assert rule.env_var_name == ""

    def test_create_env_vars_rule__with_empty_env_var_values__succeeds(self, env_rule_config: dict[str, Any]) -> None:
        """Test EnvVarsMatchesRule creation succeeds with empty env_var_values."""
        # Arrange
        env_rule_config["env_var_values"] = []

        # Act
        rule = EnvVarsMatchesRule(**env_rule_config)

        # Assert
        assert rule.env_var_values == []

    def test_create_env_vars_rule__with_single_value__succeeds(self, env_rule_config: dict[str, Any]) -> None:
        """Test EnvVarsMatchesRule creation succeeds with single env_var_values."""
        # Arrange
        env_rule_config["env_var_values"] = ["production"]

        # Act
        rule = EnvVarsMatchesRule(**env_rule_config)

        # Assert
        assert rule.env_var_values == ["production"]


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="env_rule")
def fixture_env_rule(env_rule_config: dict[str, Any]) -> EnvVarsMatchesRule:
    """Create EnvVarsMatchesRule instance from configuration."""
    return EnvVarsMatchesRule(**env_rule_config)


# =========================================================================== #
# ========================== EVALUATION TESTS ============================= #
# =========================================================================== #


class TestEnvVarsMatchesRuleEvaluation:
    """Test EnvVarsMatchesRule evaluation functionality."""

    def test_evaluate__with_matching_env_var__returns_true(self, env_rule: EnvVarsMatchesRule) -> None:
        """Test evaluation returns True when environment variable matches expected value."""
        # Mock environment variable to simulate matching condition
        with patch.dict(os.environ, {"TEST_ENV": "true"}):
            # Act
            result = env_rule.evaluate(ValueError("Test exception"))

            # Assert
            assert result is True

    def test_evaluate__with_different_matching_env_var__returns_true(self, env_rule: EnvVarsMatchesRule) -> None:
        """Test evaluation returns True when environment variable matches another expected value."""
        # Mock environment variable to simulate matching condition
        with patch.dict(os.environ, {"TEST_ENV": "1"}):
            # Act
            result = env_rule.evaluate(RuntimeError("Another test exception"))

            # Assert
            assert result is True

    def test_evaluate__with_non_matching_env_var__returns_false(self, env_rule: EnvVarsMatchesRule) -> None:
        """Test evaluation returns False when environment variable doesn't match expected values."""
        # Mock environment variable to simulate non-matching condition
        with patch.dict(os.environ, {"TEST_ENV": "false"}):
            # Act
            result = env_rule.evaluate(KeyError("Test exception"))

            # Assert
            assert result is False

    def test_evaluate__with_missing_env_var__returns_false(self, env_rule: EnvVarsMatchesRule) -> None:
        """Test evaluation returns False when environment variable is not set."""
        # Mock environment to ensure TEST_ENV is not set
        with patch.dict(os.environ, {}, clear=True):
            # Act
            result = env_rule.evaluate(ValueError("Test exception"))

            # Assert
            assert result is False

    def test_evaluate__with_case_sensitive_matching__returns_true(self, env_rule: EnvVarsMatchesRule) -> None:
        """Test evaluation is case-sensitive for environment variable values."""
        # Mock environment variable with exact case match
        with patch.dict(os.environ, {"TEST_ENV": "true"}):
            # Act
            result = env_rule.evaluate(ValueError("Test exception"))

            # Assert
            assert result is True

    def test_evaluate__with_case_sensitive_non_matching__returns_false(self, env_rule: EnvVarsMatchesRule) -> None:
        """Test evaluation is case-sensitive and returns False for different case."""
        # Mock environment variable with different case
        with patch.dict(os.environ, {"TEST_ENV": "True"}):
            # Act
            result = env_rule.evaluate(ValueError("Test exception"))

            # Assert
            assert result is False

    def test_evaluate__with_empty_env_var_name__returns_true(self) -> None:
        """Test evaluation returns True when env_var_name is empty (edge case behavior)."""
        # Arrange
        config = {
            "rule_type": "env_vars_matches",
            "env_var_name": "",
            "env_var_values": ["test"],
        }
        rule = EnvVarsMatchesRule(**config)

        # Act
        result = rule.evaluate(ValueError("Test exception"))

        # Assert
        assert result is True

    def test_evaluate__with_multiple_values_first_matches__returns_true(self) -> None:
        """Test evaluation returns True when first value in list matches."""
        # Arrange
        config = {
            "rule_type": "env_vars_matches",
            "env_var_name": "MULTI_TEST",
            "env_var_values": ["dev", "staging", "prod"],
        }
        rule = EnvVarsMatchesRule(**config)

        # Mock environment variable to match first value
        with patch.dict(os.environ, {"MULTI_TEST": "dev"}):
            # Act
            result = rule.evaluate(ValueError("Test exception"))

            # Assert
            assert result is True

    def test_evaluate__with_multiple_values_last_matches__returns_true(self) -> None:
        """Test evaluation returns True when last value in list matches."""
        # Arrange
        config = {
            "rule_type": "env_vars_matches",
            "env_var_name": "MULTI_TEST",
            "env_var_values": ["dev", "staging", "prod"],
        }
        rule = EnvVarsMatchesRule(**config)

        # Mock environment variable to match last value
        with patch.dict(os.environ, {"MULTI_TEST": "prod"}):
            # Act
            result = rule.evaluate(ValueError("Test exception"))

            # Assert
            assert result is True

    def test_evaluate__with_different_exception_types__works_consistently(self, env_rule: EnvVarsMatchesRule) -> None:
        """Test evaluation works consistently regardless of exception type."""
        # Mock environment variable to simulate matching condition
        with patch.dict(os.environ, {"TEST_ENV": "true"}):
            # Act & Assert - Test with different exception types
            assert env_rule.evaluate(ValueError("Value error")) is True
            assert env_rule.evaluate(RuntimeError("Workflow error")) is True
            assert env_rule.evaluate(KeyError("Key error")) is True
            assert env_rule.evaluate(Exception("Generic exception")) is True
