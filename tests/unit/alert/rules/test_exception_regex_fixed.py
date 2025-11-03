"""Tests for ExceptionRegexRule model.

These tests verify that ExceptionRegexRule instances can be correctly created
from configuration and function as expected.
"""

import re
from typing import Any

import pytest
from pydantic import ValidationError

from samara.alert.rules.exception_regex import ExceptionRegexRule
from samara.exceptions import SamaraWorkflowError

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="exception_rule_config")
def fixture_exception_rule_config() -> dict[str, Any]:
    """Provide a representative exception regex rule configuration."""
    return {"rule_type": "exception_regex", "pattern": "error"}


# =========================================================================== #
# ============================= HELPER FUNCTIONS ========================== #
# =========================================================================== #


def create_exception_regex_rule(pattern: str) -> ExceptionRegexRule:
    """Create ExceptionRegexRule with proper configuration."""
    config: dict[str, Any] = {"rule_type": "exception_regex", "pattern": pattern}
    return ExceptionRegexRule(**config)  # type: ignore[arg-type]


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestExceptionRegexRuleValidation:
    """Test ExceptionRegexRule model validation and instantiation."""

    def test_create_exception_regex_rule__with_valid_config__succeeds(
        self, exception_rule_config: dict[str, Any]
    ) -> None:
        """Test ExceptionRegexRule creation with valid configuration."""
        # Act
        rule = ExceptionRegexRule(**exception_rule_config)

        # Assert
        assert rule.rule_type == "exception_regex"
        assert rule.pattern == "error"

    def test_create_exception_regex_rule__with_missing_rule_type__raises_validation_error(
        self, exception_rule_config: dict[str, Any]
    ) -> None:
        """Test ExceptionRegexRule creation fails when rule_type is missing."""
        # Arrange
        del exception_rule_config["rule_type"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            ExceptionRegexRule(**exception_rule_config)

    def test_create_exception_regex_rule__with_missing_pattern__raises_validation_error(
        self, exception_rule_config: dict[str, Any]
    ) -> None:
        """Test ExceptionRegexRule creation fails when pattern is missing."""
        # Arrange
        del exception_rule_config["pattern"]

        # Assert
        with pytest.raises(ValidationError):
            # Act
            ExceptionRegexRule(**exception_rule_config)

    def test_create_exception_regex_rule__with_wrong_rule_type__raises_validation_error(
        self, exception_rule_config: dict[str, Any]
    ) -> None:
        """Test ExceptionRegexRule creation fails when rule type is incorrect."""
        # Arrange
        exception_rule_config["rule_type"] = "wrong_rule_type"

        # Assert
        with pytest.raises(ValidationError):
            # Act
            ExceptionRegexRule(**exception_rule_config)

    def test_create_exception_regex_rule__with_empty_pattern__succeeds(
        self, exception_rule_config: dict[str, Any]
    ) -> None:
        """Test ExceptionRegexRule creation succeeds with empty pattern."""
        # Arrange
        exception_rule_config["pattern"] = ""

        # Act
        rule = ExceptionRegexRule(**exception_rule_config)

        # Assert
        assert rule.pattern == ""

    def test_create_exception_regex_rule__with_complex_pattern__succeeds(
        self, exception_rule_config: dict[str, Any]
    ) -> None:
        """Test ExceptionRegexRule creation succeeds with complex regex pattern."""
        # Arrange
        exception_rule_config["pattern"] = r"\d{4}-\d{2}-\d{2}.*connection.*failed"

        # Act
        rule = ExceptionRegexRule(**exception_rule_config)

        # Assert
        assert rule.pattern == r"\d{4}-\d{2}-\d{2}.*connection.*failed"

    def test_create_exception_regex_rule__with_special_regex_chars__succeeds(
        self, exception_rule_config: dict[str, Any]
    ) -> None:
        """Test ExceptionRegexRule creation succeeds with regex special characters."""
        # Arrange
        exception_rule_config["pattern"] = r"^(ERROR|WARN).*\[.*\]$"

        # Act
        rule = ExceptionRegexRule(**exception_rule_config)

        # Assert
        assert rule.pattern == r"^(ERROR|WARN).*\[.*\]$"


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="exception_rule")
def fixture_exception_rule(exception_rule_config: dict[str, Any]) -> ExceptionRegexRule:
    """Create ExceptionRegexRule instance from configuration."""
    return ExceptionRegexRule(**exception_rule_config)


# =========================================================================== #
# ========================== EVALUATION TESTS ============================= #
# =========================================================================== #


class TestExceptionRegexRuleEvaluation:
    """Test ExceptionRegexRule evaluation functionality."""

    def test_evaluate__with_matching_simple_pattern__returns_true(self, exception_rule: ExceptionRegexRule) -> None:
        """Test evaluation returns True when exception message matches simple pattern."""
        # Arrange
        exception = ValueError("Database connection error occurred")

        # Act
        result = exception_rule.evaluate(exception)

        # Assert
        assert result is True

    def test_evaluate__with_non_matching_pattern__returns_false(self, exception_rule: ExceptionRegexRule) -> None:
        """Test evaluation returns False when exception message doesn't match pattern."""
        # Arrange
        exception = ValueError("Network timeout occurred")

        # Act
        result = exception_rule.evaluate(exception)

        # Assert
        assert result is False

    def test_evaluate__with_case_insensitive_matching__returns_true(self) -> None:
        """Test evaluation with case-insensitive regex pattern."""
        # Arrange
        rule = create_exception_regex_rule(r"(?i)ERROR")
        exception = ValueError("Database Error occurred")

        # Act
        result = rule.evaluate(exception)

        # Assert
        assert result is True

    def test_evaluate__with_partial_matching__returns_true(self, exception_rule: ExceptionRegexRule) -> None:
        """Test evaluation returns True when pattern matches part of exception message."""
        # Arrange
        exception = SamaraWorkflowError("A critical error has been detected in the system")

        # Act
        result = exception_rule.evaluate(exception)

        # Assert
        assert result is True

    def test_evaluate__with_exact_matching__returns_true(self) -> None:
        """Test evaluation with exact pattern matching."""
        # Arrange
        rule = create_exception_regex_rule(r"^Connection failed$")
        exception = ConnectionError("Connection failed")

        # Act
        result = rule.evaluate(exception)

        # Assert
        assert result is True

    def test_evaluate__with_exact_matching_mismatch__returns_false(self) -> None:
        """Test evaluation with exact pattern that doesn't match."""
        # Arrange
        rule = create_exception_regex_rule(r"^Connection failed$")
        exception = ConnectionError("Connection failed due to timeout")

        # Act
        result = rule.evaluate(exception)

        # Assert
        assert result is False

    def test_evaluate__with_complex_regex_pattern__returns_true(self) -> None:
        """Test evaluation with complex regex pattern."""
        # Arrange
        rule = create_exception_regex_rule(r"\d{4}-\d{2}-\d{2}.*connection.*failed")
        exception = ValueError("2024-01-15 12:30:45 - Database connection failed")

        # Act
        result = rule.evaluate(exception)

        # Assert
        assert result is True

    def test_evaluate__with_special_regex_chars__returns_true(self) -> None:
        """Test evaluation with regex special characters."""
        # Arrange
        rule = create_exception_regex_rule(r"\[ERROR\].*timeout")
        exception = TimeoutError("[ERROR] Request timeout after 30 seconds")

        # Act
        result = rule.evaluate(exception)

        # Assert
        assert result is True

    def test_evaluate__with_multiline_exception__returns_true(self) -> None:
        """Test evaluation with multiline exception messages."""
        # Arrange
        rule = create_exception_regex_rule(r"Stack trace:.*")
        multiline_msg = "Stack trace:\n  File 'main.py', line 42\n    raise SamaraWorkflowError()"
        exception = SamaraWorkflowError(multiline_msg)

        # Act
        result = rule.evaluate(exception)

        # Assert
        assert result is True

    def test_evaluate__with_empty_exception_message__returns_false(self, exception_rule: ExceptionRegexRule) -> None:
        """Test evaluation with exception that has empty message."""
        # Arrange
        exception = ValueError("")

        # Act
        result = exception_rule.evaluate(exception)

        # Assert
        assert result is False

    def test_evaluate__with_different_exception_types__works_consistently(self) -> None:
        """Test evaluation works consistently with different exception types."""
        # Arrange
        rule = create_exception_regex_rule("failed")

        # Act & Assert - Test with different exception types
        assert rule.evaluate(ValueError("Operation failed")) is True
        assert rule.evaluate(SamaraWorkflowError("Process failed")) is True
        assert rule.evaluate(KeyError("Key lookup failed")) is True
        assert rule.evaluate(Exception("Generic operation failed")) is True

    def test_evaluate__with_none_exception_converted_to_string__returns_result(self) -> None:
        """Test evaluation handles exception conversion to string properly."""
        # Arrange
        rule = create_exception_regex_rule("None")

        # Create custom exception that str() returns "None"
        class CustomException(Exception):
            """Custom exception for testing."""

            def __str__(self) -> str:
                return "None"

        exception = CustomException()

        # Act
        result = rule.evaluate(exception)

        # Assert
        assert result is True

    def test_evaluate__with_empty_pattern__returns_true(self) -> None:
        """Test evaluation returns True when pattern is empty (edge case behavior)."""
        # Arrange
        rule = create_exception_regex_rule("")
        exception = ValueError("Any exception message")

        # Act
        result = rule.evaluate(exception)

        # Assert
        assert result is True

    def test_evaluate__with_invalid_regex_pattern__raises_regex_error(self) -> None:
        """Test evaluation raises error when regex pattern is invalid."""
        # Arrange
        rule = create_exception_regex_rule(r"[unclosed bracket")
        exception = ValueError("Test message")

        # Assert
        with pytest.raises(re.error):
            # Act
            rule.evaluate(exception)
