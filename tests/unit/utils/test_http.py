"""Tests for HTTP utility classes and functionality.

These tests verify HttpBase class functionality, Retry model validation,
HTTP request sending, retry logic, and error handling scenarios.
"""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests
from pydantic import ValidationError
from samara.utils.http import HttpBase, Retry

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="valid_http_config")
def fixture_valid_http_config() -> dict[str, Any]:
    """Provide a valid HTTP base configuration."""
    return {
        "url": "https://hooks.slack.com/services/webhook",
        "method": "POST",
        "headers": {"Content-Type": "application/json", "Authorization": "Bearer token123"},
        "timeout": 30,
        "retry": {"max_attempts": 2, "delay_in_seconds": 1},
    }


@pytest.fixture(name="valid_retry_config")
def fixture_valid_retry_config() -> dict[str, Any]:
    """Provide a valid retry configuration."""
    return {"max_attempts": 3, "delay_in_seconds": 2}


# =========================================================================== #
# ========================== RETRY MODEL TESTS ============================= #
# =========================================================================== #


class TestRetryValidation:
    """Test Retry model validation and instantiation."""

    def test_create_retry__with_valid_config__succeeds(self, valid_retry_config: dict[str, Any]) -> None:
        """Test Retry creation with valid configuration."""
        # Act
        retry = Retry(**valid_retry_config)

        # Assert
        assert retry.max_attempts == 3
        assert retry.delay_in_seconds == 2

    def test_create_retry__with_max_attempts_too_high__raises_validation_error(
        self, valid_retry_config: dict[str, Any]
    ) -> None:
        """Test Retry creation fails when max_attempts exceeds limit."""
        # Arrange
        valid_retry_config["max_attempts"] = 4

        # Assert
        with pytest.raises(ValidationError):
            Retry(**valid_retry_config)

    def test_create_retry__with_negative_max_attempts__raises_validation_error(
        self, valid_retry_config: dict[str, Any]
    ) -> None:
        """Test Retry creation fails with negative max_attempts."""
        # Arrange
        valid_retry_config["max_attempts"] = -1

        # Assert
        with pytest.raises(ValidationError):
            Retry(**valid_retry_config)

    def test_create_retry__with_negative_delay__raises_validation_error(
        self, valid_retry_config: dict[str, Any]
    ) -> None:
        """Test Retry creation fails with negative delay."""
        # Arrange
        valid_retry_config["delay_in_seconds"] = -1

        # Assert
        with pytest.raises(ValidationError):
            Retry(**valid_retry_config)


# =========================================================================== #
# ===================== HTTP BASE VALIDATION TESTS ======================= #
# =========================================================================== #


class TestHttpBaseValidation:
    """Test HttpBase model validation and instantiation."""

    def test_create_http_base__with_valid_config__succeeds(self, valid_http_config: dict[str, Any]) -> None:
        """Test HttpBase creation with valid configuration."""
        # Act
        http_base = HttpBase(**valid_http_config)

        # Assert
        assert str(http_base.url) == "https://hooks.slack.com/services/webhook"
        assert http_base.method == "POST"
        assert http_base.headers == {
            "Content-Type": "application/json",
            "Authorization": "Bearer token123",
        }
        assert http_base.timeout == 30
        assert isinstance(http_base.retry, Retry)
        assert http_base.retry.max_attempts == 2
        assert http_base.retry.delay_in_seconds == 1

    def test_create_http_base__with_invalid_url__raises_validation_error(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test HttpBase creation fails with invalid URL."""
        # Arrange
        valid_http_config["url"] = "not-a-valid-url"

        # Assert
        with pytest.raises(ValidationError):
            HttpBase(**valid_http_config)

    def test_create_http_base__with_zero_timeout__raises_validation_error(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test HttpBase creation fails with zero timeout."""
        # Arrange
        valid_http_config["timeout"] = 0

        # Assert
        with pytest.raises(ValidationError):
            HttpBase(**valid_http_config)

    def test_create_http_base__with_negative_timeout__raises_validation_error(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test HttpBase creation fails with negative timeout."""
        # Arrange
        valid_http_config["timeout"] = -5

        # Assert
        with pytest.raises(ValidationError):
            HttpBase(**valid_http_config)

    def test_create_http_base__with_default_headers__uses_empty_dict(self, valid_http_config: dict[str, Any]) -> None:
        """Test HttpBase creation uses empty dict when headers not provided."""
        # Arrange
        del valid_http_config["headers"]

        # Act
        http_base = HttpBase(**valid_http_config)

        # Assert
        assert http_base.headers == {}

    def test_create_http_base__with_missing_required_field__raises_validation_error(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test HttpBase creation fails when required field is missing."""
        # Arrange
        del valid_http_config["url"]

        # Assert
        with pytest.raises(ValidationError):
            HttpBase(**valid_http_config)


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="http_base")
def fixture_http_base(valid_http_config: dict[str, Any]) -> HttpBase:
    """Create HttpBase instance from valid configuration."""
    return HttpBase(**valid_http_config)


# =========================================================================== #
# ======================= HTTP REQUEST TESTS ============================== #
# =========================================================================== #


class TestHttpBaseRequest:
    """Test HttpBase HTTP request functionality."""

    def test_make_http_request__with_valid_payload__sends_http_request_successfully(self, http_base: HttpBase) -> None:
        """Test successful HTTP request sending with proper request parameters."""
        # Mock the requests library because we don't want to make real HTTP calls
        with patch("samara.utils.http.requests.request") as mock_request:
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_request.return_value = mock_response

            # Act
            payload = {"title": "Critical System Alert", "message": "Database connection failed"}
            http_base._make_http_request(payload=payload)

            # Assert HTTP request made with correct parameters
            mock_request.assert_called_once_with(
                method="POST",
                url="https://hooks.slack.com/services/webhook",
                headers={"Content-Type": "application/json", "Authorization": "Bearer token123"},
                data='{"title": "Critical System Alert", "message": "Database connection failed"}',
                timeout=30,
            )
            mock_response.raise_for_status.assert_called_once()

    def test_make_http_request__with_get_method__uses_get_request(self, valid_http_config: dict[str, Any]) -> None:
        """Test request uses GET method when configured."""
        # Arrange
        valid_http_config["method"] = "GET"
        http_base = HttpBase(**valid_http_config)

        # Mock the requests library because we don't want to make real HTTP calls
        with patch("samara.utils.http.requests.request") as mock_request:
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_request.return_value = mock_response

            # Act
            http_base._make_http_request(payload={"test": "data"})

            # Assert
            mock_request.assert_called_once()
            _, kwargs = mock_request.call_args
            assert kwargs["method"] == "GET"

    def test_make_http_request__with_empty_headers__sends_without_headers(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test request works with empty headers dictionary."""
        # Arrange
        valid_http_config["headers"] = {}
        http_base = HttpBase(**valid_http_config)

        # Mock the requests library because we don't want to make real HTTP calls
        with patch("samara.utils.http.requests.request") as mock_request:
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_request.return_value = mock_response

            # Act
            http_base._make_http_request(payload={"test": "data"})

            # Assert
            mock_request.assert_called_once()
            _, kwargs = mock_request.call_args
            assert kwargs["headers"] == {}

    def test_make_http_request__with_special_characters__handles_encoding_correctly(self, http_base: HttpBase) -> None:
        """Test request handling with special characters in payload."""
        # Mock the requests library because we don't want to make real HTTP calls
        with patch("samara.utils.http.requests.request") as mock_request:
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_request.return_value = mock_response

            # Act
            payload = {"title": "Ålørt: Üñíçødé Tëst", "message": "Body with émojis 🔥 and spëcial chars: áéíóú"}
            http_base._make_http_request(payload=payload)

            # Assert
            mock_request.assert_called_once()
            _, kwargs = mock_request.call_args
            # JSON dumps escapes non-ASCII characters by default
            expected_json = (
                '{"title": "\\u00c5l\\u00f8rt: \\u00dc\\u00f1\\u00ed\\u00e7\\u00f8d\\u00e9 T\\u00ebst", '
                '"message": "Body with \\u00e9mojis \\ud83d\\udd25 and sp\\u00ebcial chars: '
                '\\u00e1\\u00e9\\u00ed\\u00f3\\u00fa"}'
            )
            assert kwargs["data"] == expected_json

    def test_make_http_request__with_empty_payload__sends_with_empty_data(self, http_base: HttpBase) -> None:
        """Test request sending with empty payload results in empty JSON object."""
        # Mock the requests library because we don't want to make real HTTP calls
        with patch("samara.utils.http.requests.request") as mock_request:
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_request.return_value = mock_response

            # Act
            http_base._make_http_request(payload={})

            # Assert
            mock_request.assert_called_once()
            _, kwargs = mock_request.call_args
            assert kwargs["data"] == "{}"


# =========================================================================== #
# ============================ RETRY TESTS ================================= #
# =========================================================================== #


class TestHttpBaseRetry:
    """Test HttpBase retry logic and error handling."""

    def test_make_http_request__with_retry_success_on_second_attempt__retries_and_succeeds(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test request succeeds on retry after initial failure."""
        # Arrange
        valid_http_config["retry"] = {"max_attempts": 2, "delay_in_seconds": 1}
        http_base = HttpBase(**valid_http_config)

        # Mock the requests library to fail first, succeed second
        with (
            patch("samara.utils.http.requests.request") as mock_request,
            patch("samara.utils.http.time.sleep") as mock_sleep,
        ):
            # First call fails, second succeeds
            mock_response_success = MagicMock()
            mock_response_success.raise_for_status.return_value = None
            mock_request.side_effect = [
                requests.ConnectionError("Network error"),
                mock_response_success,
            ]

            # Act
            http_base._make_http_request(payload={"test": "data"})

            # Assert
            assert mock_request.call_count == 2
            mock_sleep.assert_called_once_with(1)

    def test_make_http_request__with_all_retries_exhausted__raises_exception(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test request raises the underlying exception when all retries are exhausted."""
        # Arrange
        valid_http_config["retry"] = {"max_attempts": 1, "delay_in_seconds": 1}
        http_base = HttpBase(**valid_http_config)

        # Mock the requests library to always fail
        with (
            patch("samara.utils.http.requests.request") as mock_request,
            patch("samara.utils.http.time.sleep"),
        ):
            mock_request.side_effect = requests.ConnectionError("Persistent network error")

            # Act & Assert
            with pytest.raises(requests.ConnectionError):
                http_base._make_http_request({"test": "data"})

            assert mock_request.call_count == 2

    def test_make_http_request__with_no_retries__raises_exception(self, valid_http_config: dict[str, Any]) -> None:
        """Test request raises immediately when max_attempts is 0 and the request fails."""
        # Arrange
        valid_http_config["retry"] = {"max_attempts": 0, "delay_in_seconds": 1}
        http_base = HttpBase(**valid_http_config)

        # Mock the requests library to fail
        with patch("samara.utils.http.requests.request") as mock_request:
            mock_request.side_effect = requests.ConnectionError("Network error")

            # Act & Assert
            with pytest.raises(requests.ConnectionError):
                http_base._make_http_request({"test": "data"})

            # Verify only one attempt was made
            assert mock_request.call_count == 1

    def test_make_http_request__with_delay_between_retries__sleeps_correct_duration(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test request waits correct duration between retry attempts."""
        # Arrange
        valid_http_config["retry"] = {"max_attempts": 2, "delay_in_seconds": 5}
        http_base = HttpBase(**valid_http_config)

        # Mock the requests library to always fail
        with (
            patch("samara.utils.http.requests.request") as mock_request,
            patch("samara.utils.http.time.sleep") as mock_sleep,
        ):
            mock_request.side_effect = requests.ConnectionError("Network error")

            # Act & Assert
            with pytest.raises(requests.ConnectionError):
                http_base._make_http_request({"test": "data"})

            # Assert correct sleep duration called between retries
            assert mock_sleep.call_count == 2
            mock_sleep.assert_called_with(5)

    def test_make_http_request__with_maximum_retries_and_all_fail__exhausts_all_attempts(
        self, valid_http_config: dict[str, Any]
    ) -> None:
        """Test request exhausts all attempts when max_attempts is at maximum (3) and all fail."""
        # Arrange - Use maximum allowed retries
        valid_http_config["retry"] = {"max_attempts": 3, "delay_in_seconds": 1}
        http_base = HttpBase(**valid_http_config)

        # Mock the requests library to always fail
        with (
            patch("samara.utils.http.requests.request") as mock_request,
            patch("samara.utils.http.time.sleep"),
        ):
            mock_request.side_effect = requests.ConnectionError("Network error")

            # Act & Assert - exception raised only after all attempts exhausted
            with pytest.raises(requests.ConnectionError):
                http_base._make_http_request({"test": "data"})

            # Assert all attempts made (initial + 3 retries = 4 total)
            assert mock_request.call_count == 4
