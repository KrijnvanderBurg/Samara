"""Unit tests for the HttpAction class."""

from unittest.mock import patch

import pytest
import requests

from samara.exceptions import SamaraActionError
from samara.utils.http import Retry
from samara.workflow.actions.http import HttpAction


class TestHttpAction:
    """Test cases for HttpAction."""

    def test_execute__calls_execute_and_make_http_request(self) -> None:
        """Test that execute calls _execute which then calls _make_http_request."""
        # Arrange
        retry = Retry(max_attempts=2, delay_in_seconds=1)
        action = HttpAction.model_construct(
            id_="test_http_action",
            description="Test HTTP action",
            action_type="http",
            url="https://example.com/api",
            method="POST",
            timeout=30,
            retry=retry,
            payload={"test_key": "test_value"},
            enabled=True,
        )

        with patch.object(action, "_make_http_request") as mock_make_http_request:
            # Act
            action.execute()

            # Assert
            mock_make_http_request.assert_called_once_with({"test_key": "test_value"})

    def test_execute__with_empty_payload__calls_make_http_request_with_empty_dict(self) -> None:
        """Test that execute calls _make_http_request with empty dict when payload is not provided."""
        # Arrange
        retry = Retry(max_attempts=0, delay_in_seconds=1)
        action = HttpAction.model_construct(
            id_="test_http_action",
            description="Test HTTP action",
            action_type="http",
            url="https://example.com/api",
            method="GET",
            timeout=30,
            retry=retry,
            enabled=True,
        )

        with patch.object(action, "_make_http_request") as mock_make_http_request:
            # Act
            action.execute()

            # Assert
            mock_make_http_request.assert_called_once_with({})

    def test_execute__when_request_fails__wraps_as_samara_action_error(self) -> None:
        """Test that a RequestException from _make_http_request is wrapped as SamaraActionError."""
        # Arrange
        retry = Retry(max_attempts=0, delay_in_seconds=1)
        action = HttpAction.model_construct(
            id_="test_http_action",
            description="Test HTTP action",
            action_type="http",
            url="https://example.com/api",
            method="GET",
            timeout=30,
            retry=retry,
            enabled=True,
        )

        with patch.object(action, "_make_http_request", side_effect=requests.ConnectionError("network down")):
            # Act & Assert
            with pytest.raises(SamaraActionError, match="test_http_action.*network down"):
                action.execute()
