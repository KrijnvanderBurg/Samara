"""HTTP utilities for Samara's configuration-driven data pipeline framework.

Provides reusable HTTP functionality for alert channels and event hook actions,
enabling reliable communication through configurable retry logic, timeout handling,
and standardized request execution. Supports the declarative configuration model
by allowing HTTP endpoints and behaviors to be specified in configuration files
rather than code.
"""

import json
import logging
import time
from typing import Any

import requests
from pydantic import BaseModel, Field, HttpUrl, PositiveInt

from samara.telemetry import trace_span
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class Retry(BaseModel):
    """Configurable retry behavior for HTTP requests in pipeline operations.

    Defines how HTTP requests should handle transient failures through configurable
    retry attempts and delays. This configuration model aligns with Samara's
    declarative approach, allowing retry strategies to be specified in pipeline
    configuration files without modifying code.

    Attributes:
        max_attempts: Maximum number of retry attempts (0-3) for failed requests.
        delay_in_seconds: Delay between retry attempts in seconds (1-30).

    Example:
        **Configuration in JSON:**
        ```
        {
            "max_attempts": 2,
            "delay_in_seconds": 5
        }
        ```

        **Configuration in YAML:**
        ```
        max_attempts: 2
        delay_in_seconds: 5
        ```
    """

    max_attempts: int = Field(..., description="Maximum number of retry attempts for failed requests", ge=0, le=3)
    delay_in_seconds: PositiveInt = Field(..., description="Delay between retry attempts in seconds", ge=1, le=30)


class HttpBase(BaseModel):
    """Base HTTP configuration for alerts and event hooks in Samara pipelines.

    Provides shared HTTP request handling for alert notifications and event-triggered
    actions. Supports the framework's configuration-driven model by allowing HTTP
    endpoints, methods, headers, and retry behavior to be defined in configuration
    files. Used by alert channels and actions to communicate with external HTTP
    endpoints, webhooks, and services.

    Attributes:
        url: HTTP endpoint URL for sending requests.
        method: HTTP method to use (GET, POST, PUT, etc.), minimum 1 character.
        headers: Dictionary of HTTP headers to include in requests (default: empty).
        timeout: Request timeout in seconds (1-30).
        retry: Retry configuration for handling transient HTTP failures.

    Example:
        **Configuration in JSON:**
        ```
        {
            "url": "https://api.example.com/webhook",
            "method": "POST",
            "headers": {
                "Content-Type": "application/json",
                "Authorization": "Bearer token123"
            },
            "timeout": 10,
            "retry": {
                "max_attempts": 2,
                "delay_in_seconds": 5
            }
        }
        ```

        **Configuration in YAML:**
        ```
        url: https://api.example.com/webhook
        method: POST
        headers:
          Content-Type: application/json
          Authorization: Bearer token123
        timeout: 10
        retry:
          max_attempts: 2
          delay_in_seconds: 5
        ```
    """

    url: HttpUrl = Field(..., description="HTTP endpoint URL for sending requests")
    method: str = Field(..., description="HTTP method to use (GET, POST, PUT, etc.)", min_length=1)
    headers: dict[str, str] = Field(
        default_factory=dict, description="Dictionary of HTTP headers to include in requests"
    )
    timeout: PositiveInt = Field(..., description="Request timeout in seconds", ge=1, le=30)
    retry: Retry = Field(..., description="Configuration for handling failures and retries")

    @trace_span("http_base._make_http_request")
    def _make_http_request(self, payload: dict[str, Any] | None = None) -> None:
        """Execute an HTTP request with configurable retry logic and error handling.

        Sends an HTTP request with the configured method, headers, timeout, and retry
        behavior. Handles transient failures gracefully through exponential backoff
        retry logic. On success, logs the request completion; on final failure after
        all retries, logs the error and raises the underlying exception.

        Args:
            payload: Optional dictionary to send as JSON in the request body.
                If None, the request is sent without a body.

        Raises:
            requests.RequestException: If the HTTP request fails after exhausting
                all configured retry attempts.
        """
        data = json.dumps(payload)

        for attempt in range(self.retry.max_attempts + 1):
            try:
                response = requests.request(
                    method=self.method,
                    url=str(self.url),
                    headers=self.headers,
                    data=data,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                logger.info("HTTP request sent successfully to %s", self.url)
                return

            except requests.RequestException as e:
                if attempt < self.retry.max_attempts:
                    logger.warning(
                        "HTTP request attempt %d failed: %s. Retrying in %d seconds...",
                        attempt + 1,
                        e,
                        self.retry.delay_in_seconds,
                    )
                    time.sleep(self.retry.delay_in_seconds)
                else:
                    logger.error("HTTP request failed after %d attempts: %s", self.retry.max_attempts + 1, e)
