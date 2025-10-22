"""Send alerts through HTTP webhooks to external endpoints.

This module implements HTTP-based alert delivery for sending notifications
to webhook endpoints or external HTTP services. It enables configuration-driven
alerting without requiring code changes to support different webhook targets,
custom headers, or retry strategies.
"""

import logging
from typing import Literal

from pydantic import Field
from typing_extensions import override

from samara.alert.channels.base import ChannelModel
from samara.utils.http import HttpBase
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class HttpChannel(HttpBase, ChannelModel):
    """Send alert messages to HTTP endpoints or webhooks.

    Delivers alerts to HTTP endpoints with configurable methods, headers, and
    retry behavior. Supports custom HTTP headers for authentication and content
    negotiation, making it suitable for integration with external systems, APIs,
    and webhook platforms.

    Inherits HTTP request handling from HttpBase, providing automatic retry logic
    with configurable delays and timeout behavior. The channel type is automatically
    set to "http" for use in alert channel discrimination.

    Attributes:
        channel_type: Literal "http" discriminator for channel type resolution.
        id_: Unique identifier for this alert channel.
        description: Human-readable description of the channel's purpose.
        enabled: Whether this channel is active for sending alerts.
        url: HTTP endpoint URL where alerts are sent.
        method: HTTP method (POST, PUT, etc.) for the alert request.
        headers: Optional dictionary of HTTP headers (Authorization, Content-Type, etc.).
        timeout: Request timeout in seconds.
        retry: Retry configuration with max attempts and delay between retries.

    Example:
        >>> from samara.alert.channels.http import HttpChannel
        >>> config = {
        ...     "id": "webhook_alerts",
        ...     "description": "Send alerts to external webhook",
        ...     "enabled": True,
        ...     "type": "http",
        ...     "url": "https://alerts.example.com/webhook",
        ...     "method": "POST",
        ...     "headers": {"Authorization": "Bearer token123"},
        ...     "timeout": 10,
        ...     "retry": {"max_attempts": 2, "delay_in_seconds": 5}
        ... }
        >>> channel = HttpChannel(**config)
        >>> channel.alert("Error", "Database connection failed")

        **Configuration in JSON:**
        ```
        {
            "alert": {
                "channels": [
                    {
                        "id": "webhook_alerts",
                        "description": "Send alerts to external webhook",
                        "enabled": true,
                        "type": "http",
                        "url": "https://alerts.example.com/webhook",
                        "method": "POST",
                        "headers": {
                            "Authorization": "Bearer token123",
                            "Content-Type": "application/json"
                        },
                        "timeout": 10,
                        "retry": {
                            "max_attempts": 2,
                            "delay_in_seconds": 5
                        }
                    }
                ]
            }
        }
        ```

        **Configuration in YAML:**
        ```
        alert:
          channels:
            - id: webhook_alerts
              description: Send alerts to external webhook
              enabled: true
              type: http
              url: https://alerts.example.com/webhook
              method: POST
              headers:
                Authorization: Bearer token123
                Content-Type: application/json
              timeout: 10
              retry:
                max_attempts: 2
                delay_in_seconds: 5
        ```

    Note:
        The alert payload is automatically formatted as JSON with "title" and
        "message" fields. Ensure the receiving endpoint can handle this format.
        Failed requests are retried according to the configured retry policy
        with exponential backoff delays between attempts.
    """

    channel_type: Literal["http"] = Field(..., description="Channel type discriminator")

    @override
    def _alert(self, title: str, body: str) -> None:
        """Send an alert message via HTTP request to the configured endpoint.

        Constructs a JSON payload with the alert title and message, then sends
        it to the configured HTTP endpoint using the specified method. Request
        execution includes automatic retry logic with exponential backoff for
        transient failures.

        Args:
            title: Alert title used in the JSON payload's "title" field.
            body: Alert message body used in the JSON payload's "message" field.

        Raises:
            requests.RequestException: If the HTTP request fails after exhausting
                all configured retry attempts. Includes connection errors, timeouts,
                and HTTP error status codes (4xx, 5xx).

        Note:
            The JSON payload structure sent to the endpoint:
            ```
            {
                "title": "Alert Title",
                "message": "Alert message body"
            }
            ```
            Ensure the receiving HTTP endpoint expects this payload format.
        """
        payload = {"title": title, "message": body}
        self._make_http_request(payload)
