"""HTTP action for making HTTP requests in ETL pipeline event hooks.

This module implements the HTTP action that enables ETL pipelines to communicate
with external HTTP endpoints when pipeline events occur (onStart, onSuccess,
onFailure, onFinally). It supports configurable HTTP methods, custom headers,
payloads, timeouts, and retry logic for robust external integrations.
"""

import logging
from typing import Any, Literal

from pydantic import Field
from typing_extensions import override

from samara.utils.http import HttpBase
from samara.utils.logger import get_logger
from samara.workflow.actions.base import ActionBase

logger: logging.Logger = get_logger(__name__)


class HttpAction(HttpBase, ActionBase):
    """Send HTTP requests to external endpoints when pipeline events occur.

    This action enables communication with external HTTP services (webhooks,
    APIs, etc.) as part of pipeline event hooks. It supports custom headers,
    request payloads, configurable timeouts, and automatic retry logic for
    reliable external integrations in the configuration-driven pipeline.

    Attributes:
        id_: Unique identifier for this action (inherited from ActionBase).
        description: Human-readable description of this action's purpose
            (inherited from ActionBase).
        enabled: Whether this action executes when the event is triggered
            (inherited from ActionBase).
        url: HTTP endpoint URL to send requests to (inherited from HttpBase).
        method: HTTP method to use: GET, POST, PUT, PATCH, DELETE, etc.
            (inherited from HttpBase).
        headers: Optional HTTP headers to include in requests
            (inherited from HttpBase).
        timeout: Request timeout in seconds, range 1-30 (inherited from HttpBase).
        retry: Retry configuration with max_attempts and delay_in_seconds
            (inherited from HttpBase).
        action_type: Always "http" for HTTP actions.
        payload: Optional JSON payload to send in the request body.

    Example:
        Execute when a pipeline succeeds. Include the parent hook key:

        **Configuration in JSON:**
        ```
        {
            "onSuccess": [
                {
                    "id": "notify-webhook",
                    "description": "Notify external webhook on pipeline completion",
                    "enabled": true,
                    "action_type": "http",
                    "url": "https://api.example.com/webhook/pipeline-complete",
                    "method": "POST",
                    "headers": {
                        "Content-Type": "application/json",
                        "Authorization": "Bearer secret-token"
                    },
                    "payload": {
                        "status": "success",
                        "timestamp": "2025-10-22T10:30:00Z"
                    },
                    "timeout": 30,
                    "retry": {
                        "max_attempts": 2,
                        "delay_in_seconds": 5
                    }
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        onSuccess:
          - id: notify-webhook
            description: Notify external webhook on pipeline completion
            enabled: true
            action_type: http
            url: https://api.example.com/webhook/pipeline-complete
            method: POST
            headers:
              Content-Type: application/json
              Authorization: Bearer secret-token
            payload:
              status: success
              timestamp: 2025-10-22T10:30:00Z
            timeout: 30
            retry:
              max_attempts: 2
              delay_in_seconds: 5
        ```

    Note:
        - The action executes only if enabled is true
        - Payloads are sent as JSON regardless of method type
        - Retry logic applies exponential backoff between attempts
        - Failed actions log warnings but do not fail the pipeline execution
    """

    action_type: Literal["http"] = Field(..., description="Action type discriminator")
    payload: dict[str, Any] = Field(default_factory=dict, description="Optional payload data to send in the request")

    @override
    def _execute(self) -> None:
        """Send the HTTP request with configured parameters and retry logic.

        Makes an HTTP request to the configured endpoint using the specified
        method, headers, payload, and timeout. If the request fails, automatic
        retries occur with configurable delays between attempts. Logs both
        successful requests and failures for observability.

        Raises:
            requests.RequestException: If the HTTP request fails after exhausting
                all configured retry attempts, including connection errors,
                timeouts, or non-2xx HTTP responses.

        Note:
            Even if this action fails, the pipeline continues execution as
            actions are not critical to pipeline success. Check logs for
            details if requests fail unexpectedly.
        """
        logger.info("Executing HTTP action: %s", self.id_)
        self._make_http_request(self.payload)
        logger.info("HTTP action completed: %s", self.id_)
