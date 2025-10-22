"""Alert channel base classes and interfaces.

This module provides the abstract base for implementing alert channels
in configuration-driven pipelines. It establishes a common interface
that enables different notification systems (email, HTTP webhooks, files)
to work consistently within the alert framework.
"""

import logging
from abc import ABC, abstractmethod

from pydantic import Field

from samara import BaseModel
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class ChannelModel(BaseModel, ABC):
    """Define abstract interface for alert channel implementations.

    Concrete channel implementations must inherit from this base class
    and implement the _alert method to handle channel-specific message
    delivery. Each subclass should define a channel_type field with a
    specific Literal value to ensure type safety and proper type discrimination
    in configuration-based routing.

    Attributes:
        id_: Unique identifier for the alert channel used in configuration
            and logs. Must be non-empty.
        description: Human-readable description of the alert channel purpose
            or configuration.
        enabled: Boolean flag controlling whether this channel is active.
            Disabled channels do not send alerts.

    Example:
        **Configuration in JSON:**
        ```
        {
            "channels": [
                {
                    "type": "email",
                    "id": "support_team",
                    "description": "Alert the support team",
                    "enabled": true,
                    "recipients": ["support@company.com"],
                    "smtpServer": "smtp.company.com",
                    "smtpPort": 587
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        channels:
          - type: email
            id: support_team
            description: Alert the support team
            enabled: true
            recipients:
              - support@company.com
            smtpServer: smtp.company.com
            smtpPort: 587
        ```

    Note:
        The `id_` field uses an alias to map to the configuration key `id`
        for cleaner configuration files while avoiding Python's built-in
        identifier naming.
    """

    id_: str = Field(..., alias="id", description="Unique identifier for the alert channel", min_length=1)
    description: str = Field(..., description="Description of the alert channel")
    enabled: bool = Field(..., description="Whether this channel is enabled")

    def alert(self, title: str, body: str) -> None:
        """Send alert message through this channel.

        This public interface method orchestrates the alert sending process
        by logging the attempt, invoking the channel-specific implementation,
        and logging the result. Subclasses should not override this method;
        instead implement _alert to define channel-specific behavior.

        Args:
            title: Alert subject or title line. Should concisely identify
                the alert type or trigger condition.
            body: Alert message content with details about the event,
                error, or condition that triggered the alert.

        Example:
            >>> channel.alert(
            ...     title="Pipeline Failed",
            ...     body="ETL job 'daily_sync' failed at 14:32 UTC"
            ... )
        """
        logger.debug("Sending alert through channel: %s", self.id_)
        self._alert(title=title, body=body)
        logger.info("Alert sent through %s channel", self.id_)

    @abstractmethod
    def _alert(self, title: str, body: str) -> None:
        """Implement channel-specific alert delivery logic.

        Subclasses must implement this method to handle sending alerts
        through their specific channel (email, HTTP, file, etc.).
        This method is called by the public alert() method after logging.

        Args:
            title: Alert subject line from the calling code.
            body: Alert message body with event details.

        Raises:
            Exception: Implementation may raise exceptions for delivery
                failures, which will propagate to the alert() caller.
                Callers should handle exceptions appropriately.

        Note:
            Implementations should not perform their own logging as
            alert() handles entry/exit logging. Focus only on the
            channel-specific delivery mechanism.
        """
