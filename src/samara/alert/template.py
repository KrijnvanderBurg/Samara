"""Alert message template formatting and customization.

This module provides configuration-driven templates for customizing alert
message content. Templates enable consistent formatting of alert titles and
bodies across all alert channels through simple JSON/YAML configuration.
"""

import logging

from pydantic import Field

from samara import BaseModel
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class AlertTemplate(BaseModel):
    """Format alert messages with customizable title and body templates.

    This class applies consistent formatting to alert titles and message bodies
    by wrapping them with configurable prefixes and suffixes. Templates enable
    users to add custom branding, metadata, or formatting to all alerts without
    modifying individual channels.

    Attributes:
        prepend_title: Prefix text added to all alert titles.
        append_title: Suffix text added to all alert titles.
        prepend_body: Prefix text added to all alert message bodies.
        append_body: Suffix text added to all alert message bodies.

    Example:
        >>> template = AlertTemplate(
        ...     prepend_title="[ALERT] ",
        ...     append_title="",
        ...     prepend_body="Priority: HIGH\\n",
        ...     append_body="\\n---\\nEnd of message"
        ... )
        >>> template.format_title("Database Connection Failed")
        '[ALERT] Database Connection Failed'

        **Configuration in JSON:**
        ```
        {
            "channels": [
                {
                    "type": "email",
                    "recipients": ["admin@example.com"],
                    "template": {
                        "prepend_title": "[ALERT] ",
                        "append_title": "",
                        "prepend_body": "Priority: HIGH\\n",
                        "append_body": "\\n---\\nEnd of message"
                    }
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        channels:
          - type: email
            recipients:
              - admin@example.com
            template:
              prepend_title: "[ALERT] "
              append_title: ""
              prepend_body: "Priority: HIGH\\n"
              append_body: "\\n---\\nEnd of message"
        ```
    """

    prepend_title: str = Field(..., description="Text to prepend to alert titles")
    append_title: str = Field(..., description="Text to append to alert titles")
    prepend_body: str = Field(..., description="Text to prepend to alert messages")
    append_body: str = Field(..., description="Text to append to alert messages")

    def format_body(self, message: str) -> str:
        """Apply template formatting to an alert message body.

        Wraps the provided message with configured prefix and suffix text,
        enabling consistent formatting across all alert channels.

        Args:
            message: The alert message body to format.

        Returns:
            The formatted message with prepend and append templates applied.
        """
        return f"{self.prepend_body}{message}{self.append_body}"

    def format_title(self, title: str) -> str:
        """Apply template formatting to an alert title.

        Wraps the provided title with configured prefix and suffix text,
        enabling consistent formatting across all alert channels.

        Args:
            title: The alert title to format.

        Returns:
            The formatted title with prepend and append templates applied.
        """
        return f"{self.prepend_title}{title}{self.append_title}"
