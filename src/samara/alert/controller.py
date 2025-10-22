"""Alert controller - Orchestrate alert processing and notification delivery.

This module provides the AlertController class that coordinates alert
processing, channel management, and trigger-based notification delivery
from configuration. It enables declarative alert workflows where
pipeline authors define channels, rules, and conditions through JSON or YAML
configuration files rather than code.
"""

import logging
from pathlib import Path
from typing import Any, Final, Self

from pydantic import Field, ValidationError

from samara import BaseModel
from samara.alert.channels import channel_union
from samara.alert.trigger import AlertTrigger
from samara.exceptions import SamaraAlertConfigurationError, SamaraIOError
from samara.utils.file import FileHandlerContext
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

ALERT: Final = "alert"


class AlertController(BaseModel):
    """Coordinate alert channels, trigger rules, and notification delivery.

    The AlertController is the root component of the alert system that manages
    alert channels (email, HTTP, file), trigger rules that determine when to
    alert, and templates for formatting alert messages. It evaluates conditions
    on pipeline events and delivers notifications through configured channels.

    Attributes:
        channels: List of configured alert channels (email, HTTP, file, etc.)
            for handling different alert destinations and delivery mechanisms.
        triggers: List of alert trigger rules that evaluate pipeline events
            and determine which channels should receive notifications.

    Example:
        Pipeline authors configure alerts in the pipeline definition file:

        **Configuration in JSON:**
        ```
            {
                "alert": {
                    "channels": [
                        {
                            "id": "email_alerts",
                            "type": "email",
                            "recipients": ["ops@example.com"],
                            "fromAddress": "alerts@pipeline.local"
                        },
                        {
                            "id": "http_webhook",
                            "type": "http",
                            "url": "https://hooks.example.com/alerts",
                            "method": "POST",
                            "retryPolicy": {
                                "maxAttempts": 3,
                                "backoffMs": 1000
                            }
                        }
                    ],
                    "triggers": [
                        {
                            "id": "on_failure",
                            "when": ["PipelineFailure"],
                            "channelIds": ["email_alerts", "http_webhook"],
                            "templateId": "failure_template"
                        }
                    ]
                }
            }
        ```

        **Configuration in YAML:**
        ```
            alert:
              channels:
                - id: email_alerts
                  type: email
                  recipients:
                    - ops@example.com
                  fromAddress: alerts@pipeline.local
                - id: http_webhook
                  type: http
                  url: https://hooks.example.com/alerts
                  method: POST
                  retryPolicy:
                    maxAttempts: 3
                    backoffMs: 1000
              triggers:
                - id: on_failure
                  when:
                    - PipelineFailure
                  channelIds:
                    - email_alerts
                    - http_webhook
                  templateId: failure_template
        ```

    Note:
        The alert section must be defined at the root level of the pipeline
        configuration file. Channels and triggers are evaluated in the order
        defined. Each trigger references channel IDs that must exist in the
        channels list, otherwise validation will fail.
    """

    channels: list[channel_union] = Field(..., description="List of configured channels")
    triggers: list[AlertTrigger] = Field(..., description="List of alert trigger rules")

    @classmethod
    def from_file(cls, filepath: Path) -> Self:
        """Load alert configuration from a JSON or YAML file.

        Reads and parses an alert configuration file, then instantiates an
        AlertController with the defined channels, triggers, and rules. Supports
        both JSON and YAML formats with automatic detection based on file extension.

        Args:
            filepath: Path to the alert configuration file (JSON or YAML format).
                The file must contain an "alert" key at the root level with
                "channels" and "triggers" sections.

        Returns:
            Fully configured AlertController instance ready for processing alerts.

        Raises:
            SamaraIOError: If file cannot be read (file not found, permission denied,
                read timeout, etc.) or if the file is not valid JSON/YAML.
            SamaraAlertConfigurationError: If the configuration is invalid, missing
                the required "alert" section, or contains invalid channel/trigger
                definitions that fail validation.

        Example:
            >>> alert_config = AlertController.from_file(
            ...     Path("pipeline/config/alerts.json")
            ... )
            >>> alert_config.evaluate_trigger_and_alert(
            ...     title="Pipeline Failed",
            ...     body="Check logs for details",
            ...     exception=RuntimeError("Transform failed")
            ... )
        """
        logger.info("Creating AlertManager from file: %s", filepath)

        try:
            handler = FileHandlerContext.from_filepath(filepath=filepath)
            dict_: dict[str, Any] = handler.read()
        except (OSError, ValueError) as e:
            logger.error("Failed to read alert configuration file: %s", e)
            raise SamaraIOError(f"Cannot load alert configuration from '{filepath}': {e}") from e

        try:
            alert = cls(**dict_[ALERT])
            logger.info("Successfully created AlertManager from configuration file: %s", filepath)
            return alert
        except KeyError as e:
            raise SamaraAlertConfigurationError(f"Missing 'alert' section in configuration file '{filepath}'") from e
        except ValidationError as e:
            raise SamaraAlertConfigurationError(f"Invalid alert configuration in file '{filepath}': {e}") from e

    def evaluate_trigger_and_alert(self, title: str, body: str, exception: Exception) -> None:
        """Evaluate trigger conditions and deliver alerts through matched channels.

        Iterates through configured trigger rules, evaluates their conditions
        against the provided exception, and sends formatted alerts through all
        channels specified in matching triggers. This method enables event-driven
        notifications where pipeline failures, timeouts, or custom conditions
        automatically trigger appropriate alerts.

        Args:
            title: Alert title/subject line. Will be formatted by trigger templates
                before sending to channels.
            body: Alert message body containing details about the event. Will be
                formatted by trigger templates before sending to channels.
            exception: Exception that triggered the alert. Trigger rules use
                exception type and attributes to determine if conditions are met.

        Example:
            >>> try:
            ...     process_data()
            ... except Exception as e:
            ...     alert_controller.evaluate_trigger_and_alert(
            ...         title="Data Processing Failed",
            ...         body=f"Error: {str(e)}",
            ...         exception=e
            ...     )

        Note:
            Only trigger rules whose conditions match the exception type will fire.
            Each fired trigger sends the alert to all its configured channels.
            Alerts are sent sequentially; a failure in one channel does not
            prevent sending to subsequent channels.
        """

        for trigger in self.triggers:
            if trigger.should_fire(exception=exception):
                logger.debug("Trigger '%s' conditions met; processing alert", trigger.id_)

                formatted_title = trigger.template.format_title(title)
                formatted_body = trigger.template.format_body(body)

                for channel_id in trigger.channel_ids:
                    # Find the channel by id
                    for channel in self.channels:
                        if channel.id_ == channel_id:
                            formatted_title = trigger.template.format_title(title)
                            formatted_body = trigger.template.format_body(body)

                            # Send alert through the channel instance
                            channel.alert(title=formatted_title, body=formatted_body)
                            logger.debug("Sent alert to channel '%s'", channel.id_)
                            break
