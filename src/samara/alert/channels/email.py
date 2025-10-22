"""Email alert channel - send alerts via SMTP.

This module provides SMTP-based email alerting, enabling users to send
alert notifications to one or more recipients through configured email servers
with full authentication and failure handling support.
"""

import logging
import smtplib
from email.mime.text import MIMEText
from typing import Literal

from pydantic import EmailStr, Field, SecretStr, StrictInt, StrictStr
from typing_extensions import override

from samara.alert.channels.base import ChannelModel
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class EmailChannel(ChannelModel):
    """Send alerts via email using SMTP.

    This channel delivers alert notifications through SMTP servers with
    full authentication support. It enables reliable email delivery to
    multiple recipients with configurable retry policies.

    Attributes:
        channel_type: Always "email" to identify this channel type.
        id: Unique identifier for this channel in alert configuration.
        description: Optional description of this channel's purpose.
        smtp_server: SMTP server hostname or IP address to use for sending.
        smtp_port: SMTP server port (typically 587 for TLS or 25 for plaintext).
        username: Username for SMTP authentication.
        password: Password for SMTP authentication (stored securely).
        from_email: Email address shown as the sender of alert messages.
        to_emails: One or more recipient email addresses for alerts.

    Example:
        **Configuration in JSON:**
        ```
        {
            "alerts": [
                {
                    "id": "email-alerts",
                    "type": "alert",
                    "channels": [
                        {
                            "id": "production-email",
                            "type": "email",
                            "smtpServer": "smtp.gmail.com",
                            "smtpPort": 587,
                            "username": "alerts@company.com",
                            "password": "app-password-here",
                            "fromEmail": "alerts@company.com",
                            "toEmails": ["ops@company.com", "admin@company.com"]
                        }
                    ]
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        alerts:
          - id: email-alerts
            type: alert
            channels:
              - id: production-email
                type: email
                smtpServer: smtp.gmail.com
                smtpPort: 587
                username: alerts@company.com
                password: app-password-here
                fromEmail: alerts@company.com
                toEmails:
                  - ops@company.com
                  - admin@company.com
        ```
    """

    channel_type: Literal["email"] = Field(..., description="Channel type discriminator")
    smtp_server: StrictStr = Field(..., description="SMTP server hostname or IP address", min_length=1)
    smtp_port: StrictInt = Field(..., description="SMTP server port number", gt=0, le=65535)
    username: StrictStr = Field(..., description="SMTP authentication username", min_length=1)
    password: SecretStr = Field(..., description="SMTP authentication password")
    from_email: EmailStr = Field(..., description="Sender email address")
    to_emails: list[EmailStr] = Field(..., description="List of recipient email addresses", min_length=1)

    @override
    def _alert(self, title: str, body: str) -> None:
        """Send alert notification via email to configured recipients.

        Constructs an email message with the provided title and body,
        then delivers it through the configured SMTP server to all
        recipients specified in the channel configuration.

        Args:
            title: Alert title used as the email subject line.
            body: Alert message content used as the email body text.

        Raises:
            smtplib.SMTPException: If SMTP connection, authentication,
                or sending fails. Failures are logged with full error details.

        Example:
            >>> channel = EmailChannel(
            ...     smtp_server="smtp.example.com",
            ...     smtp_port=587,
            ...     username="bot@example.com",
            ...     password="secret",
            ...     from_email="bot@example.com",
            ...     to_emails=["admin@example.com"]
            ... )
            >>> channel._alert("Pipeline Failed", "Job run_2025 failed at 10:30 UTC")
        """
        # Create simple text message
        msg = MIMEText(body, "plain")
        msg["From"] = self.from_email
        msg["To"] = ", ".join(self.to_emails)
        msg["Subject"] = title

        try:
            # Create SMTP session
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.login(self.username, self.password.get_secret_value())
                server.sendmail(self.from_email, list(self.to_emails), msg.as_string())

            logger.info("Email alert sent successfully to %s", ", ".join(self.to_emails))
        except smtplib.SMTPException as exc:
            logger.error("Failed to send email alert: %s", exc)
            raise
