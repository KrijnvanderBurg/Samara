"""File alert channel - Write alerts to file destinations.

This module provides file-based alert delivery enabling users to persist
alerts to log files or other file destinations. It supports configurable
file paths and handles file system operations with appropriate error reporting."""

import logging
from pathlib import Path
from typing import Literal

from pydantic import Field
from typing_extensions import override

from samara.alert.channels.base import ChannelModel
from samara.telemetry import trace_span
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class FileChannel(ChannelModel):
    """Write alert messages to file destinations.

    This channel implementation persists alerts to configured file paths,
    creating files if they do not exist. Alerts are appended line-by-line
    with title and body combined in a readable format.

    Attributes:
        channel_type: Always "file" for file channels.
        file_path: Path to the destination file where alerts are written.

    Example:
        >>> from pathlib import Path
        >>> config = {
        ...     "channel": {
        ...         "type": "file",
        ...         "filePath": "/var/log/alerts.log"
        ...     }
        ... }
        >>> channel = FileChannel(**config["channel"])
        >>> channel._alert("Pipeline Failed", "Job 'daily_sync' failed at 14:30")

        **Configuration in JSON:**
        ```
        {
            "alerts": [
                {
                    "channel": {
                        "type": "file",
                        "filePath": "/var/log/pipeline_alerts.log"
                    },
                    "triggers": [...]
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        alerts:
          - channel:
              type: file
              filePath: /var/log/pipeline_alerts.log
            triggers: [...]
        ```

    Note:
        The parent directory of the specified file path must exist and
        be writable. File write failures are logged as errors and re-raised.
    """

    channel_type: Literal["file"] = Field(..., description="Channel type discriminator")
    file_path: Path = Field(..., description="Path to the file where alerts should be written")

    @override
    @trace_span("file_channel._alert")
    def _alert(self, title: str, body: str) -> None:
        """Write an alert message to the configured file.

        Appends the alert to the file with title and body separated by a colon,
        creating the file if it does not exist. Each alert is written as a new
        line with automatic newline termination.

        Args:
            title: The alert title to identify the alert type or source.
            body: The alert message containing details about the event.

        Raises:
            OSError: When file write operations fail due to permission errors,
                path issues, or disk space problems.

        Note:
            The parent directory must exist before calling this method.
            Check that the configured file_path points to a writable location.
        """
        try:
            with open(self.file_path, "a", encoding="utf-8") as file:
                file.write(f"{title}: {body}\n")
            logger.info("Alert written to file: %s", self.file_path)
        except OSError as e:
            logger.error("Failed to write alert to file %s: %s", self.file_path, e)
            raise
