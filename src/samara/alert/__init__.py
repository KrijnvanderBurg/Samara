"""Alert module - Configure and execute notifications based on pipeline events.

This module provides the alerting system for configuration-driven pipelines,
enabling users to define alert rules and notification channels in their
pipeline configuration. It focuses on rule-based triggers and multi-channel
delivery, allowing alerts to be sent via email, HTTP webhooks, or files
when pipeline events (success, failure, start, end) occur.
"""

import logging

from samara.alert.controller import AlertController
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


__all__ = ["AlertController"]
