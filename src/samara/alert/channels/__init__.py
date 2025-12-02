"""Alert channels - Configurable notification destinations for pipeline events.

This package provides pluggable alert channels that enable notifications
through multiple communication platforms. Each channel implements a unified
interface supporting configuration-driven setup and consistent behavior
across different delivery mechanisms (email, HTTP webhooks, file logging).

Channel implementations handle transport-specific concerns while exposing
a common configuration schema, allowing pipeline authors to swap notification
targets without modifying pipeline definitions.
"""

import logging
from typing import Annotated

from pydantic import Discriminator

from samara.alert.channels.base import ChannelModel
from samara.alert.channels.email import EmailChannel
from samara.alert.channels.file import FileChannel
from samara.alert.channels.http import HttpChannel
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)

__all__ = ["ChannelModel", "ChannelUnion"]

ChannelUnion = Annotated[
    EmailChannel | HttpChannel | FileChannel,
    Discriminator("channel_type"),
]
