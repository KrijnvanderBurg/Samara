"""Exception regex rule - Match exception messages against regex patterns.

This module provides a rule implementation that evaluates exceptions by
testing their messages against configurable regular expression patterns,
enabling flexible exception-based alert triggering in data pipelines.
"""

import logging
import re
from typing import Literal

from pydantic import Field

from samara.alert.rules.base import AlertRule
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class ExceptionRegexRule(AlertRule):
    """Match exception messages against regular expression patterns.

    This rule evaluates to True if the exception message matches the
    configured regular expression pattern. This enables dynamic alert
    triggering based on exception message content without modifying
    pipeline code.

    Attributes:
        rule_type: Discriminator for rule type selection (always "exception_regex").
        pattern: Regular expression pattern to match against exception messages.
            If empty, the rule evaluates to True (no filtering).

    Example:
        >>> from samara.alert.rules import ExceptionRegexRule
        >>> rule = ExceptionRegexRule(
        ...     rule_type="exception_regex",
        ...     pattern=r".*connection.*timeout.*"
        ... )
        >>> rule.evaluate(Exception("Connection timeout occurred"))
        True

        **Configuration in JSON:**
        ```
        {
            "rules": [
                {
                    "rule_type": "exception_regex",
                    "pattern": ".*connection.*timeout.*"
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        rules:
          - rule_type: exception_regex
            pattern: ".*connection.*timeout.*"
        ```

    Note:
        Pattern matching is performed using Python's `re.search()`, which
        finds the pattern anywhere in the exception message. Use anchors
        (^ and $) for exact matching if needed.
    """

    rule_type: Literal["exception_regex"] = Field(..., description="Rule type discriminator")
    pattern: str = Field(..., description="Regular expression pattern to match against exception messages")

    def evaluate(self, exception: Exception) -> bool:
        """Determine if the exception message matches the regex pattern.

        Evaluates the exception by testing its string representation against
        the configured regular expression pattern using Python's `re.search()`.
        If no pattern is configured, returns True (no filtering applied).

        Args:
            exception: The exception to evaluate against the pattern.

        Returns:
            True if the pattern matches the exception message or no pattern
            is configured, False if the pattern does not match.
        """
        if not self.pattern:
            logger.debug("No exception_regex pattern configured; skipping regex check.")
            return True

        message = str(exception)

        if re.search(self.pattern, message):
            logger.debug("Exception message matches regex: '%s'", self.pattern)
            return True

        logger.debug("Exception message '%s' does not match regex: '%s'", message, self.pattern)
        return False
