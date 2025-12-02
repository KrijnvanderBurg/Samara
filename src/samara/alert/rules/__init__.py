"""Rule-based system for alert trigger conditions.

This module provides alert trigger rules that define conditions for when
alerts should be fired within a data pipeline. Rules evaluate exceptions
and workflow state to determine alert triggering based on configuration.

The rule system follows a discriminator-based union pattern, enabling
pipeline authors to mix different rule types within the same configuration.
Each rule type handles a specific evaluation strategy for alert conditions.
"""

from typing import Annotated

from pydantic import Discriminator

from .env_vars_matches import EnvVarsMatchesRule
from .exception_regex import ExceptionRegexRule

__all__ = [
    "EnvVarsMatchesRule",
    "ExceptionRegexRule",
]


AlertRuleUnion = Annotated[
    EnvVarsMatchesRule | ExceptionRegexRule,
    Discriminator("rule_type"),
]
