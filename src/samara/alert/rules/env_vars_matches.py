"""Environment variable matching rule for alert triggers.

This module provides a rule that matches current environment variables
against configured expected values, enabling conditional alert triggering
based on runtime environment conditions.
"""

import logging
import os
from typing import Literal

from pydantic import Field

from samara.alert.rules.base import AlertRule
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class EnvVarsMatchesRule(AlertRule):
    """Match alert trigger based on environment variable values.

    This rule evaluates to True if the current environment contains a
    configured variable that matches one of the expected values. Use this
    rule to conditionally activate alerts based on runtime environment
    conditions (e.g., environment name, feature flags, deployment context).

    Attributes:
        rule_type: Fixed discriminator value "env_vars_matches" for configuration.
        env_var_name: Name of the environment variable to inspect.
        env_var_values: List of acceptable values to match against.

    Example:
        >>> rule = EnvVarsMatchesRule(
        ...     rule_type="env_vars_matches",
        ...     env_var_name="ENVIRONMENT",
        ...     env_var_values=["production", "staging"]
        ... )
        >>> rule.evaluate(None)  # True if ENVIRONMENT is "production" or "staging"

        **Configuration in JSON:**
        ```
        {
            "triggers": [
                {
                    "type": "env_vars_matches",
                    "rules": [
                        {
                            "type": "env_vars_matches",
                            "env_var_name": "ENVIRONMENT",
                            "env_var_values": ["production", "staging"]
                        }
                    ]
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        triggers:
          - type: env_vars_matches
            rules:
              - type: env_vars_matches
                env_var_name: ENVIRONMENT
                env_var_values:
                  - production
                  - staging
        ```

    Note:
        If env_var_name is empty or not set, this rule evaluates to True,
        allowing the alert to proceed without environment filtering.
    """

    rule_type: Literal["env_vars_matches"] = Field(..., description="Rule type discriminator")
    env_var_name: str = Field(..., description="Name of the environment variable to check")
    env_var_values: list[str] = Field(..., description="List of expected values for the environment variable")

    def evaluate(self, exception: Exception) -> bool:
        """Evaluate whether the environment variable matches expected values.

        Inspect the current environment for the configured variable name and
        determine if its value matches one of the expected values. If no
        env_var_name is configured, this rule permits the alert (returns True).

        Args:
            exception: The exception object from the pipeline execution. Not used
                by this rule but required by the AlertRule interface.

        Returns:
            True if the environment variable matches one of the expected values,
            or if no env_var_name is configured. False if the variable does not
            match any expected values or is not present in the environment.

        Example:
            >>> os.environ["ENVIRONMENT"] = "production"
            >>> rule = EnvVarsMatchesRule(
            ...     rule_type="env_vars_matches",
            ...     env_var_name="ENVIRONMENT",
            ...     env_var_values=["production", "staging"]
            ... )
            >>> rule.evaluate(None)  # Returns True
        """
        if not self.env_var_name:
            logger.debug("No env_vars_matches configured; skipping environment variable check.")
            return True

        actual_value = os.environ.get(self.env_var_name, None)

        if actual_value in self.env_var_values:
            logger.debug(
                "Environment variable '%s' value '%s' matches expected values: %s",
                self.env_var_name,
                actual_value,
                self.env_var_values,
            )
            return True

        logger.debug("No environment variable conditions are satisfied.")
        return False
