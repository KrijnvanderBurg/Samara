"""Alert rule base classes and registry for trigger configuration.

This module provides the foundation for the rule-based alert trigger system,
enabling pipeline authors to define conditions that determine when alerts are
triggered. It includes abstract base classes that define the contract for
implementing custom alert rules."""

import logging
from abc import ABC, abstractmethod

from samara import BaseModel
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class AlertRule(BaseModel, ABC):
    """Define conditions that trigger alerts on pipeline events.

    Base class for implementing alert trigger rules that evaluate whether
    specific conditions are met for a given exception. Rules are evaluated
    within the alert system to determine if an alert should be triggered.
    Subclasses must implement the evaluate method to define custom logic.

    Attributes:
        rule_type: Name identifying the rule type used in configuration.

    Example:
        **Configuration in JSON:**
        ```
        {
            "alert": {
                "channels": [...],
                "triggers": [
                    {
                        "onEvent": "onFailure",
                        "rules": [
                            {
                                "type": "custom_rule_type",
                                "property1": "value1"
                            }
                        ]
                    }
                ]
            }
        }
        ```

        **Configuration in YAML:**
        ```
        alert:
          channels:
            - ...
          triggers:
            - onEvent: onFailure
              rules:
                - type: custom_rule_type
                  property1: value1
        ```

    Note:
        Custom rules must implement the evaluate method to define
        the condition logic for triggering alerts.

    See Also:
        samara.alert.trigger.AlertTrigger: For trigger configuration
        samara.alert.channels.base.AlertChannel: For alert delivery
    """

    @abstractmethod
    def evaluate(self, exception: Exception) -> bool:
        """Evaluate whether the rule conditions are met for this exception.

        Implement this method to define custom logic that determines
        if an alert should be triggered based on the exception.

        Args:
            exception: The exception to evaluate against rule conditions.

        Returns:
            True if the rule conditions are met and alert should trigger,
            False otherwise.

        Raises:
            This method should not raise exceptions; return False for
            invalid states or edge cases.
        """
