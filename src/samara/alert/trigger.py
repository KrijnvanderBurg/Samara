"""Alert triggers - Configuration-driven rules for channel routing.

This module provides the trigger rules system that routes alerts to appropriate
notification channels based on flexible rule conditions. Triggers enable
sophisticated alert logic by evaluating exception properties and environment
context to determine which channels should receive notifications."""

import logging

from pydantic import Field

from samara import BaseModel
from samara.alert.template import AlertTemplate
from samara.telemetry import trace_span
from samara.utils.logger import get_logger

from .rules import AlertRuleUnion

logger: logging.Logger = get_logger(__name__)


class AlertTrigger(BaseModel):
    """Route alerts to channels based on flexible rule conditions.

    Represents a single trigger rule that determines when and to which channels
    alerts should be routed during pipeline execution. Triggers evaluate rules
    against exception properties and environment context to enable sophisticated
    alert logic without code changes.

    Attributes:
        id_: Unique identifier for the trigger rule.
        enabled: Whether this rule is currently active.
        description: Human-readable description of the trigger purpose.
        channel_ids: List of channel identifiers that receive alerts matching this rule.
        template: Template configuration for formatting alert messages.
        rules: List of rules that must all evaluate to True for the trigger to fire (AND logic).

    Example:
        >>> from samara.alert.trigger import AlertTrigger
        >>> trigger = AlertTrigger(
        ...     id="prod-critical-alerts",
        ...     enabled=True,
        ...     description="Alert production errors",
        ...     channel_ids=["ops-email"],
        ...     template=AlertTemplate(...),
        ...     rules=[...]
        ... )
        >>> trigger.should_fire(exception)
        True

        **Configuration in JSON:**
        ```
            {
                "alert": {
                    "triggers": [
                        {
                            "id": "production-external-errors",
                            "enabled": true,
                            "description": "Alert on production failures",
                            "channel_ids": ["alert-operations-team"],
                            "template": {
                                "prepend_title": "ETL Pipeline Alert - ",
                                "append_title": " - Team XYZ",
                                "prepend_body": "Attention: ETL Pipeline Alert",
                                "append_body": "Please take action."
                            },
                            "rules": [
                                {
                                    "rule_type": "env_vars_matches",
                                    "env_var_name": "ENVIRONMENT",
                                    "env_var_values": ["production"]
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
              triggers:
                - id: production-external-errors
                  enabled: true
                  description: Alert on production failures
                  channel_ids:
                    - alert-operations-team
                  template:
                    prepend_title: "ETL Pipeline Alert - "
                    append_title: " - Team XYZ"
                    prepend_body: "Attention: ETL Pipeline Alert"
                    append_body: "Please take action."
                  rules:
                    - rule_type: env_vars_matches
                      env_var_name: ENVIRONMENT
                      env_var_values:
                        - production
        ```

    Note:
        All rules must evaluate to True (AND logic) for the trigger to fire.
        If no rules are configured, the trigger fires by default. Triggers
        enable routing without modifying pipeline code.
    """

    id_: str = Field(..., alias="id", description="Unique identifier for the trigger rule", min_length=1)
    enabled: bool = Field(..., description="Whether this rule is currently active")
    description: str = Field(..., description="Description of the trigger rule")
    channel_ids: list[str] = Field(
        ..., description="List of channel identifiers that should receive alerts matching this rule"
    )
    template: AlertTemplate = Field(..., description="Template configuration for formatting alert messages")
    rules: list[AlertRuleUnion] = Field(
        ..., description="List of rules that must all evaluate to True for the trigger to fire"
    )

    @trace_span("alert_trigger.should_fire")
    def should_fire(self, exception: Exception) -> bool:
        """Evaluate whether trigger conditions are met.

        Evaluates all configured rules against the exception using AND logic.
        All rules must evaluate to True for the trigger to activate. If no rules
        are configured, the trigger fires by default (fail-safe behavior).

        Args:
            exception: The exception to evaluate against trigger rules.

        Returns:
            True if all rules evaluate to True or no rules configured, False otherwise.

        Example:
            >>> try:
            ...     process_data()
            ... except Exception as exc:
            ...     if trigger.should_fire(exc):
            ...         channel.send_alert(exc)
        """
        if not self.enabled:
            logger.debug("Trigger '%s' is disabled; skipping trigger check.", self.id_)
            return False

        # If no rules are configured, the trigger should fire (default behavior)
        if not self.rules:
            logger.debug("No rules configured for trigger '%s'; trigger will fire.", self.id_)
            return True

        # All rules must evaluate to True (AND logic)
        for rule in self.rules:
            if not rule.evaluate(exception):
                logger.debug(
                    "Rule '%s' for trigger '%s' evaluated to False; trigger will not fire.", rule.rule_type, self.id_
                )
                return False

        logger.debug("All rules for trigger '%s' evaluated to True; trigger will fire.", self.id_)
        return True
