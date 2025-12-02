"""Actions module - Base classes for workflow action handlers.

This module provides the foundation for defining actions that execute
at specific pipeline stages. Actions enable custom logic execution during
pipeline lifecycle events (onStart, onSuccess, onFailure, onFinally).
"""

from abc import abstractmethod

from pydantic import Field

from samara import BaseModel
from samara.telemetry import trace_span
from samara.utils.logger import get_logger

logger = get_logger(__name__)


class ActionBase(BaseModel):
    """Abstract base class for workflow actions executed during pipeline lifecycle events.

    Actions are triggered at specific pipeline stages (onStart, onSuccess, onFailure, onFinally)
    and can perform custom logic such as notifications, data processing, or state management.
    Subclasses must implement the _execute method with specific action behavior.

    Attributes:
        id_: Unique identifier for the action. Used for logging and tracking execution.
        description: Human-readable description of the action's purpose and behavior.
        enabled: Controls whether the action executes. Disabled actions are skipped silently.

    Example:
        **Configuration in JSON:**
        ```
        {
          "hooks": {
            "onSuccess": [
              {
                "id": "notify-team",
                "description": "Send success notification to team",
                "enabled": true,
                "type": "email",
                "recipients": ["team@example.com"]
              }
            ]
          }
        }
        ```

        **Configuration in YAML:**
        ```
        hooks:
          onSuccess:
            - id: notify-team
              description: Send success notification to team
              enabled: true
              type: email
              recipients:
                - team@example.com
        ```

    See Also:
        ActionController: For managing action execution and lifecycle.
    """

    id_: str = Field(..., alias="id", description="Unique identifier for the action.", min_length=1)
    description: str = Field(..., description="A description of the action.")
    enabled: bool = Field(..., description="Whether the action is enabled.")

    @trace_span("action_base.execute")
    def execute(self) -> None:
        """Execute the action if enabled, delegating to the implementation-specific method.

        Silently skips execution if the action is disabled. This method handles the
        enable/disable check and delegates actual execution to the _execute method.

        Example:
            >>> action = EmailAction(
            ...     id="notify",
            ...     description="Send notification",
            ...     enabled=True,
            ...     recipients=["user@example.com"]
            ... )
            >>> action.execute()  # Executes the email action
        """
        if not self.enabled:
            logger.debug("Action '%s' is disabled; skipping execution.", self.id_)
            return
        self._execute()

    @abstractmethod
    def _execute(self) -> None:
        """Execute the action's specific implementation logic.

        This abstract method must be implemented by subclasses to define
        the actual behavior of the action (e.g., sending emails, making HTTP calls,
        writing to files).

        Raises:
            NotImplementedError: When called on ActionBase directly or if subclass
                does not provide an implementation.

        Note:
            This method is only called by execute() if the action is enabled.
            Implementation should handle any failures appropriately, using logging
            for debugging and raising exceptions for unexpected errors.
        """
