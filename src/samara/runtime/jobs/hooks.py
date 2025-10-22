"""Execute actions at key pipeline lifecycle stages.

This module provides hook management for defining configuration-driven actions
that execute at job lifecycle events (start, error, success, completion).
"""

from pydantic import Field

from samara import BaseModel
from samara.runtime.actions import HooksActionsUnion


class Hooks(BaseModel):
    """Manage actions triggered at job lifecycle events.

    Execute custom actions at key pipeline stages within a job lifecycle.
    Each hook field accepts a list of configurable actions that run when
    their corresponding event occurs, enabling flexible alerting and
    post-processing workflows.

    Attributes:
        onStart: Actions to execute when the job begins processing.
        onError: Actions to execute if the job encounters an error.
        onSuccess: Actions to execute when the job completes successfully.
        onFinally: Actions to execute when the job ends, regardless of outcome.

    Example:
        **Configuration in JSON:**
        ```
        {
            "onStart": [
                {
                    "type": "http",
                    "url": "https://example.com/webhook",
                    "method": "POST"
                },
                {
                    "type": "email",
                    "recipients": ["admin@example.com"],
                    "subject": "Job started"
                }
            ],
            "onSuccess": [
                {
                    "type": "email",
                    "recipients": ["admin@example.com"],
                    "subject": "Job completed successfully"
                }
            ],
            "onError": [
                {
                    "type": "file",
                    "path": "logs/errors.log"
                }
            ],
            "onFinally": []
        }
        ```

        **Configuration in YAML:**
        ```
        onStart:
          - type: http
            url: https://example.com/webhook
            method: POST
          - type: email
            recipients:
              - admin@example.com
            subject: Job started
        onSuccess:
          - type: email
            recipients:
              - admin@example.com
            subject: Job completed successfully
        onError:
          - type: file
            path: logs/errors.log
        onFinally: []
        ```

    Note:
        Actions execute sequentially in the order defined. If any action fails,
        execution continues with the remaining actions. Use the onFinally hook
        for cleanup operations that must run regardless of job outcome.

    See Also:
        HooksActionsUnion: Supported action types and their configurations.
    """

    onStart: list[HooksActionsUnion] = Field(default_factory=list, description="Actions to perform on Job start.")
    onError: list[HooksActionsUnion] = Field(default_factory=list, description="Actions to perform on Job error.")
    onSuccess: list[HooksActionsUnion] = Field(default_factory=list, description="Actions to perform on Job success.")
    onFinally: list[HooksActionsUnion] = Field(
        default_factory=list, description="Actions to perform on Job end, regardless of success or error."
    )

    def on_start(self) -> None:
        """Execute all actions defined in the onStart hook.

        Runs each action sequentially when the job begins processing.
        """
        for action in self.onStart:
            action.execute()

    def on_error(self) -> None:
        """Execute all actions defined in the onError hook.

        Runs each action sequentially when the job encounters an error.
        """
        for action in self.onError:
            action.execute()

    def on_success(self) -> None:
        """Execute all actions defined in the onSuccess hook.

        Runs each action sequentially when the job completes successfully.
        """
        for action in self.onSuccess:
            action.execute()

    def on_finally(self) -> None:
        """Execute all actions defined in the onFinally hook.

        Runs each action sequentially after job completion, regardless of
        success or error outcome. Use for cleanup or finalization operations.
        """
        for action in self.onFinally:
            action.execute()
