"""Controller for managing hooks."""

from pydantic import Field

from samara import BaseModel
from samara.runtime.actions import HooksActionsUnion


class Hooks(BaseModel):
    """A controller for managing hooks in a model.

    Args:
        model (BaseModel): The model to manage hooks for.
    """

    onStart: list[HooksActionsUnion] = Field(default_factory=list, description="Actions to perform on Job start.")
    onError: list[HooksActionsUnion] = Field(default_factory=list, description="Actions to perform on Job error.")
    onSuccess: list[HooksActionsUnion] = Field(default_factory=list, description="Actions to perform on Job success.")
    onFinally: list[HooksActionsUnion] = Field(
        default_factory=list, description="Actions to perform on Job end, regardless of success or error."
    )

    def on_start(self) -> None:
        """Execute all actions defined in the onStart hook."""
        for action in self.onStart:
            action.execute()

    def on_error(self) -> None:
        """Execute all actions defined in the onError hook."""
        for action in self.onError:
            action.execute()

    def on_success(self) -> None:
        """Execute all actions defined in the onSuccess hook."""
        for action in self.onSuccess:
            action.execute()

    def on_finally(self) -> None:
        """Execute all actions defined in the onFinally hook."""
        for action in self.onFinally:
            action.execute()
