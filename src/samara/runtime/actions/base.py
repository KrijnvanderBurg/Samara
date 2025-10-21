"""Actions base"""

from abc import abstractmethod

from pydantic import Field
from samara import BaseModel
from samara.utils.logger import get_logger

logger = get_logger(__name__)


class ActionBase(BaseModel):
    """Base class for defining actions in hooks.

    Attributes:
        id (str): Unique identifier for the action.
        enabled (bool): Whether the action is enabled.
        parameters (dict): A dictionary of parameters for the action.
    """

    id_: str = Field(..., alias="id", description="Unique identifier for the action.", min_length=1)
    description: str = Field(..., description="A description of the action.")
    enabled: bool = Field(..., description="Whether the action is enabled.")

    def execute(self) -> None:
        """Execute the action."""
        if not self.enabled:
            logger.debug("Action '%s' is disabled; skipping execution.", self.id_)
            return
        self._execute()

    @abstractmethod
    def _execute(self) -> None:
        """Execute the action."""
