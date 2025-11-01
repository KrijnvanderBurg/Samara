"""Event hook actions - Register available action types for pipeline event handlers.

This module provides the action types that can be triggered by pipeline events
(onStart, onSuccess, onFailure, onFinally). It enables pipeline authors to
configure external integrations and notifications at key execution points
through the configuration-driven pipeline definition.
"""

from samara.workflow.actions.http import HttpAction

# Register HttpAction as the available action type for pipeline event hooks.
# When additional action types are implemented, combine them using a union
# with Annotated and Discriminator: Annotated[HttpAction | OtherAction, Discriminator("action_type")]
HooksActionsUnion = HttpAction
