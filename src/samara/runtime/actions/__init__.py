"""HTTP actions and other action implementations for Spark.

This module imports all available action functions to register them with the
HooksActionsUnion. Each action function is automatically registered
when imported.
"""

from samara.runtime.actions.http import HttpAction

# from samara.runtime.actions.move_or_copy_job_files import MoveOrCopyJobFiles

# When there's only one action type, use it directly
# When there are multiple, use: Annotated[HttpAction | OtherAction, Discriminator("action")]
HooksActionsUnion = HttpAction
