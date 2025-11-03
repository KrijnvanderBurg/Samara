"""Unit tests for the Hooks controller."""

from unittest.mock import patch

import pytest

from samara.exceptions import SamaraWorkflowError
from samara.workflow.actions.base import ActionBase
from samara.workflow.jobs.hooks import Hooks


class MockAction(ActionBase):
    """A minimal mock action for unit testing hooks."""

    def _execute(self) -> None:
        """Execute the action - does nothing."""


def test_hooks_initializes_with_empty_lists() -> None:
    """Test that Hooks initializes with empty action lists by default."""
    hooks = Hooks()

    assert hooks.onStart == []
    assert hooks.onError == []
    assert hooks.onSuccess == []
    assert hooks.onFinally == []


def test_on_start_executes_action() -> None:
    """Test that on_start executes the action."""
    action = MockAction(id="test_action", description="Test action", enabled=True)

    with patch.object(action, "_execute") as mock_execute:
        # Use model_construct to bypass Pydantic validation for testing
        hooks = Hooks.model_construct(onStart=[action])
        hooks.on_start()
        mock_execute.assert_called_once()


def test_on_error_executes_action() -> None:
    """Test that on_error executes the action."""
    action = MockAction(id="test_action", description="Test action", enabled=True)

    with patch.object(action, "_execute") as mock_execute:
        hooks = Hooks.model_construct(onError=[action])
        hooks.on_error()
        mock_execute.assert_called_once()


def test_on_success_executes_action() -> None:
    """Test that on_success executes the action."""
    action = MockAction(id="test_action", description="Test action", enabled=True)

    with patch.object(action, "_execute") as mock_execute:
        hooks = Hooks.model_construct(onSuccess=[action])
        hooks.on_success()
        mock_execute.assert_called_once()


def test_on_finally_executes_action() -> None:
    """Test that on_finally executes the action."""
    action = MockAction(id="test_action", description="Test action", enabled=True)

    with patch.object(action, "_execute") as mock_execute:
        hooks = Hooks.model_construct(onFinally=[action])
        hooks.on_finally()
        mock_execute.assert_called_once()


def test_hooks_only_execute_their_own_actions() -> None:
    """Test that each hook type only executes its own actions."""
    start_action = MockAction(id="start_action", description="Start action", enabled=True)
    error_action = MockAction(id="error_action", description="Error action", enabled=True)

    with patch.object(start_action, "_execute") as mock_start, patch.object(error_action, "_execute") as mock_error:
        hooks = Hooks.model_construct(onStart=[start_action], onError=[error_action])
        hooks.on_start()

        mock_start.assert_called_once()
        mock_error.assert_not_called()


def test_hooks_propagate_exceptions() -> None:
    """Test that hooks propagate exceptions from failing actions."""
    action = MockAction(id="test_action", description="Test action", enabled=True)

    with patch.object(action, "_execute", side_effect=SamaraWorkflowError("Action failed")):
        hooks = Hooks.model_construct(onStart=[action])

        with pytest.raises(SamaraWorkflowError):
            hooks.on_start()
