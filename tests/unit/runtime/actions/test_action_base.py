"""Unit tests for the ActionBase class."""

from unittest.mock import patch

import pytest

from samara.runtime.actions.base import ActionBase


class MockAction(ActionBase):
    """A minimal mock action for unit testing ActionBase."""

    def _execute(self) -> None:
        """Execute the action - does nothing."""


class TestActionBase:
    """Test cases for ActionBase."""

    def test_execute__when_enabled_true__calls_execute_implementation(self) -> None:
        """Test that execute calls _execute when enabled is True."""
        # Arrange
        action = MockAction(id="test_action", description="Test action", enabled=True)

        with patch.object(action, "_execute") as mock_execute:
            # Act
            action.execute()

            # Assert
            mock_execute.assert_called_once()

    def test_execute__when_enabled_false__skips_execution(self) -> None:
        """Test that execute skips _execute when enabled is False."""
        # Arrange
        action = MockAction(id="test_action", description="Test action", enabled=False)

        with patch.object(action, "_execute") as mock_execute:
            # Act
            action.execute()

            # Assert
            mock_execute.assert_not_called()

    def test_execute__when_enabled_false__does_not_raise_exception(self) -> None:
        """Test that execute completes successfully when action is disabled."""
        # Arrange
        action = MockAction(id="test_action", description="Test action", enabled=False)

        # Act & Assert - should not raise any exception
        action.execute()

    def test_action_base__requires_id(self) -> None:
        """Test that ActionBase requires an id field."""
        with pytest.raises(ValueError):
            MockAction(description="Test action", enabled=True)  # type: ignore[call-arg]

    def test_action_base__requires_description(self) -> None:
        """Test that ActionBase requires a description field."""
        with pytest.raises(ValueError):
            MockAction(id="test_action", enabled=True)  # type: ignore[call-arg]

    def test_action_base__requires_enabled(self) -> None:
        """Test that ActionBase requires an enabled field."""
        with pytest.raises(ValueError):
            MockAction(id="test_action", description="Test action")  # type: ignore[call-arg]

    def test_action_base__id_cannot_be_empty(self) -> None:
        """Test that ActionBase id cannot be an empty string."""
        with pytest.raises(ValueError, match="String should have at least 1 character"):
            MockAction(id="", description="Test action", enabled=True)

    def test_action_base__id_alias_works(self) -> None:
        """Test that ActionBase id field works with 'id' alias."""
        # Arrange & Act
        action = MockAction.model_validate({"id": "test_action", "description": "Test action", "enabled": True})

        # Assert
        assert action.id_ == "test_action"
