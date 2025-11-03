"""Tests for AlertController orchestration logic.

These tests focus only on the AlertController's unique responsibility:
coordinating triggers and channels. Individual channel and trigger behaviors
are tested in their respective test files.
"""

from pathlib import Path
from unittest.mock import patch

from samara.alert.controller import AlertController
from samara.exceptions import SamaraWorkflowError


class TestAlertControllerOrchestration:
    """Test AlertController trigger and channel orchestration."""

    def test_evaluate_trigger_and_alert__with_multiple_triggers__calls_correct_channels(self, tmp_path: Path) -> None:
        """Test that multiple triggers each call their configured channels."""
        # Arrange - create controller with multiple triggers
        config = {
            "channels": [
                {
                    "channel_type": "file",
                    "id": "channel-1",
                    "description": "First test channel",
                    "file_path": tmp_path / "channel1.log",
                    "enabled": True,
                },
                {
                    "channel_type": "file",
                    "id": "channel-2",
                    "description": "Second test channel",
                    "file_path": tmp_path / "channel2.log",
                    "enabled": True,
                },
            ],
            "triggers": [
                {
                    "id": "always-fire",
                    "enabled": True,
                    "description": "Always fires regardless of conditions",
                    "channel_ids": ["channel-1"],
                    "template": {
                        "prepend_title": "[ALWAYS] ",
                        "append_title": "",
                        "prepend_body": "",
                        "append_body": "",
                    },
                    "rules": [],
                },
                {
                    "id": "critical-only",
                    "enabled": True,
                    "description": "Fires only for critical exceptions",
                    "channel_ids": ["channel-2"],
                    "template": {
                        "prepend_title": "[CRITICAL] ",
                        "append_title": "",
                        "prepend_body": "URGENT: ",
                        "append_body": "",
                    },
                    "rules": [{"rule_type": "exception_regex", "pattern": "Critical", "flags": ["IGNORECASE"]}],
                },
            ],
        }
        controller = AlertController(**config)

        # Mock channels to verify orchestration
        with (
            patch.object(controller.channels[0], "_alert") as mock_channel1,
            patch.object(controller.channels[1], "_alert") as mock_channel2,
        ):
            test_exception = ValueError("Critical system failure")

            # Act
            controller.evaluate_trigger_and_alert("System Alert", "Database error", test_exception)

            # Assert - both triggers fire
            mock_channel1.assert_called_once_with(title="[ALWAYS] System Alert", body="Database error")
            mock_channel2.assert_called_once_with(title="[CRITICAL] System Alert", body="URGENT: Database error")

    def test_evaluate_trigger_and_alert__with_disabled_trigger__skips_disabled(self, tmp_path: Path) -> None:
        """Test that disabled triggers are not executed."""
        # Arrange
        config = {
            "channels": [
                {
                    "channel_type": "file",
                    "id": "test-channel",
                    "description": "Test channel",
                    "file_path": tmp_path / "test.log",
                    "enabled": True,
                }
            ],
            "triggers": [
                {
                    "id": "enabled-trigger",
                    "enabled": True,
                    "description": "Test enabled trigger",
                    "channel_ids": ["test-channel"],
                    "template": {"prepend_title": "", "append_title": "", "prepend_body": "", "append_body": ""},
                    "rules": [],
                },
                {
                    "id": "disabled-trigger",
                    "enabled": False,
                    "description": "Test disabled trigger",
                    "channel_ids": ["test-channel"],
                    "template": {"prepend_title": "", "append_title": "", "prepend_body": "", "append_body": ""},
                    "rules": [],
                },
            ],
        }
        controller = AlertController(**config)

        with patch.object(controller.channels[0], "_alert") as mock_channel:
            # Act
            controller.evaluate_trigger_and_alert("Test Alert", "Test message", SamaraWorkflowError("error"))

            # Assert - only called once (by enabled trigger)
            mock_channel.assert_called_once()

    def test_evaluate_trigger_and_alert__with_nonexistent_channel__continues_gracefully(self, tmp_path: Path) -> None:
        """Test that triggers referencing non-existent channels don't crash."""
        # Arrange
        config = {
            "channels": [
                {
                    "channel_type": "file",
                    "id": "existing-channel",
                    "description": "Existing test channel",
                    "file_path": tmp_path / "test.log",
                    "enabled": True,
                }
            ],
            "triggers": [
                {
                    "id": "test-trigger",
                    "enabled": True,
                    "description": "Test trigger for nonexistent channel handling",
                    "channel_ids": ["existing-channel", "nonexistent-channel"],  # One exists, one doesn't
                    "template": {"prepend_title": "", "append_title": "", "prepend_body": "", "append_body": ""},
                    "rules": [],
                }
            ],
        }
        controller = AlertController(**config)

        with patch.object(controller.channels[0], "_alert") as mock_channel:
            # Act - should not raise exception
            controller.evaluate_trigger_and_alert("Test Alert", "Test message", SamaraWorkflowError("error"))

            # Assert - existing channel still called
            mock_channel.assert_called_once()

    def test_evaluate_trigger_and_alert__with_template_formatting__applies_correctly(self, tmp_path: Path) -> None:
        """Test that trigger templates are applied to messages."""
        # Arrange
        config = {
            "channels": [
                {
                    "channel_type": "file",
                    "id": "test-channel",
                    "description": "Test channel for formatting",
                    "file_path": tmp_path / "test.log",
                    "enabled": True,
                }
            ],
            "triggers": [
                {
                    "id": "formatting-trigger",
                    "enabled": True,
                    "description": "Test trigger for template formatting",
                    "channel_ids": ["test-channel"],
                    "template": {
                        "prepend_title": "[PREFIX] ",
                        "append_title": " [SUFFIX]",
                        "prepend_body": "BODY START: ",
                        "append_body": " :BODY END",
                    },
                    "rules": [],
                }
            ],
        }
        controller = AlertController(**config)

        with patch.object(controller.channels[0], "_alert") as mock_channel:
            # Act
            controller.evaluate_trigger_and_alert("Original Title", "Original Body", SamaraWorkflowError("error"))

            # Assert - template formatting applied
            mock_channel.assert_called_once_with(
                title="[PREFIX] Original Title [SUFFIX]",
                body="BODY START: Original Body :BODY END",
            )
