"""Unit tests for the Samara CLI module."""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from samara.cli import cli
from samara.exceptions import (
    ExitCode,
    SamaraIOError,
    SamaraValidationError,
    SamaraWorkflowConfigurationError,
    SamaraWorkflowError,
)
from samara.workflow.controller import WorkflowController


class TestValidateCommand:
    """Test cases for validate command."""

    def test_validate__with_valid_configuration__exits_with_success(self) -> None:
        """Test validate command completes successfully when configuration file is valid."""
        # Arrange
        runner = CliRunner()
        mock_workflow = Mock()

        # Act
        with patch.object(WorkflowController, "from_file", return_value=mock_workflow):
            result = runner.invoke(cli, ["validate", "--workflow-filepath", "/test/workflow.json"])

        # Assert
        assert result.exit_code == 0

    def test_validate__when_workflow_configuration_is_invalid__exits_with_configuration_error(self) -> None:
        """Test validate command returns configuration error when workflow configuration is malformed."""
        # Arrange
        runner = CliRunner()

        # Act
        with patch.object(WorkflowController, "from_file", side_effect=SamaraWorkflowConfigurationError("test")):
            result = runner.invoke(cli, ["validate", "--workflow-filepath", "/test/workflow.json"])

        # Assert
        assert result.exit_code == ExitCode.CONFIGURATION_ERROR

    def test_validate__when_workflow_io_error_occurs__exits_with_io_error(self) -> None:
        """Test validate command returns IO error when workflow configuration file cannot be accessed."""
        # Arrange
        runner = CliRunner()

        # Act
        with patch.object(WorkflowController, "from_file", side_effect=SamaraIOError("test")):
            result = runner.invoke(cli, ["validate", "--workflow-filepath", "/test/workflow.json"])

        # Assert
        assert result.exit_code == ExitCode.IO_ERROR

    def test_validate__when_workflow_configuration_fails__exits_with_error(self) -> None:
        """Test validate command returns error when workflow configuration fails to load."""
        # Arrange
        runner = CliRunner()

        # Act
        with patch.object(WorkflowController, "from_file", side_effect=SamaraValidationError("test")):
            result = runner.invoke(cli, ["validate", "--workflow-filepath", "/test/workflow.json"])

        # Assert
        assert result.exit_code == ExitCode.VALIDATION_ERROR

    def test_validate__when_unexpected_error_occurs__exits_with_unexpected_error_code(self) -> None:
        """Test validate command returns unexpected error code when an unhandled exception occurs."""
        # Arrange
        runner = CliRunner()

        # Act
        with patch.object(WorkflowController, "from_file", side_effect=SamaraWorkflowError):
            result = runner.invoke(cli, ["validate", "--workflow-filepath", "/test/workflow.json"])

        # Assert
        assert result.exit_code == ExitCode.UNEXPECTED_ERROR


class TestRunCommand:
    """Test cases for run command."""

    def test_run__with_valid_configuration__executes_pipeline_and_exits_with_success(self) -> None:
        """Test run command successfully executes ETL pipeline with valid configuration."""
        # Arrange
        runner = CliRunner()
        mock_workflow = Mock()

        # Act
        with patch.object(WorkflowController, "from_file", return_value=mock_workflow):
            result = runner.invoke(cli, ["run", "--workflow-filepath", "/test/workflow.json"])

        # Assert
        assert result.exit_code == 0
        mock_workflow.execute_all.assert_called_once()

    @pytest.mark.parametrize(
        "exception_class,expected_exit_code",
        [
            (SamaraIOError, ExitCode.IO_ERROR),
            (SamaraWorkflowConfigurationError, ExitCode.CONFIGURATION_ERROR),
            (SamaraValidationError, ExitCode.VALIDATION_ERROR),
            (SamaraWorkflowError, ExitCode.JOB_ERROR),
        ],
    )
    def test_run__when_workflow_error_occurs__exits_with_correct_code(
        self, exception_class, expected_exit_code
    ) -> None:
        """Test run command returns correct exit code for various workflow errors."""
        # Arrange
        runner = CliRunner()

        # Act
        with patch.object(WorkflowController, "from_file", side_effect=exception_class("Test error")):
            result = runner.invoke(cli, ["run", "--workflow-filepath", "/test/workflow.json"])

        # Assert
        assert result.exit_code == expected_exit_code

    def test_run__when_job_execution_fails__exits_with_job_error(self) -> None:
        """Test run command returns job error code when execute_all() raises SamaraWorkflowError."""
        # Arrange
        runner = CliRunner()
        mock_workflow = Mock()
        mock_workflow.execute_all.side_effect = SamaraWorkflowError("Job execution failed")

        # Act
        with patch.object(WorkflowController, "from_file", return_value=mock_workflow):
            result = runner.invoke(cli, ["run", "--workflow-filepath", "/test/workflow.json"])

        # Assert
        assert result.exit_code == ExitCode.JOB_ERROR

    def test_run__when_user_interrupts__exits_gracefully(self) -> None:
        """Test run command exits gracefully when user sends keyboard interrupt signal."""
        # Arrange
        runner = CliRunner()

        # Act
        with patch.object(WorkflowController, "from_file", side_effect=KeyboardInterrupt):
            result = runner.invoke(cli, ["run", "--workflow-filepath", "/test/workflow.json"])

        # Assert
        # CLI intercepts KeyboardInterrupt and converts to exit code 98
        assert result.exit_code == ExitCode.KEYBOARD_INTERRUPT

    def test_run__when_unexpected_error_occurs__exits_with_unexpected_error_code(self) -> None:
        """Test run command returns unexpected error code when an unhandled exception occurs."""
        # Arrange
        runner = CliRunner()

        # Act
        with patch.object(WorkflowController, "from_file", side_effect=RuntimeError):
            result = runner.invoke(cli, ["run", "--workflow-filepath", "/test/workflow.json"])

        # Assert
        assert result.exit_code == ExitCode.UNEXPECTED_ERROR


class TestExportSchemaCommand:
    """Test cases for export-schema command."""

    def test_export_schema__with_valid_output_path__creates_schema_file_and_exits_with_success(self) -> None:
        """Test export-schema command successfully creates schema file with parent directories."""
        # Arrange
        runner = CliRunner()
        mock_schema = {"type": "object", "properties": {}}

        # Act
        # Mock schema generation to avoid dependency on actual schema definition
        with (
            patch.object(WorkflowController, "export_schema", return_value=mock_schema),
            runner.isolated_filesystem(),
        ):
            # Test with nested path to verify directory creation
            result = runner.invoke(cli, ["export-schema", "--output-filepath", "subdir/nested/schema.json"])

            # Assert
            assert result.exit_code == 0
            assert Path("subdir/nested/schema.json").exists()

    def test_export_schema__when_file_write_fails__exits_with_io_error(self) -> None:
        """Test export-schema command returns IO error when file cannot be written."""
        # Arrange
        runner = CliRunner()
        mock_schema = {"type": "object", "properties": {}}

        # Act
        # Mock file write failure to test error handling without requiring actual permission issues
        with (
            patch.object(WorkflowController, "export_schema", return_value=mock_schema),
            patch("builtins.open", side_effect=OSError("Permission denied")),
        ):
            result = runner.invoke(cli, ["export-schema", "--output-filepath", "/invalid/path/schema.json"])

            # Assert
            assert result.exit_code == ExitCode.IO_ERROR

    def test_export_schema__when_unexpected_error_occurs__exits_with_unexpected_error_code(self) -> None:
        """Test export-schema command returns unexpected error code when an unhandled exception occurs."""
        # Arrange
        runner = CliRunner()

        # Act
        with patch.object(WorkflowController, "export_schema", side_effect=SamaraWorkflowError("Unexpected error")):
            result = runner.invoke(cli, ["export-schema", "--output-filepath", "schema.json"])

        # Assert
        assert result.exit_code == ExitCode.UNEXPECTED_ERROR

    def test_export_schema__when_user_interrupts__exits_gracefully(self) -> None:
        """Test export-schema command exits gracefully when user sends keyboard interrupt signal."""
        # Arrange
        runner = CliRunner()

        # Act
        with patch.object(WorkflowController, "export_schema", side_effect=KeyboardInterrupt):
            result = runner.invoke(cli, ["export-schema", "--output-filepath", "schema.json"])

        # Assert
        # CLI intercepts KeyboardInterrupt and converts to exit code 98
        assert result.exit_code == ExitCode.KEYBOARD_INTERRUPT


class TestCliGroup:
    """Test cases for the CLI group."""

    def test_cli__when_help_flag_provided__displays_all_commands(self) -> None:
        """Test CLI displays help information with all available commands when --help flag is used."""
        # Arrange
        runner = CliRunner()

        # Act
        result = runner.invoke(cli, ["--help"])

        # Assert
        assert result.exit_code == 0
        assert "validate" in result.output
        assert "run" in result.output
        assert "export-schema" in result.output

    def test_cli__when_log_level_specified__accepts_valid_level(self) -> None:
        """Test CLI accepts valid log level option without crashing."""
        # Arrange
        runner = CliRunner()
        mock_workflow = Mock()

        # Act
        # Testing one valid level is sufficient - Click validates the choice constraint
        with patch.object(WorkflowController, "from_file", return_value=mock_workflow):
            result = runner.invoke(
                cli,
                ["--log-level", "DEBUG", "validate", "--workflow-filepath", "/test/workflow.json"],
            )

        # Assert
        assert result.exit_code == 0

    def test_cli__when_invalid_log_level_specified__exits_with_error(self) -> None:
        """Test CLI rejects invalid log level with error."""
        # Arrange
        runner = CliRunner()

        # Act
        result = runner.invoke(
            cli,
            ["--log-level", "INVALID", "validate", "--workflow-filepath", "/test/workflow.json"],
        )

        # Assert
        # Invalid log level causes ValueError which Click catches and returns exit code 1
        assert result.exit_code == 1

    def test_cli__when_no_command_provided__displays_help_text(self) -> None:
        """Test CLI displays help text when invoked without any command."""
        # Arrange
        runner = CliRunner()

        # Act
        result = runner.invoke(cli, [])

        # Assert
        # Click shows usage information when no command is given
        assert "Commands:" in result.output

    def test_cli__when_user_interrupts__exits_gracefully(self) -> None:
        """Test CLI exits gracefully when user sends keyboard interrupt signal."""
        # Arrange
        runner = CliRunner()

        # Act
        with patch.object(WorkflowController, "from_file", side_effect=KeyboardInterrupt):
            result = runner.invoke(cli, ["validate", "--workflow-filepath", "/test/workflow.json"])

        # Assert
        # CLI intercepts KeyboardInterrupt and converts to exit code 98
        assert result.exit_code == ExitCode.KEYBOARD_INTERRUPT
