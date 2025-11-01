"""Unit tests for the WorkflowController module."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from pydantic import ValidationError
from samara.workflow.controller import WorkflowController

from samara.exceptions import SamaraIOError, SamaraWorkflowConfigurationError

# =========================================================================== #
# ============================== CONFIG (dict) ============================== #
# =========================================================================== #


@pytest.fixture(name="workflow_config")
def fixture_workflow_config(tmp_path: Path) -> dict[str, Any]:
    """Provide a valid workflow configuration dict with temporary paths.

    Args:
        tmp_path: pytest temporary directory fixture.

    Returns:
        dict: A valid workflow configuration dictionary.
    """
    # Create temporary files for extract/load locations
    input_file = tmp_path / "input.json"
    input_file.write_text("[]", encoding="utf-8")

    output_file = tmp_path / "output.json"
    output_file.write_text("[]", encoding="utf-8")

    return {
        "id": "test-workflow",
        "description": "Test workflow configuration",
        "enabled": True,
        "jobs": [
            {
                "id": "test_job",
                "description": "Test job description",
                "enabled": True,
                "engine_type": "spark",
                "extracts": [
                    {
                        "id": "extract1",
                        "extract_type": "file",
                        "method": "batch",
                        "data_format": "json",
                        "options": {},
                        "location": str(input_file),
                        "schema": "",
                    }
                ],
                "transforms": [
                    {
                        "id": "transform1",
                        "upstream_id": "extract1",
                        "options": {},
                        "functions": [],
                    }
                ],
                "loads": [
                    {
                        "id": "load1",
                        "upstream_id": "transform1",
                        "load_type": "file",
                        "method": "batch",
                        "location": str(output_file),
                        "schema_export": "",
                        "options": {},
                        "mode": "overwrite",
                        "data_format": "json",
                    }
                ],
                "hooks": {
                    "onStart": [],
                    "onError": [],
                    "onSuccess": [],
                    "onFinally": [],
                },
            }
        ],
    }


def test_workflow_creation__from_config__creates_valid_model(workflow_config: dict[str, Any]) -> None:
    """Create a WorkflowController from config and assert its attributes."""
    controller = WorkflowController(**workflow_config)

    assert isinstance(controller.jobs, list)
    assert len(controller.jobs) == 1
    assert controller.jobs[0].id_ == "test_job"


# =========================================================================== #
# ========================== VALIDATION TESTS ============================= #
# =========================================================================== #


class TestWorkflowControllerValidation:
    """Test WorkflowController model validation."""

    def test_create_workflow_controller__with_missing_jobs__raises_validation_error(
        self, workflow_config: dict[str, Any]
    ) -> None:
        """Test WorkflowController creation fails when jobs field is missing."""
        del workflow_config["jobs"]

        with pytest.raises(ValidationError):
            WorkflowController(**workflow_config)

    def test_create_workflow_controller__with_invalid_job__raises_validation_error(
        self, workflow_config: dict[str, Any]
    ) -> None:
        """Test WorkflowController creation fails with invalid job configuration."""
        workflow_config["jobs"][0]["engine_type"] = "invalid_engine"

        with pytest.raises(ValidationError):
            WorkflowController(**workflow_config)

    def test_create_workflow_controller__with_missing_name__raises_validation_error(
        self, workflow_config: dict[str, Any]
    ) -> None:
        """Test WorkflowController creation fails when name field is missing."""
        del workflow_config["id"]

        with pytest.raises(ValidationError):
            WorkflowController(**workflow_config)

    def test_create_workflow_controller__with_empty_name__raises_validation_error(
        self, workflow_config: dict[str, Any]
    ) -> None:
        """Test WorkflowController creation fails when name is empty."""
        workflow_config["id"] = ""

        with pytest.raises(ValidationError):
            WorkflowController(**workflow_config)

    def test_create_workflow_controller__with_missing_description__raises_validation_error(
        self, workflow_config: dict[str, Any]
    ) -> None:
        """Test WorkflowController creation fails when description is missing."""
        del workflow_config["description"]

        with pytest.raises(ValidationError):
            WorkflowController(**workflow_config)

    def test_create_workflow_controller__with_missing_enabled__raises_validation_error(
        self, workflow_config: dict[str, Any]
    ) -> None:
        """Test WorkflowController creation fails when enabled is missing."""
        del workflow_config["enabled"]

        with pytest.raises(ValidationError):
            WorkflowController(**workflow_config)

    def test_create_workflow_controller__with_enabled_false__succeeds(self, workflow_config: dict[str, Any]) -> None:
        """Test WorkflowController creation succeeds with enabled set to False."""
        workflow_config["enabled"] = False

        controller = WorkflowController(**workflow_config)

        assert controller.enabled is False


# =========================================================================== #
# ============================= MODEL FIXTURE =============================== #
# =========================================================================== #


@pytest.fixture(name="workflow_controller")
def fixture_workflow_controller(workflow_config: dict[str, Any]) -> WorkflowController:
    """Instantiate a WorkflowController from the provided config."""
    return WorkflowController(**workflow_config)


# =========================================================================== #
# ========================== FROM_FILE TESTS ============================== #
# =========================================================================== #


class TestWorkflowControllerFromFile:
    """Test WorkflowController.from_file() class method."""

    def test_from_file__with_valid_json_config__succeeds(self, workflow_config: dict[str, Any], tmp_path: Path) -> None:
        """Test from_file creates WorkflowController from valid JSON file."""
        config_file = tmp_path / "workflow_config.json"
        config_data = {"workflow": workflow_config}
        config_file.write_text(json.dumps(config_data), encoding="utf-8")

        controller = WorkflowController.from_file(config_file)

        assert isinstance(controller, WorkflowController)
        assert len(controller.jobs) == 1

    def test_from_file__with_nonexistent_file__raises_samara_io_error(self, tmp_path: Path) -> None:
        """Test from_file raises SamaraIOError when file does not exist."""
        nonexistent_file = tmp_path / "nonexistent.json"

        with pytest.raises(SamaraIOError):
            WorkflowController.from_file(nonexistent_file)

    def test_from_file__with_missing_workflow_section__raises_configuration_error(
        self, workflow_config: dict[str, Any], tmp_path: Path
    ) -> None:
        """Test from_file raises error when 'workflow' section is missing."""
        config_file = tmp_path / "invalid_config.json"
        config_file.write_text(json.dumps(workflow_config), encoding="utf-8")

        with pytest.raises(SamaraWorkflowConfigurationError):
            WorkflowController.from_file(config_file)


# =========================================================================== #
# ========================== EXECUTE_ALL TESTS ============================ #
# =========================================================================== #


class TestWorkflowControllerExecuteAll:
    """Test WorkflowController.execute_all() method."""

    def test_execute_all__when_disabled__skips_job_execution(self, workflow_controller: WorkflowController) -> None:
        """Test execute_all skips all jobs when workflow is disabled."""
        workflow_controller.enabled = False
        mock_job = Mock()
        mock_job.id = "mock_job"
        workflow_controller.jobs = [mock_job]

        workflow_controller.execute_all()

        # Job should not be executed
        mock_job.execute.assert_not_called()

    def test_execute_all__with_single_job__calls_job_execute(self, workflow_controller: WorkflowController) -> None:
        """Test execute_all calls execute on single job."""
        mock_job = Mock()
        mock_job.id = "mock_job"
        workflow_controller.jobs = [mock_job]

        workflow_controller.execute_all()

        mock_job.execute.assert_called_once()

    def test_execute_all__with_multiple_jobs__calls_execute_on_all(self, workflow_controller: WorkflowController) -> None:
        """Test execute_all calls execute on all jobs in order."""
        mock_job1 = Mock()
        mock_job1.id = "job1"
        mock_job2 = Mock()
        mock_job2.id = "job2"
        workflow_controller.jobs = [mock_job1, mock_job2]

        workflow_controller.execute_all()

        mock_job1.execute.assert_called_once()
        mock_job2.execute.assert_called_once()

    def test_execute_all__when_job_fails__propagates_exception(self, workflow_controller: WorkflowController) -> None:
        """Test execute_all propagates exception when a job fails."""
        mock_job = Mock()
        mock_job.id = "failing_job"
        mock_job.execute.side_effect = Exception("Job execution failed")
        workflow_controller.jobs = [mock_job]

        with pytest.raises(Exception):
            workflow_controller.execute_all()


# =========================================================================== #
# =========================== EXPORT_SCHEMA TESTS ========================= #
# =========================================================================== #


class TestWorkflowControllerExportSchema:
    """Test WorkflowController.export_schema() method."""

    def test_export_schema__returns_dict(self) -> None:
        """Test export_schema returns a dictionary."""
        schema = WorkflowController.export_schema()

        assert isinstance(schema, dict)

    def test_export_schema__contains_required_top_level_keys(self) -> None:
        """Test export_schema contains standard JSON Schema keys."""
        schema = WorkflowController.export_schema()

        assert "properties" in schema
        assert "required" in schema
        assert "title" in schema
        assert "type" in schema

    def test_export_schema__has_correct_type(self) -> None:
        """Test export_schema indicates object type."""
        schema = WorkflowController.export_schema()

        assert schema["type"] == "object"

    def test_export_schema__contains_all_field_properties(self) -> None:
        """Test export_schema includes all WorkflowController fields."""
        schema = WorkflowController.export_schema()

        properties = schema["properties"]
        assert "id" in properties
        assert "description" in properties
        assert "enabled" in properties
        assert "jobs" in properties

    def test_export_schema__id_field_has_correct_type_and_constraints(self) -> None:
        """Test export_schema defines id field correctly."""
        schema = WorkflowController.export_schema()

        id_field = schema["properties"]["id"]
        assert id_field["type"] == "string"
        assert id_field["minLength"] == 1
        assert "description" in id_field

    def test_export_schema__description_field_has_correct_type(self) -> None:
        """Test export_schema defines description field correctly."""
        schema = WorkflowController.export_schema()

        description_field = schema["properties"]["description"]
        assert description_field["type"] == "string"

    def test_export_schema__enabled_field_has_correct_type(self) -> None:
        """Test export_schema defines enabled field correctly."""
        schema = WorkflowController.export_schema()

        enabled_field = schema["properties"]["enabled"]
        assert enabled_field["type"] == "boolean"

    def test_export_schema__jobs_field_is_array(self) -> None:
        """Test export_schema defines jobs field as array."""
        schema = WorkflowController.export_schema()

        jobs_field = schema["properties"]["jobs"]
        assert jobs_field["type"] == "array"

    def test_export_schema__all_fields_are_required(self) -> None:
        """Test export_schema marks all fields as required."""
        schema = WorkflowController.export_schema()

        required_fields = schema["required"]
        assert "id" in required_fields
        assert "description" in required_fields
        assert "enabled" in required_fields
        assert "jobs" in required_fields

    def test_export_schema__includes_nested_definitions(self) -> None:
        """Test export_schema includes definitions for nested models."""
        schema = WorkflowController.export_schema()

        # Schema should contain definitions or $defs for nested models
        assert "$defs" in schema or "definitions" in schema

    def test_export_schema__is_idempotent(self) -> None:
        """Test export_schema returns same result when called multiple times."""
        schema1 = WorkflowController.export_schema()
        schema2 = WorkflowController.export_schema()

        assert schema1 == schema2
