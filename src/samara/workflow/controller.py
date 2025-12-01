"""Workflow controller - Manage ETL pipeline configuration and execution.

This module provides workflow configuration management using Pydantic models
for type safety and validation. It enables pipeline authors to define job
execution sequences through configuration files in JSON or YAML formats.
"""

from pathlib import Path
from typing import Any, Final, Self

from pydantic import Field, ValidationError
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaValue

from samara import BaseModel
from samara.exceptions import SamaraIOError, SamaraWorkflowConfigurationError
from samara.telemetry import trace_span
from samara.utils.file import FileHandlerContext
from samara.utils.logger import get_logger
from samara.workflow.jobs import JobUnion

logger = get_logger(__name__)

WORKFLOW: Final = "workflow"


class PreserveFieldOrderJsonSchema(GenerateJsonSchema):
    """Preserve field definition order in JSON schema generation.

    By default, Pydantic sorts JSON schema keys alphabetically. This custom
    generator preserves the order of fields as they appear in model definitions,
    making schema outputs more readable and consistent with configuration intent.
    """

    def sort(self, value: JsonSchemaValue, parent_key: str | None = None) -> JsonSchemaValue:
        """Return unmodified JSON schema value to preserve field order.

        Args:
            value: The JSON schema value (not sorted in this implementation).
            parent_key: Optional parent key context (unused).

        Returns:
            The unmodified value, preserving original field definition order.
        """
        _ = parent_key
        return value


class WorkflowController(BaseModel):
    """Configure and execute ETL pipeline jobs.

    This class serves as the central configuration holder for the entire
    framework, enabling pipeline authors to define job sequences through
    declarative configuration. It provides type-safe access to global settings
    and orchestrates job execution within the ETL pipeline.

    Attributes:
        id_: Unique identifier for the workflow configuration instance.
        description: Human-readable description of the workflow purpose.
        enabled: Whether this workflow should execute its jobs.
        jobs: List of jobs to execute sequentially in the pipeline.

    Example:
        >>> config_path = Path("pipeline_config.json")
        >>> workflow = WorkflowController.from_file(config_path)
        >>> workflow.execute_all()

        **Configuration in JSON:**
        ```
        {
            "workflow": {
                "id": "daily_etl_pipeline",
                "description": "Daily data processing pipeline",
                "enabled": true,
                "jobs": [
                    {
                        "type": "job",
                        "id": "load_customers",
                        "extracts": [...],
                        "transforms": [...],
                        "loads": [...]
                    }
                ]
            }
        }
        ```

        **Configuration in YAML:**
        ```
        workflow:
          id: daily_etl_pipeline
          description: Daily data processing pipeline
          enabled: true
          jobs:
            - type: job
              id: load_customers
              extracts: [...]
              transforms: [...]
              loads: [...]
        ```

    Note:
        Each job in the pipeline is executed sequentially. If any job fails,
        execution stops and an exception is raised. Jobs are responsible for
        clearing their own engine-specific registries after execution.
    """

    id_: str = Field(..., alias="id", description="Unique identifier for the workflow configuration", min_length=1)
    description: str = Field(..., description="Description of the workflow configuration")
    enabled: bool = Field(..., description="Whether this workflow is enabled")
    jobs: list[JobUnion] = Field(..., description="List of jobs to execute in the ETL pipeline")

    @classmethod
    @trace_span("workflow_controller.from_file")
    def from_file(cls, filepath: Path) -> Self:
        """Load workflow configuration from a file.

        Reads and parses a configuration file (JSON or YAML) to create a fully
        configured WorkflowController instance. Automatically detects file format
        and handles deserialization, validation, and error reporting.

        Args:
            filepath: Path to the configuration file (JSON or YAML format).

        Returns:
            A fully configured WorkflowController instance ready for execution.

        Raises:
            SamaraIOError: If file I/O fails (file not found, permission denied,
                unreadable format, etc.).
            SamaraWorkflowConfigurationError: If the file is missing the required
                'workflow' section or contains invalid configuration data.

        Example:
            >>> from pathlib import Path
            >>> config_file = Path("config.json")
            >>> workflow = WorkflowController.from_file(config_file)
            >>> workflow.execute_all()

        Note:
            The configuration file must contain a top-level 'workflow' key
            with all required WorkflowController fields (id, description,
            enabled, jobs).
        """
        logger.info("Creating WorkflowManager from file: %s", filepath)

        try:
            handler = FileHandlerContext.from_filepath(filepath=filepath)
            dict_: dict[str, Any] = handler.read()
        except (OSError, ValueError) as e:
            logger.error("Failed to read workflow configuration file: %s", e)
            raise SamaraIOError(f"Cannot load workflow configuration from '{filepath}': {e}") from e

        try:
            workflow = cls(**dict_[WORKFLOW])
            logger.info("Successfully created WorkflowManager from configuration file: %s", filepath)
            return workflow
        except KeyError as e:
            raise SamaraWorkflowConfigurationError(
                f"Missing 'workflow' section in configuration file '{filepath}'"
            ) from e
        except ValidationError as e:
            raise SamaraWorkflowConfigurationError(f"Invalid workflow configuration in file '{filepath}': {e}") from e

    @classmethod
    def export_schema(cls) -> dict[str, Any]:
        """Export JSON schema for configuration documentation and validation.

        Generate the complete JSON schema definition for WorkflowController,
        including all nested models and validation rules. The schema preserves
        field definition order for improved readability and can be used for
        documentation generation, IDE autocompletion, and configuration validation.

        Returns:
            Complete JSON schema dictionary (JSON Schema Draft 2020-12 format)
            with all field definitions, constraints, and descriptions.

        Example:
            >>> schema = WorkflowController.export_schema()
            >>> print(schema['properties']['id']['type'])
            'string'
            >>> print(schema['required'])
            ['id', 'description', 'enabled', 'jobs']

        Note:
            This schema is useful for generating configuration templates,
            validating external configuration sources, or providing IDE hints
            for configuration files.
        """
        logger.debug("Exporting WorkflowController JSON schema")
        return cls.model_json_schema(schema_generator=PreserveFieldOrderJsonSchema)

    @trace_span("workflow_controller.execute_all")
    def execute_all(self) -> None:
        """Execute all jobs in the ETL pipeline sequentially.

        Iterates through all configured jobs in order and executes each one.
        Each job is responsible for clearing its own engine-specific registries
        after execution completes. If any job fails, execution stops immediately
        and an exception is raised.

        Note:
            - Respects the 'enabled' flag; returns early if disabled
            - Logs progress for each job execution
            - Jobs execute sequentially; parallel execution not supported
            - Each job must handle its own engine cleanup after execution

        Raises:
            Any exception raised by individual job.execute() calls during
            pipeline execution.

        Example:
            >>> workflow = WorkflowController.from_file(Path("config.json"))
            >>> workflow.execute_all()
        """
        if not self.enabled:
            logger.info("Workflow is disabled")
            return

        logger.info("Executing all %d jobs in ETL pipeline", len(self.jobs))

        for i, job in enumerate(self.jobs):
            logger.info("Executing job %d/%d: %s", i + 1, len(self.jobs), job.id_)
            job.execute()

        logger.info("All jobs in ETL pipeline executed successfully")
