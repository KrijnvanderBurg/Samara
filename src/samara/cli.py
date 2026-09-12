"""CLI command definitions for configuration-driven pipeline management.

This module provides command-line interface commands for managing ETL pipelines
through configuration files. It focuses on three core operations: validating
pipeline configurations, executing pipelines, and exporting JSON schemas for
configuration documentation.

All commands support detailed error handling and proper exit codes to facilitate
CI/CD integration and operational monitoring.
"""

import json
from pathlib import Path

import click

from samara.exceptions import (
    ExitCode,
    SamaraIOError,
    SamaraValidationError,
    SamaraWorkflowConfigurationError,
    SamaraWorkflowError,
)
from samara.settings import get_settings
from samara.telemetry import get_tracer, setup_telemetry, trace_span
from samara.utils.logger import get_logger, set_logger
from samara.workflow.controller import WorkflowController

logger = get_logger(__name__)
tracer = get_tracer()


@click.group()
@click.version_option(package_name="samara")
@click.option(
    "--log-level",
    default=None,
    help="Set the logging level (default: INFO or from environment variable).",
)
@click.option(
    "--trace-parent",
    default=None,
    type=str,
    help="W3C trace parent header for distributed tracing continuation",
)
@click.option(
    "--trace-state",
    default=None,
    type=str,
    help="W3C trace state header for distributed tracing",
)
@click.option(
    "--otlp-traces-endpoint",
    default=None,
    type=str,
    help="OTLP endpoint for trace export (e.g., https://otel-collector:4318/v1/traces)",
)
@click.option(
    "--otlp-logs-endpoint",
    default=None,
    type=str,
    help="OTLP endpoint for logs export (e.g., https://otel-collector:4318/v1/logs)",
)
def cli(
    log_level: str | None = None,
    trace_parent: str | None = None,
    trace_state: str | None = None,
    otlp_traces_endpoint: str | None = None,
    otlp_logs_endpoint: str | None = None,
) -> None:
    """Samara: Configuration-driven workflow framework for Apache Spark and Polars.

    Build and execute data workflows through declarative JSON/YAML configuration
    instead of writing code. Define extracts, transforms, and loads with built-in
    support for validation and schema management.

    Args:
        log_level: The logging level as a string. Must be one of DEBUG, INFO,
            WARNING, ERROR, or CRITICAL (case-insensitive). If not specified,
            uses the value from application settings (SAMARA_LOG_LEVEL env var)
            or defaults to INFO level.
        trace_parent: W3C trace parent header for distributed tracing continuation
        trace_state: W3C trace state header for distributed tracing
        otlp_traces_endpoint: OTLP endpoint URL for exporting traces. Supports:
            - OTEL Collector (recommended): Routes traces through central collector
            - Direct backends: Jaeger, Zipkin, or any OTLP-compatible service
        otlp_logs_endpoint: OTLP endpoint URL for exporting logs. Supports:
            - OTEL Collector (recommended): Routes logs through central collector
            - Direct backends: Loki, or any OTLP-compatible service

    Commands:
        validate: Validate workflow configurations without execution
        run: Execute workflow
        export-schema: Generate JSON schema for workflow configs
    """
    settings = get_settings()
    log_level = log_level or settings.log_level or "INFO"
    set_logger(level=log_level)

    # Initialize telemetry once at startup with trace continuation support
    setup_telemetry(
        service_name="samara",
        otlp_traces_endpoint=otlp_traces_endpoint or settings.otlp_traces_endpoint,
        otlp_logs_endpoint=otlp_logs_endpoint or settings.otlp_logs_endpoint,
        traceparent=trace_parent or settings.trace_parent,
        tracestate=trace_state or settings.trace_state,
    )


@cli.command()
@click.option(
    "--workflow-filepath",
    required=True,
    type=click.Path(exists=False, path_type=Path),
    help="Path to workflow configuration file",
)
@trace_span("validate_workflow")
def validate(
    workflow_filepath: Path,
) -> None:
    """Validate workflow configuration files.

    Load and validate the workflow configuration file to ensure it conforms
    to the expected schema and contains valid settings. This command performs
    fail-fast validation, making it suitable for local development and CI/CD
    workflows.

    Args:
        workflow_filepath: Path to the workflow configuration file in JSON
            or YAML format. The file must exist and define valid workflow
            extracts, transforms, and loads.

    Raises:
        click.exceptions.Exit: Exits with appropriate exit code on error.
    """
    try:
        logger.info("Starting `validate` command")
        logger.info("Workflow config: %s", str(workflow_filepath))

        try:
            _ = WorkflowController.from_file(filepath=workflow_filepath)
        except SamaraIOError as e:
            logger.error("Cannot access workflow configuration file: %s", e)
            raise click.exceptions.Exit(e.exit_code) from e
        except SamaraWorkflowConfigurationError as e:
            logger.error("Workflow configuration is invalid: %s", e)
            raise click.exceptions.Exit(e.exit_code) from e
        except SamaraValidationError as e:
            logger.error("Validation failed: %s", e)
            raise click.exceptions.Exit(e.exit_code) from e

        logger.info("Workflow validation completed successfully")
        logger.info("Command executed successfully with exit code %d (%s).", ExitCode.SUCCESS, ExitCode.SUCCESS.name)

    except click.exceptions.Exit:
        # Re-raise Click's Exit exceptions (these are our controlled exits with proper codes)
        raise
    except KeyboardInterrupt as e:
        logger.warning("Process interrupted by user")
        raise click.exceptions.Exit(ExitCode.KEYBOARD_INTERRUPT) from e
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Unexpected exception %s: %s", type(e).__name__, str(e))
        logger.error("Exception details:", exc_info=True)
        raise click.exceptions.Exit(ExitCode.UNEXPECTED_ERROR) from e


@cli.command()
@click.option(
    "--workflow-filepath",
    required=True,
    type=click.Path(exists=False, path_type=Path),
    help="Path to workflow configuration file",
)
@trace_span("run_pipeline")
def run(
    workflow_filepath: Path,
) -> None:
    """Execute the workflow.

    Load the workflow configuration, then execute the complete workflow.
    The workflow processes all defined jobs in sequence, applying configured
    transforms to ingest, transform, and load data according to specifications.

    Args:
        workflow_filepath: Path to the workflow configuration file in JSON or YAML
            format. Defines the complete workflow including data sources,
            transformation chains, and output destinations.

    Raises:
        click.exceptions.Exit: Exits with appropriate exit code on error.
    """
    try:
        logger.info("Starting `run` command")
        logger.info("Workflow config: %s", str(workflow_filepath))

        try:
            workflow = WorkflowController.from_file(filepath=workflow_filepath)
            logger.info("Executing workflow jobs...")
            workflow.execute_all()
            logger.info("Workflow completed successfully")
            logger.info(
                "Command executed successfully with exit code %d (%s).", ExitCode.SUCCESS, ExitCode.SUCCESS.name
            )
        except SamaraIOError as e:
            logger.error("Cannot access workflow configuration file: %s", e)
            raise click.exceptions.Exit(e.exit_code) from e
        except SamaraWorkflowConfigurationError as e:
            logger.error("Workflow configuration is invalid: %s", e)
            raise click.exceptions.Exit(e.exit_code) from e
        except SamaraValidationError as e:
            logger.error("Configuration validation failed: %s", e)
            raise click.exceptions.Exit(e.exit_code) from e
        except SamaraWorkflowError as e:
            logger.error("Workflow job failed: %s", e)
            raise click.exceptions.Exit(e.exit_code) from e

    except click.exceptions.Exit:
        # Re-raise Click's Exit exceptions (these are our controlled exits with proper codes)
        raise
    except KeyboardInterrupt as e:
        logger.warning("Process interrupted by user")
        raise click.exceptions.Exit(ExitCode.KEYBOARD_INTERRUPT) from e
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Unexpected exception %s: %s", type(e).__name__, str(e))
        logger.error("Exception details:", exc_info=True)
        raise click.exceptions.Exit(ExitCode.UNEXPECTED_ERROR) from e


@cli.command("export-schema")
@click.option(
    "--output-filepath",
    required=True,
    type=click.Path(path_type=Path),
    help="Path where the JSON schema file will be saved",
)
@trace_span("export_workflow_schema")
def export_schema(output_filepath: Path) -> None:
    """Generate and save the workflow configuration JSON schema.

    Export the complete JSON Schema for workflow (ETL pipeline) configurations.
    This schema documents all valid configuration keys, types, constraints, and
    structure for pipeline definitions. The exported schema can be used for
    configuration file validation, IDE auto-completion, and documentation.

    Args:
        output_filepath: Path where the JSON schema file will be written.
            Parent directories are created if they do not exist. The file will
            be formatted with 4-space indentation for readability.

    Raises:
        click.exceptions.Exit: Exits with appropriate exit code on error.
            - ExitCode.SUCCESS: Schema exported successfully
            - ExitCode.IO_ERROR: Cannot write schema file to specified path
            - ExitCode.KEYBOARD_INTERRUPT: User interrupted execution
            - ExitCode.UNEXPECTED_ERROR: Unexpected workflow error

    Note:
        The generated schema includes all supported transforms, source types,
        and load destinations. Use this schema to validate custom workflow
        configurations or integrate with schema validation tooling in your
        development workflow.
    """
    try:
        logger.info("Starting `export-schema` command")
        logger.info("Exporting workflow configuration schema to: %s", str(output_filepath))

        try:
            schema = WorkflowController.export_schema()

            # Ensure parent directory exists
            output_filepath.parent.mkdir(parents=True, exist_ok=True)

            # Write schema to file with pretty formatting
            with open(output_filepath, "w", encoding="utf-8") as f:
                json.dump(schema, f, indent=4, ensure_ascii=False)

            logger.info("Workflow configuration schema exported successfully to: %s", str(output_filepath))
            logger.info(
                "Command executed successfully with exit code %d (%s).", ExitCode.SUCCESS, ExitCode.SUCCESS.name
            )
        except OSError as e:
            logger.error("Failed to write schema file: %s", e)
            raise click.exceptions.Exit(ExitCode.IO_ERROR) from e

    except click.exceptions.Exit:
        # Re-raise Click's Exit exceptions (these are our controlled exits with proper codes)
        raise
    except KeyboardInterrupt as e:
        logger.warning("Process interrupted by user")
        raise click.exceptions.Exit(ExitCode.KEYBOARD_INTERRUPT) from e
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Unexpected exception %s: %s", type(e).__name__, str(e))
        logger.error("Exception details:", exc_info=True)
        raise click.exceptions.Exit(ExitCode.UNEXPECTED_ERROR) from e
