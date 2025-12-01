"""CLI command definitions for configuration-driven pipeline management.

This module provides command-line interface commands for managing ETL pipelines
through configuration files. It focuses on three core operations: validating
pipeline configurations with optional alert testing, executing pipelines with
integrated alerting, and exporting JSON schemas for configuration documentation.

All commands support detailed error handling and proper exit codes to facilitate
CI/CD integration and operational monitoring.
"""

import json
import logging
import os
import time
from pathlib import Path

import click

from samara.alert import AlertController
from samara.exceptions import (
    ExitCode,
    SamaraAlertConfigurationError,
    SamaraAlertTestError,
    SamaraIOError,
    SamaraValidationError,
    SamaraWorkflowConfigurationError,
    SamaraWorkflowError,
)
from samara.settings import get_settings
from samara.telemetry import get_meter, get_tracer, setup_telemetry, trace_span
from samara.utils.logger import get_logger, set_logger
from samara.workflow.controller import WorkflowController

logger: logging.Logger = get_logger(__name__)
tracer = get_tracer()
meter = get_meter()

# Single metrics with labels for command and status. Using a single counter and
# histogram keeps the metrics surface small and lets us filter by labels
# (e.g. command="run") in dashboards.
invocations_counter = meter.create_counter(
    name="cli_invocations_total",
    description="Number of times CLI commands have been invoked",
)

duration_histogram = meter.create_histogram(
    name="cli_duration_seconds",
    description="Duration of CLI commands in seconds",
)


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
    "--otlp-metrics-endpoint",
    default=None,
    type=str,
    help="OTLP endpoint for metrics export (e.g., https://otel-collector:4318/v1/metrics)",
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
    otlp_metrics_endpoint: str | None = None,
    otlp_logs_endpoint: str | None = None,
) -> None:
    """Samara: Configuration-driven workflow framework for Apache Spark and Polars.

    Build and execute data workflows through declarative JSON/YAML configuration
    instead of writing code. Define extracts, transforms, and loads with built-in
    support for alerts, validation, and schema management.

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
        otlp_metrics_endpoint: OTLP endpoint URL for exporting metrics. Supports:
            - OTEL Collector (recommended): Routes metrics through central collector
            - Direct backends: Prometheus, or any OTLP-compatible service
        otlp_logs_endpoint: OTLP endpoint URL for exporting logs. Supports:
            - OTEL Collector (recommended): Routes logs through central collector
            - Direct backends: Loki, or any OTLP-compatible service

    Commands:
        validate: Validate workflow configurations without execution
        run: Execute workflow with integrated alerting
        export-schema: Generate JSON schema for workflow configs
    """
    settings = get_settings()
    log_level = log_level or settings.log_level or "INFO"
    set_logger(level=log_level)

    # Initialize telemetry once at startup with trace continuation support
    setup_telemetry(
        service_name="samara",
        otlp_traces_endpoint=otlp_traces_endpoint or settings.otlp_traces_endpoint,
        otlp_metrics_endpoint=otlp_metrics_endpoint or settings.otlp_metrics_endpoint,
        otlp_logs_endpoint=otlp_logs_endpoint or settings.otlp_logs_endpoint,
        traceparent=trace_parent or settings.trace_parent,
        tracestate=trace_state or settings.trace_state,
    )


@cli.command()
@click.option(
    "--alert-filepath",
    required=True,
    type=click.Path(exists=False, path_type=Path),
    help="Path to alert configuration file",
)
@click.option(
    "--workflow-filepath",
    required=True,
    type=click.Path(exists=False, path_type=Path),
    help="Path to workflow configuration file",
)
@click.option(
    "--test-exception",
    type=str,
    default=None,
    help="Test exception message to trigger alert testing",
)
@click.option(
    "--test-env-var",
    multiple=True,
    type=str,
    help="Test env vars (KEY=VALUE)",
)
@trace_span("validate_workflow")
def validate(
    alert_filepath: Path,
    workflow_filepath: Path,
    test_exception: str | None,
    test_env_var: tuple[str, ...],
) -> None:
    """Validate workflow configuration files with optional alert testing.

    Load and validate both alert and workflow configuration files to ensure they
    conform to expected schemas and contain valid settings. This command performs
    fail-fast validation without alerting on configuration errors (unlike the run
    command), making it suitable for local development and CI/CD workflows where
    validation failures should not trigger alerts.

    Optionally trigger a test alert to verify alert system functionality using
    a test exception message or environment variables.

    Args:
        alert_filepath: Path to the alert configuration file in JSON or YAML
            format. The file must exist and contain valid alert configuration
            with triggers and channels.
        workflow_filepath: Path to the workflow configuration file in JSON
            or YAML format. The file must exist and define valid workflow
            extracts, transforms, and loads.
        test_exception: Optional test exception message string. When provided,
            triggers a test alert to verify alert system functionality. If
            provided with test_env_var, this takes precedence for the message.
        test_env_var: Optional environment variables to set before validation
            in KEY=VALUE format. Useful for testing environment-dependent
            configurations without affecting system environment permanently.

    Raises:
        click.exceptions.Exit: Exits with appropriate exit code on error.

    Note:
        This command does NOT send alerts on configuration errors, only on
        test alerts if explicitly requested. This prevents alert fatigue
        during development and validation cycles. For the actual workflow
        execution with alert integration, use the 'run' command.
    """
    invocations_counter.add(1, {"command": "validate"})
    start_time = time.perf_counter()
    try:
        logger.info("Starting validation command")
        logger.info("Workflow config: %s", workflow_filepath)
        logger.info("Alert config: %s", alert_filepath)

        # Parse test env vars
        test_env_vars = None
        if test_env_var:
            test_env_vars = {}
            for env_var_str in test_env_var:
                key, value = env_var_str.split("=", 1)
                test_env_vars[key] = value

        # Set test env vars if provided
        if test_env_vars:
            for key, value in test_env_vars.items():
                os.environ[key] = value

        try:
            alert = AlertController.from_file(filepath=alert_filepath)
        except SamaraIOError as e:
            logger.error("Cannot access alert configuration file: %s", e)
            raise click.exceptions.Exit(e.exit_code)
        except SamaraAlertConfigurationError as e:
            logger.error("Alert configuration is invalid: %s", e)
            raise click.exceptions.Exit(e.exit_code)

        try:
            _ = WorkflowController.from_file(filepath=workflow_filepath)
            # Not alerting on exceptions as a validate command is often run locally or from CICD
            # and thus an alert would be drowning out real alerts
        except SamaraIOError as e:
            logger.error("Cannot access workflow configuration file: %s", e)
            raise click.exceptions.Exit(e.exit_code)
        except SamaraWorkflowConfigurationError as e:
            logger.error("Workflow configuration is invalid: %s", e)
            raise click.exceptions.Exit(e.exit_code)
        except SamaraValidationError as e:
            logger.error("Validation failed: %s", e)
            raise click.exceptions.Exit(e.exit_code)

        # Trigger test exception if specified (either message or env vars)
        if test_exception or test_env_vars:
            try:
                message = test_exception or "Test alert triggered"
                raise SamaraAlertTestError(message)
            except SamaraAlertTestError as e:
                alert.evaluate_trigger_and_alert(title="Test Alert", body="Test alert", exception=e)
                raise click.exceptions.Exit(e.exit_code)

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
    finally:
        duration = time.perf_counter() - start_time
        logger.info("Validate command completed in %.2f seconds", duration)
        duration_histogram.record(duration, {"command": "validate"})


@cli.command()
@click.option(
    "--alert-filepath",
    required=True,
    type=click.Path(exists=False, path_type=Path),
    help="Path to alert configuration file",
)
@click.option(
    "--workflow-filepath",
    required=True,
    type=click.Path(exists=False, path_type=Path),
    help="Path to workflow configuration file",
)
@trace_span("run_pipeline")
def run(
    alert_filepath: Path,
    workflow_filepath: Path,
) -> None:
    """Execute the workflow with integrated alert monitoring.

    Load workflow and alert configurations, then execute the complete workflow.
    The workflow processes all defined jobs in sequence, applying configured
    transforms to ingest, transform, and load data according to specifications.
    Errors during workflow execution are captured and alerts are sent based on
    configured alert rules and triggers.

    Args:
        alert_filepath: Path to the alert configuration file in JSON or YAML
            format. Defines alert channels (email, HTTP, file) and trigger rules
            that determine when and how alerts are sent during execution.
        workflow_filepath: Path to the workflow configuration file in JSON or YAML
            format. Defines the complete workflow including data sources,
            transformation chains, and output destinations.

    Raises:
        click.exceptions.Exit: Exits with appropriate exit code on error.

    Note:
        All exceptions during workflow execution trigger alert evaluation,
        allowing configured alert rules to send notifications based on
        error type and severity. This enables operational visibility into
        workflow failures and automating incident response workflows.
    """
    invocations_counter.add(1, {"command": "run"})
    start_time = time.perf_counter()
    # Increment the run command counter metric
    try:
        logger.info("Starting workflow execution command")
        logger.info("Workflow config: %s", workflow_filepath)
        logger.info("Alert config: %s", alert_filepath)

        try:
            alert = AlertController.from_file(filepath=alert_filepath)
        except SamaraIOError as e:
            logger.error("Cannot access alert configuration file: %s", e)
            raise click.exceptions.Exit(e.exit_code)
        except SamaraAlertConfigurationError as e:
            logger.error("Alert configuration is invalid: %s", e)
            raise click.exceptions.Exit(e.exit_code)

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
            alert.evaluate_trigger_and_alert(
                title="Workflow Configuration File Error",
                body="Failed to read workflow configuration file",
                exception=e,
            )
            raise click.exceptions.Exit(e.exit_code)
        except SamaraWorkflowConfigurationError as e:
            logger.error("Workflow configuration is invalid: %s", e)
            alert.evaluate_trigger_and_alert(
                title="Workflow Configuration Error", body="Invalid workflow configuration", exception=e
            )
            raise click.exceptions.Exit(e.exit_code)
        except SamaraValidationError as e:
            logger.error("Configuration validation failed: %s", e)
            alert.evaluate_trigger_and_alert(
                title="Workflow Validation Error", body="Configuration validation failed", exception=e
            )
            raise click.exceptions.Exit(e.exit_code)
        except SamaraWorkflowError as e:
            logger.error("Workflow job failed: %s", e)
            alert.evaluate_trigger_and_alert(
                title="Workflow Execution Error", body="Workflow error during execution", exception=e
            )
            raise click.exceptions.Exit(e.exit_code)

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
    finally:
        duration = time.perf_counter() - start_time
        logger.info("Run command completed in %.2f seconds", duration)
        duration_histogram.record(duration, {"command": "run"})


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
    invocations_counter.add(1, {"command": "export-schema"})
    start_time = time.perf_counter()
    try:
        logger.info("Running 'export-schema' command...")
        logger.info("Exporting workflow configuration schema to: %s", output_filepath)

        try:
            schema = WorkflowController.export_schema()

            # Ensure parent directory exists
            output_filepath.parent.mkdir(parents=True, exist_ok=True)

            # Write schema to file with pretty formatting
            with open(output_filepath, "w", encoding="utf-8") as f:
                json.dump(schema, f, indent=4, ensure_ascii=False)

            logger.info("Workflow configuration schema exported successfully to: %s", output_filepath)
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
    finally:
        duration = time.perf_counter() - start_time
        duration_histogram.record(duration, {"command": "export-schema"})
