# Samara CLI Reference

This document describes the command-line interface (CLI) for Samara, providing details on available commands, options, and environment variables.

## Overview

Samara provides a command-line interface to validate configurations and execute data pipelines. All commands follow the pattern `python -m samara [command] [options]`.

## Configuration File Formats

Samara accepts both **YAML** (`.yaml`, `.yml`) and **JSON** (`.json`, `.jsonc`) configuration files. Both formats are functionally equivalent, and the framework automatically detects the format based on the file extension. You can mix formats—for example, use YAML for workflow configuration and JSON for alert configuration, or vice versa.

## Commands

### validate

Validates configuration files and optionally tests alert routing rules without executing the pipeline. This is useful for checking configuration integrity and alert functionality before deployment.

```bash
python -m samara validate \
    --alert-filepath="path/to/alerts.yaml" \   # Path to alert configuration file (.yaml, .yml, .json, .jsonc)
    --workflow-filepath="path/to/job.yaml" \    # Path to pipeline workflow configuration (.yaml, .yml, .json, .jsonc)
    [--test-exception="error message"] \       # Optional: Simulates an error to test alert routing
    [--test-env-var="KEY=VALUE"]                 # Optional: Set environment variables for testing triggers
```

Example:
```bash
python -m samara validate \
    --alert-filepath="examples/join_select/alert.jsonc" \
    --workflow-filepath="examples/join_select/job.jsonc" \
    --test-exception="Failed to connect to database" \
    --test-env-var="ENVIRONMENT=PROD"
```

### run

Executes the configured data pipeline using the provided configuration files.

```bash
python -m samara run \
    --alert-filepath path/to/alerts.yaml \   # Path to alert configuration file (.yaml, .yml, .json, .jsonc)
    --workflow-filepath path/to/job.yaml     # Path to pipeline workflow configuration (.yaml, .yml, .json, .jsonc)
```

Example:
```bash
python -m samara run \
    --alert-filepath="examples/join_select/slack_alerts.jsonc" \
    --workflow-filepath="examples/join_select/job.jsonc"
```

### export-schema

Exports the workflow configuration JSON schema to a file. This schema enables IDE features like autocompletion, validation, and inline documentation when editing configuration files.

```bash
python -m samara export-schema \
    --output-filepath="path/to/workflow_schema.json"  # Path where the JSON schema will be saved
```

Example:
```bash
python -m samara export-schema --output-filepath="dist/workflow_schema.json"
```

**Using the exported schema:**

Once exported, reference the schema in your configuration files to enable IDE support:

```jsonc
{
    "$schema": "path/to/workflow_schema.json",
    "workflow": {
        "id": "my-pipeline",
        // IDE now provides autocompletion and validation
    }
}
```

This provides:
- **Autocompletion** of field names and values as you type
- **Inline validation** showing errors for invalid configurations
- **Documentation tooltips** displaying field descriptions and constraints
- **Type checking** ensuring values match expected types

## Global Options

The following options can be used with any command:

```bash
--log-level LEVEL                           # Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
--otlp-traces-endpoint ENDPOINT             # OpenTelemetry endpoint for traces (e.g., http://localhost:4318/v1/traces)
--otlp-metrics-endpoint ENDPOINT            # OpenTelemetry endpoint for metrics (e.g., http://localhost:4318/v1/metrics)
--traceparent TRACEPARENT                   # W3C Trace Context traceparent for distributed tracing
--tracestate TRACESTATE                     # W3C Trace Context tracestate for distributed tracing
-v, --version                               # Show version information and exit
```

Example with global options:
```bash
python -m samara run \
    --log-level DEBUG \
    --otlp-traces-endpoint "http://localhost:4318/v1/traces" \
    --otlp-metrics-endpoint "http://localhost:4318/v1/metrics" \
    --traceparent "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" \
    --alert-filepath ./alerts.jsonc \
    --workflow-filepath ./pipeline.jsonc
```

## Environment Variables

Samara optionally uses the following environment variables. CLI arguments always take priority over environment variables.

### Logging

Controls the verbosity of application output across all Samara components.

```bash
SAMARA_LOG_LEVEL=DEBUG                      # Logging level for the application (DEBUG, INFO, WARNING, ERROR, CRITICAL)
LOG_LEVEL=DEBUG                             # Fallback if SAMARA_LOG_LEVEL is not set
```

Standard Python logging levels are supported: `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`. If neither variable is set, Samara defaults to `INFO` level. The `SAMARA_LOG_LEVEL` variable takes precedence over `LOG_LEVEL`.

Example:
```bash
export SAMARA_LOG_LEVEL=DEBUG
python -m samara run --alert-filepath ./alerts.jsonc --workflow-filepath ./pipeline.jsonc
```

### Telemetry

Configures OpenTelemetry endpoints for sending traces and metrics. Both can be configured independently—send to different backends or the same OTEL Collector.

```bash
SAMARA_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces     # OTLP HTTP endpoint for traces
SAMARA_OTLP_METRICS_ENDPOINT=http://localhost:4318/v1/metrics   # OTLP HTTP endpoint for metrics
```

If not set, telemetry is not exported. Both traces and metrics can be configured independently—send traces to one backend and metrics to another, or send both to the same OpenTelemetry Collector.

Example:
```bash
export SAMARA_OTLP_TRACES_ENDPOINT="http://localhost:4318/v1/traces"
export SAMARA_OTLP_METRICS_ENDPOINT="http://localhost:4318/v1/metrics"
python -m samara run --alert-filepath ./alerts.jsonc --workflow-filepath ./pipeline.jsonc
```

### Distributed Tracing

Enables Samara to participate in distributed tracing by continuing an existing trace from an upstream system.

```bash
SAMARA_TRACEPARENT=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01           # W3C Trace Context traceparent
SAMARA_TRACESTATE=vendorname=opaquevalue                                             # W3C Trace Context tracestate
```

Both variables follow the [W3C Trace Context](https://www.w3.org/TR/trace-context/) specification.

Example:
```bash
# Continue an existing trace from upstream system
export SAMARA_TRACEPARENT="00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
python -m samara run --alert-filepath ./alerts.jsonc --workflow-filepath ./pipeline.jsonc

# CLI arguments override environment variables
export SAMARA_TRACEPARENT="00-OLD_TRACE_ID-b7ad6b7169203331-01"
python -m samara run \
    --traceparent="00-NEW_TRACE_ID-b7ad6b7169203331-01" \
    --alert-filepath ./alerts.jsonc \
    --workflow-filepath ./pipeline.jsonc
```

## Exit Codes

- `0`: SUCCESS - Command executed successfully
- `2`: USAGE_ERROR - Command line usage error
- `10`: INVALID_ARGUMENTS - Invalid command line arguments
- `20`: IO_ERROR - Input/output error (file access issues)
- `30`: CONFIGURATION_ERROR - General configuration error
- `31`: ALERT_CONFIGURATION_ERROR - Alert configuration specific error
- `32`: WORKFLOW_CONFIGURATION_ERROR - Workflow configuration specific error
- `40`: VALIDATION_ERROR - Configuration validation failed
- `41`: ALERT_TEST_ERROR - Alert testing functionality failed
- `50`: JOB_ERROR - Error during pipeline execution
- `98`: KEYBOARD_INTERRUPT - User interrupted the operation
- `99`: UNEXPECTED_ERROR - Unhandled exception or unexpected error
