# Samara CLI Reference

This document describes the command-line interface (CLI) for Samara, providing details on available commands, options, and environment variables.

## Overview

Samara provides a command-line interface to validate configurations and execute data pipelines. All commands follow the pattern `python -m samara [command] [options]`.

## Configuration File Formats

Samara accepts both **YAML** (`.yaml`, `.yml`) and **JSON** (`.json`, `.jsonc`) configuration files. Both formats are functionally equivalent, and the framework automatically detects the format based on the file extension. You can mix formats—for example, use YAML for runtime configuration and JSON for alert configuration, or vice versa.

## Commands

### validate

Validates configuration files and optionally tests alert routing rules without executing the pipeline. This is useful for checking configuration integrity and alert functionality before deployment.

```bash
python -m samara validate \
    --alert-filepath="path/to/alerts.yaml" \   # Path to alert configuration file (.yaml, .yml, .json, .jsonc)
    --runtime-filepath="path/to/job.yaml" \    # Path to pipeline runtime configuration (.yaml, .yml, .json, .jsonc)
    [--test-exception="error message"] \       # Optional: Simulates an error to test alert routing
    [--test-env-var="KEY=VALUE"]                 # Optional: Set environment variables for testing triggers
```

Example:
```bash
python -m samara validate \
    --alert-filepath="examples/join_select/alert.jsonc" \
    --runtime-filepath="examples/join_select/job.jsonc" \
    --test-exception="Failed to connect to database" \
    --test-env-var="ENVIRONMENT=PROD"
```

### run

Executes the configured data pipeline using the provided configuration files.

```bash
python -m samara run \
    --alert-filepath path/to/alerts.yaml \   # Path to alert configuration file (.yaml, .yml, .json, .jsonc)
    --runtime-filepath path/to/job.yaml \    # Path to pipeline runtime configuration (.yaml, .yml, .json, .jsonc)
    [--log-level LEVEL]                      # Optional: Override logging level
```

Example:
```bash
python -m samara run \
    --alert-filepath="examples/join_select/slack_alerts.jsonc" \
    --runtime-filepath="examples/join_select/job.jsonc" \
    --log-level="DEBUG"
```

### export-schema

Exports the runtime configuration JSON schema to a file. This schema enables IDE features like autocompletion, validation, and inline documentation when editing configuration files.

```bash
python -m samara export-schema \
    --output-filepath="path/to/runtime_schema.json"  # Path where the JSON schema will be saved
```

Example:
```bash
python -m samara export-schema --output-filepath="dist/runtime_schema.json"
```

**Using the exported schema:**

Once exported, reference the schema in your configuration files to enable IDE support:

```jsonc
{
    "$schema": "path/to/runtime_schema.json",
    "runtime": {
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
--log-level LEVEL     # Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
-v, --version         # Show version information and exit
```

## Environment Variables

Samara respects the following environment variables:

- `FLINT_LOG_LEVEL`: Sets the logging level for the application
- `LOG_LEVEL`: Used as fallback if `FLINT_LOG_LEVEL` is not set

Both variables accept standard Python logging levels: `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`. If neither is set, Samara defaults to `INFO` level.

Example:
```bash
# Set logging level via environment variable
export FLINT_LOG_LEVEL=DEBUG
python -m samara run --alert-filepath ./alerts.jsonc --runtime-filepath ./pipeline.jsonc
```

## Exit Codes

- `0`: SUCCESS - Command executed successfully
- `2`: USAGE_ERROR - Command line usage error
- `10`: INVALID_ARGUMENTS - Invalid command line arguments
- `20`: IO_ERROR - Input/output error (file access issues)
- `30`: CONFIGURATION_ERROR - General configuration error
- `31`: ALERT_CONFIGURATION_ERROR - Alert configuration specific error
- `32`: RUNTIME_CONFIGURATION_ERROR - Runtime configuration specific error
- `40`: VALIDATION_ERROR - Configuration validation failed
- `41`: ALERT_TEST_ERROR - Alert testing functionality failed
- `50`: JOB_ERROR - Error during pipeline execution
- `98`: KEYBOARD_INTERRUPT - User interrupted the operation
- `99`: UNEXPECTED_ERROR - Unhandled exception or unexpected error
