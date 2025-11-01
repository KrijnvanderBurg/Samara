# Samara Documentation
Welcome to the Samara documentation. Samara is a configuration-driven data processing framework that lets you define entire data pipelines through JSON configuration files rather than code.

## [Getting Started](./getting_started.md)
Install Samara, run example pipelines, and learn how to create your own data pipelines using configuration files. This guide provides step-by-step instructions for new users and includes examples of basic pipeline configurations.

## [CLI Reference](./cli.md)
Complete reference for Samara's command-line interface with commands for:
- `validate` - Check configuration files and alerting before execution
- `run` - Execute data pipelines
- `export-schema` - Generate JSON schema for IDE autocompletion and validation

Includes supported options, environment variables, and exit codes for troubleshooting.

## [Architecture](./architecture.md)
Understand Samara's design principles and how the framework processes pipelines:
- Design Principles: Type safety, engine agnosticism, composability, and other core concepts
- Pipeline Execution Flow: How configurations are parsed and executed
- Component Structure: Class relationships and system organization
- Extension Mechanisms: How to extend Samara with custom transforms

## Core Systems
Samara's architecture consists of two integrated systems that work together through configuration:

### [Workflow System](./workflow/README.md)
The workflow system orchestrates ETL pipelines through configuration files:
- **Extracts**: Configure data sources (CSV, JSON, databases)
- **Transforms**: Chain operations through configuration
- **Loads**: Define outputs with formats and parameters
 
ETL engines and specific configurations:
- **[Spark Engine](./workflow/spark.md)**: Spark-specific configuration options
- **Polars Engine**: Under development.

### [Alert System](./alert/README.md)
Configure notifications when pipeline errors occur:
- [**Channels**](./alert/channels.md): Configure where alerts are sent (email, HTTP webhooks, file logs)
- [**Triggers**](./alert/triggers.md): Define when alerts are sent (rule-based conditions)
- **Templates** (TBD): Format alert messages with custom templates

## [Example Configurations](../examples/)
The examples folder includes complete examples of:
- Workflow pipeline configurations
- Spark-specific configurations
- Alert system configurations

These examples demonstrate how to combine Samara's components to build complete data processing solutions without writing code.
