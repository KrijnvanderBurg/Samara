# Samara: Configuration-Driven Data Processing Framework

Samara is a framework that lets you **define entire data pipelines through configuration files rather than code**. This declarative approach is the core approach - transforming data engineering from a programming task to a configuration exercise.

## Core Concept

**Configuration over code**: Define complete data pipelines in JSON with:
- Data sources and connection details
- Transformation chains and their parameters
- Output destinations and formats
- Event-triggered actions and alerts

## Key Components

### Data Pipeline Definition
- **Extracts**: Configure data sources (CSV, JSON, databases) with all connection parameters
- **Transforms**: Chain operations (`select`, `filter`, `join`, `cast`) through simple JSON configuration
- **Loads**: Define outputs with formats, paths, and write modes

### Engine Flexibility
- **Multi-engine architecture**: Support for different ETL engines implementations like Pandas, Polars and more.
- **Engine-agnostic configurations**: Same pipeline definition works across different processing backends

### Auxiliary Systems
- **Alert System**: Configurable notifications via email, HTTP webhooks, and files based on rule-based triggers. Extendable to any communication platform.
- **Event Hooks**: Execute custom actions at key pipeline stages (`onStart`, `onFailure`, `onSuccess`, `onFinally`). Extendable to any custom logic.

## Benefits

- **Standardization**: Consistent pipeline patterns across projects and teams
- **Maintainability**: Changes require updating JSON, not refactoring code
- **Accessibility**: Pipeline logic becomes readable to non-developers
- **Version Control**: Pipeline definitions evolve with clear diffs in version control
- **Reduced Development Time**: Eliminate boilerplate code for common operations

By moving pipeline definition from code to configuration, Samara makes data processing more accessible, consistent, and maintainable while providing the flexibility to extend with custom transforms when needed.