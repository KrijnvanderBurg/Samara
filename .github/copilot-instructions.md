# Samara
Samara is a Python framework for building ETL data pipelines through JSON/YAML configuration files instead of code. Users define data sources, transformations, outputs, and alerts in config files. Samara parses and validates these configs using Pydantic models, then executes the pipeline on Apache Spark (with Polars support in development).

## Project structure
- `src/samara/` - Application source code
  - `workflow/` - ETL pipeline components (extracts, transforms, loads)
  - `alert/` - Notification system (email, HTTP webhooks, file channels)
  - `cli.py` - CLI commands (validate, run, export-schema)
- `tests/` - Unit, integration, and end-to-end tests
  - `unit/` - Component tests
  - `e2e/` - Full pipeline tests with real configs
  - `conftest.py` - Shared pytest fixtures
- `docs/` - User documentation (Markdown)
- `examples/` - Sample pipeline configurations

## Development environment
All dependencies and tool configurations are defined in `pyproject.toml`. Poetry virtualenv is disabled - dependencies are installed directly:

```bash
.github/scripts/poetry-install.sh    # Install dependencies
```

## Running tests
```bash
.github/scripts/run-pytest.sh        # Run all tests with coverage
```

## Running pipelines
Execute a pipeline with configuration files:

```bash
python -m samara run \
  --alert-filepath examples/yaml_products_cleanup/alert.yaml \
  --workflow-filepath examples/yaml_products_cleanup/job.yaml
```

Validate configurations without running:

```bash
python -m samara validate \
  --alert-filepath <path> \
  --workflow-filepath <path>
```
