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

## Code quality checks
Run these before committing:

```bash
.github/scripts/run-ruff-formatter.sh    # Format code
.github/scripts/run-ruff-linter.sh       # Lint with ruff
.github/scripts/run-mypy.sh              # Type check with mypy
.github/scripts/run-pyright.sh           # Type check with pyright
.github/scripts/run-pylint.sh            # Lint with pylint
.github/scripts/run-flake8.sh            # Lint with flake8
.github/scripts/run-bandit.sh            # Security checks
.github/scripts/run-semgrep.sh           # Security pattern matching
.github/scripts/run-vulture.sh           # Dead code detection
.github/scripts/run-trufflehog.sh        # Secret scanning
```

## Building package and documentation
```bash
.github/scripts/run-build-package.sh     # Build Python package
.github/scripts/run-sphinx.sh docs/      # Build Sphinx documentation
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
