<p align="center">
  <img src="docs/logo2.svg"/>
</p>

<h1 align="center">Samara</h1>

<p align="center">
  <b>An extensible framework for configuration-driven data pipelines</b>
</p>

<p align="center">
  <a href="https://github.com/krijnvanderburg/Samara/stargazers">⭐ Star this repo</a> •
  <a href="./docs/README.md">📚 Documentation</a> •
  <a href="https://github.com/krijnvanderburg/Samara/issues">🐛 Report Issues</a> •
  <a href="https://github.com/krijnvanderburg/Samara/discussions">💬 Join Discussions</a>
</p>

<p align="center">
  <a href="https://github.com/krijnvanderburg/Samara/releases">📥 Releases</a> •
  <a href="https://github.com/krijnvanderburg/Samara/blob/main/CHANGELOG.md">📝 Changelog (TBD)</a> •
  <a href="https://github.com/krijnvanderburg/Samara/blob/main/CONTRIBUTING.md">🤝 Contributing</a>
</p>

<p align="center">
  <b>Built by Krijn van der Burg for the Data Engineering community</b>
</p>

---

Samara transforms data engineering by shifting from custom code to declarative configuration for complete ETL pipeline workflows. The framework handles all execution details while you focus on what your data should do, not how to implement it. This configuration-driven approach standardizes pipeline patterns across teams, reduces complexity for ETL jobs, improves maintainability, and makes data workflows accessible to users with limited programming experience.

The processing engine is abstracted away through configuration, making it easy to switch engines or run the same pipeline in different environments. The current version supports Apache Spark, with Polars support in development.

## ⚡ Quick Start

### Installation
```bash
# Clone the repository
git clone https://github.com/krijnvanderburg/Samara.git
cd Samara

# Install dependencies
poetry install
```

### Run an example pipeline
```bash
python -m samara run \
  --alert-filepath="examples/yaml_products_cleanup/alert.yaml" \
  --workflow-filepath="examples/yaml_products_cleanup/job.yaml"
```

## 📚 Documentation
Samara's documentation guides you through installation, configuration, and development:

- **[Getting Started](./docs/getting_started.md)** - Installation and basic concepts
- **[Example Pipelines](./examples/)** - Ready-to-run examples demonstrating key features
- **[CLI Reference](./docs/cli.md)** - Command-line interface options and examples
- **[Configuration Reference](./docs/README.md)** - Complete syntax guide for all configuration options
  - **[Workflow System](./docs/workflow/README.md)** - ETL pipeline configuration (extracts, transforms, loads)
  - **[Alert System](./docs/alert/README.md)** - Error handling and notification configuration
- **[Architecture](./docs/architecture.md)** - Design principles and framework structure
- **[Custom Extensions](./docs/architecture.md#extending-with-custom-transforms)** - Building your own transforms

For complete documentation covering all aspects of Samara, visit the documentation home page.

## 🔍 Example: Product Cleanup Pipeline (YAML)
Running this pipeline demonstrates data cleaning operations using YAML configuration format:

- **Drop duplicates**: Removes duplicate product entries from the catalog
  - Empty `columns: []` array means check all columns for duplicates
  - Reduces 12 rows to 10 unique products

- **Type casting**: Converts string columns to appropriate data types
  - `price` converted from string to double for numeric operations
  - `stock_quantity` converted to integer for inventory tracking
  - `is_available` converted to boolean for logical operations

- **Column selection**: Projects only relevant columns for the final output
  - Excludes `last_updated` field from final dataset
  - Each column in the select list can be easily modified through configuration

### Configuration: [`examples/yaml_products_cleanup/job.yaml`](./examples/yaml_products_cleanup/job.yaml)
**Flexible Configuration**: Define pipelines in YAML or JSON—both formats are fully supported and functionally equivalent. Choose the format that best fits your team preferences.
```yaml
workflow:
  id: product-cleanup-pipeline
  description: ETL pipeline for cleaning and standardizing product catalog data
  enabled: true

  jobs:
    - id: clean-products
      description: Remove duplicates, cast types, and select relevant columns from product data
      enabled: true
      engine_type: spark

      # Extract product data from CSV file
      extracts:
        - id: extract-products
          extract_type: file
          data_format: csv
          location: examples/products_cleanup/products/
          method: batch
          options:
            delimiter: ","
            header: true
            inferSchema: false
          schema: examples/products_cleanup/products_schema.json

      # Transform the data: remove duplicates, cast types, and select columns
      transforms:
        - id: transform-clean-products
          upstream_id: extract-products
          options: {}
          functions:
            # Step 1: Remove duplicate rows based on all columns
            - function_type: dropDuplicates
              arguments:
                columns: []  # Empty array means check all columns for duplicates

            # Step 2: Cast columns to appropriate data types
            - function_type: cast
              arguments:
                columns:
                  - column_name: price
                    cast_type: double
                  - column_name: stock_quantity
                    cast_type: integer
                  - column_name: is_available
                    cast_type: boolean
                  - column_name: last_updated
                    cast_type: date

            # Step 3: Select only the columns we need for the output
            - function_type: select
              arguments:
                columns:
                  - product_id
                  - product_name
                  - category
                  - price
                  - stock_quantity
                  - is_available

      # Load the cleaned data to output
      loads:
        - id: load-clean-products
          upstream_id: transform-clean-products
          load_type: file
          data_format: csv
          location: examples/products_cleanup/output
          method: batch
          mode: overwrite
          options:
            header: true
          schema_export: ""

      # Event hooks for pipeline lifecycle
      hooks:
        onStart: []
        onFailure: []
        onSuccess: []
        onFinally: []
```

## 🚀 Getting Help
- [**Documentation**](./docs/README.md): Refer to the Configuration Reference section for detailed syntax
- [**Examples**](./examples/): Explore working samples in the examples directory
- [**Community**](https://github.com/krijnvanderburg/Samara/issues): Ask questions and report issues on GitHub Issues
- [**Source Code**](./src/samara/): Browse the implementation in the src/samara directory

## 🤝 Contributing
Contributions are welcome! Feel free to submit a pull request and message Krijn van der Burg on [linkedin](https://linkedin.com/in/krijnvanderburg/).

## 📄 License
This project is licensed under the Creative Commons Attribution 4.0 International License (CC-BY-4.0) - see the LICENSE file for details.
