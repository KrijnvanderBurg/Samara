
# Samara Examples

This directory contains complete, ready-to-run examples demonstrating Samara's configuration-driven approach to data pipelines.

## Configuration Formats

Samara supports both **YAML** and **JSON/JSONC** configuration formats. Both are functionally equivalent—choose the format that best fits your team preferences.

---

## 🔍 Example 1: Product Cleanup Pipeline (YAML)

Running this pipeline demonstrates data cleaning operations using YAML configuration format:

- **Drop duplicates**: Removes duplicate product entries from the catalog
  - Empty `columns: []` array means check all columns for duplicates
  - Reduces 12 rows to 10 unique products

- **Type casting**: Converts string columns to appropriate data types
  - `price` converted from string to double for numeric operations
  - `stock_quantity` converted to integer for inventory tracking
  - `is_available` converted to boolean for logical operations
  - `last_updated` converted to date for temporal analysis

- **Column selection**: Projects only relevant columns for the final output
  - Excludes `last_updated` field from final dataset
  - Each column in the select list can be easily modified through configuration

### Configuration: [`examples/yaml_products_cleanup/job.yaml`](./yaml_products_cleanup/job.yaml)

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
          location: examples/yaml_products_cleanup/products/
          method: batch
          options:
            delimiter: ","
            header: true
            inferSchema: false
          schema: examples/yaml_products_cleanup/products_schema.json

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
          location: examples/yaml_products_cleanup/output
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

---

## 🔍 Example 2: Customer Order Analysis (JSON)

Running this pipeline demonstrates multi-source data integration using JSON configuration format:

- **Multi-format extraction**: Seamlessly reads from both CSV and JSON sources
  - Source options like delimiters and headers are configurable through the configuration file
  - Schema validation ensures data type safety and consistency across all sources

- **Flexible transformation chain**: Performed in order as given
  - First a `join` to combine both datasets on `customer_id`
  - Then applies a `select` transform to project only needed columns
  - Each transform function can be easily customized through its arguments

- **Configurable loading**: Writes results as CSV with customizable settings
  - Easily change to Parquet, Delta, or other formats by modifying `data_format`
  - Output mode (overwrite/append) controlled by a simple parameter
  - Output to multiple formats or locations by creating another load entry

### Configuration: [`examples/join_select/job.jsonc`](./join_select/job.jsonc)
```jsonc
{
    "workflow": {
        "id": "customer-orders-pipeline",
        "description": "ETL pipeline for processing customer orders data",
        "enabled": true,
        "jobs": [
            {
                "id": "silver",
                "description": "Combine customer and order source data into a single dataset",
                "enabled": true,
                "engine_type": "spark", // Specifies the processing engine to use
                "extracts": [
                    {
                        "id": "extract-customers",
                        "extract_type": "file", // Read from file system
                        "data_format": "csv", // CSV input format
                        "location": "examples/join_select/customers/", // Source directory
                        "method": "batch", // Process all files at once
                        "options": {
                            "delimiter": ",", // CSV delimiter character
                            "header": true, // First row contains column names
                            "inferSchema": false // Use provided schema instead of inferring
                        },
                        "schema": "examples/join_select/customers_schema.json" // Path to schema definition
                    },
                    {
                        "id": "extract-orders",
                        "extract_type": "file",
                        "data_format": "json", // JSON input format
                        "location": "examples/join_select/orders/",
                        "method": "batch",
                        "options": {
                            "multiLine": true, // Each JSON object may span multiple lines
                            "inferSchema": false // Use provided schema instead of inferring
                        },
                        "schema": "examples/join_select/orders_schema.json"
                    }
                ],
                "transforms": [
                    {
                        "id": "transform-join-orders",
                        "upstream_id": "extract-customers", // First input dataset from extract stage
                        "options": {},
                        "functions": [
                            {
                                "function_type": "join", // Join customers with orders
                                "arguments": { 
                                    "other_upstream_id": "extract-orders", // Second dataset to join
                                    "on": ["customer_id"], // Join key
                                    "how": "inner" // Join type (inner, left, right, full)
                                }
                            },
                            {
                                "function_type": "select", // Select only specific columns
                                "arguments": {
                                    "columns": ["name", "email", "signup_date", "order_id", "order_date", "amount"]
                                }
                            }
                        ]
                    }
                ],
                "loads": [
                    {
                        "id": "load-customer-orders",
                        "upstream_id": "transform-join-orders", // Input dataset for this load
                        "load_type": "file", // Write to file system
                        "data_format": "csv", // Output as CSV
                        "location": "examples/join_select/output", // Output directory
                        "method": "batch", // Write all data at once
                        "mode": "overwrite", // Replace existing files if any
                        "options": {
                            "header": true // Include header row with column names
                        },
                        "schema_export": "" // No schema export
                    }
                ],
                "hooks": {
                    "onStart": [], // Actions to execute before pipeline starts
                    "onFailure": [], // Actions to execute if pipeline fails
                    "onSuccess": [], // Actions to execute if pipeline succeeds
                    "onFinally": [] // Actions to execute after pipeline completes (success or failure)
                }
            }
        ]
    }
}
```

---

For more examples and detailed documentation, see:
- [Getting Started Guide](../docs/getting_started.md)
- [Workflow Configuration Reference](../docs/workflow/README.md)
- [Transform Functions Reference](../docs/workflow/spark.md)

