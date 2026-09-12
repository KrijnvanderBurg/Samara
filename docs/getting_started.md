# Getting Started
This document explains how to install the framework, run your first data pipeline using the provided examples, and create and configure your own custom pipelines using configuration files instead of writing code.

### Installation
```bash
# Clone the repository
git clone https://github.com/krijnvanderburg/Samara.git
cd Samara

# Install dependencies
poetry install
```

### Run the Example Pipeline
Start with the included example pipeline:

```bash
python -m samara run \
  --workflow-filepath="examples/yaml_products_cleanup/job.yaml"
```

## Configuration Formats

Samara supports both **YAML** and **JSON** (including JSONC with comments) configuration formats. Both formats are functionally equivalent—choose the one that best fits your team preferences.

**YAML example:**
```yaml
workflow:
  id: my-pipeline
  enabled: true
```

**JSON example:**
```json
{
  "workflow": {
    "id": "my-pipeline",
    "enabled": true
  }
}
```

All examples in this documentation use JSON/JSONC, but you can convert any configuration to YAML format. The framework automatically detects the format based on the file extension (`.yaml`/`.yml` or `.json`/`.jsonc`).

## Creating Your Own Pipeline
A pipeline configuration requires the following structure:

```jsonc
{
    "workflow": {
        "id": "unique-pipeline-id", // Unique identifier for the pipeline
        "description": "Pipeline description", // Brief description of what the pipeline does
        "enabled": true, // Whether the pipeline is active
        "jobs": [
            {
                "id": "bronze", // Identifier for this job
                "description": "", // Description of the job's purpose
                "enabled": true, // Whether this job is active
                "engine_type": "spark", // Processing engine to use
                "extracts": [ /* Data sources */ ],
                "transforms": [ /* Data transformations */ ],
                "loads": [ /* Output destinations */ ]
            }
        ]
    }
}
```

### Extract Configuration
The extract section defines your data sources:

```jsonc
"extracts": [
    {
        "id": "extract-customers", // Unique identifier for this extract
        "extract_type": "file", // Source type: file
        "method": "batch", // Extraction method: batch or streaming
        "data_format": "csv", // Format: csv, json, parquet, etc.
        "location": "data/customers/", // Source path
        "options": {
            "delimiter": ",", // Format-specific options
            "header": true // First row contains column names
        },
        "schema": "schemas/customers_schema.json" // Optional schema definition
    }
]
```

### Transform Configuration
Transformations define how your data is processed:

```jsonc
"transforms": [
    {
        "id": "transform-join-orders", // Unique identifier for this transform
        "upstream_id": "extract-customers", // Input dataset
        "functions": [
            {
                "function_type": "join", // Transformation type
                "arguments": { 
                    "other_upstream_id": "extract-orders", // Second dataset for joining
                    "on": ["customer_id"], // Join key(s)
                    "how": "inner" // Join type: inner, left, right, full
                }
            }
        ]
    }
]
```

### Load Configuration
The load section specifies where results are written:

```jsonc
"loads": [
    {
        "id": "load-customer-orders", // Unique identifier for this load
        "upstream_id": "transform-join-orders", // Input dataset
        "load_type": "file", // Destination type
        "method": "batch", // Load method: batch or streaming
        "data_format": "csv", // Output format
        "location": "output/", // Destination path
        "mode": "overwrite", // Write mode: overwrite, append, ignore, error
        "options": {
            "header": true // Format-specific options
        },
        "schema_export": "" // Path to export schema, or empty to skip
    }
]
```

For more information on configuration options, see the [Spark configuration reference](../docs/workflow/spark.md).

## IDE Support with JSON Schema

Samara can export a JSON schema that enables autocompletion, validation, and inline documentation in your IDE when editing configuration files.

**Export the schema:**

```bash
python -m samara export-schema --output-filepath="./workflow_schema.json"
```

**Reference it in your configuration:**

```jsonc
{
    "$schema": "./workflow_schema.json",
    "workflow": {
        "id": "my-pipeline",
        // Your IDE now provides:
        // - Autocompletion of field names
        // - Validation of required fields
        // - Documentation tooltips
        // - Type checking
    }
}
```

This dramatically improves the configuration authoring experience, catching errors before execution and providing guidance through inline documentation.

## Example: Customer Order Pipeline
The example below executes a complete pipeline that showcases Samara's key capabilities:

- **Multi-format extraction**: Reads from both CSV and JSON sources
  - Source options like delimiters and headers are configurable
  - Schema validation ensures data type safety and consistency

- **Transformation chain**: Functions are applied in order
  - A `join` combines both datasets on `customer_id`
  - A `select` projects only the needed columns

- **Configurable loading**: Writes results as CSV with customizable settings
  - Change to Parquet, Delta, or other formats by modifying `data_format`
  - Output mode (overwrite/append) controlled by the `mode` parameter

#### Configuration: examples/json_join_select/job.jsonc
```jsonc
{
    "workflow": {
        "id": "customer-orders-pipeline",
        "description": "ETL pipeline for processing customer orders data",
        "enabled": true,
        "jobs": [
            {
                "id": "bronze",
                "description": "",
                "enabled": true,
                "engine_type": "spark", // Specifies the processing engine to use
                "extracts": [
                    {
                        "id": "extract-customers",
                        "extract_type": "file", // Read from file system
                        "data_format": "csv", // CSV input format
                        "location": "examples/json_join_select/customers/", // Source directory
                        "method": "batch", // Process all files at once
                        "options": {
                            "delimiter": ",", // CSV delimiter character
                            "header": true, // First row contains column names
                            "inferSchema": false // Use provided schema instead of inferring
                        },
                        "schema": "examples/json_join_select/customers_schema.json" // Path to schema definition
                    },
                    {
                        "id": "extract-orders",
                        "extract_type": "file",
                        "data_format": "json", // JSON input format
                        "location": "examples/json_join_select/orders/",
                        "method": "batch",
                        "options": {
                            "multiLine": true, // Each JSON object may span multiple lines
                            "inferSchema": false // Use provided schema instead of inferring
                        },
                        "schema": "examples/json_join_select/orders_schema.json"
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
                        "location": "examples/json_join_select/output", // Output directory
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
