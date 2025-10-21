# Getting Started
This document explains how to install the framework, run your first data pipeline using the provided examples, and create and configure your own custom pipelines using configuration files instead of writing code.

### Installation
```bash
# Clone the repository
git clone https://github.com/krijnvanderburg/config-driven-ETL-framework.git
cd config-driven-ETL-framework

# Install dependencies
poetry install
```

### Running the Example Pipeline
Start with the included example pipeline:

```bash
python -m samara run \
  --alert-filepath="examples/join_select/alert.jsonc" \
  --runtime-filepath="examples/join_select/job.jsonc"
```

## Configuration Formats

Samara supports both **YAML** and **JSON** (including JSONC with comments) configuration formats. Both formats are functionally equivalent—choose the one that best fits your team preferences.

**YAML example:**
```yaml
runtime:
  id: my-pipeline
  enabled: true
```

**JSON example:**
```json
{
  "runtime": {
    "id": "my-pipeline",
    "enabled": true
  }
}
```

All examples in this documentation use JSON/JSONC, but you can convert any configuration to YAML format. The framework automatically detects the format based on the file extension (`.yaml`/`.yml` or `.json`/`.jsonc`).

## Creating Your Own Pipeline
The structure is as follows, all of the following fields are required

```jsonc
{
    "runtime": {
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
        "extract_type": "file", // Source type: file, database, etc.
        "data_format": "csv", // Format: csv, json, parquet, etc.
        "location": "examples/join_select/customers/", // Source path
        "options": {
            "delimiter": ",", // Format-specific options
            "header": true // First row contains column names
        },
        "schema": "examples/join_select/customers_schema.json" // Optional schema definition
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
        "data_format": "csv", // Output format
        "location": "examples/join_select/output", // Destination path
        "mode": "overwrite" // Write mode: overwrite or append
    }
]
```

For more information on configuration options, see:
- Extract Options
- Transform Functions
- Load Options

## IDE Support with JSON Schema

Samara can export a JSON schema that enables autocompletion, validation, and inline documentation in your IDE when editing configuration files.

**Export the schema:**

```bash
python -m samara export-schema --output-filepath="./runtime_schema.json"
```

**Reference it in your configuration:**

```jsonc
{
    "$schema": "./runtime_schema.json",
    "runtime": {
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

## Running the Example Pipeline
The quickest way to start is by running the provided example:

```bash
python -m samara run \
  --alert-filepath="examples/join_select/alert.jsonc" \
  --runtime-filepath="examples/join_select/job.jsonc"
```

### 🔍 Example: Customer Order
Running this command executes a complete pipeline that showcases Samara's key capabilities:

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

#### Configuration: examples/join_select/job.jsonc
```jsonc
{
    "runtime": {
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
