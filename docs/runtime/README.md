# Workflow

The workflow system orchestrates ETL pipelines through configuration files. Define your data sources, transformations, and destinations in YAML or JSON—no coding required.

## Overview

Samara's workflow executes data pipelines defined entirely through configuration. This declarative approach transforms data engineering from a programming task to a configuration exercise, making pipelines more maintainable and accessible.

## Configuration Formats

All workflow configurations can be written in either **YAML** or **JSON** format. Both formats are functionally equivalent:

- **YAML** (`.yaml`, `.yml`): More readable, supports comments natively
- **JSON/JSONC** (`.json`, `.jsonc`): Structured format, widely supported in tools

The framework automatically detects the format based on the file extension. All examples in this documentation use JSON/JSONC, but can be converted to YAML format.

## How It Works

1. **Extract** data from various sources (CSV, JSON, databases, etc.)
2. **Transform** data through function chains, referencing upstream components by ID
3. **Load** processed data to destinations in specified formats

## Running a Pipeline

Execute a pipeline using the Samara CLI:

```bash
python -m samara run \
    --workflow-filepath="examples/join_select/job.jsonc" \
    --alert-filepath="examples/join_select/alert.jsonc"
```

## Configuration Structure

A Samara pipeline is defined by a YAML or JSON configuration file with this structure:

**JSON format:**
```jsonc
{
    "workflow": {
        "id": "unique-pipeline-id",         // Unique identifier
        "description": "Pipeline description", 
        "enabled": true,                    // Enable/disable the entire workflow
        "jobs": [
            /* Job definitions */
        ]
    }
}
```

**YAML format:**
```yaml
workflow:
  id: unique-pipeline-id        # Unique identifier
  description: Pipeline description
  enabled: true                 # Enable/disable the entire workflow
  jobs:
    # Job definitions
```

## Jobs

Each job defines a complete ETL workflow with its own sources, transformations, and destinations. Jobs execute sequentially.

```jsonc
{
    "id": "job-id",                         // Unique job identifier
    "description": "Job description",
    "enabled": true,                        // Enable/disable this specific job
    "engine_type": "spark",                 // Processing engine to use
    "extracts": [/* data sources */],
    "transforms": [/* processing steps */],
    "loads": [/* destinations */],
    "hooks": {
        "onStart": [/* Actions on start */],
        "onFailure": [/* Actions on error */],
        "onSuccess": [/* Actions on success */],
        "onFinally": [/* Actions always */]
    }
}
```

## Components

A Samara pipeline consists of three core components:

```
Pipeline
├── Extracts - Read data from source systems 
├── Transforms - Apply business logic and data processing
└── Loads - Write results to destination systems
```

## Validation Rules

### ID Uniqueness

All component IDs within a job must be unique across extracts, transforms, and loads. Duplicate IDs will cause validation errors.

### Upstream ID References

Both transforms and loads use `upstream_id` to reference their data source. These references must follow strict validation rules:

1. **Must exist**: The `upstream_id` must match the ID of an existing component:
   - Transforms can reference: extracts or other transforms
   - Loads can reference: extracts or transforms

2. **Must appear before**: The referenced component must be defined earlier in the configuration. Forward references are not allowed.

3. **Cannot self-reference**: A transform cannot reference itself as the `upstream_id`.

4. **Order matters**: Components are processed sequentially. A transform can only reference:
   - Any extract (all extracts are available)
   - Any transform that appears before it in the transforms list

**Example of valid references:**
```jsonc
{
    "extracts": [
        {"id": "extract-a", ...},
        {"id": "extract-b", ...}
    ],
    "transforms": [
        {"id": "transform-1", "upstream_id": "extract-a", ...},      // ✓ Valid: references extract
        {"id": "transform-2", "upstream_id": "transform-1", ...},    // ✓ Valid: references previous transform
        {"id": "transform-3", "upstream_id": "extract-b", ...}       // ✓ Valid: references extract
    ],
    "loads": [
        {"id": "load-1", "upstream_id": "transform-2", ...},         // ✓ Valid: references any transform
        {"id": "load-2", "upstream_id": "extract-a", ...}            // ✓ Valid: can reference extract directly
    ]
}
```

**Example of invalid references:**
```jsonc
{
    "transforms": [
        {"id": "transform-1", "upstream_id": "transform-1", ...},    // ✗ Invalid: self-reference
        {"id": "transform-2", "upstream_id": "transform-3", ...},    // ✗ Invalid: forward reference
        {"id": "transform-3", "upstream_id": "nonexistent", ...}     // ✗ Invalid: doesn't exist
    ]
}
```

### Special Case: Join Transform

The `join` transform function has an additional `other_upstream_id` parameter in its arguments to specify the second dataframe for the join operation. This parameter follows the same validation rules as `upstream_id` - it must reference an existing extract or transform defined earlier in the configuration.

```jsonc
{
    "id": "join-customers-orders",
    "upstream_id": "extract-customers",           // First dataframe (left side of join)
    "functions": [
        {
            "function_type": "join",
            "arguments": {
                "other_upstream_id": "extract-orders",  // Second dataframe (right side of join)
                "on": ["customer_id"],
                "how": "inner"
            }
        }
    ]
}
```

### Extracts

Extracts read data into the registry, identified by their unique `id`.

```jsonc
{
    "id": "extract-customers",              // Unique identifier for this extract
    "extract_type": "file",                 // Source type: file, database, etc.
    "method": "batch",                      // batch | streaming
    "data_format": "csv",                   // Format: csv, json, parquet, etc.
    "location": "path/to/data.csv",         // Source path or connection string
    "schema": "path/to/schema.json",        // Optional: JSON schema file or string
    "options": {
        "header": true,                     // Format-specific reader options
        "delimiter": ","
    }
}
```

**Supported Formats:** CSV, JSON, Parquet, Avro, ORC, Text, JDBC, Delta (with appropriate dependencies)

### Transforms

Transforms apply functions to data from upstream components, creating a processing chain.

```jsonc
{
    "id": "transform-clean",                // Unique identifier
    "upstream_id": "extract-customers",     // Reference to input data source (see Validation Rules)
    "options": {},                          // Optional configuration
    "functions": [                          // List of transformation functions
        {
            "function_type": "filter",      // Function name
            "arguments": {                  // Function-specific parameters
                "condition": "age > 18"
            }
        },
        {
            "function_type": "select",
            "arguments": {
                "columns": ["name", "email"]
            }
        },
        {
            "function_type": "cast",
            "arguments": {
                "columns": [{"column_name": "age", "cast_type": "IntegerType"}]
            }
        }
    ]
}
```

**Function Application:** Transformations are applied in sequence, with each function's output feeding into the next.

### Loads

Loads write data from upstream components to destinations.

```jsonc
{
    "id": "load-output",                    // Unique identifier
    "upstream_id": "transform-clean",       // Reference to input data source (see Validation Rules)
    "load_type": "file",                    // Destination type
    "method": "batch",                      // batch | streaming
    "data_format": "parquet",               // Output format
    "location": "output/processed/",        // Destination path or connection
    "schema_export": "output/schema.json",  // Optional: export schema to this path
    "mode": "overwrite",                    // Write mode: overwrite, append, etc.
    "options": {
        "compression": "snappy"             // Format-specific writer options
    }
}
```

## Engine Support

Samara currently supports Apache Spark (`"engine_type": "spark"`) as its primary execution engine. The framework is designed for multi-engine support, with additional engines planned for future releases.

See Spark configuration for engine-specific options.

## Complete Example

Below is a complete example of a pipeline that:
- Extracts customer and order data from different sources and formats
- Cleans customer data by removing duplicates and filtering for valid emails
- Joins customer and order data
- Selects relevant columns for output
- Writes the result to Parquet files

```jsonc
{
    "workflow": {
        "id": "customer-analytics",
        "description": "Process customer orders with analytics",
        "enabled": true,
        "jobs": [
            {
                "id": "bronze-to-silver",
                "description": "Clean and join raw data",
                "enabled": true,
                "engine_type": "spark",
                "extracts": [
                    {
                        "id": "extract-customers",
                        "extract_type": "file",
                        "method": "batch",
                        "data_format": "csv",
                        "location": "data/customers.csv",
                        "schema": "schemas/customers.json",
                        "options": {
                            "header": true,                // First row contains column names
                            "inferSchema": false           // Use defined schema instead of inferring
                        }
                    },
                    {
                        "id": "extract-orders",
                        "extract_type": "file",
                        "method": "batch",
                        "data_format": "json",
                        "location": "data/orders/",
                        "schema": "schemas/orders.json",
                        "options": {}
                    }
                ],
                "transforms": [
                    {
                        "id": "clean-customers",
                        "upstream_id": "extract-customers",
                        "options": {},
                        "functions": [
                            {"function_type": "dropduplicates", "arguments": {"columns": ["customer_id"]}},
                            {"function_type": "filter", "arguments": {"condition": "email IS NOT NULL"}}
                        ]
                    },
                    {
                        "id": "join-orders",
                        "upstream_id": "clean-customers",
                        "options": {},
                        "functions": [
                            {
                                "function_type": "join",
                                "arguments": {"right_id": "extract-orders", "on": ["customer_id"], "how": "inner"}
                            },
                            {
                                "function_type": "select",
                                "arguments": {"columns": ["customer_id", "name", "email", "order_id", "amount"]}
                            }
                        ]
                    }
                ],
                "loads": [
                    {
                        "id": "load-parquet",
                        "upstream_id": "join-orders",
                        "load_type": "file",
                        "method": "batch",
                        "data_format": "parquet",
                        "location": "output/customer_orders/",
                        "schema_export": "output/schema.json",
                        "mode": "overwrite",
                        "options": {
                            "compression": "snappy"         // Use Snappy compression for Parquet files
                        }
                    }
                ],
                "hooks": {
                    "onStart": [],                         // Actions before pipeline starts
                    "onFailure": [],                       // Actions if pipeline fails
                    "onSuccess": [],                       // Actions if pipeline succeeds
                    "onFinally": []                        // Actions that always run
                }
            }
        ]
    }
}
```

Note: The event hooks system is still in development but the fields are required in the configuration structure. You can leave them as empty arrays for now.