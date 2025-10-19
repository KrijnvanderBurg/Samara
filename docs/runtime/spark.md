# Spark Runtime Implementation

Configuration reference for Spark engine.

## Extract

### File Extract

```jsonc
{
    "id": "extract-data",
    "extract_type": "file",
    "method": "batch",                      // batch | streaming
    "data_format": "csv",
    "location": "path/to/data.csv",
    "schema": "schemas/data.json",          // JSON file path, JSON string, or empty ""
    "options": {
        // CSV
        "header": true,
        "delimiter": ",",
        "inferSchema": false,
        "dateFormat": "yyyy-MM-dd",
        // JSON
        "multiLine": true,
        // Parquet
        "mergeSchema": false
    }
}
```

## Transform

### Select

```jsonc
{"function_type": "select", "arguments": {"columns": ["id", "name", "email"]}}
```

### Filter

```jsonc
{"function_type": "filter", "arguments": {"condition": "age >= 18 AND status = 'active'"}}
```

Condition uses Spark SQL syntax: `AND`, `OR`, `NOT`, `=`, `!=`, `>`, `<`, `>=`, `<=`, `IN`, `LIKE`, `IS NULL`

### Agg

```jsonc
{
    "function_type": "agg",
    "arguments": {
        "aggregations": [
            {"column_name": "sales", "agg_function": "sum", "alias": "total_sales"},
            {"column_name": "sales", "agg_function": "avg", "alias": "avg_sales"},
            {"column_name": "quantity", "agg_function": "max", "alias": "max_quantity"},
            {"column_name": "order_id", "agg_function": "count", "alias": "total_orders"}
        ]
    }
}
```

Applies aggregation functions across the entire DataFrame without grouping, returning a single row with aggregated results. Common aggregation functions: `sum`, `avg`, `count`, `min`, `max`, `first`, `last`, `collect_list`, `collect_set`, `countDistinct`, `stddev`, `variance`, `approx_count_distinct`, `kurtosis`, `skewness`. The `alias` field is optional; if not provided, defaults to `{column_name}_{agg_function}`.

### GroupBy

```jsonc
{
    "function_type": "groupby",
    "arguments": {
        "columns": ["category", "region"],
        "aggregations": [
            {"column_name": "sales", "agg_function": "sum", "alias": "total_sales"},
            {"column_name": "quantity", "agg_function": "avg", "alias": "avg_quantity"},
            {"column_name": "order_id", "agg_function": "count", "alias": "order_count"}
        ]
    }
}
```

Groups data by specified columns and applies aggregation functions. Common aggregation functions: `sum`, `avg`, `count`, `min`, `max`, `first`, `last`, `collect_list`, `collect_set`, `countDistinct`, `stddev`, `variance`. The `alias` field is optional; if not provided, defaults to `{column_name}_{agg_function}`.

### Cast

```jsonc
{
    "function_type": "cast",
    "arguments": {
        "columns": [
            {"column_name": "age", "cast_type": "IntegerType"},
            {"column_name": "signup_date", "cast_type": "DateType"}
        ]
    }
}
```

### Drop

```jsonc
{"function_type": "drop", "arguments": {"columns": ["temp_col", "internal_id"]}}
```

### Drop Duplicates

```jsonc
{"function_type": "dropduplicates", "arguments": {"columns": ["customer_id"]}}
```

Empty `columns: []` deduplicates on all columns.

### Join

```jsonc
{
    "function_type": "join", "arguments": { "other_upstream_id": "extract-orders", "on": ["customer_id"], "how": "inner"}
}
```

### OrderBy

```jsonc
{
    "function_type": "orderby",
    "arguments": {
        "columns": [
            {"column_name": "category", "order": "asc"},
            {"column_name": "price", "order": "desc"}
        ]
    }
}
```

Sorts the DataFrame by one or more columns. Each column can be sorted in ascending (`asc`) or descending (`desc`) order. The `order` field is optional and defaults to `asc` if not specified. Multiple columns are sorted in the order they appear in the configuration.

### With Column

```jsonc
{
    "function_type": "withcolumn",
    "arguments": {
        "columns": [
            {"column_name": "full_name", "expression": "concat(first_name, ' ', last_name)"},
            {"column_name": "age_group", "expression": "CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END"}
        ]
    }
}
```

Expression uses Spark SQL syntax with all built-in functions.

## Load

### File Load

```jsonc
{
    "id": "load-output",
    "upstream_id": "transform-data",
    "load_type": "file",
    "method": "batch",                      // batch | streaming
    "data_format": "parquet",
    "location": "output/processed/",
    "schema_export": "output/schema.json", // Empty "" to skip schema export
    "mode": "overwrite",                    // overwrite | append | ignore | error
    "options": {
        // CSV
        "header": true,
        "delimiter": ",",
        // Parquet
        "compression": "snappy",
        "partitionBy": ["year", "month"],
        // JSON
        "compression": "gzip",
        // Streaming only
        "checkpointLocation": "/tmp/checkpoint/",
        "trigger": "processingTime='10 seconds'"
    }
}
```

## Common Options by Format

**CSV**
```jsonc
"options": {
    "header": true,
    "delimiter": ",",
    "inferSchema": false,
    "dateFormat": "yyyy-MM-dd"
}
```

**JSON**
```jsonc
"options": {
    "multiLine": true,
    "dateFormat": "yyyy-MM-dd'T'HH:mm:ss"
}
```

**Parquet**
```jsonc
"options": {
    "mergeSchema": false,
    "pathGlobFilter": "*.parquet"
}
```

For complete options, see [Spark Data Sources documentation](https://spark.apache.org/docs/latest/sql-data-sources.html).


## Complete Example

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
                "engine_type": "spark",
                "extracts": [
                    {
                        "id": "extract-customers",
                        "extract_type": "file",
                        "data_format": "csv",
                        "location": "examples/customer_orders/customers.csv",
                        "method": "batch",
                        "options": {
                            "delimiter": ",",
                            "header": true,
                            "inferSchema": false
                        },
                        "schema": "examples/customer_orders/customers_schema.json"
                    }
                ],
                "transforms": [
                    {
                        "id": "transform-join-orders",
                        "upstream_id": "extract-customers",
                        "options": {},
                        "functions": [
                            { "function_type": "drop", "arguments": {"columns": ["temp_col"]} },
                            { "function_type": "select", "arguments": {"columns": ["name", "email", "signup_date", "order_id", "order_date", "amount"]} }
                        ]
                    }
                ],
                "loads": [
                    {
                        "id": "load-customer-orders",
                        "upstream_id": "transform-join-orders",
                        "load_type": "file",
                        "data_format": "csv",
                        "location": "examples/customer_orders/output",
                        "method": "batch",
                        "mode": "overwrite",
                        "options": {
                            "header": true
                        }
                    }
                ],
                "hooks": {
                    "onStart": [],
                    "onFailure": [],
                    "onSuccess": [],
                    "onFinally": []
                }
            }
        ]
    }
}

```

