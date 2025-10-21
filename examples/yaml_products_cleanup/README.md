# Product Cleanup Pipeline Example

This example demonstrates a data cleaning pipeline that performs three key operations: removing duplicates, casting data types, and selecting specific columns.

## Overview

The pipeline processes a product catalog CSV file containing duplicate entries and improperly typed columns. It demonstrates:

1. **dropDuplicates**: Removes duplicate product entries from the dataset
2. **cast**: Converts string columns to appropriate data types (double, integer, boolean, date)
3. **select**: Selects only the required columns for the final output

## Data Flow

```
products.csv (with duplicates and string types)
    ↓
[Extract] Read CSV with schema
    ↓
[Transform: dropDuplicates] Remove duplicate rows
    ↓
[Transform: cast] Convert types (price→double, stock_quantity→integer, is_available→boolean, last_updated→date)
    ↓
[Transform: select] Select relevant columns (excluding last_updated)
    ↓
[Load] Write cleaned CSV to output/
```

## Input Data

**File**: `products/products.csv`

Contains product catalog data with:
- Duplicate entries (rows 1 & 4 are identical, rows 2 & 7 are identical)
- String-typed columns that should be numeric, boolean, or date types
- 12 rows total, 10 unique products

**Schema**: `products_schema.json`

All columns initially defined as strings to demonstrate the `cast` operation.

## Transformations

### 1. Drop Duplicates
```yaml
function_type: dropDuplicates
arguments:
  columns: []  # Empty array = check all columns
```

Removes exact duplicate rows, reducing 12 rows to 10 unique products.

### 2. Cast Types
```yaml
function_type: cast
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
```

Converts columns from strings to proper data types for better data quality and downstream processing.

### 3. Select Columns
```yaml
function_type: select
arguments:
  columns:
    - product_id
    - product_name
    - category
    - price
    - stock_quantity
    - is_available
```

Selects only the necessary columns, excluding `last_updated` from the final output.

## Output

**Location**: `output/` directory

A cleaned CSV file containing:
- 10 unique product records (duplicates removed)
- Properly typed columns
- Only selected columns

## Running the Pipeline

```bash
# Run the pipeline using the Samara CLI
samara job run --config examples/products_cleanup/job.yaml
```

## Configuration Format

This example uses **YAML** format instead of JSON/JSONC, demonstrating that Samara supports multiple configuration formats for flexibility and readability.

## Key Takeaways

- **Configuration-driven**: Complex data cleaning operations defined entirely in YAML
- **Composable transformations**: Multiple transform functions chained together
- **Type safety**: Explicit schema and casting ensure data quality
- **No code required**: All logic expressed through declarative configuration
