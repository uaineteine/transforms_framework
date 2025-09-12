# Schema Validation for New Sampling System

This document describes the new schema validation feature that has been added to the transforms framework for the new sampling system.

## Overview

Schema validation is now available for data loaded through the **new sampling system** (using `sampling_state.json` files). This feature validates that loaded DataFrames match the expected schema defined in the `dtypes` field of the sampling configuration.

**Important:** The legacy system (`payload.json`) remains completely unchanged and does not include schema validation capabilities.

## Features

### ✅ What Schema Validation Does

- **Column Validation**: Ensures all expected columns are present in loaded data
- **Type Awareness**: Reports expected vs actual data types for transparency
- **Framework Support**: Works with PySpark, Pandas, and Polars DataFrames
- **Clear Error Messages**: Provides detailed feedback when validation fails
- **Selective Application**: Only affects new sampling system, legacy system untouched
- **Configurable**: Can be easily enabled or disabled

### ✅ What's Validated

1. **Column Presence**: All columns defined in `dtypes` must exist in the loaded data
2. **Column Names**: Exact matching of column names (case-sensitive)
3. **Schema Reporting**: Displays expected schema vs actual loaded schema

## Usage

### Basic Usage (Schema Validation Enabled by Default)

```python
from pyspark.sql import SparkSession
from transformslib.tables.collections.supply_load import SupplyLoad

spark = SparkSession.builder.appName("MyApp").getOrCreate()

# New sampling system with schema validation (default)
supply_frames = SupplyLoad(job_id=1, spark=spark)
```

### Disable Schema Validation

```python
# New sampling system without schema validation
supply_frames = SupplyLoad(job_id=1, spark=spark, enable_schema_validation=False)
```

### Legacy System (Unchanged)

```python
# Legacy system - no schema validation available
supply_frames = SupplyLoad(job_id=1, run_id=2, spark=spark)
```

## Configuration

Schema validation uses the `dtypes` field in your `sampling_state.json` file:

```json
{
  "sample_files": [
    {
      "table_name": "my_table",
      "input_file_path": "path/to/data.csv",
      "file_format": "csv",
      "dtypes": {
        "COLUMN1": {
          "dtype_source": "String",
          "dtype_output": "String"
        },
        "COLUMN2": {
          "dtype_source": "Int64",
          "dtype_output": "Int64"
        }
      }
    }
  ]
}
```

## Example Output

### Successful Validation

```
Loading table 'positions' from ../test_tables/job_1/positions.csv (format: csv)
Validating schema for table 'positions'...
Expected Schema:
  AGE: Int64 -> Int64
  NAME: String -> String
  POSITION: String -> String
  SKILL: String -> String
Schema check - Column 'AGE': expected Int64, got StringType()
Schema check - Column 'NAME': expected String, got StringType()
Schema check - Column 'POSITION': expected String, got StringType()
Schema check - Column 'SKILL': expected String, got StringType()
✓ Schema validation passed for table 'positions'
```

### Failed Validation

```
✗ Schema validation failed for table 'my_table': Missing columns in table 'my_table': ['MISSING_COLUMN']. 
  Actual columns: ['AGE', 'NAME', 'POSITION', 'SKILL']
```

## Error Handling

When schema validation fails:

1. **Missing Columns**: Raises `SchemaValidationError` with details about missing columns
2. **Extra Columns**: Shows warnings but doesn't fail validation
3. **Type Mismatches**: Currently reports differences but doesn't fail validation (configurable)

## Supported Data Types

The schema validator supports these type mappings:

- **String**: Text data
- **Int64**: Integer data
- **Double**: Floating-point data  
- **Date**: Date data
- **Boolean**: Boolean data

Additional types can be easily added to the `SchemaValidator` class.

## Benefits

1. **Data Quality**: Catch schema mismatches early in the data pipeline
2. **Documentation**: Clear visibility into expected vs actual data structure
3. **Debugging**: Helpful error messages for troubleshooting data issues
4. **Flexibility**: Can be disabled when not needed
5. **Non-Breaking**: Legacy system continues to work exactly as before

## Implementation Details

- New module: `transformslib/tables/schema_validator.py`
- Updated class: `SupplyLoad` in `transformslib/tables/collections/supply_load.py`
- New parameter: `enable_schema_validation` (default: `True`)
- Exception type: `SchemaValidationError`

## Migration Guide

### For Existing Users

**No action required.** The feature is backwards compatible:

- New sampling system users get schema validation by default
- Legacy system users are completely unaffected
- Schema validation can be disabled if needed

### For New Users

Simply ensure your `sampling_state.json` includes accurate `dtypes` information for best results.

## Testing

A comprehensive test suite has been implemented to verify:

- ✅ Schema validation works for valid schemas
- ✅ Schema validation fails appropriately for invalid schemas  
- ✅ Schema validation can be disabled
- ✅ Legacy system remains unaffected
- ✅ All DataFrame types (PySpark, Pandas, Polars) are supported

## Troubleshooting

### Common Issues

1. **Case Sensitivity**: Column names must match exactly (including case)
2. **CSV Column Names**: Spark may convert CSV headers to uppercase
3. **Missing dtypes**: Tables without `dtypes` will show a warning but still load

### Debug Information

Enable debug output by examining the console logs during loading. The validator shows:
- Expected vs actual column names
- Expected vs actual data types
- Detailed error messages for mismatches