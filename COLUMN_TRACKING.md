# Column Tracking Enhancement

## Overview

The transforms framework now includes enhanced column tracking functionality that captures input and output column names for transforms that modify table schemas.

## New Features

### TransformEvent Enhancement

The `TransformEvent` class now includes two new optional parameters:

- `input_columns` (dict[str, list[str]]): Maps table names to their column lists before transformation
- `output_columns` (dict[str, list[str]]): Maps table names to their column lists after transformation

### Supported Transforms

The following transforms now automatically track column changes:

- **DropVariable**: Tracks columns before/after dropping specified variables
- **SubsetTable**: Tracks columns before/after subsetting to specified columns  
- **RenameTable**: Tracks columns before/after renaming specified columns
- **JoinTable**: Tracks columns from both input tables and resulting joined table
- **ConcatColumns**: Tracks columns before/after adding concatenated column
- **PartitionByValue**: Tracks input columns and output columns for all partitioned tables
- **UnionTables**: Tracks columns from both input tables and resulting union table

### Smart Column Tracking

- Column tracking is only enabled for transforms that actually modify table schemas
- Transforms that only modify data values (filters, sorting, etc.) do not populate column information to avoid overhead
- Maintains full backward compatibility - existing code continues to work unchanged

## Usage Example

```python
from transformslib.transforms.atomiclib import DropVariable

# Apply a transform
drop_transform = DropVariable("unwanted_column")
supply_frames = drop_transform.apply(supply_frames, df="my_table")

# Access column tracking information
log_info = drop_transform.log_info
print(f"Input columns: {log_info.input_columns}")
print(f"Output columns: {log_info.output_columns}")
```

## Benefits

- Complete visibility into schema changes across transformations
- Better debugging and auditing capabilities for data pipelines
- Support for multi-table operations with comprehensive tracking
- Foundation for automated data lineage and impact analysis