# New Macros Documentation

This document describes the two new macros added to the transforms framework.

## ConcatenateIDs Macro

**Purpose**: Concatenates two ID columns with an underscore separator.

**Parameters**:
- `input_tables`: TableCollection - A collection of input tables to be transformed
- `input_columns`: list[str] - List containing exactly two column names to concatenate
- `output_column`: str - Name of the new column to create with concatenated values

**Usage**:
```python
from transformslib.transforms.macrolib import ConcatenateIDs

# Create the macro
concat_macro = ConcatenateIDs(
    input_tables=supply_frames,
    input_columns=["column1", "column2"],
    output_column="combined_id"
)

# Apply to a table
supply_frames = concat_macro.apply(df="table_name", output_var="combined_id")
```

**Example**:
- Input columns: "age" (1, 2, 3) and "name" ("John", "Jane", "Bob")  
- Output column: "concatenated_id" ("1_John", "2_Jane", "3_Bob")

## DropMissingIDs Macro

**Purpose**: Drops missing IDs by removing rows with NA values in the 'synthetic' variable from a table.

**Parameters**:
- `input_tables`: TableCollection - A collection of input tables to be transformed

**Usage**:
```python
from transformslib.transforms.macrolib import DropMissingIDs

# Create the macro
drop_macro = DropMissingIDs(
    input_tables=supply_frames
)

# Apply to a table
supply_frames = drop_macro.apply(df="table_name")
```

**Note**: This macro specifically targets the 'synthetic' column and will remove rows where this column contains NA/null values.

## Implementation Details

Both macros:
- Follow the established framework patterns (similar to TopBottomCode)
- Use existing atomic transforms (ConcatColumns and DropVariable)
- Include proper error handling and validation
- Support the framework's logging and event tracking system
- Are automatically discovered and listed by `listmacro()`

## Example Template

See `templates/template_new_macros.py` for a complete working example of both macros in action.