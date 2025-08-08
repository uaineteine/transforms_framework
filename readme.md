# Metaplus Transforms Framework

This framework provides a structure for managing Spark DataFrames and applying transformations in a pipeline, with event tracking and metadata management.

## Main Components

### 1. [`PipelineEvent`](metaplus_table.py)
Handles events related to table operations. Stores event type, message, and description for tracking actions performed on tables.

### 2. [`MetaplusTable`](metaplus_table.py)
Wraps a Spark DataFrame and provides:
- Metadata (table name, source path)
- Event logging
- Conversion to Pandas DataFrame
- Static method to load tables from files

**Example:**
```python
from pyspark.sql import SparkSession
from metaplus_table import MetaplusTable

spark = SparkSession.builder.getOrCreate()
tbl = MetaplusTable.load(spark, "path/to/data.parquet")
```

### 3. [`Transform`](transforms_class.py)
Base class for all transformations. Inherits from [`PipelineEvent`](metaplus_table.py) and provides:
- Metadata about the transformation
- Method to apply transformations to [`MetaplusTable`](metaplus_table.py)
- Automatic dumping of transformation metadata to JSON

**To implement a custom transform:**
```python
from transforms_class import Transform

class MyTransform(Transform):
    def transforms(self, tbl, tbl2=None):
        # Custom transformation logic
        return tbl.df
```

### EXAMPLE TRANSFORMS

* [`VariableTransform`](transforms_class.py)
Specializes [`Transform`](transforms_class.py) for operations on specific variables (columns).

* [`DropVariable`](transforms_class.py)
Drops a specified variable (column) from a [`MetaplusTable`](metaplus_table.py).

**Example:**
```python
from transforms_class import DropVariable

new_df = DropVariable("age")(tbl)
```

## Usage

1. Load your data into a [`MetaplusTable`](metaplus_table.py).
2. Create and apply transforms (e.g., [`DropVariable`](transforms_class.py)).
3. Each transform logs metadata and outputs a JSON file describing the transformation.

## File Structure

- [`metaplus_table.py`](metaplus_table.py): Table/event classes
- [`transforms_class.py`](transforms_class.py): Transformation classes

## Notes

- All transformations should inherit from [`Transform`](transforms_class.py) and implement the `transforms` method.
- The framework is designed for