# Meta Transforms Framework

This program provides a data transformation framework for working with tables (DataFrames) in PySpark, Pandas, or Polars. It tracks all transformations and important events in a structured, auditable way using JSON logs.

## Main Components

![UML diagram](diagrams/plantuml.png)

### 1. MetaFrame
**Purpose:** Wraps a DataFrame and tracks its metadata.

**Features:**
- Supports loading tables from CSV, Parquet, or SAS files into Spark, Pandas, or Polars DataFrames.
- Can convert between DataFrame types (Pandas, Polars, PySpark).
- Stores table metadata including source path, table name, and frame type.
- Provides utility methods for DataFrame type conversion.

**Example:**
```python
from pyspark.sql import SparkSession
from metaframe import MetaFrame

spark = SparkSession.builder.getOrCreate()
tbl = MetaFrame.load(spark=spark, path="test.csv", format="csv", table_name="test_table", frame_type="pyspark")
```

### 2. PipelineEvent
**Purpose:** Represents an event (e.g., loading a table, applying a transform).

**Features:**
- Stores event type, message, description, timestamp, and a unique UUID.
- Can log itself as a JSON object to a file.
- Base class for all pipeline events.

**Example JSON output for a load event:**
```json
{
  "event_type": "load",
  "message": "Loaded table from test.csv as csv (pyspark)",
  "description": "Loaded test_table from test.csv",
  "uuid": "b2e7c8e2-7d4e-4c7e-8b8e-2f6e7c8e2d4e",
  "timestamp": "2025-08-10T12:34:56.789012",
  "log_location": "events_log/job_1/test_table_events.json"
}
```

### 3. PipelineTable
**Purpose:** Extends MetaFrame to include event tracking and logging capabilities.

**Features:**
- Inherits from MetaFrame and adds event tracking functionality.
- Stores a list of `PipelineEvent` objects describing actions performed on the table.
- Can save all events to a JSON log file.
- Automatically logs load events when tables are loaded.

**Example:**
```python
from pipeline_table import PipelineTable

tbl = PipelineTable.load(spark=spark, path="test.csv", format="csv", table_name="test_table", frame_type="pyspark")
```

### 4. PipelineTables
**Purpose:** A collection manager for multiple PipelineTable objects with dictionary-like access.

**Features:**
- Manages multiple PipelineTable instances in a single collection.
- Provides dictionary-style access to tables by name.
- Supports adding, removing, and checking for tables.
- Can save events for all tables in the collection at once.
- Maintains both a list of tables and a dictionary for named access.

**Example:**
```python
from pipeline_table import PipelineTable, PipelineTables

# Load multiple tables
test_table = PipelineTable.load(spark=spark, path="test_tables/test.csv", format="csv", table_name="test_table", frame_type="pyspark")
test2_table = PipelineTable.load(spark=spark, path="test_tables/test2.csv", format="csv", table_name="test2_table", frame_type="pyspark")

# Create collection
tables_list = [test_table, test2_table]
pt_collection = PipelineTables(tables_list)

# Access tables by name
first_table = pt_collection["test_table"]
print(f"Collection has {pt_collection.ntables} tables")
print(f"Available tables: {list(pt_collection.named_tables.keys())}")

# Save events for all tables
pt_collection.save_events()
```

### 5. SupplyLoad
**Purpose:** A specialized collection manager for loading and managing supply data from JSON configuration files.

**Features:**
- Extends PipelineTables to provide automated loading of multiple data sources.
- Loads tables from a JSON configuration file.
- Designed for scenarios where you need to load multiple related datasets from a single configuration source.
- All tables are loaded as PySpark DataFrames by default.

**Example:**
```python
from supply_load import SupplyLoad

# Load multiple tables from JSON configuration
supply_frames = SupplyLoad("test_tables/payload.json", spark=spark)
print("Original columns:", supply_frames["test_table"].columns)

# Apply transformations
supply_frames = DropVariable("age")(supply_frames, df="test_table")

# Save events for all tables
supply_frames.save_events()
```

### 6. Transform and Subclasses
**Purpose:** Encapsulate transformations (e.g., dropping a column).

**Features:**
- Each transform is a subclass of `PipelineEvent` and logs itself when applied.
- `Transform` is the base class for all transformations.
- `TableTransform` handles transformations that act on specific variables.
- `SimpleTransform` handles transformations that act on a single variable.
- Example: `DropVariable` removes a column from the DataFrame and logs the action.

**Example:**
```python
from transforms import DropVariable

tbl = DropVariable("age")(tbl)
```

**Example JSON output for a transform event:**
```json
{
  "event_type": "transform",
  "message": "DropVariable",
  "description": "Removes this variable from a dataframe",
  "uuid": "c3f9e8d2-1a2b-4c3d-9e8d-2c1a2b4c3d9e",
  "timestamp": "2025-08-10T12:35:01.123456",
  "log_location": "events_log/job_1/test_table_events.json",
  "name": "DropVariable",
  "created_variables": null,
  "renamed_variables": null,
  "deleted_variables": ["age"],
  "target_table": "test_table",
  "target_variable": "age"
}
```

### 7. FrameTypeVerifier
**Purpose:** Ensures the DataFrame matches the expected type (PySpark, Pandas, Polars).

---

## Example Workflows

### Method 1: Direct Table Loading

1. **Load a table:**
    ```python
    from pyspark.sql import SparkSession
    from pipeline_table import PipelineTable
    
    spark = SparkSession.builder.master("local").appName("TransformTest").getOrCreate()
    tbl = PipelineTable.load(spark=spark, path="test.csv", format="csv", table_name="test_table", frame_type="pyspark")
    ```
    Automatically logs a "load" event.

2. **Apply a transformation:**
    ```python
    from transforms import DropVariable
    
    tbl = DropVariable("age")(tbl)
    ```
    Logs a "transform" event describing the column dropped.

3. **Save all events:**
    ```python
    tbl.save_events()
    ```
    Writes all events to a JSON file for auditing.

### Method 2: Payload-Based Loading (SupplyLoad)

For scenarios where you need to load multiple tables from a configuration file, you can use the `SupplyLoad` class with a JSON payload:

1. **Create a payload configuration file** (`test_tables/payload.json`):
    ```json
    {
      "job_id": 1,
      "run_id": 2,
      "supply": [
        {
          "name": "test_table",
          "format": "csv",
          "path": "test_tables/test.csv"
        },
        {
          "name": "test_table2",
          "format": "csv",
          "path": "test_tables/test2.csv"
        }
      ]
    }
    ```

2. **Load multiple tables using the payload:**
    ```python
    from pyspark.sql import SparkSession
    from transforms import DropVariable
    from supply_load import SupplyLoad

    # Create Spark session
    spark = SparkSession.builder.master("local").appName("TransformTest").getOrCreate()

    # Load pipeline tables from payload
    supply_frames = SupplyLoad("test_tables/payload.json", spark=spark)
    print("Original columns:", supply_frames["test_table"].columns)

    # Apply transformations to specific tables
    supply_frames = DropVariable("age")(supply_frames, df="test_table")

    # Show results
    print("Transformed columns:", supply_frames["test_table"].columns)
    supply_frames["test_table"].show()

    # Save events for all tables
    supply_frames.save_events()
    ```

This approach is particularly useful when:
- You need to load multiple related datasets
- You want to configure your data sources externally
- You need to manage complex data pipelines with many input sources
- You want to version control your data source configurations separately from your code

### Method 3: PipelineTables Collection

For managing multiple tables with more control and flexibility, use the `PipelineTables` collection:

1. **Load multiple tables individually:**
    ```python
    from pyspark.sql import SparkSession
    from transforms import DropVariable
    from pipeline_table import PipelineTable, PipelineTables

    # Create Spark session
    spark = SparkSession.builder.master("local").appName("TransformTest").getOrCreate()

    # Load multiple tables
    test_table = PipelineTable.load(spark=spark, path="test_tables/test.csv", format="csv", table_name="test_table", frame_type="pyspark")
    test2_table = PipelineTable.load(spark=spark, path="test_tables/test2.csv", format="csv", table_name="test2_table", frame_type="pyspark")
    
    # Create collection with initial tables
    tables_list = [test_table, test2_table]
    pt_collection = PipelineTables(tables_list)
    
    print(f"Collection created with {pt_collection.ntables} tables")
    print(f"Available tables: {list(pt_collection.named_tables.keys())}")
    ```

2. **Demonstrate collection operations:**
    ```python
    # Access tables by name
    first_table = pt_collection["test_table"]
    print(f"First table columns: {first_table.columns}")
    
    # Check if table exists
    if "test2_table" in pt_collection:
        print("test2_table exists in collection")
    
    # Apply transforms to specific tables
    pt_collection = DropVariable("age")(pt_collection["test_table"])
    
    print(f"Transformed table columns: {pt_collection['test_table'].columns}")
    pt_collection["test_table"].show()
    ```

3. **Show all tables and save events:**
    ```python
    # Show all tables in collection
    for i, table in enumerate(pt_collection.tables):
        print(f"Table {i+1}: {table.table_name} - Columns: {table.columns}")
        table.df.show()
    
    # Save events for all tables in the collection
    pt_collection.save_events()
    ```

This approach provides:
- Fine-grained control over table loading and management
- Dictionary-style access to tables by name
- Easy iteration over all tables in the collection
- Bulk event saving for all tables
- Flexibility to add/remove tables dynamically

---

## Event Logging

- Events are saved in `events_log/job_1/{table_name}_events.json`.
- Each event is a JSON object, one per line.
- This provides a complete audit trail of all actions performed on each table.

---

## File Structure

- [`metaframe.py`](metaframe.py): MetaFrame class for DataFrame wrapping and type conversion
- [`pipeline_table.py`](pipeline_table.py): PipelineTable and PipelineTables classes for event tracking and collection management
- [`pipeline_event.py`](pipeline_event.py): Event and PipelineEvent classes for logging
- [`transforms.py`](transforms.py): Transformation classes (Transform, TableTransform, SimpleTransform, DropVariable)
- [`supply_load.py`](supply_load.py): SupplyLoad class for loading multiple tables from JSON configuration
- [`template_load_mf.py`](template_load_mf.py): Example demonstrating PipelineTables collection usage
- [`template_load_pipe.py`](template_load_pipe.py): Example demonstrating SupplyLoad usage

---

## Notes

- All transformations should inherit from `Transform` and implement the `transforms` method.
- The framework is designed for transparency and auditability in data pipelines by logging every important action as a structured JSON event.
- The `PipelineTable` class is the main entry point for users who want event tracking functionality.
- `PipelineTables` provides collection management for multiple tables with dictionary-like access.
- `SupplyLoad` extends `PipelineTables` to provide automated loading from JSON configuration files.

---

## Summary

This framework makes your data pipeline transparent and auditable by logging every important action as a structured JSON event. You can trace exactly what happened to each table, when, and why. The framework provides three main approaches for managing tables:

1. **Direct table loading** with `PipelineTable` for single table operations
2. **Payload-based loading** with `SupplyLoad` for configuration-driven multi-table loading
3. **Collection management** with `PipelineTables` for flexible multi-table operations

Each approach provides the same event tracking and audit capabilities while offering different levels of control and automation for your specific use case.