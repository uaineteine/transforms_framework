# Tables

![UML diagram](../diagrams/tables.png)

### 1. TableName
**Purpose:** Represents a standardised name for a table.

### 2. MultiTable
**Purpose:** Manages multiple dataframes and tables in complex data pipelines.

**Features:**
- Stores and manages different types of dataframe objects.
- Facilitates operations across multiple tables, such as applying the same transformation to all tables in the collection.
- Provides methods for accessing and manipulating tables within the collection.

### 3. Metaframe
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

### 4. TableCollection
**Purpose:** A collection manager for multiple PipelineTable objects with dictionary-like access.

**Features:**
- Manages multiple PipelineTable instances in a single collection.
- Provides dictionary-style access to tables by name.
- Supports adding, removing, and checking for tables.
- Can save events for all tables in the collection at once.
- Maintains both a list of tables and a dictionary for named access.
- **NEW:** Advanced table selection by patterns, prefixes, suffixes, and ranges.
- **NEW:** Returns references to the same table objects, so modifications are reflected across collections.

**Example:**
```python
from pipeline_table import PipelineTable, PipelineTables

# Load multiple tables
test_table = PipelineTable.load(spark=spark, path="test_tables/test.csv", format="csv", table_name="test_table", frame_type="pyspark")
test2_table = PipelineTable.load(spark=spark, path="test_tables/test2.csv", format="csv", table_name="test2_table", frame_type="pyspark")
clus_table = PipelineTable.load(spark=spark, path="test_tables/clus_data.csv", format="csv", table_name="clus_data", frame_type="pyspark")

# Create collection
tables_list = [test_table, test2_table, clus_table]
pt_collection = PipelineTables(tables_list)

# Access tables by name
first_table = pt_collection["test_table"]
print(f"Collection has {pt_collection.ntables} tables")
print(f"Available tables: {list(pt_collection.named_tables.keys())}")

# NEW: Select tables by patterns
clus_tables = pt_collection.select_by_names("clus_*")  # All tables starting with "clus_"
specific_tables = pt_collection.select_by_names("test_table", "test2_table")  # Specific tables
mixed_tables = pt_collection.select_by_names("clus_*", "test_table")  # Multiple patterns

# NEW: Convenience methods
prefix_tables = pt_collection.select_by_prefix("clus_")  # Same as "clus_*"
suffix_tables = pt_collection.select_by_suffix("_data")  # All tables ending with "_data"
range_tables = pt_collection.select_by_range("test_table", "test2_table")  # Tables in lexicographic range

# NEW: Custom filtering
large_tables = pt_collection.filter_tables(lambda t: len(t.df) > 1000)  # Tables with >1000 rows

# Save events for all tables
pt_collection.save_events()
```

### 5. SupplyLoad
**Purpose:** A specialised collection manager for loading and managing supply data from JSON configuration files.

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

