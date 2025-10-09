# Databricks Compatibility

The transforms framework is fully compatible with Databricks environments and includes specific handling for common Databricks scenarios.

## Event Logging in Databricks

The framework automatically handles event logging in Databricks environments. Event logs are written using standard Python file I/O, which works reliably with both local file systems and DBFS (Databricks File System).

### Automatic Fallback

If the `adaptiveio` library encounters issues (such as attempting to use an unavailable Spark session), the framework automatically falls back to standard Python file I/O. This ensures that:

- Event logging works reliably in all Databricks environments
- No Spark session is required for event logging operations
- DBFS paths are handled correctly

### Environment Detection

The framework automatically detects when running in Databricks by checking the system user. When the user is `root` (typical in Databricks), the `executed_user` field in event logs is set to "databricks".

## Usage in Databricks

No special configuration is required to use the framework in Databricks. Simply use it as you would in a local environment:

```python
from pyspark.sql import SparkSession
from transformslib.transforms import DropVariable
from transformslib.tables.collections.supply_load import SupplyLoad

# Get or create Spark session (Databricks provides this by default)
spark = SparkSession.builder.getOrCreate()

# Load data using SupplyLoad
supply_frames = SupplyLoad(spark=spark)

# Apply transforms
supply_frames = DropVariable("unwanted_column").apply(supply_frames, df="my_table")

# Save results - events are logged automatically
supply_frames.save_all(spark=spark)
```

## File Paths

When working with Databricks, you can use either:
- **DBFS paths**: `dbfs:/path/to/data` or `/dbfs/path/to/data`
- **Local paths**: `/tmp/local/path`

The framework handles both transparently.

## Troubleshooting

### Event Logging Warnings

If you see warnings like:
```
Warning: adaptiveio failed (...), using standard file I/O
```

This is expected behavior and indicates the framework successfully fell back to standard file I/O. Your events are still being logged correctly.

### Spark Session Availability

While the framework doesn't require a Spark session for event logging, you still need to ensure the Spark session is available when:
- Reading/writing PySpark DataFrames
- Applying transforms to PySpark DataFrames
- Calling operations that trigger Spark actions (like `.count()`, `.show()`, etc.)

In Databricks notebooks, the Spark session (`spark`) is automatically available. In Databricks jobs, you can get it with:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
```

## Best Practices

1. **Keep Spark Session Active**: Don't explicitly stop the Spark session until all operations are complete
2. **Use DBFS for Persistence**: Store output data and event logs in DBFS paths for durability
3. **Set Environment Variables**: Configure `TNSFRMS_LOG_LOC` and `TNSFRMS_OUT_TABLE_LOC` to use DBFS paths

Example environment variables for Databricks:
```python
import os
os.environ["TNSFRMS_LOG_LOC"] = "dbfs:/mnt/data/logs/job_{job_id}"
os.environ["TNSFRMS_OUT_TABLE_LOC"] = "dbfs:/mnt/data/output/job_{job_id}"
os.environ["TNSFRMS_JOB_ID"] = "my_job_123"
```
