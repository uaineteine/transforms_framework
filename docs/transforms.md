# Transforms

## Overview

The `Transform` framework encapsulates data transformations in a modular, auditable, and extensible way. Each transform is a subclass of `PipelineEvent` and logs itself when applied, ensuring full auditability of data changes.

---

## UML Diagram

![UML diagram](../diagrams/transforms.png)

---

## Transform Classes

### Purpose
Encapsulate and standardize data transformation logic.

### Key Features
- **Auditability:** Each transform logs itself when applied.
- **Extensibility:** Abstract base class `Transform` defines the interface for all transformations.

---

## Example Usage

```python
from transforms import DropVariable

tbl = DropVariable("age")(tbl)
```

---

## Transform Logging and File Locations

### transforms.json File

Each transform operation is logged to a `transforms.json` file that contains detailed information about the transformation, including input/output tables, variables, timestamps, and metadata.

#### File Location

The `transforms.json` file is stored in the following location structure:
```
events_log/job_{job_id}/transforms.json
```

For example, for job ID 1, the file would be located at:
```
events_log/job_1/transforms.json
```

#### Reading transforms.json

The framework provides several functions in `transformslib.transforms.reader` for working with transform logs:

##### Key Functions

**`transform_log_loc(job_id: int, run_id: int, debug: bool = False) -> str`**
- Constructs the file path to the transformation log for a specific job
- Returns the full path to the `transforms.json` file
- Note: `run_id` parameter is currently unused in path construction

```python
from transformslib.transforms.reader import transform_log_loc

# Get the path to transforms.json for job 1
log_path = transform_log_loc(job_id=1, run_id=1)
print(log_path)  # outputs: events_log/job_1/transforms.json
```

**`load_transform_log(job_id: int, run_id: int, debug: bool = False) -> list`**
- Loads and parses the transform log for a specific job and run ID
- Returns a list of event dictionaries parsed from the JSON file
- Raises `FileNotFoundError` if the log file doesn't exist

```python
from transformslib.transforms.reader import load_transform_log

# Load all transform events for job 1
events = load_transform_log(job_id=1, run_id=1)
for event in events:
    print(f"Transform: {event.get('name')}")
    print(f"Timestamp: {event.get('timestamp')}")
```

**`does_transform_log_exist(job_id: int, run_id: int) -> bool`**
- Checks whether the transformation log file exists for a specific job
- Returns `True` if the file exists, `False` otherwise

```python
from transformslib.transforms.reader import does_transform_log_exist

# Check if transform log exists before loading
if does_transform_log_exist(job_id=1, run_id=1):
    events = load_transform_log(job_id=1, run_id=1)
else:
    print("No transform log found for this job")
```

### Output Locations for DAG Reports

Transform visualizations and DAG reports are generated as HTML files and stored in a dedicated output directory.

#### DAG Report Output Location

**`output_loc(job_id: int, run_id: int) -> str`**
- Returns the output location for transform DAG visualization reports
- Files are saved to the `transform_dags/` directory
- Report filename format: `transform_dag_job{job_id}_run{run_id}.html`

```python
from transformslib.mapping.dag import output_loc

# Get output location for DAG report
report_path = output_loc(job_id=1, run_id=1)
print(report_path)  # outputs: transform_dags/transform_dag_job1_run1.html
```

#### Generating DAG Reports

```python
from transformslib.mapping.dag import render_dag

# Generate and save DAG visualization for job 1, run 1
html_report = render_dag(job_id=1, run_id=1, height=900)
# The report is automatically saved to the location returned by output_loc()
```

### transforms.json File Structure

The `transforms.json` file contains one JSON object per line, with each object representing a transform event. Key fields include:

- `event_type`: Type of event (typically "transform")
- `timestamp`: ISO 8601 timestamp of when the transform was applied
- `name`: Name of the transform (e.g., "DropVariable", "ConcatColumns")
- `log_info`: Contains details about input/output tables and variables
- `transform_type`: Category of transform (e.g., "TableTransform")
- `testable_transform`: Boolean indicating if the transform can be tested

Example structure:
```json
{
  "event_type": "transform",
  "timestamp": "2025-08-25T04:14:57.862503+00:00",
  "name": "DropVariable",
  "log_info": {
    "input_tables": ["positions"],
    "output_tables": ["positions"],
    "input_variables": [["age", "name"]],
    "output_variables": ["concatenated_id"]
  },
  "transform_type": "TableTransform",
  "testable_transform": true
}
```

---

## Example: Importing the Library

Below is a screenshot showing how to import and use the transforms library in your code:

![Example: Importing the library](https://raw.githubusercontent.com/uaineteine/transforms_framework/refs/heads/master/docs/screenshots/example_importing%20library.PNG)

