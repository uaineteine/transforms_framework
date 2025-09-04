# Events

### 1. EventLog
**Purpose:** Handles event logging and saving of events during pipeline execution.

**Features:**
- Manages the logging of events generated during the data pipeline's execution.
- Provides methods to save events to a JSON file for auditing and tracking purposes.
- Supports different logging levels and destinations.

![UML diagram](../diagrams/events.png)

### 2. PipelineEvent
**Purpose:** Represents an event (e.g., loading a table, applying a transform).

**Features:**
- Stores detailed information about each event, including event type, message, description, timestamp, and a unique UUID.
- Provides methods to format the event information as a JSON object.
- Can be extended to represent specific types of events, such as load events or transform events.
- Includes a log_location attribute to track where the event is logged.
- Serves as a base class for all pipeline events, ensuring a consistent structure and interface.

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

