
from events.eventlog import EventLog

import os

class PipelineEvent(EventLog):
    """
    Specialised event class for pipeline operations with additional metadata.
    
    This class extends the base EventLog class to include pipeline-specific information
    such as descriptive messages and detailed descriptions. It's designed for logging
    data pipeline operations like data loading, transformations, and processing steps.
    
    Attributes:
        sub_type (str): The subtype identifier for pipeline events.
        message (str): A brief message describing the pipeline operation.
        description (str): A detailed description of the pipeline operation.
        
    Example:
        >>> event = PipelineEvent(
        ...     event_type="transform",
        ...     message="Data cleaning completed",
        ...     description="Removed 150 null values from column 'age'",
        ...     log_location="events_log/job_1/pipeline_events.json"
        ... )
        >>> event.log()
    """

    def __init__(self, event_type: str, message: str, description: str = "", log_location: str = ""):
        """
        Initialize a PipelineEvent with type, message, and optional description.

        Args:
            event_type (str): The type of pipeline event (e.g., "load", "transform", "save").
            message (str): A brief message describing the pipeline operation.
            description (str, optional): A detailed description of the operation. Defaults to "".
            log_location (str, optional): The file path where the event should be logged.
                                        Defaults to "".

        Example:
            >>> event = PipelineEvent(
            ...     "load",
            ...     "Data loaded successfully",
            ...     "Loaded 1000 rows from data.parquet",
            ...     "logs/pipeline.log"
            ... )
            >>> print(event.message)  # "Data loaded successfully"
            >>> print(event.description)  # "Loaded 1000 rows from data.parquet"
        """
        super().__init__(event_type, log_location)
        self.sub_type = "pipeline_event"
        self.message = message
        self.description = description

#modular test
if __name__ == "__main__":
    log_dir = "events_log"
    job_name = "job_1"
    log_path = os.path.join(log_dir, job_name, "pipeline_event_test.json")
    if not os.path.exists(os.path.dirname(log_path)):
        os.makedirs(os.path.dirname(log_path))

    event1 = PipelineEvent(
        event_type="INFO",
        message="First event logged.",
        description="This is the first test event.",
        log_location=log_path
    )
    event1.log()

    event2 = PipelineEvent(
        event_type="ERROR",
        message="Second event logged.",
        description="This is the second test event.",
        log_location=log_path
    )
    event2.log()

    print(f"Events logged to {log_path}")
