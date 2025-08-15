import uuid
from datetime import datetime, timezone
import json
import os

class Event:
    """
    Base class for logging events with unique identifiers and timestamps.
    
    This class provides the foundation for event logging functionality, including
    automatic generation of unique identifiers, timestamps, and JSON serialization
    capabilities. It serves as the parent class for more specialised event types.
    
    Attributes:
        event_type (str): The type/category of the event.
        uuid (str): A unique identifier for the event.
        timestamp (str): ISO format timestamp when the event was created.
        log_location (str): The file path where the event should be logged.
        
    Example:
        >>> event = Event("INFO", "events_log/job_1/events.json")
        >>> event.log()  # Writes event to the specified log file
    """

    def __init__(self, event_type: str, log_location: str = ""):
        """
        Initialize an Event with type and optional log location.

        Args:
            event_type (str): The type or category of the event (e.g., "INFO", "ERROR", "WARNING").
            log_location (str, optional): The file path where the event should be logged.
                                        Defaults to "".

        Example:
            >>> event = Event("INFO", "logs/application.log")
            >>> print(event.event_type)  # "INFO"
            >>> print(event.uuid)  # "550e8400-e29b-41d4-a716-446655440000"
        """
        self.event_type = event_type
        self.uuid = str(uuid.uuid4())
        self.timestamp = datetime.now(timezone.utc).isoformat()
        self.log_location = log_location

    def __repr__(self):
        """
        Return a JSON representation of the event, excluding log_location.
        
        This method creates a JSON string representation of the event's attributes,
        excluding the log_location field to avoid exposing file paths in logs.

        Returns:
            str: JSON string representation of the event.

        Example:
            >>> event = Event("INFO", "logs/app.log")
            >>> print(event.__repr__())
            # {"event_type": "INFO", "uuid": "...", "timestamp": "..."}
        """
        # Exclude 'df' from the dictionary representation
        dict_repr = {k: v for k, v in self.__dict__.items() if k != "log_location"}
        return json.dumps(dict_repr, indent=2, ensure_ascii=True)

    def log(self):
        """
        Write the event to the specified log file.
        
        This method serializes the event to JSON and appends it to the log file.
        If the log directory doesn't exist, it will be created automatically.

        Raises:
            ValueError: If no log location is specified.
            OSError: If there are issues creating directories or writing to the file.
            Exception: If there are issues serializing the event to JSON.

        Example:
            >>> event = Event("INFO", "logs/application.log")
            >>> event.log()  # Writes event to logs/application.log
        """
        if self.log_location == "":
            raise ValueError("No log location specified for the event.")
        # Ensure the directory exists
        os.makedirs(os.path.dirname(self.log_location), exist_ok=True)
        # Append the event to the log file
        with open(self.log_location, "a", encoding="utf-8") as f:
            f.write(self.__repr__() + "\n")

class PipelineEvent(Event):
    """
    Specialised event class for pipeline operations with additional metadata.
    
    This class extends the base Event class to include pipeline-specific information
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
