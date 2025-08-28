import uuid
from datetime import datetime, timezone
import json
import os

class EventLog:
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
        Initialise an Event with type and optional log location.

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
        self.excuted_user = os.getenv("USER") or os.getenv("USERNAME") or "unknown"
        self.log_exclusions = ["log_location", "condition_map", "df", "spark", "log_exclusions"]

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
        dict_repr = {k: v for k, v in self.__dict__.items() if k not in self.log_exclusions}
        return json.dumps(dict_repr, indent=2, ensure_ascii=True)

    def log(self):
        """
        Write the event to the specified log file.
        
        This method serialises the event to JSON and appends it to the log file.
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
