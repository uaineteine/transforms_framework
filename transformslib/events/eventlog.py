from .jsonlog import JSONLog

class Event(JSONLog):
    """
    Specialised JSONLog event with type and optional description.
    """

    def __init__(self, event_type: str, event_payload, event_description: str = "", log_location: str = "", macro_uuid: str = None):
        """
        Create an Event.

        Args:
            event_type (str): Category of the event (e.g., "INFO", "ERROR").
            event_payload (Any): Data or details for the event.
            event_description (str, optional): Free-text description. Defaults to "".
            log_location (str, optional): File path for writing. Defaults to "".
            macro_uuid (str, optional): UUID of the macro operation this event is part of. Defaults to None.

        Example:
            >>> event = Event("INFO", {"job": "sync"}, "Job started", "logs/app.json")
            >>> print(event.event_type)  # "INFO"
        """
        super().__init__(event_payload, log_location=log_location, macro_uuid=macro_uuid)

        # store additional values
        self.event_type = event_type
        self.event_description = event_description
