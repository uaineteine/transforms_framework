import uuid
from datetime import datetime, timezone
import json
import os

class JSONLog:
    """
    Log events as JSON with a UUID, timestamp, and optional file output.
    """

    def __init__(self, log_payload, log_location: str = "", indent_depth: int = 2):
        """
        Create a JSONLog event.

        Args:
            log_payload (Any): Data to log.
            log_location (str, optional): File path for writing. Defaults to "".
            indent_depth (int, optional): JSON indentation. Defaults to 2.
        """
        #included in log
        self.log = log_payload
        self.uuid = str(uuid.uuid4())
        self.timestamp = datetime.now(timezone.utc).isoformat()
        self.executed_user = os.getenv("USER") or os.getenv("USERNAME") or "unknown"

        #excluded from log
        self.log_exclusions = ["log_location", "indent_depth"]
        self.log_location = log_location
        self.indent_depth = indent_depth

    def __repr__(self):
        """
        Return the event as a JSON string.
        """
        dict_repr = {k: v for k, v in self.__dict__.items() if k not in self.log_exclusions}
        return json.dumps(dict_repr, indent=self.indent_depth, ensure_ascii=True)

    def log(self):
        """
        Write the event to the specified log file.
        """
        if self.log_location == "":
            raise ValueError("No log location specified for the event.")
        os.makedirs(os.path.dirname(self.log_location), exist_ok=True)
        with open(self.log_location, "a", encoding="utf-8") as f:
            f.write(self.__repr__() + "\n")
