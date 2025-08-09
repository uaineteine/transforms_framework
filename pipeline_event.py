import uuid
from datetime import datetime
import json


class PipelineEvent:
    """
    Class to handle events related to a table.
    """

    def __init__(self, event_type: str, message: str, description: str = "", log_location: str = ""):
        self.event_type = event_type
        self.message = message
        self.description = description
        self.uuid = str(uuid.uuid4())
        self.timestamp = datetime.utcnow().isoformat()
        self.log_location = log_location

    def __repr__(self):
        return json.dumps(self.__dict__, indent=2, ensure_ascii=True)

    def log(self):
        if self.log_location == "":
            raise ValueError("No log location specified for the event.")
        with open(self.log_location, "a", encoding="utf-8") as f:
            f.write(self.__repr__() + "\n")


if __name__ == "__main__":
    log_path = "pipeline_event_test.json"

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
