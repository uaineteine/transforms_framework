import uuid
from datetime import datetime, timezone
import json
import os

def _create_uuid() -> str:
    return str(uuid.uuid4())

class JSONLog:
    """
    Log events as JSON with a UUID, timestamp, and optional file output.
    """

    def gen_new_uuid(self):
        self.uuid = _create_uuid()

    def __init__(self, log_payload, log_location: str = "", indent_depth: int = 2, persistent_uuid:bool = False, macro_uuid: str = None):
        """
        Create a JSONLog event.

        Args:
            log_payload (Any): Data to log.
            log_location (str, optional): File path for writing. Defaults to "".
            indent_depth (int, optional): JSON indentation. Defaults to 2.
            persistent_uuid (bool, optional): Create new UUID on each log event or keep the same one. Default is false.
            macro_uuid (str, optional): UUID of the macro operation this event is part of. Defaults to None.
        """
        #included in log
        self.log_info = log_payload
        self.persistent_uuid = persistent_uuid
        self.uuid = _create_uuid()
        self.timestamp = datetime.now(timezone.utc).isoformat()
        self.executed_user = os.getenv("USER") or os.getenv("USERNAME") or "unknown"
        self.macro_uuid = macro_uuid

        #excluded from log
        self.log_exclusions = ["log_location", "indent_depth", "log_exclusions" , "persistent_uuid"]
        self.log_location = log_location
        self.indent_depth = indent_depth

    def __repr__(self):
        """
        Return the event as a JSON string, recursively converting objects to dicts.
        """
        #update uuid
        if self.persistent_uuid == False:
            self.gen_new_uuid()

        def recursive_convert(obj):
            if isinstance(obj, dict):
                return {k: recursive_convert(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [recursive_convert(v) for v in obj]
            elif hasattr(obj, "__dict__"):
                # Convert custom objects to dict recursively
                return recursive_convert(obj.__dict__)
            else:
                return obj

        dict_repr = {k: recursive_convert(v) for k, v in self.__dict__.items() if k not in self.log_exclusions}

        # Add class_type explicitly
        dict_repr["class_type"] = self.class_type

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

    @property
    def class_type(self) -> str:
        """
        Returns the name of the class as a string.

        This property provides a dynamic way to identify the type of the current instance.
        It is especially useful for logging, debugging, or serialization tasks where knowing
        the class name is helpful. Subclasses automatically inherit this behavior without
        needing to override or redefine it.

        Returns:
            str: The name of the class (e.g., "PipelineEvent", "TransformEvent").
        """
        return self.__class__.__name__
