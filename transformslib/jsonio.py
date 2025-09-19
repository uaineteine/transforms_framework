import json 
from textio import read_raw_text

def load_json(src_path: str, spark=None) -> dict:
    """Load a json file using an agonist backend between os and spark."""

    file_str = read_raw_text(src_path, spark=spark)
    return json.loads(file_str)

def load_json_newline(src_path: str, spark=None) -> list:
    """Load a json file using an agonist backend between os and spark."""

    file_str = read_raw_text(src_path, spark=spark)
    events = []
    for obj_str in file_str.split("\n"):
        if obj_str.strip():
            obj_str = obj_str if obj_str.startswith("{") else "{" + obj_str
            events.append(json.loads(obj_str))
    
    return events
