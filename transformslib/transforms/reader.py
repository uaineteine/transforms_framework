import os
import json

main_dir = "events_log"

def load_transform_log(job_id:int, run_id:int, debug:bool=False) -> list:
    """Load the transform log for a specific job and run ID."""

    log_file = os.path.join(main_dir, f"job_{job_id}", "transforms.json")
    
    events = []
    if not os.path.exists(log_file):
        raise FileNotFoundError(f"Log file not found: {log_file}")
    else:
        with open(log_file, "r", encoding="utf-8") as f:
            content = f.read().strip()
            for obj_str in content.split("\n{"):
                obj_str = obj_str if obj_str.startswith("{") else "{" + obj_str
                events.append(json.loads(obj_str))
    return events



