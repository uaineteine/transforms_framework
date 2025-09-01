import os
import json

main_dir = "events_log"

def load_transform_log(job_id:int, run_id:int, debug:bool=False):
    """Load the transform log for a specific job and run ID."""
    log_file = os.path.join(main_dir, f"job_{job_id}", "transforms.json")
    if not os.path.exists(log_file):
        raise FileNotFoundError(f"Log file not found: {log_file}")
    else:
        with open(log_file, "r", encoding="utf-8") as f:
            return json.load(f)
