import os
import json

main_dir = "events_log"

def load_transform_log(job_id, run_id):
    log_file = os.path.join(f"job_{job_id}", "transforms.json")
    if not os.path.exists(log_file):
        raise FileNotFoundError(f"Log file not found: {log_file}")
    else:
        with open(log_file, "r", encoding="utf-8") as f:
            return json.load(f)

