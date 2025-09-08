import os
import json
from datetime import datetime, timedelta

main_dir = "events_log"

def transform_log_loc(job_id: int, run_id: int, debug: bool = False) -> str:
    """
    Constructs the file path to the transformation log for a specific job.

    The path is built using the `main_dir` directory and includes the job ID.
    The file is assumed to be named `transforms.json` and located in a subdirectory
    named `job_<job_id>`.

    Parameters
    ----------
    job_id : int
        The identifier for the job.
    run_id : int
        The identifier for the run (currently unused in path construction).
    debug : bool, optional
        Flag to indicate debug mode (currently unused), by default False.

    Returns
    -------
    str
        The full path to the `transforms.json` file for the specified job.
    """
    log_file_path = os.path.join(main_dir, f"job_{job_id}", "transforms.json")
    return log_file_path

def does_transform_log_exist(job_id: int, run_id: int) -> bool:
    """
    Checks whether the transformation log file exists for a specific job.

    This function uses the `transform_log_loc` function to construct the expected
    file path and then verifies its existence using `os.path.exists`.

    Parameters
    ----------
    job_id : int
        The identifier for the job.
    run_id : int
        The identifier for the run.

    Returns
    -------
    bool
        True if the transformation log file exists, False otherwise.
    """
    return os.path.exists(transform_log_loc(job_id, run_id))

def load_transform_log(job_id:int, run_id:int, debug:bool=False) -> list:
    """Load the transform log for a specific job and run ID."""

    log_file = transform_log_loc(job_id, run_id, debug=debug)
    
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

# Sort by timestamp to build a consistent versioned lineage
def parse_ts(event: dict) -> datetime:
    """
    Extracts and parses the ISO 8601 timestamp from an event dictionary.

    This function retrieves the value associated with the "timestamp" key in the
    provided dictionary, normalizes it by replacing a trailing 'Z' with '+00:00'
    to indicate UTC, and attempts to convert it into a `datetime` object.

    If the timestamp is missing or cannot be parsed, it returns `datetime.min`.

    Parameters:
        event (dict): A dictionary expected to contain a "timestamp" key with
                      an ISO 8601 formatted string.

    Returns:
        datetime: A parsed `datetime` object if successful, otherwise `datetime.min`.
    """
    ts = event.get("timestamp", "") or ""
    if ts.endswith("Z"):
        ts = ts.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return datetime.min

def format_timedelta(td: timedelta) -> str:
    """
    Format a timedelta as Hh Mm Ss string.
    """
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if seconds > 0 or not parts:  # always show something
        parts.append(f"{seconds}s")

    return " ".join(parts)
