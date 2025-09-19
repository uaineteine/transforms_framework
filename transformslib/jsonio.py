"""
jsonio.py

Utility functions for loading JSON files from local paths or abfss:// paths.
Uses textio.read_raw_text for backend-agnostic file reading, supporting both OS and Spark.
Provides functions for loading standard JSON files and newline-delimited JSON objects.

Functions:
    load_json(src_path: str, spark=None) -> dict
        Load a JSON file and return its contents as a dictionary.

    load_json_newline(src_path: str, spark=None) -> list
        Load a newline-delimited JSON file and return a list of JSON objects.

Author: Daniel
Created: September 2025
"""

import json 
from transformslib.textio import read_raw_text, save_raw_text

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

def append_json_newline(obj: dict, dst_path: str, spark=None):
    """
    Append a dict to a newline-delimited JSON file.
    For abfss://, reads the file, appends in memory, and overwrites.
    For local files, uses append mode.
    """
    if dst_path.startswith("abfss://"):
        # Read existing objects
        objs = load_json_newline(dst_path, spark=spark)
        objs.append(obj)
        # Overwrite file
        lines = [json.dumps(o) for o in objs]
        file_str = "\n".join(lines)
        save_raw_text(dst_path, file_str, spark=spark)
    else:
        # Local: append directly
        with open(dst_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj) + "\n")

def write_json(obj: dict, dst_path: str, spark=None):
    """Write a dict to a JSON file using an agonist backend between os and spark."""

    file_str = json.dumps(obj, indent=2)
    save_raw_text(dst_path, file_str, spark=spark)
