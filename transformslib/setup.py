"""
This module provides utilities for managing the JAVA_HOME environment variable.

It includes functions to detect whether JAVA_HOME is set and to configure it programmatically.
"""

from typing import Union
import os
from transformslib.templates import read_template_safe

JVENV = "JAVA_HOME"
"""str: The name of the environment variable used to store the Java installation path."""

REQ_VARIABLES = [JVENV, "HADOOP_HOME"]
"""list: A list of required environment variables for Java-related operations."""

def detect_if_java_home_set() -> bool:
    """
    Checks whether the JAVA_HOME environment variable is set and points to a valid directory.

    Returns:
        bool: True if JAVA_HOME is set and the path exists, False otherwise.
    """
    java_home = os.environ.get(JVENV)
    if java_home and os.path.exists(java_home):
        return True
    return False

def setup_java_home(java_home_path: str):
    """
    Sets the JAVA_HOME environment variable to the specified path.

    Args:
        java_home_path (str): The path to the Java installation directory.

    Raises:
        ValueError: If the provided path does not exist.
    """
    if not os.path.exists(java_home_path):
        raise ValueError(f"Provided JAVA_HOME path does not exist: {java_home_path}")
    os.environ[JVENV] = java_home_path

def detect_if_hadoop_home_set() -> bool:
    """
    Checks whether the HADOOP_HOME environment variable is set and points to a valid directory.

    Returns:
        bool: True if HADOOP_HOME is set and the path exists, False otherwise.
    """
    home = os.environ.get("HADOOP_HOME")
    if home and os.path.exists(home):
        return True
    return False

def setup_hadoop_home(hadoop_home_path: str):
    """
    Sets the HADOOP_HOME environment variable to the specified path.

    Args:
        hadoop_home_path (str): The path to the HADOOP installation directory.

    Raises:
        ValueError: If the provided path does not exist.
    """
    if not os.path.exists(hadoop_home_path):
        raise ValueError(f"Provided HADOOP_HOME path does not exist: {hadoop_home_path}")
    os.environ["HADOOP_HOME"] = hadoop_home_path

def list_all_environment_variables() -> dict:
    """
    Lists all environment variables currently set in the system.

    Returns:
        dict: A dictionary containing all environment variables as key-value pairs.
    """
    return dict(os.environ)

def print_all_environment_variables():
    """
    Prints all environment variables currently set in the system in a formatted way.
    """
    env_vars = os.environ
    print("Environment Variables:")
    print("-" * 120)
    for key, value in sorted(env_vars.items()):
        print(f"{key}: {value}")
    print("-" * 120)
    print(f"Total environment variables: {len(env_vars)}")

def check_required_variables() -> bool:
    """
    Checks whether all required environment variables are set and point to valid directories.

    Returns:
        bool: True if all required environment variables are set and their paths exist, False otherwise.
    """
    for var in REQ_VARIABLES:
        value = os.environ.get(var)
        if not value or not os.path.exists(value):
            return False
    return True

def check_variable_set(var_name: str) -> bool:
    """
    Checks whether a specific environment variable is set and points to a valid directory.

    Args:
        var_name (str): The name of the environment variable to check.
    Returns:
        bool: True if the environment variable is set and the path exists, False otherwise. 
    """
    value = os.environ.get(var_name)
    if value and os.path.exists(value):
        return True
    return False

def set_default_variables():
    """
    Sets default values found in defconfig.txt if they don't exist in the environment.
    """

    def_conf_filename = "defconfig.txt"

    def_config = ""
    try:
        def_config = read_template_safe(def_conf_filename)
        if def_config is None:
            raise FileNotFoundError(f"Config file '{def_conf_filename}' not found in package")
    except Exception as e:
        raise FileNotFoundError(f"Config file '{def_conf_filename}' not found: {e}")
    
    def_config = def_config.splitlines()
    count = set_variables_from_dicts(def_config)

    if count > 0:
        print("Default environment variables have been set where necessary.")

def set_variables_from_dicts(var_dicts: list[str]) -> int:
    """
    Sets environment variables from a list of dictionaries.

    Args:
        var_dicts (list): A list of dictionaries containing environment variable key-value

    Returns:
        int: The number of environment variables that were set.
    """
    count = 0
    for line in var_dicts:
        if line.strip() and not line.startswith("#"):
            key_value = line.split("=", 1)
            if len(key_value) == 2:
                key, value = key_value
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key not in os.environ:
                    print(f"Setting environment variable: {key}={value}")
                    os.environ[key] = value
                    count += 1
    
    return count

def set_job_id(new_job_id: Union[int, str]):
    """
    Sets the job ID in the environment variable TNSFRMS_JOB_ID.

    Args:
        new_job_id (Union[int, str]): The job ID to set.
    """
    if isinstance(new_job_id, int):
        if new_job_id < 0:
            raise ValueError("Job ID must be a non-negative integer or a string.")
        os.environ["TNSFRMS_JOB_ID"] = str(new_job_id)
    elif isinstance(new_job_id, str):
        if not new_job_id.isdigit() or int(new_job_id) < 0:
            raise ValueError("Job ID string must represent a non-negative integer.")
        os.environ["TNSFRMS_JOB_ID"] = new_job_id
    else:
        raise ValueError("Job ID must be an integer or string.")

def set_run_id(new_run_id: Union[int, str]):
    """
    Sets the run ID in the environment variable TNSFRMS_RUN_ID.

    Args:
        new_run_id (Union[int, str]): The run ID to set.
    """
    if isinstance(new_run_id, int):
        if new_run_id < 0:
            raise ValueError("Run ID must be a non-negative integer or a string.")
        os.environ["TNSFRMS_RUN_ID"] = str(new_run_id)
    elif isinstance(new_run_id, str):
        if not new_run_id.isdigit() or int(new_run_id) < 0:
            raise ValueError("Run ID string must represent a non-negative integer.")
        os.environ["TNSFRMS_RUN_ID"] = new_run_id
    else:
        raise ValueError("Run ID must be an integer or string.")
