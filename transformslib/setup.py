"""
This module provides utilities for managing the JAVA_HOME environment variable.

It includes functions to detect whether JAVA_HOME is set and to configure it programmatically.
"""

import os

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
