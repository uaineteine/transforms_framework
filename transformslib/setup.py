import os

JVENV = "JAVA_HOME"

def detect_if_java_home_set() -> bool:
    """Detects if the JAVA_HOME environment variable is set and points to a valid directory."""
    java_home = os.environ.get(JVENV)
    if java_home and os.path.exists(java_home):
        return True
    return False

def setup_java_home(java_home_path: str):
    """Sets the JAVA_HOME environment variable to the specified path."""
    if not os.path.exists(java_home_path):
        raise ValueError(f"Provided JAVA_HOME path does not exist: {java_home_path}")
    os.environ[JVENV] = java_home_path
