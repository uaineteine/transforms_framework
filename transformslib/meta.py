"""
This module defines constant parameters used across the application.

Version:
    1.0
"""

# CONSTANT PARAMETERS FOR MODULE

module_version = "1.0"
"""str: The version number of this module.
Used to track compatibility and changes across releases.
"""

def expected_meta_version(this_version:str) -> bool:
    """Returns a bool if the input version matches the meta version of the library framework it was run on"""
    return module_version == this_version