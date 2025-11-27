"""
Schema validation utilities for the new sampling system.

This module provides schema validation functionality for data loaded through 
the new sampling system (sampling_state.json format). It validates that loaded
DataFrames match the expected schema defined in the dtypes field.
"""

from typing import Dict

def get_schema_summary(expected_dtypes: Dict[str, Dict[str, str]]) -> str:
    """
    Get a human-readable summary of the expected schema.
    
    Args:
        expected_dtypes: Dictionary mapping column names to dtype information
        
    Returns:
        str: A formatted string describing the expected schema
    """
    summary_lines = ["Expected Schema:"]
    for col_name, dtype_info in expected_dtypes.items():
        dtype_source = dtype_info.get('dtype_source', 'Unknown')
        dtype_output = dtype_info.get('dtype_output', dtype_source)
        summary_lines.append(f"  {col_name}: {dtype_source} -> {dtype_output}")
    
    return "\n".join(summary_lines)