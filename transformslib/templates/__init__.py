"""
Templates module for transformslib.

This module provides utilities for accessing bundled template files
that are included in the package distribution.
"""

import os
import pkgutil
from typing import Optional


def get_template_path(template_name: str) -> str:
    """
    Get the full path to a bundled template file.
    
    Args:
        template_name: Name of the template file (e.g., 'template.html')
        
    Returns:
        Full path to the template file
        
    Raises:
        FileNotFoundError: If the template file doesn't exist
    """
    # Get the directory where this module is located
    templates_dir = os.path.dirname(os.path.abspath(__file__))
    template_path = os.path.join(templates_dir, template_name)
    
    if not os.path.exists(template_path):
        raise FileNotFoundError(f"Template file '{template_name}' not found in {templates_dir}")
    
    return template_path


def read_template(template_name: str, encoding: str = 'utf-8') -> str:
    """
    Read the contents of a bundled template file.
    
    Args:
        template_name: Name of the template file (e.g., 'template.html')
        encoding: Text encoding to use when reading the file
        
    Returns:
        Content of the template file as a string
        
    Raises:
        FileNotFoundError: If the template file doesn't exist
    """
    template_path = get_template_path(template_name)
    with open(template_path, 'r', encoding=encoding) as f:
        return f.read()


def get_template_data(template_name: str) -> bytes:
    """
    Get the raw bytes content of a bundled template file using pkgutil.
    This method works even when the package is bundled in a ZIP file.
    
    Args:
        template_name: Name of the template file (e.g., 'template.html')
        
    Returns:
        Raw bytes content of the template file
        
    Raises:
        FileNotFoundError: If the template file doesn't exist
    """
    try:
        data = pkgutil.get_data('transformslib.templates', template_name)
        if data is None:
            raise FileNotFoundError(f"Template file '{template_name}' not found in package")
        return data
    except Exception as e:
        raise FileNotFoundError(f"Template file '{template_name}' not found: {e}")


def read_template_safe(template_name: str, encoding: str = 'utf-8') -> str:
    """
    Safely read the contents of a bundled template file using pkgutil.
    This method works even when the package is bundled in a ZIP file.
    
    Args:
        template_name: Name of the template file (e.g., 'template.html')
        encoding: Text encoding to use when decoding the file
        
    Returns:
        Content of the template file as a string
        
    Raises:
        FileNotFoundError: If the template file doesn't exist
    """
    data = get_template_data(template_name)
    return data.decode(encoding)


def list_templates() -> list:
    """
    List all available template files in the templates directory.
    
    Returns:
        List of template file names
    """
    templates_dir = os.path.dirname(os.path.abspath(__file__))
    template_files = []
    
    for item in os.listdir(templates_dir):
        item_path = os.path.join(templates_dir, item)
        if os.path.isfile(item_path) and not item.startswith('__') and not item.endswith('.pyc'):
            template_files.append(item)
    
    return sorted(template_files)