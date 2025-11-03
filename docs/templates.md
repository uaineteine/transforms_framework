# Bundled Templates in TransformsLib

This document explains how to use the bundled template files feature in the transformslib package.

## Overview

The transformslib package now includes a templates system that allows you to bundle static files (like HTML templates, CSS, JSON configurations, etc.) directly into the package distribution. This means when you deploy the package, these template files are available without needing separate file management.

## Features

- Templates are bundled into the package during build
- Accessible via simple Python API
- Works with both development and deployed environments
- Supports various file types (HTML, CSS, JS, JSON, TXT)
- Safe reading methods that work even when package is bundled in ZIP files

## Usage

### Basic Usage

```python
from transformslib.templates import read_template_safe, list_templates

# List all available templates
templates = list_templates()
print("Available templates:", templates)

# Read a template file
template_content = read_template_safe('template.html')
```

### Advanced Usage

```python
from transformslib.templates import (
    read_template_safe, 
    get_template_path, 
    get_template_data
)

# Get the full path to a template (for development)
template_path = get_template_path('template.html')

# Read raw bytes (useful for binary files)
template_bytes = get_template_data('template.html')

# Process template with variable substitution
template_content = read_template_safe('template.html')
processed = template_content.replace('{{ title }}', 'My Report')
```

## Available Templates

The package includes these built-in templates:

### template.html
A general-purpose HTML template with:
- Clean, responsive design
- Template variable placeholders
- Professional styling
- Ready for customization

### report_template.html
A data reporting template with:
- Dashboard-style layout
- Metrics display sections
- Tables for data presentation
- Event log area
- Transform operation tracking

## Template Variable System

Templates use a simple variable substitution system with the format `{{ variable_name | default: "default_value" }}`.

Example:
```html
<title>{{ title | default: "Default Title" }}</title>
<h1>Job ID: {{ job_id | default: "N/A" }}</h1>
```

You can process these with any templating engine:

### With string.Template (built-in)
```python
from string import Template
from transformslib.templates import read_template_safe

# Read template and create Template object
template_content = read_template_safe('template.html')
# Convert to string.Template format first
template_content = template_content.replace('{{ ', '$').replace(' }}', '')
template = Template(template_content)

# Substitute variables
result = template.safe_substitute(
    title="My Custom Report",
    job_id="12345"
)
```

### With Jinja2
```python
from jinja2 import Template
from transformslib.templates import read_template_safe

template_content = read_template_safe('template.html')
template = Template(template_content)

result = template.render(
    title="My Custom Report",
    job_id="12345",
    run_id="67890"
)
```

## Adding Your Own Templates

1. Place template files in `transformslib/templates/`
2. The build system automatically includes files matching these patterns:
   - `*.html`
   - `*.css`
   - `*.js`
   - `*.json`
   - `*.txt`

3. Rebuild the package: `python build.py`

## Integration with Framework

Templates can be integrated with the transforms framework for generating reports:

```python
from transformslib.templates import read_template_safe
from transformslib.tables.collections.supply_load import SupplyLoad

# Load your data pipeline
supply_frames = SupplyLoad("payload.json")

# Apply transforms
# ... your transform operations ...

# Generate report
template = read_template_safe('report_template.html')
report = template.replace('{{ total_tables }}', str(len(supply_frames.tables)))
report = report.replace('{{ status }}', 'Success')

# Save report
with open('pipeline_report.html', 'w') as f:
    f.write(report)
```

## API Reference

### `list_templates() -> List[str]`
Returns a list of all available template files.

### `read_template_safe(template_name: str, encoding: str = 'utf-8') -> str`
Safely reads a template file and returns its content as a string. Works even when the package is bundled.

### `get_template_path(template_name: str) -> str`
Returns the full path to a template file. Useful during development.

### `get_template_data(template_name: str) -> bytes`
Returns the raw bytes of a template file. Useful for binary files.

## Examples

See `examples/template_usage_example.py` for a complete working example demonstrating all the template features.