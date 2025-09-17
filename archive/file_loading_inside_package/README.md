# TransformsLib Examples

This directory contains example scripts demonstrating various features of the transformslib package.

## Template Usage Examples

### `template_usage_example.py`
Demonstrates how to use the bundled template files feature:
- List available templates
- Read template content
- Basic template variable processing
- Save processed templates

**Run with:**
```bash
PYTHONPATH=. python examples/template_usage_example.py
```

### `framework_integration_example.py`
Shows how to integrate bundled templates with the transforms framework:
- Generate pipeline reports using templates
- Integration with SupplyLoad and transforms
- Example data processing workflow
- HTML report generation

**Run with:**
```bash
PYTHONPATH=. python examples/framework_integration_example.py
```

## Generated Files

The examples create HTML files in `/tmp/` that you can open in a web browser:
- `processed_template.html` - Basic template with filled variables
- `processed_report.html` - Report template with sample data
- `framework_integration_report.html` - Full pipeline report example

## Template Files Used

These examples use the bundled template files from `transformslib.templates`:
- `template.html` - General-purpose HTML template
- `report_template.html` - Data reporting dashboard template

## Integration Notes

The templates are designed to work with any templating engine:
- Basic string replacement (as shown in examples)
- Jinja2 templates
- Django templates
- string.Template from Python standard library

For production use, consider using a proper templating engine for more sophisticated variable substitution and control structures.