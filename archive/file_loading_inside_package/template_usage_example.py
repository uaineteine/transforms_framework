#!/usr/bin/env python3
"""
Example script demonstrating how to use bundled template files
from the transformslib package.
"""

from transformslib.templates import (
    read_template_safe, 
    list_templates, 
    get_template_path
)


def main():
    """Demonstrate template usage."""
    print("=== Transformslib Template Usage Demo ===\n")
    
    # List all available templates
    print("Available templates:")
    templates = list_templates()
    for template in templates:
        print(f"  - {template}")
    print()
    
    # Read a template file
    if "template.html" in templates:
        print("Reading template.html:")
        try:
            template_content = read_template_safe("template.html")
            print(f"  Template size: {len(template_content)} characters")
            print(f"  First 200 characters:")
            print(f"  {template_content[:200]}...")
            print()
            
            # Show template path
            template_path = get_template_path("template.html")
            print(f"  Template path: {template_path}")
            print()
            
        except Exception as e:
            print(f"  Error reading template: {e}")
    
    # Example of template processing (basic string replacement)
    if "template.html" in templates:
        print("Example template processing:")
        try:
            template_content = read_template_safe("template.html")
            
            # Simple template variable replacement
            processed = template_content.replace(
                "{{ title | default: \"Transforms Framework Template\" }}", 
                "My Custom Transform Report"
            )
            processed = processed.replace(
                "{{ version | default: \"0.12.0\" }}", 
                "1.0.0"
            )
            processed = processed.replace(
                "{{ job_id | default: \"N/A\" }}", 
                "12345"
            )
            processed = processed.replace(
                "{{ run_id | default: \"N/A\" }}", 
                "67890"
            )
            
            # Save processed template
            output_file = "/tmp/processed_template.html"
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(processed)
            
            print(f"  Processed template saved to: {output_file}")
            print(f"  You can open this file in a web browser to see the result.")
            print()
            
        except Exception as e:
            print(f"  Error processing template: {e}")
    
    # Example with report template
    if "report_template.html" in templates:
        print("Reading report_template.html:")
        try:
            report_content = read_template_safe("report_template.html")
            print(f"  Report template size: {len(report_content)} characters")
            
            # Simple processing for demo
            processed_report = report_content.replace(
                "{{ total_tables | default: \"0\" }}", 
                "5"
            )
            processed_report = processed_report.replace(
                "{{ total_transforms | default: \"0\" }}", 
                "12"
            )
            processed_report = processed_report.replace(
                "{{ processing_time | default: \"N/A\" }}", 
                "2.5s"
            )
            
            # Save processed report
            output_file = "/tmp/processed_report.html"
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(processed_report)
            
            print(f"  Processed report saved to: {output_file}")
            print()
            
        except Exception as e:
            print(f"  Error processing report template: {e}")


if __name__ == "__main__":
    main()