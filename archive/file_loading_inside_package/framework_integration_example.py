#!/usr/bin/env python3
"""
Integration example showing how to use bundled templates
with the transforms framework to generate reports.
"""

import json
from datetime import datetime
from transformslib.templates import read_template_safe


def generate_transform_report(tables_info=None, transforms_applied=None):
    """
    Example function that generates an HTML report using bundled templates.
    
    Args:
        tables_info: List of dictionaries with table information
        transforms_applied: List of transform operations performed
    
    Returns:
        HTML report as string
    """
    # Default data for demonstration
    if tables_info is None:
        tables_info = [
            {"name": "customer_data", "rows": 10000, "columns": 15, "status": "Success"},
            {"name": "transaction_data", "rows": 50000, "columns": 8, "status": "Success"},
            {"name": "product_data", "rows": 2500, "columns": 12, "status": "Success"},
        ]
    
    if transforms_applied is None:
        transforms_applied = [
            {"name": "DropVariable", "target": "customer_data", "description": "Removed PII columns"},
            {"name": "SimpleFilter", "target": "transaction_data", "description": "Filtered by date range"},
            {"name": "JoinTable", "target": "customer_data+transaction_data", "description": "Joined customer and transaction data"},
        ]
    
    # Read the report template
    template = read_template_safe('report_template.html')
    
    # Calculate summary metrics
    total_tables = len(tables_info)
    total_transforms = len(transforms_applied)
    total_rows = sum(table["rows"] for table in tables_info)
    
    # Generate timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Generate table rows HTML
    table_rows_html = ""
    for table in tables_info:
        status_class = "success" if table["status"] == "Success" else "error"
        table_rows_html += f"""
        <tr>
            <td>{table['name']}</td>
            <td>{table['rows']:,}</td>
            <td>{table['columns']}</td>
            <td class="status-{status_class.lower()}">{table['status']}</td>
            <td>{timestamp}</td>
        </tr>"""
    
    # Generate transform operations HTML
    transform_ops_html = ""
    for transform in transforms_applied:
        transform_ops_html += f"""
        <li>
            <strong>{transform['name']}</strong> on {transform['target']}<br>
            <span class="timestamp">{transform['description']}</span>
        </li>"""
    
    # Generate event log (simplified)
    event_log = f"""[{timestamp}] Pipeline started
[{timestamp}] Loaded {total_tables} tables ({total_rows:,} total rows)
[{timestamp}] Applied {total_transforms} transformations
[{timestamp}] Pipeline completed successfully"""
    
    # Replace template variables
    report = template.replace('{{ report_title | default: "Data Transform Pipeline Report" }}', 
                             'Transform Framework Pipeline Report')
    report = report.replace('{{ timestamp | default: "N/A" }}', timestamp)
    report = report.replace('{{ total_tables | default: "0" }}', str(total_tables))
    report = report.replace('{{ total_transforms | default: "0" }}', str(total_transforms))
    report = report.replace('{{ processing_time | default: "N/A" }}', '2.3s')
    report = report.replace('{{ status | default: "Success" }}', 'Success')
    report = report.replace('{{ status_class | default: "success" }}', 'success')
    report = report.replace('{{ transform_operations | default: "<li>No transform operations recorded</li>" }}', 
                           transform_ops_html)
    report = report.replace('{{ table_rows | default: "<tr><td colspan=\'5\'>No table data available</td></tr>" }}', 
                           table_rows_html)
    report = report.replace('{{ event_log | default: "No events logged" }}', 
                           event_log.replace('\n', '<br>\n'))
    
    return report


def main():
    """Demonstrate template-based report generation."""
    print("=== Transform Framework Report Generation Demo ===\n")
    
    # Generate a sample report
    report_html = generate_transform_report()
    
    # Save the report
    output_file = "/tmp/framework_integration_report.html"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report_html)
    
    print(f"✅ Generated transform framework report: {output_file}")
    print(f"📊 Report includes pipeline summary, table details, and event log")
    print(f"🌐 Open the file in a web browser to view the formatted report")
    print()
    
    # Show how this could integrate with actual framework code
    print("=== Integration with Framework Example ===")
    print("""
# Example integration with SupplyLoad:
from transformslib.tables.collections.supply_load import SupplyLoad
from transformslib.transforms.atomiclib import DropVariable
from transformslib.templates import read_template_safe

# Load your data
supply_frames = SupplyLoad("payload.json")

# Apply transforms
supply_frames = DropVariable("sensitive_column")(supply_frames, df="customer_data")

# Generate report data
tables_info = []
for table_name, table in supply_frames.named_tables.items():
    tables_info.append({
        "name": table_name,
        "rows": len(table.df),
        "columns": len(table.columns),
        "status": "Success"
    })

# Generate and save report
report = generate_transform_report(tables_info)
with open("pipeline_report.html", "w") as f:
    f.write(report)
""")


if __name__ == "__main__":
    main()