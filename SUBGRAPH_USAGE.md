# Example: Using Subgraph DAG Functionality

The transforms framework now supports isolating disconnected subgraphs in DAGs and rendering them as separate tabs in the web interface.

## New Functions

### Core Functions

- `split_disconnected_components(G)` - Splits a directed graph into disconnected components
- `build_subgraph_dags(logs)` - Builds multiple DAGs from transform logs, separated by disconnected components
- `build_subgraph_dag_htmls(job_id, run_id, height)` - Builds HTML for each subgraph
- `build_subgraph_dags_html(job_id, run_id, height)` - Creates complete HTML with tabbed interface
- `render_subgraph_dags(job_id, run_id, height)` - Main function to generate and save subgraph DAGs

### WebCanvas Extensions

- `generate_tabbed_main(tab_contents)` - Creates tabbed interface HTML
- Updated `generate_script()` with tab switching functionality

## Usage Examples

### Basic Usage

```python
from transformslib.mapping import dag

# Generate subgraph DAGs with tabbed interface
dag.render_subgraph_dags(job_id=1, run_id=1)
```

### Advanced Usage

```python
from transformslib.mapping import dag

# Get individual subgraph information
subgraph_htmls = dag.build_subgraph_dag_htmls(job_id=1, run_id=1)

for i, subgraph in enumerate(subgraph_htmls):
    print(f"Subgraph {i+1}: {subgraph['title']}")
    print(f"  Nodes: {subgraph['node_count']}")
    print(f"  Edges: {subgraph['edge_count']}")

# Generate custom HTML
html_content = dag.build_subgraph_dags_html(job_id=1, run_id=1)
```

## Backward Compatibility

The original functions remain unchanged:
- `render_dag(job_id, run_id)` - Still generates single DAG view
- `build_dag(job_id, run_id)` - Still returns single DAG HTML

## Output Files

- Single DAG: `transform_dags/transform_dag_job{job_id}_run{run_id}.html`
- Subgraph DAGs: `transform_dags/transform_dag_job{job_id}_run{run_id}_subgraphs.html`

## Features

- **Automatic Detection**: Disconnected components are automatically detected using NetworkX
- **Tabbed Interface**: Each subgraph gets its own tab with component name and node count
- **Interactive**: Click tabs to switch between different subgraphs
- **Responsive**: Layout adapts to different screen sizes
- **Backward Compatible**: Original single DAG functionality is preserved