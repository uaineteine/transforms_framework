from pyvis.network import Network
import networkx as nx
import os
from datetime import datetime, timedelta
from typing import List, Optional, Union

from transformslib.mapping import webcanvas
from transformslib.transforms import reader
from transformslib import meta

def calculate_total_runtime(timestamps: List[str], fmt: str = "%Y-%m-%dT%H:%M:%S") -> Optional[timedelta]:
    """
    Calculate total runtime from a list of timestamp strings.

    Args:
        job_id (int): Job ID
        run_id (int): Run ID
        height (int|float|str, optional): Height of the graph. If int/float: interpreted as pixels. If str: passed directly (e.g., "100%").
    
    Returns:
        Optional[timedelta]: Total runtime as a timedelta object, or None if timestamps are empty.
    """
    if len(timestamps) == 0:
        return None
    
    try:
        parsed_times = [datetime.fromisoformat(ts) for ts in timestamps]
        start = min(parsed_times)
        end = max(parsed_times)
        return end - start
    except ValueError as e:
        print(f"Timestamp parsing error: {e}")
        return None

def output_loc(job_id:int, run_id:int) -> str:
    """Function to return a transforms report output location"""
    report_name = f"transform_dag_job{job_id}_run{run_id}.html"
    return os.path.join("transform_dags", report_name)


def set_default_network_options(net: Network) -> Network:
    """
    Apply default PyVis network options with hierarchical tree layout.

    Args:
        net (Network): The PyVis Network instance.

    Returns:
        Network: The same network instance with options applied.
    """
    options = """
    var options = {
        "interaction": {
            "hover": true,
            "hoverDelay": 100,
            "multiselect": false,
            "tooltipDelay": 100
        },
        "layout": {
            "hierarchical": {
                "enabled": true,
                "direction": "UD",
                "sortMethod": "directed",
                "levelSeparation": 150,
                "nodeSpacing": 100
            }
        },
        "edges": {
            "arrows": {
                "to": {
                    "enabled": true,
                    "scaleFactor": 1
                }
            }
        },
        "physics": {
            "enabled": true,
            "hierarchicalRepulsion": {
                "centralGravity": 0.0,
                "springLength": 100,
                "springConstant": 0.01,
                "nodeDistance": 120,
                "damping": 0.09
            },
            "solver": "hierarchicalRepulsion",
            "stabilization": {
                "enabled": true,
                "iterations": 1000
            }
        }
    }
    """
    net.set_options(options)
    return net

def build_di_graph(logs:list) -> nx.DiGraph:
    """
    From a list of json logs of transforms, produce a directed graph
    """
    #quick error check
    if len(logs) == 0:
        raise ValueError("JSON log for transforms was parsed empty")

    #sort the logs in order of timestamp
    logs = sorted(logs, key=reader.parse_ts)

    # Build table-versioned DAG (nodes = tables; new node for each output at event time)
    G = nx.DiGraph()
    latest_node_for_table = {}

    for event in logs:
        ts_dt = reader.parse_ts(event)
        ts_short = ts_dt.strftime("%H:%M:%S.%f")

        log_info = event.get("log_info", {}) or {}
        input_tables = log_info.get("input_tables", []) or []
        output_tables = log_info.get("output_tables", []) or []
        transform_name = event.get("name", "unknown")
        is_testable = bool(event.get("testable_transform", False))

        # Determine input nodes (latest version so far)
        input_nodes = []
        for tbl in input_tables:
            if tbl in latest_node_for_table:
                input_nodes.append(latest_node_for_table[tbl])
            else:
                # Starting table node (no prior version seen yet)
                node_id = f"{tbl}_start"
                G.add_node(
                    node_id,
                    label=tbl,
                    color="lightblue",
                    title=f"Start table: {tbl}",
                    size=35
                )
                latest_node_for_table[tbl] = node_id
                input_nodes.append(node_id)

        # Create new output nodes (one per table, versioned by time)
        output_nodes = []
        for tbl in output_tables:
            node_id = f"{tbl}_{ts_short.replace(':', '_').replace('.', '_')}"
            node_color = "lightgreen" if is_testable else "lightgrey"

            # Tooltip with helpful metadata
            title_parts = [
                f"Transform: {transform_name}",
                f"Time: {event.get('timestamp', '')}",
                f"User: {event.get('executed_user', '')}",
                f"Testable: {'Yes' if is_testable else 'No'}",
            ]
            if event.get("event_description"):
                title_parts.append(f"Description: {event['event_description']}")
            if input_tables:
                title_parts.append(f"Inputs: {', '.join(input_tables)}")
            if output_tables:
                title_parts.append(f"Outputs: {', '.join(output_tables)}")

            # Add row counts if present
            input_row_counts = log_info.get("input_row_counts", {})
            output_row_counts = log_info.get("output_row_counts", {})
            if tbl in output_row_counts:
                title_parts.append(f"Output Rows: {output_row_counts[tbl]}")
            if tbl in input_row_counts:
                title_parts.append(f"Input Rows: {input_row_counts[tbl]}")

            title = "\n".join(title_parts)

            G.add_node(
                node_id,
                label=tbl,
                color=node_color,
                title=title,
                size=25
            )
            latest_node_for_table[tbl] = node_id
            output_nodes.append(node_id)

        # Connect input nodes → output nodes, label edges with transform name
        for inp_node in input_nodes:
            for out_node in output_nodes:
                G.add_edge(inp_node, out_node, label=transform_name)
    
    return G


def build_dag(job_id:int, run_id:int, height: Union[int, float, str] = 900) -> str:
    """
    Build a PyVis DAG with hierarchical tree layout where nodes are tables (versioned per event) and edges are transforms.

    Args:
        job_id (int): Job identifier.
        run_id (int): Run identifier.
        height (int|float|str, optional): Height in pixels (int/float) or a CSS string (e.g., "100%").

    Returns:
        An HTML string of the dag
    """

    # Load transform events
    logs = reader.load_transform_log(job_id=job_id, run_id=run_id)

    # Check meta version
    this_version = logs[0].get("meta_version", "")
    meta.expected_meta_version(this_version)

    # Height handling
    if isinstance(height, (int, float)):
        height_str = f"{int(height)}px"
    elif isinstance(height, str):
        height_str = height
    else:
        raise TypeError("height must be int, float, or str")
    
    # Render PyVis
    net = Network(
        height=height_str,
        width="100%",
        directed=True,
        notebook=False,
        cdn_resources="in_line",
        layout=True  # Enable layout for hierarchical organization
    )
    net = set_default_network_options(net)

    # Build table-versioned DAG (nodes = tables; new node for each output at event time)
    G = build_di_graph(logs)
    net.from_nx(G)

    # Calculate total runtime
    timestamps = [evt.get("timestamp") for evt in logs if evt.get("timestamp")]
    total_runtime = calculate_total_runtime(timestamps)
    runtime_str = reader.format_timedelta(total_runtime) if total_runtime else "Unknown"

    # Generate Pyvis HTML and extract head and body segments
    pyvis_html = net.generate_html()

    def _extract_between(html: str, start_tag: str, end_tag: str) -> str:
        lower_html = html.lower()
        start_idx = lower_html.find(start_tag)
        if start_idx == -1:
            return ""
        # find the '>' of the start tag
        gt_idx = lower_html.find(">", start_idx)
        if gt_idx == -1:
            return ""
        content_start = gt_idx + 1
        end_idx = lower_html.find(end_tag, content_start)
        if end_idx == -1:
            return ""
        return html[content_start:end_idx]

    pyvis_head_inner = _extract_between(pyvis_html, "<head", "</head>")
    pyvis_body_inner = _extract_between(pyvis_html, "<body", "</body>")

    # Get current timestamp for report generation time
    report_generated_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Calculate node and edge counts from the NetworkX graph
    node_count = G.number_of_nodes()
    edge_count = G.number_of_edges()

    # Compose final HTML using webcanvas building blocks
    head_html = webcanvas.generate_head()
    # Inject pyvis head resources before closing </head>
    if pyvis_head_inner:
        head_html = head_html.replace("</head>", f"{pyvis_head_inner}</head>")
    
    # Add custom JavaScript to handle node hover events and update right panel
    custom_js = """
    <script>
        // Function to update the right-hand panel with node/edge information
        function updateInfoPanel(title, content) {
            const detailsTitle = document.getElementById('selected-info').closest('div').querySelector('.details-title');
            const selectedInfo = document.getElementById('selected-info');
            
            if (detailsTitle) {
                detailsTitle.textContent = title;
            }
            if (selectedInfo) {
                selectedInfo.innerHTML = content;
            }
        }
        
        // Function to format tooltip content for the right panel
        function formatTooltipContent(tooltipText) {
            const lines = tooltipText.split('\\n');
            let html = '';
            
            lines.forEach(line => {
                if (line.trim() === '') return;
                
                if (line.includes(':')) {
                    const [label, value] = line.split(':', 2);
                    html += `<p><strong>${label.trim()}:</strong> ${value.trim()}</p>`;
                } else {
                    html += `<p>${line.trim()}</p>`;
                }
            });
            
            return html || '<p>No details available</p>';
        }
        
        // Wait for network to be available and add event listeners
        function setupNetworkEvents() {
            if (typeof network !== 'undefined') {
                // Handle node hover
                network.on("hoverNode", function (params) {
                    const nodeId = params.node;
                    const nodeData = network.body.data.nodes.get(nodeId);
                    
                    if (nodeData && nodeData.title) {
                        const title = nodeData.label || 'Node Details';
                        const content = formatTooltipContent(nodeData.title);
                        updateInfoPanel(title, content);
                    }
                });
                
                // Handle edge hover
                network.on("hoverEdge", function (params) {
                    const edgeId = params.edge;
                    const edgeData = network.body.data.edges.get(edgeId);
                    
                    if (edgeData) {
                        const title = edgeData.label || 'Transform';
                        const content = `<p><strong>Transform:</strong> ${edgeData.label || 'Unknown'}</p>`;
                        updateInfoPanel(title, content);
                    }
                });
                
                // Reset to default when not hovering
                network.on("blurNode", function (params) {
                    updateInfoPanel('Selected Item Details', '<p>Select a node or edge in the graph to see its details here.</p>');
                });
                
                network.on("blurEdge", function (params) {
                    updateInfoPanel('Selected Item Details', '<p>Select a node or edge in the graph to see its details here.</p>');
                });
            } else {
                // Retry after a short delay if network is not ready
                setTimeout(setupNetworkEvents, 100);
            }
        }
        
        // Setup when DOM is loaded
        document.addEventListener('DOMContentLoaded', function() {
            setupNetworkEvents();
        });
    </script>
    """
    
    # Inject custom JavaScript before closing </head>
    head_html = head_html.replace("</head>", f"{custom_js}</head>")

    header_html = webcanvas.generate_header(header_name=f"Transform DAG: job {job_id}, run {run_id}", runtime=runtime_str, version=this_version)
    main_html = webcanvas.generate_main(CONTENT=pyvis_body_inner or "<p class=\"text-center text-gray-400 text-lg\">Pyvis graph content missing.</p>")

    full_html = (
        f"{webcanvas.generate_doctype()}\n"
        "<html lang=\"en\" class=\"h-full bg-gray-100\">\n"
        f"{head_html}\n"
        "<body class=\"flex flex-col h-full overflow-hidden\">\n"
        f"    {header_html}\n"
        f"    {main_html}\n"
        f"    {webcanvas.generate_script(report_generated_time, node_count, edge_count)}\n"
        "</body>\n"
        "</html>\n"
    )

    return full_html

def render_dag(job_id:int, run_id:int, height: Union[int, float, str] = 900) -> str:
    """
    Build a PyVis DAG with hierarchical tree layout where nodes are tables (versioned per event) and edges are transforms. Saves this to file defined by the output_loc function.

    Args:
        job_id (int): Job identifier.
        run_id (int): Run identifier.
        height (int|float|str, optional): Height in pixels (int/float) or a CSS string (e.g., "100%").
    """
    full_html = build_dag(job_id, run_id, height=height)

    # Save UTF-8 HTML
    html_file = output_loc(job_id=job_id, run_id=run_id)\

    os.makedirs(os.path.dirname(html_file), exist_ok=True)
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(full_html)
    print("DAG saved to: " + html_file)

# Embed in Streamlit
#st.components.v1.html(html_content, height=800, scrolling=True)
