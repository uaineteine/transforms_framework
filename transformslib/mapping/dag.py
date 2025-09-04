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
        height (int|float|str, optional): Height of the graph. 
            - If int/float: interpreted as pixels.
            - If str: passed directly (e.g., "100%").
    
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

def format_timedelta(td: timedelta) -> str:
    """
    Format a timedelta as Hh Mm Ss string.
    """
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if seconds > 0 or not parts:  # always show something
        parts.append(f"{seconds}s")

    return " ".join(parts)


def output_loc(job_id:int, run_id:int) -> str:
    """Function to return a transforms report output location"""
    report_name = f"transform_dag_job{job_id}_run{run_id}.html"
    return os.path.join("transform_dags", report_name)

def build_dag(job_id:int, run_id:int, height: Union[int, float, str] = 900):
    """
    Method for building the dag with an output html file
    
    Args:
        job_id
        run_id
        height_amt (optional) in pixels
    """

    logs = reader.load_transform_log(job_id=1, run_id=1)
    
    if len(logs) == 0:
        raise ValueError("JSON log for transforms was parsed empty")

    #check meta version
    this_version = logs[0].get("meta_version" "")
    meta.expected_meta_version(this_version)

    # Build DAG
    input_dfs = [log["params"]["path"] for log in logs if log["transform_type"].startswith("read_file")]
    output_files = [log["params"]["path"] for log in logs if log["transform_type"].startswith("write_file")]

    G = nx.DiGraph()
    for df in input_dfs:
        G.add_node(df, label=os.path.basename(df), color="lightblue", title=f"Input: {df}")

    transform_nodes = []
    for i, log in enumerate(logs):
        if log["transform_type"].startswith(("read_file", "write_file")):
            continue

        func_name = log["transform_type"]
        timestamp = log["timestamp"]
        node_name = f"{func_name}_{timestamp}"
        transform_nodes.append(node_name)

        extra_stats = log.get("extra", {}).get("stats", {})
        extra_info = "\n".join([f"{k}: {v}" for k, v in extra_stats.items()]) if extra_stats else ""

        is_testable = log["testable_transform"]
        title_parts = [
            f"transform_type: {func_name}",
            f"Meta Version: {log.get('meta_version', '')}",
            f"User: {log.get('executed_user', '')}",
            f"Tested: {'✅' if is_testable else '❌'}",
            f"Message: {log.get('msg', '')}",
        ]

        if extra_info:
            title_parts.append(extra_info)

        title_parts.append(f"Params: {log.get('params', '')}")
        title = "\n".join(title_parts)

        node_color = "lightgreen" if log.get("pass_bool", True) else "red"
        G.add_node(node_name, label=func_name, color=node_color, title=title)

        if i == 0:
            for df in input_dfs:
                G.add_edge(df, node_name)
        else:
            j = i - 1
            while j >= 0 and logs[j]["transform_type"].startswith(("read_file", "write_file")):
                j -= 1
            
            prev_node = f"{logs[j]['transform_type']}_{logs[j]['timestamp']}" if j >= 0 else input_dfs[0]
            G.add_edge(prev_node, node_name)

    # The previous code block continues here

    for out_file in output_files:
        last_transform_node = transform_nodes[-1] if transform_nodes else input_dfs[0]
        G.add_node(out_file, label=os.path.basename(out_file), color="orange", title=f"Output: {out_file}")
        G.add_edge(last_transform_node, out_file)

    # Height handling
    if isinstance(height, (int, float)):
        height_str = f"{int(height)}px"
    elif isinstance(height, str):
        height_str = height
    else:
        raise TypeError("height must be int, float, or str")
    
    # Render Pyvis
    net = Network(
        height=height_str,
        width="100%",
        directed=True,
        notebook=False,
        cdn_resources="in_line"
    )

    net.from_nx(G)
    net.set_options("""
    var options = {
    "interaction": {
        "hover": true,
        "hoverDelay": 100,
        "multiselect": false,
        "tooltipDelay": 100
    },
    "physics": {
        "enabled": true,
        "stabilization": true
    }
    }
    """)

    # Calculate total runtime
    timestamps = [log["timestamp"] for log in logs if "timestamp" in log]
    total_runtime = calculate_total_runtime(timestamps)
    runtime_str = format_timedelta(total_runtime) if total_runtime else "Unknown"

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

    # Compose final HTML using webcanvas building blocks
    head_html = webcanvas.generate_head()
    # Inject pyvis head resources before closing </head>
    if pyvis_head_inner:
        head_html = head_html.replace("</head>", f"{pyvis_head_inner}</head>")

    header_html = webcanvas.generate_header(header_name=f"Transform DAG: job {job_id}, run {run_id}", runtime=runtime_str)
    main_html = webcanvas.generate_main(CONTENT=pyvis_body_inner or "<p class=\"text-center text-gray-400 text-lg\">Pyvis graph content missing.</p>")

    full_html = (
        f"{webcanvas.generate_doctype()}\n"
        "<html lang=\"en\" class=\"h-full bg-gray-100\">\n"
        f"{head_html}\n"
        "<body class=\"flex flex-col h-full overflow-hidden\">\n"
        f"    {header_html}\n"
        f"    {main_html}\n"
        f"    {webcanvas.generate_script()}\n"
        "</body>\n"
        "</html>\n"
    )

    # Save UTF-8 HTML
    html_file = output_loc(job_id=job_id, run_id=run_id)
    os.makedirs(os.path.dirname(html_file), exist_ok=True)
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(full_html)
    print("DAG saved to: " + html_file)

# Embed in Streamlit
#st.components.v1.html(html_content, height=800, scrolling=True)
