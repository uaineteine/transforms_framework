from pyvis.network import Network
import networkx as nx
import os
from datetime import datetime, timedelta
from typing import List, Optional

from transformslib.transforms import reader
from transformslib import meta

def calculate_total_runtime(timestamps: List[str], fmt: str = "%Y-%m-%dT%H:%M:%S") -> Optional[timedelta]:
    """
    Calculate total runtime from a list of timestamp strings.

    Args:
        timestamps (List[str]): List of timestamp strings.
        fmt (str): Format of the timestamp strings (default is ISO 8601 without milliseconds).

    Returns:
        Optional[timedelta]: Total runtime as a timedelta object, or None if timestamps are empty.
    """
    if not timestamps:
        return None

    try:
        parsed_times = [datetime.strptime(ts, fmt) for ts in timestamps]
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

def build_dag(job_id:int, run_id:int):
    """
    Method for building the dag with an output html file
    
    Args: job_id, run_id
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

    # Render Pyvis
    net = Network(
        height="700px",
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

    if total_runtime:
        runtime_label = f"""
        <div style='text-align:center; font-size:20px; font-weight:bold; margin:20px;'>
            Total Runtime: {total_runtime}
        </div>
        """
    else:
        runtime_label = "<div style='text-align:center; font-size:20px; font-weight:bold; margin:20px;'>Total Runtime: Unknown</div>"

    # Save UTF-8 HTML manually
    html_file = output_loc(job_id=job_id, run_id=run_id)
    os.makedirs(os.path.dirname(html_file), exist_ok=True)
    html_content = runtime_label + net.generate_html()
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html_content)
    print("DAG saved to: " + html_file)

# Embed in Streamlit
#st.components.v1.html(html_content, height=800, scrolling=True)
