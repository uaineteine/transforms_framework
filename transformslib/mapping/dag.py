from pyvis.network import Network
import networkx as nx
import os

from transformslib.transforms import reader

def output_loc(job_id:int, run_id:int) -> str:
    """Function to return a transforms report output location"""
    report_name = f"transform_dag_job{job_id}_run{run_id}.html"

def build_dag(job_id:int, run_id:int):
    """
    Method for building the dag with an output html file
    
    Args: job_id, run_id
    """
    logs = reader.load_transform_log(job_id=1, run_id=1)

    # Build DAG
    input_dfs = [log["params"]["path"] for log in logs if log["function"].startswith("read_file")]
    output_files = [log["params"]["path"] for log in logs if log["function"].startswith("write_file")]

    G = nx.DiGraph()
    for df in input_dfs:
        G.add_node(df, label=os.path.basename(df), color="lightblue", title=f"Input: {df}")

    transform_nodes = []
    for i, log in enumerate(logs):
        if log["function"].startswith(("read_file", "write_file")):
            continue

        func_name = log["function"]
        timestamp = log["timestamp"]
        node_name = f"{func_name}_{timestamp}"
        transform_nodes.append(node_name)

        extra_stats = log.get("extra", {}).get("stats", {})
        extra_info = "\n".join([f"{k}: {v}" for k, v in extra_stats.items()])

        # The previous code block continues here

        title = (
            f"Function: {func_name}\n"
            f"Version: {log.get('version', '')}\n"
            f"User: {log.get('user', '')}\n"
            f"Status: {'✅' if log.get('pass_bool', True) else '❌'}\n"
            f"Message: {log.get('msg', '')}\n"
            f"{extra_info}\n"
            f"Params: {log.get('params', '')}"
        )

        node_color = "lightgreen" if log.get("pass_bool", True) else "red"
        G.add_node(node_name, label=func_name, color=node_color, title=title)

        if i == 0:
            for df in input_dfs:
                G.add_edge(df, node_name)
        else:
            j = i - 1
            while j >= 0 and logs[j]["function"].startswith(("read_file", "write_file")):
                j -= 1
            
            prev_node = f"{logs[j]['function']}_{logs[j]['timestamp']}" if j >= 0 else input_dfs[0]
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

    # Save UTF-8 HTML manually
    html_file = output_loc(job_id = job_id, run_id = run_id)
    html_content = net.generate_html()
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html_content)
    print("DAG saved to: " + html_file)

# Embed in Streamlit
#st.components.v1.html(html_content, height=800, scrolling=True)
