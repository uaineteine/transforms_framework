import json
import networkx as nx
import matplotlib.pyplot as plt
from networkx.drawing.nx_pydot import graphviz_layout
from datetime import datetime

# Load JSON events
json_file = "templates/events_log/job_1/debug/transforms.json"
events = []
with open(json_file, "r") as f:
    content = f.read().strip()
    for obj_str in content.split("\n{"):
        obj_str = obj_str if obj_str.startswith("{") else "{" + obj_str
        events.append(json.loads(obj_str))

# Sort by timestamp
def parse_ts(e):
    return datetime.fromisoformat(e["timestamp"].replace("Z", "+00:00"))
events.sort(key=parse_ts)

G = nx.DiGraph()

# Track latest node for each table
latest_node_for_table = {}

for event in events:
    ts = parse_ts(event).strftime("%H:%M:%S.%f")
    log_info = event.get("log_info", {})
    input_tables = log_info.get("input_tables", [])
    output_tables = log_info.get("output_tables", [])
    transform_name = event.get("name", "unknown")
    testable = event.get("testable_transform", False)

    # Determine input nodes (latest version at this point)
    input_nodes = []
    for tbl in input_tables:
        if tbl in latest_node_for_table:
            input_nodes.append(latest_node_for_table[tbl])
        else:
            # starting table, create node
            node_id = f"{tbl}_start"
            G.add_node(node_id, label=tbl, color="lightblue")
            latest_node_for_table[tbl] = node_id
            input_nodes.append(node_id)

    # Create new output nodes (one per table)
    output_nodes = []
    for tbl in output_tables:
        node_id = f"{tbl}_{ts.replace(':', '_').replace('.', '_')}"
        node_color = "lightgreen" if testable else "lightgrey"
        G.add_node(node_id, label=tbl, color=node_color)
        latest_node_for_table[tbl] = node_id
        output_nodes.append(node_id)

    # Connect input nodes → output nodes
    for inp_node in input_nodes:
        for out_node in output_nodes:
            edge_label = f"{transform_name}\n{ts}"
            G.add_edge(inp_node, out_node, label=edge_label, len=6)

# Layout
pos = graphviz_layout(G, prog="dot")
plt.figure(figsize=(16, 36))
node_colors = [G.nodes[n].get("color", "lightgrey") for n in G.nodes()]
nx.draw_networkx_nodes(G, pos, node_size=2200, node_color=node_colors)

nx.draw_networkx_edges(G, pos, arrowstyle="->", arrowsize=20)
labels = {n: G.nodes[n].get("label", n) for n in G.nodes()}
nx.draw_networkx_labels(G, pos, labels=labels, font_size=10, font_weight="bold")
edge_labels = nx.get_edge_attributes(G, "label")
nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color="blue", font_size=8)

plt.title("Pipeline DAG (blue=input, green=testable, grey=not testable)\nVersioned nodes per transform")
plt.axis("off")
plt.show()
