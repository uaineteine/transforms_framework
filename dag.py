import json
import networkx as nx
import matplotlib.pyplot as plt
from networkx.drawing.nx_pydot import graphviz_layout

# Load multiple JSON objects from a file
json_file = "templates/events_log/job_1/debug/transforms.json"
events = []

with open(json_file, "r") as f:
    content = f.read().strip()
    for obj_str in content.split("\n{"):
        obj_str = obj_str if obj_str.startswith("{") else "{" + obj_str
        events.append(json.loads(obj_str))

# Create a directed graph
G = nx.DiGraph()

# Add edges based on input/output tables
for event in events:
    log_info = event.get("log_info", {})
    input_tables = log_info.get("input_tables", [])
    output_tables = log_info.get("output_tables", [])
    transform_name = event.get("name", "unknown")

    for inp in input_tables:
        G.add_node(inp, type="table")
    for out in output_tables:
        G.add_node(out, type="table")
    for inp in input_tables:
        for out in output_tables:
            G.add_edge(inp, out, label=transform_name)

# Use pydot for hierarchical layout
pos = graphviz_layout(G, prog="dot")  # top-down layout

plt.figure(figsize=(12, 8))
nx.draw_networkx_nodes(G, pos, node_size=2000, node_color="lightblue")
nx.draw_networkx_edges(G, pos, arrowstyle="->", arrowsize=20)
nx.draw_networkx_labels(G, pos, font_size=10, font_weight="bold")

edge_labels = nx.get_edge_attributes(G, "label")
nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color="red", font_size=8)

plt.title("Pipeline DAG")
plt.axis("off")
plt.show()
