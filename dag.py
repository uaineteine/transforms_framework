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
    testable = event.get("testable_transform", False)

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

# Add edges and mark output nodes as testable/not
for event in events:
    log_info = event.get("log_info", {})
    input_tables = log_info.get("input_tables", [])
    output_tables = log_info.get("output_tables", [])
    transform_name = event.get("name", "unknown")
    testable = event.get("testable_transform", False)

    # Add input table nodes (always grey, since they aren't "produced" here)
    for inp in input_tables:
        if inp not in G:
            G.add_node(inp, type="table", color="lightgrey")

    # Add output table nodes (color depends on testable flag)
    for out in output_tables:
        node_color = "lightgreen" if testable else "lightgrey"
        G.add_node(out, type="table", color=node_color)

    # Add edges from inputs → outputs
    for inp in input_tables:
        for out in output_tables:
            G.add_edge(inp, out, label=transform_name)

# Layout
pos = graphviz_layout(G, prog="dot")  # top-down layout

plt.figure(figsize=(12, 8))

# Draw nodes with their assigned colors
node_colors = [G.nodes[n].get("color", "lightgrey") for n in G.nodes()]
nx.draw_networkx_nodes(G, pos, node_size=2000, node_color=node_colors)

# Draw edges
nx.draw_networkx_edges(G, pos, arrowstyle="->", arrowsize=20)

# Labels
nx.draw_networkx_labels(G, pos, font_size=10, font_weight="bold")
edge_labels = nx.get_edge_attributes(G, "label")
nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color="blue", font_size=8)

plt.title("Pipeline DAG (green = testable, grey = not testable)")
plt.axis("off")
plt.show()
