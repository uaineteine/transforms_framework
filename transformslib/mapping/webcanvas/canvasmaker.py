import tailwindcss

def generate_doctype() -> str:
    return '<!DOCTYPE html>'

def generate_css() -> str:
    return """
    <style>
        body {
            font-family: 'Inter', sans-serif;
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
        }
        .info-panel-scroll::-webkit-scrollbar {
            width: 8px;
        }
        .info-panel-scroll::-webkit-scrollbar-track {
            background: #f1f5f9;
            border-radius: 10px;
        }
        .info-panel-scroll::-webkit-scrollbar-thumb {
            background: #cbd5e1;
            border-radius: 10px;
        }
        .info-panel-scroll::-webkit-scrollbar-thumb:hover {
            background: #94a3b8;
        }
        #network-container {
            width: 100%;
            height: 100%;
            display: flex;
            justify-content: center;
            align-items: center;
        }
        .text-bevel {
            text-shadow: 2px 2px 3px rgba(0,0,0,0.5), -2px -2px 3px rgba(255,255,255,0.2);
        }
    </style>
    """

def generate_head() -> str:
    return f"""
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Network Visualization Dashboard</title>
    <!-- Tailwind CSS CDN -->
    <style>{tailwindcss.tailwind}</style>
    <!-- Google Fonts -->
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    {generate_css()}
</head>
"""

def generate_script() -> str:
    return """
    <script>
        document.addEventListener('DOMContentLoaded', () => {
            const now = new Date();
            document.getElementById('lastUpdated').innerText = now.toLocaleString();
            document.getElementById('nodeCount').innerText = 'N/A';
            document.getElementById('edgeCount').innerText = 'N/A';
        });
    </script>
    """

def generate_header(header_name="Network Graph", runtime:str="9h 9m 9s") -> str:
    return f"""
    <header class="bg-indigo-700 text-white p-4 sm:p-6 shadow-lg flex items-center justify-between z-10">
        <div class="flex-1">
            <h1 class="text-2xl sm:text-3xl font-bold tracking-tight text-bevel">{header_name}</h1>
        </div>
        <div class="text-right flex items-center space-x-4">
            <p id="runtime" class="font-medium text-lg">Runtime: {runtime}</p>
            <div class="bg-white/20 h-8 w-px rounded-full"></div>
            <div id="runtimeInfo" class="text-sm opacity-80 flex flex-col items-end">
                <p><span class="font-medium">Nodes:</span> <span id="nodeCount">N/A</span></p>
                <p><span class="font-medium">Edges:</span> <span id="edgeCount">N/A</span></p>
            </div>
        </div>
    </header>
    """

def generate_main(CONTENT:str='<p class="text-center text-gray-400 text-lg">Paste your pyvis graph HTML here to see it render.</p>') -> str:
    return f"""
    <main class="flex-1 flex flex-col md:flex-row p-4 sm:p-6 overflow-hidden">

        <div id="graph-panel" class="relative bg-white rounded-xl shadow-lg flex-1 mb-4 md:mb-0 md:mr-6 overflow-hidden">
            <div id="network-container" class="h-full w-full">
                {CONTENT}
            </div>
        </div>

        <aside id="info-panel" class="bg-white rounded-xl shadow-lg p-4 sm:p-6 w-full md:w-80 flex-shrink-0 flex flex-col info-panel-scroll overflow-y-auto">
            <div>
                <h2 class="text-xl font-bold text-gray-800 mb-2">Selected Item Details</h2>
                <div id="selected-info" class="text-sm text-gray-700 leading-relaxed">
                    <p>Select a node or edge in the graph to see its details here.</p>
                </div>
            </div>
        </aside>

    </main>
    """

def generate_html() -> str:
    return f"""{generate_doctype()}
<html lang="en" class="h-full bg-gray-100">
{generate_head()}
<body class="flex flex-col h-full overflow-hidden">
    {generate_header()}
    {generate_main()}
    {generate_script()}
</body>
</html>
"""

if __name__ == "__main__":
    html_doc = generate_html()

    output_path = "canvas_example.html"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_doc)

    print(f"HTML file written to {output_path}")
