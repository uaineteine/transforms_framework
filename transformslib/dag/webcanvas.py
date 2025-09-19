from transformslib import meta
from transformslib.templates import read_template_safe

def generate_css() -> str:
    """
    Load CSS content from the mapping package.
    
    Args:
        css_filename: Name of the CSS file (e.g., 'webcanvas.css')
        
    Returns:
        Content of the CSS file as a string
        
    Raises:
        FileNotFoundError: If the CSS file doesn't exist
    """
    css_filename = "webcanvas.css"

    css_content = ""
    try:
        data = read_template_safe(css_filename)
        if data is None:
            raise FileNotFoundError(f"CSS file '{css_filename}' not found in package")
        css_content = data
    except Exception as e:
        raise FileNotFoundError(f"CSS file '{css_filename}' not found: {e}")
    

    return f"""
    <style>
        {css_content}
    </style>
    """

def generate_head() -> str:
    return f"""
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Network Visualization Dashboard</title>
    {generate_css()}
</head>
"""

def generate_script(report_generated_time: str = None, node_count: int = None, edge_count: int = None) -> str:
    # If report_generated_time is provided, use it; otherwise fall back to current time
    time_script = f"document.getElementById('lastUpdated').innerText = '{report_generated_time}';" if report_generated_time else "const now = new Date(); document.getElementById('lastUpdated').innerText = now.toLocaleString();"
    
    # Use provided counts or fall back to 'N/A'
    node_count_str = str(node_count) if node_count is not None else 'N/A'
    edge_count_str = str(edge_count) if edge_count is not None else 'N/A'
    
    return f"""
    <script>
        document.addEventListener('DOMContentLoaded', () => {{
            // Set random header color
            const colors = ['--blue-700', '--cyan-700', '--green-700', '--red-700', '--yellow-700', '--purple-700'];
            let currentColor = null;
            
            function setRandomHeaderColor() {{
                let availableColors = colors;
                
                // If we have a current color, exclude it from the selection
                if (currentColor) {{
                    availableColors = colors.filter(color => color !== currentColor);
                }}
                
                // Select random color from available options
                const randomColor = availableColors[Math.floor(Math.random() * availableColors.length)];
                currentColor = randomColor;
                document.documentElement.style.setProperty('--header-color', `var(${{randomColor}})`);
            }}
            
            function addClickEffect(element) {{
                // Add a ripple effect on click
                element.style.position = 'relative';
                element.style.overflow = 'hidden';
                
                const ripple = document.createElement('span');
                ripple.style.cssText = `
                    position: absolute;
                    border-radius: 50%;
                    background: rgba(255, 255, 255, 0.3);
                    transform: scale(0);
                    animation: ripple 0.6s linear;
                    pointer-events: none;
                    left: 50%;
                    top: 50%;
                    width: 100px;
                    height: 100px;
                    margin-left: -50px;
                    margin-top: -50px;
                `;
                
                element.appendChild(ripple);
                
                // Remove ripple after animation
                setTimeout(() => {{
                    if (ripple.parentNode) {{
                        ripple.parentNode.removeChild(ripple);
                    }}
                }}, 600);
            }}
            
            // Add ripple animation CSS
            const style = document.createElement('style');
            style.textContent = `
                @keyframes ripple {{
                    to {{
                        transform: scale(4);
                        opacity: 0;
                    }}
                }}
                
                .color-change-flash {{
                    animation: colorFlash 0.3s ease-in-out;
                }}
                
                @keyframes colorFlash {{
                    0% {{ filter: brightness(1); }}
                    50% {{ filter: brightness(1.3) saturate(1.2); }}
                    100% {{ filter: brightness(1); }}
                }}
            `;
            document.head.appendChild(style);
            
            // Set initial random color
            setRandomHeaderColor();
            
            // Add click event listener to header for color change
            const header = document.querySelector('.header');
            if (header) {{
                header.addEventListener('click', (e) => {{
                    // Add ripple effect at click position
                    addClickEffect(header);
                    
                    // Add flash animation
                    header.classList.add('color-change-flash');
                    setTimeout(() => {{
                        header.classList.remove('color-change-flash');
                    }}, 300);
                    
                    // Change color after brief delay for better visual effect
                    setTimeout(() => {{
                        setRandomHeaderColor();
                    }}, 150);
                }});
            }}
            
            // Set timestamps and counts
            {time_script}
            document.getElementById('nodeCount').innerText = '{node_count_str}';
            document.getElementById('edgeCount').innerText = '{edge_count_str}';
        }});
    </script>
    """

def generate_header(header_name="Network Graph", runtime:str="9h 9m 9s", version=meta.meta_version) -> str:
    return f"""
    <!-- Top Banner & Header -->
    <header class="header">
        <div class="flex-1">
            <h1 class="header-title">{header_name}</h1>
        </div>
        <div class="header-content">
            <p id="datasetName" class="font-medium text-lg">Runtime: {runtime}</p>
            <div class="separator"></div>
            <div class="header-info-group">
                <div id="runtimeInfo-lastupdated" class="header-single-info">
                    <p><span class="font-medium">Report Generated:</span> <span id="lastUpdated">N/A</span></p>
                </div>
                <div class="separator"></div>
                <div id="runtimeInfo-nodes" class="header-single-info">
                    <p><span class="font-medium">Nodes:</span> <span id="nodeCount">N/A</span></p>
                </div>
                <div class="separator"></div>
                <div id="runtimeInfo-edges" class="header-single-info">
                    <p><span class="font-medium">Edges:</span> <span id="edgeCount">N/A</span></p>
                </div>
                <div class="separator"></div>
                <div id="runtimeInfo-version" class="header-single-info">
                    <p><span class="font-medium">Meta Version:</span> <span id="meta_version">{version}</span></p>
                </div>
            </div>
        </div>
    </header>
    """

def generate_main(CONTENT:str='<p class="text-center text-gray-400 text-lg">Paste your pyvis graph HTML here to see it render.</p>') -> str:
    return f"""
    <main class="main-container">

        <!-- Graph Container -->
        <div id="graph-panel" class="graph-panel">
            <div id="network-container">
                {CONTENT}
            </div>
        </div>

        <!-- Right Side Info Panel -->
        <aside id="info-panel" class="info-panel info-panel-scroll">
            <div>
                <h2 class="details-title">Selected Item Details</h2>
                <div id="selected-info" class="details-content">
                    <p>Select a node or edge in the graph to see its details here.</p>
                </div>
            </div>
        </aside>

    </main>
    """

def generate_html() -> str:
    return f"""<!DOCTYPE html>
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
