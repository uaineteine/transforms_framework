def generate_doctype() -> str:
    return '<!DOCTYPE html>'

def generate_css() -> str:
    return """
    <style>
        /* Base styles for a clean layout, replicating Tailwind's defaults */
        :root {
            --bg-gray-100: #f3f4f6;
            --indigo-700: #4338ca;
            --indigo-800: #3730a3;
            --white: #ffffff;
            --gray-100: #f3f4f6;
            --gray-800: #1f2937;
            --gray-700: #374151;
            --gray-400: #9ca3af;
            --slate-100: #f1f5f9;
            --slate-300: #cbd5e1;
            --slate-400: #94a3b8;
            
            /* Color palette for random header colors */
            --blue-700: #1d4ed8;
            --cyan-700: #0e7490;
            --green-700: #15803d;
            --red-700: #b91c1c;
            --yellow-700: #a16207;
            --purple-700: #7c3aed;
            
            /* Dynamic header color - will be set by JavaScript */
            --header-color: var(--indigo-700);
        }

        html, body {
            height: 100%;
        }

        body {
            font-family: sans-serif; /* Fallback for no external fonts */
            background-color: var(--bg-gray-100);
            display: flex;
            flex-direction: column;
            height: 100%;
            overflow: hidden;
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
        }

        /* Top Banner & Header styles */
        .header {
            background-color: var(--header-color);
            color: var(--white);
            padding: 0.5rem 1rem; /* Reduced vertical padding */
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06); /* shadow-lg */
            display: flex;
            align-items: center;
            justify-content: space-between;
            position: relative;
            z-index: 10;
            cursor: pointer;
            transition: all 0.2s ease-in-out;
            user-select: none;
        }
        
        .header:hover {
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.15), 0 4px 6px -2px rgba(0, 0, 0, 0.1); /* Bigger shadow on hover */
            filter: brightness(1.1); /* Subtle brightening effect */
            transform: translateY(-2px); /* Slight upward movement */
        }
        
        .header:active {
            transform: translateY(1px); /* Press down effect when clicked */
            filter: brightness(0.95); /* Darken slightly when clicked */
            box-shadow: 0 2px 4px -1px rgba(0, 0, 0, 0.1), 0 1px 2px -1px rgba(0, 0, 0, 0.06); /* Reduced shadow when pressed */
        }

        @media (min-width: 640px) {
            .header {
                padding: 0.75rem 1.5rem; /* Reduced vertical padding on larger screens */
            }
        }

        .header-content {
            display: flex;
            align-items: center;
            text-align: right;
            gap: 1rem; /* space-x-4 */
        }

        .header-title {
            font-size: 1.5rem; /* text-2xl */
            font-weight: 700;
            letter-spacing: -0.025em; /* tracking-tight */
            text-shadow: 2px 2px 3px rgba(0,0,0,0.5), -2px -2px 3px rgba(255,255,255,0.2); /* text-bevel */
        }

        @media (min-width: 640px) {
            .header-title {
                font-size: 1.875rem; /* sm:text-3xl */
            }
        }

        /* Header Info Styles */
        .header-info-group {
            display: flex;
            align-items: center;
            font-size: 0.875rem; /* text-sm */
            opacity: 0.8;
            gap: 1rem;
        }
        
        .header-info {
            display: flex;
            flex-direction: column;
            align-items: flex-end;
            gap: 0.125rem;
        }
        
        .header-single-info {
            display: flex;
            flex-direction: column;
            align-items: flex-end;
        }

        .separator {
            background-color: rgba(255, 255, 255, 0.2);
            height: 1.5rem;
            width: 1px;
            border-radius: 9999px;
        }

        .font-medium {
            font-weight: 500;
        }

        /* Main content area (Graph and Side Panel) */
        .main-container {
            flex: 1;
            display: flex;
            flex-direction: column;
            padding: 1rem; /* p-4 */
            overflow: hidden;
        }

        @media (min-width: 768px) {
            .main-container {
                flex-direction: row;
                padding: 1.5rem; /* sm:p-6 */
            }
        }

        /* Graph Container styles */
        .graph-panel {
            position: relative;
            background-color: var(--white);
            border-radius: 0.75rem; /* rounded-xl */
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06); /* shadow-lg */
            flex: 1;
            margin-bottom: 1rem; /* mb-4 */
            overflow: hidden;
        }

        @media (min-width: 768px) {
            .graph-panel {
                margin-bottom: 0;
                margin-right: 1.5rem; /* md:mr-6 */
            }
        }

        #network-container {
            width: 100%;
            height: 100%;
            display: flex;
            justify-content: center;
            align-items: center;
        }

        /* Right Side Info Panel styles */
        .info-panel {
            background-color: var(--white);
            border-radius: 0.75rem; /* rounded-xl */
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06); /* shadow-lg */
            padding: 1rem; /* p-4 */
            width: 100%;
            flex-shrink: 0;
            display: flex;
            flex-direction: column;
            overflow-y: auto;
        }
        
        @media (min-width: 640px) {
            .info-panel {
                padding: 1.5rem; /* sm:p-6 */
            }
        }

        @media (min-width: 768px) {
            .info-panel {
                width: 20rem; /* w-80 */
            }
        }
        
        /* Custom scrollbar for better aesthetics */
        .info-panel-scroll::-webkit-scrollbar {
            width: 8px;
        }
        .info-panel-scroll::-webkit-scrollbar-track {
            background: var(--slate-100);
            border-radius: 10px;
        }
        .info-panel-scroll::-webkit-scrollbar-thumb {
            background: var(--slate-300);
            border-radius: 10px;
        }
        .info-panel-scroll::-webkit-scrollbar-thumb:hover {
            background: var(--slate-400);
        }

        /* Selected Item Details styles */
        .details-title {
            font-size: 1.25rem; /* text-xl */
            font-weight: 700;
            color: var(--gray-800);
            margin-bottom: 0.5rem; /* mb-2 */
        }
        
        .details-content {
            font-size: 0.875rem; /* text-sm */
            color: var(--gray-700);
            line-height: 1.625; /* leading-relaxed */
        }
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

def generate_header(header_name="Network Graph", runtime:str="9h 9m 9s") -> str:
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
