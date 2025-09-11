from transformslib import meta
from typing import List

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

        /* Tab Container Styles */
        .tab-container {
            flex: 1;
            display: flex;
            flex-direction: column;
            padding: 1rem;
            overflow: hidden;
        }

        .tab-nav {
            display: flex;
            background-color: var(--white);
            border-radius: 0.75rem 0.75rem 0 0;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
            margin-bottom: 0;
            overflow-x: auto;
            flex-shrink: 0;
        }

        .tab-button {
            padding: 0.75rem 1.5rem;
            background-color: transparent;
            border: none;
            cursor: pointer;
            font-weight: 500;
            color: var(--gray-700);
            transition: all 0.2s ease-in-out;
            border-bottom: 2px solid transparent;
            white-space: nowrap;
            flex-shrink: 0;
        }

        .tab-button:hover {
            background-color: var(--gray-100);
            color: var(--gray-800);
        }

        .tab-button.active {
            color: var(--header-color);
            border-bottom-color: var(--header-color);
            background-color: var(--gray-100);
        }

        .tab-button:first-child {
            border-radius: 0.75rem 0 0 0;
        }

        .tab-button:last-child {
            border-radius: 0 0.75rem 0 0;
        }

        /* Tab Content Styles */
        .tab-content {
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }

        .tab-pane {
            display: none;
            flex: 1;
            flex-direction: column;
            overflow: hidden;
        }

        .tab-pane.active {
            display: flex;
        }

        /* Main content area (Graph and Side Panel) */
        .main-container {
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }

        @media (min-width: 768px) {
            .main-container {
                flex-direction: row;
            }
        }

        /* Graph Container styles */
        .graph-panel {
            position: relative;
            background-color: var(--white);
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06); /* shadow-lg */
            flex: 1;
            margin-bottom: 1rem; /* mb-4 */
            overflow: hidden;
            border-radius: 0 0 0.75rem 0.75rem;
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
            border-radius: 0 0 0.75rem 0.75rem;
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

def generate_script(report_generated_time: str = None, node_count: int = None, edge_count: int = None, tab_contents: List[dict] = None) -> str:
    # If report_generated_time is provided, use it; otherwise fall back to current time
    time_script = f"document.getElementById('lastUpdated').innerText = '{report_generated_time}';" if report_generated_time else "const now = new Date(); document.getElementById('lastUpdated').innerText = now.toLocaleString();"
    
    # Use provided counts or fall back to 'N/A'
    node_count_str = str(node_count) if node_count is not None else 'N/A'
    edge_count_str = str(edge_count) if edge_count is not None else 'N/A'
    
    # Tab switching function (only included if tabs are used)
    tab_script = ""
    if tab_contents:
        tab_script = """
        // Tab switching functionality
        window.showTab = function(tabId) {
            // Hide all tab panes
            const panes = document.querySelectorAll('.tab-pane');
            panes.forEach(pane => pane.classList.remove('active'));
            
            // Remove active class from all tab buttons
            const buttons = document.querySelectorAll('.tab-button');
            buttons.forEach(button => button.classList.remove('active'));
            
            // Show selected tab pane
            const selectedPane = document.getElementById('pane-' + tabId);
            if (selectedPane) {
                selectedPane.classList.add('active');
            }
            
            // Add active class to selected tab button
            const selectedButton = document.getElementById('tab-' + tabId);
            if (selectedButton) {
                selectedButton.classList.add('active');
            }
        };
        
        // Setup network events for all tabs
        function setupNetworkEventsForTab(tabId) {
            const networkVarName = 'network' + (tabId ? '_' + tabId : '');
            
            function setupEvents() {
                if (typeof window[networkVarName] !== 'undefined') {
                    const network = window[networkVarName];
                    const infoPanel = document.getElementById('selected-info' + (tabId ? '-' + tabId : ''));
                    const detailsTitle = infoPanel ? infoPanel.closest('div').querySelector('.details-title') : null;
                    
                    function updateInfoPanel(title, content) {
                        if (detailsTitle) {
                            detailsTitle.textContent = title;
                        }
                        if (infoPanel) {
                            infoPanel.innerHTML = content;
                        }
                    }
                    
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
                    setTimeout(setupEvents, 100);
                }
            }
            
            setupEvents();
        }
        """
    else:
        # Single network setup (backward compatibility)
        tab_script = """
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
        """
    
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
            
            {tab_script}
            
            // Setup when DOM is loaded
            setTimeout(() => {{
                {("setupNetworkEventsForTab();" if tab_contents else "setupNetworkEvents();")}
            }}, 500);
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


def generate_tabbed_main(tab_contents: List[dict]) -> str:
    """
    Generate main content with tabbed interface for multiple graphs.
    
    Args:
        tab_contents: List of dicts with keys: 'id', 'title', 'content', 'node_count', 'edge_count'
    """
    if not tab_contents:
        return generate_main('<p class="text-center text-gray-400 text-lg">No graph content available.</p>')
    
    # Generate tab navigation
    tab_nav_html = ""
    for i, tab in enumerate(tab_contents):
        active_class = " active" if i == 0 else ""
        tab_nav_html += f"""
            <button class="tab-button{active_class}" onclick="showTab('{tab['id']}')" id="tab-{tab['id']}">
                {tab['title']} ({tab.get('node_count', 'N/A')} nodes)
            </button>
        """
    
    # Generate tab content panes
    tab_panes_html = ""
    for i, tab in enumerate(tab_contents):
        active_class = " active" if i == 0 else ""
        tab_panes_html += f"""
            <div class="tab-pane{active_class}" id="pane-{tab['id']}">
                <div class="main-container">
                    <!-- Graph Container -->
                    <div id="graph-panel-{tab['id']}" class="graph-panel">
                        <div id="network-container-{tab['id']}">
                            {tab['content']}
                        </div>
                    </div>

                    <!-- Right Side Info Panel -->
                    <aside id="info-panel-{tab['id']}" class="info-panel info-panel-scroll">
                        <div>
                            <h2 class="details-title">Selected Item Details</h2>
                            <div id="selected-info-{tab['id']}" class="details-content">
                                <p>Select a node or edge in the graph to see its details here.</p>
                            </div>
                        </div>
                    </aside>
                </div>
            </div>
        """
    
    return f"""
    <div class="tab-container">
        <!-- Tab Navigation -->
        <nav class="tab-nav">
            {tab_nav_html}
        </nav>
        
        <!-- Tab Content -->
        <div class="tab-content">
            {tab_panes_html}
        </div>
    </div>
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
