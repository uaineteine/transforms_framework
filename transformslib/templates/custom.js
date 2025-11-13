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
        
        // Setup when DOM is loaded
        document.addEventListener('DOMContentLoaded', function() {
            setupNetworkEvents();
        });