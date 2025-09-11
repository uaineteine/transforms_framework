#!/usr/bin/env python3

if __name__ == "__main__":
    import os
    import sys
    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Add the parent directory to sys.path
    parent_dir = os.path.join(current_dir, '..')
    sys.path.append(os.path.abspath(parent_dir))

    from transformslib.mapping import dag
    
    # Test the new subgraph DAG functionality
    print("Testing subgraph DAG generation...")
    
    try:
        # Generate subgraph DAGs
        html_file = dag.render_subgraph_dags(1, 1)
        print(f"Success! Subgraph DAGs saved to: {html_file}")
        
        # Also test building the DAG HTML without saving
        html_content = dag.build_subgraph_dags_html(1, 1)
        print(f"HTML content generated successfully ({len(html_content)} characters)")
        
        # Test getting the subgraph data
        subgraph_htmls = dag.build_subgraph_dag_htmls(1, 1)
        print(f"Generated {len(subgraph_htmls)} subgraph(s):")
        
        for i, sg in enumerate(subgraph_htmls):
            print(f"  - {sg['title']}: {sg['node_count']} nodes, {sg['edge_count']} edges")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()