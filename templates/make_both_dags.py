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
    
    # Generate both single and subgraph DAGs for comparison
    print("Generating single DAG...")
    dag.render_dag(1, 1)
    
    print("Generating subgraph DAGs...")
    dag.render_subgraph_dags(1, 1)
    
    print("Both DAG types generated successfully!")