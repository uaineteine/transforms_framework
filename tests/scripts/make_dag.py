if __name__ == "__main__":
    import os
    import sys
    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Add the parent directory to sys.path
    parent_dir = os.path.join(current_dir, '..', "..")
    sys.path.append(os.path.abspath(parent_dir))
    
    #start recording run time
    import time
    start_time = time.time()
    print(f"Starting test pipeline execution at {time.ctime(start_time)}")

    from transformslib import dag, set_job_id, set_run_id, set_default_variables
    
    set_default_variables()
    set_job_id(1)
    set_run_id(1)
    
    dag.render_dag()

    end_time = time.time()
    print(f"Test pipeline execution completed at {time.ctime(end_time)}")
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
    