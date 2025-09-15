if __name__ == "__main__":
    import os
    import sys
    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Add the parent directory to sys.path
    parent_dir = os.path.join(current_dir, '..')
    sys.path.append(os.path.abspath(parent_dir))

    #---TEMPLATE STARTS HERE---
    from pyspark.sql import SparkSession
    from transformslib.transforms.macrolib import *
    from transformslib.tables.collections.supply_load import SupplyLoad

    # Create Spark session
    print("Creating Spark session")
    spark = SparkSession.builder.master("local").appName("MacroTransformTest").getOrCreate()

    # load pipeline tables
    job_id = 1
    
    supply_frames = SupplyLoad(job_id, spark=spark, use_test_path=True) #sample_rows=xyz
    
    listmacro()

    print("Original salary data:")
    supply_frames["salary"].show()
    print("Salary column statistics:")
    supply_frames["salary"].df.describe(["salary"]).show()

    # -------------------------------
    # Apply TopBottomCode macro to salary table
    # -------------------------------
    print("Applying TopBottomCode macro to salary column")
    print("Setting minimum value to 450 and maximum value to 650")
    
    # Create TopBottomCode macro instance
    topbottom_macro = TopBottomCode(
        input_tables=supply_frames,
        input_variables=["salary"],  # Apply coding to salary column
        max_value=650,               # Cap values above 650
        min_value=450                # Floor values below 450
    )
    
    # Apply the macro transformation to the salary table
    supply_frames = topbottom_macro.apply(df="salary")
    
    print("After TopBottomCode transformation:")
    supply_frames["salary"].show()
    print("Salary column statistics after transformation:")
    supply_frames["salary"].df.describe(["salary"]).show()

    # Save the transformed tables
    print("Saving transformed tables...")
    supply_frames.save_all("../test_tables/output", spark=spark)
    
    print("Template macro lib example completed successfully!")