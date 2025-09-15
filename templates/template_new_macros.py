#!/usr/bin/env python3
"""
Simple template demonstrating the new ConcatenateIDs and DropMissingIDs macros.
"""
if __name__ == "__main__":
    import os
    import sys
    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Add the parent directory to sys.path
    parent_dir = os.path.join(current_dir, '..')
    sys.path.append(os.path.abspath(parent_dir))

    from pyspark.sql import SparkSession
    from transformslib.transforms.macrolib import *
    from transformslib.tables.collections.supply_load import SupplyLoad

    # Create Spark session
    print("Creating Spark session")
    spark = SparkSession.builder.master("local").appName("NewMacroDemo").getOrCreate()

    # Load pipeline tables
    job_id = 1
    supply_frames = SupplyLoad(job_id, spark=spark, use_test_path=True)
    
    print("=" * 80)
    print("NEW MACRO DEMONSTRATIONS")
    print("=" * 80)
    
    listmacro()

    print("\n" + "=" * 80)
    print("DEMONSTRATION 1: ConcatenateIDs Macro")
    print("=" * 80)
    
    # Check what columns we actually have
    table_name = "positions"
    print(f"Original {table_name} data:")
    supply_frames[table_name].show(5)
    print(f"Columns: {supply_frames[table_name].columns}")

    # Apply ConcatenateIDs macro using available columns
    available_cols = supply_frames[table_name].columns
    if len(available_cols) >= 2:
        col1, col2 = available_cols[0], available_cols[1]  # Use first two columns
        print(f"\nConcatenating '{col1}' and '{col2}' columns...")
        
        concat_macro = ConcatenateIDs(
            input_tables=supply_frames,
            input_columns=[col1, col2],
            output_column="concatenated_id"
        )
        
        # Apply the macro
        supply_frames = concat_macro.apply(df=table_name, output_var="concatenated_id")
        
        print(f"After ConcatenateIDs transformation:")
        supply_frames[table_name].show(5)
        print(f"New columns: {supply_frames[table_name].columns}")
    
    print("\n" + "=" * 80)
    print("DEMONSTRATION 2: DropMissingIDs Macro")
    print("=" * 80)
    
    # Add synthetic_aeuid column for demonstration
    print("Adding synthetic_aeuid column for demonstration...")
    from pyspark.sql.functions import lit
    df = supply_frames[table_name]
    df.df = df.df.withColumn("synthetic_aeuid", lit("test_value"))
    
    print(f"Table with synthetic_aeuid column:")
    supply_frames[table_name].show(5)
    print(f"Columns: {supply_frames[table_name].columns}")
    
    # Apply DropMissingIDs macro
    print("\nApplying DropMissingIDs macro...")
    drop_macro = DropMissingIDs(
        input_tables=supply_frames
    )
    
    supply_frames = drop_macro.apply(df=table_name)
    
    print(f"After DropMissingIDs transformation:")
    supply_frames[table_name].show(5)
    print(f"Final columns: {supply_frames[table_name].columns}")

    # Save the transformed tables
    print("\nSaving transformed tables...")
    supply_frames.save_all("../test_tables/output", spark=spark)
    
    print("\n" + "=" * 80)
    print("NEW MACRO TEMPLATE COMPLETED SUCCESSFULLY!")
    print("=" * 80)
    print("Summary:")
    print("✓ ConcatenateIDs macro: Concatenates two columns with '_' separator")
    print("✓ DropMissingIDs macro: Removes 'synthetic_aeuid' column")
    print("✓ Both macros follow the established framework patterns")
    print("=" * 80)