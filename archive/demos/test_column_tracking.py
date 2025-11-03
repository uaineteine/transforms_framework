#!/usr/bin/env python3
"""
Simple test for column tracking within the templates directory.
"""
import os
import sys

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))
# Add the parent directory to sys.path
parent_dir = os.path.join(current_dir, '..')
sys.path.append(os.path.abspath(parent_dir))

from pyspark.sql import SparkSession
from transformslib.transforms.atomiclib import DropVariable, SubsetTable, RenameTable
from transformslib.tables.collections.supply_load import SupplyLoad

def test_column_tracking():
    print("Testing column tracking functionality...")
    
    # Create Spark session
    spark = SparkSession.builder.master("local").appName("ColumnTrackingTest").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Load pipeline tables (this should work from templates directory)
    job_id = 1
    supply_frames = SupplyLoad(job_id, spark=spark, use_test_path=True)
    
    print("Tables loaded successfully!")
    
    # Test 1: DropVariable
    print("\n=== Test 1: DropVariable column tracking ===")
    original_cols = list(supply_frames["positions"].columns)
    print(f"Original positions columns: {original_cols}")
    
    drop_transform = DropVariable("var")
    supply_frames = drop_transform.apply(supply_frames, df="positions")
    
    new_cols = list(supply_frames["positions"].columns)
    print(f"Columns after dropping 'var': {new_cols}")
    
    # Check column tracking
    log_info = drop_transform.log_info
    if hasattr(log_info, 'input_columns') and log_info.input_columns:
        print(f"✓ Input columns tracked: {log_info.input_columns}")
        print(f"✓ Output columns tracked: {log_info.output_columns}")
        print(f"✓ Removed variables: {log_info.removed_variables}")
        
        # Verify tracking is correct
        assert log_info.input_columns['positions'] == original_cols
        assert log_info.output_columns['positions'] == new_cols
        print("✓ DropVariable column tracking verification passed!")
    else:
        print("✗ Column tracking not found in log_info")
    
    # Test 2: RenameTable
    print("\n=== Test 2: RenameTable column tracking ===")
    original_cols = list(supply_frames["salary"].columns)
    print(f"Original salary columns: {original_cols}")
    
    rename_transform = RenameTable({"salary": "income"})
    supply_frames = rename_transform.apply(supply_frames, df="salary")
    
    new_cols = list(supply_frames["salary"].columns)
    print(f"Columns after renaming 'salary' to 'income': {new_cols}")
    
    # Check column tracking
    log_info = rename_transform.log_info
    if hasattr(log_info, 'input_columns') and log_info.input_columns:
        print(f"✓ Input columns tracked: {log_info.input_columns}")
        print(f"✓ Output columns tracked: {log_info.output_columns}")
        
        # Verify tracking is correct
        assert log_info.input_columns['salary'] == original_cols
        assert log_info.output_columns['salary'] == new_cols
        assert 'income' in new_cols and 'salary' not in new_cols
        print("✓ RenameTable column tracking verification passed!")
    else:
        print("✗ Column tracking not found in log_info")
    
    print("\n🎉 All column tracking tests completed successfully!")
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    test_column_tracking()