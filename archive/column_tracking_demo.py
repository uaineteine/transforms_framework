#!/usr/bin/env python3
"""
Example demonstrating the new column tracking functionality.

This script shows how transforms now track input and output columns
by name for operations that modify table schemas.
"""
import os
import sys

# Add parent directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(current_dir, '..')
sys.path.append(os.path.abspath(parent_dir))

from pyspark.sql import SparkSession
from transformslib.transforms.atomiclib import DropVariable, SubsetTable, RenameTable, JoinTable
from transformslib.tables.collections.supply_load import SupplyLoad

def demonstrate_column_tracking():
    """Demonstrate the new column tracking feature."""
    print("=== Column Tracking Feature Demonstration ===\n")
    
    # Set up Spark session
    spark = SparkSession.builder.master("local").appName("ColumnTrackingDemo").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Load test data
    supply_frames = SupplyLoad(1, spark=spark, use_test_path=True)
    print("Loaded test data successfully!\n")
    
    # Example 1: DropVariable
    print("1. DropVariable Transform")
    print("-" * 30)
    original_cols = list(supply_frames["positions"].columns)
    print(f"Before: {original_cols}")
    
    drop_transform = DropVariable("var")
    supply_frames = drop_transform.apply(supply_frames, df="positions")
    
    final_cols = list(supply_frames["positions"].columns)
    print(f"After:  {final_cols}")
    
    # Show column tracking info
    log_info = drop_transform.log_info
    print(f"Tracked input columns:  {log_info.input_columns['positions']}")
    print(f"Tracked output columns: {log_info.output_columns['positions']}")
    print(f"Removed variables: {log_info.removed_variables}\n")
    
    # Example 2: RenameTable
    print("2. RenameTable Transform")
    print("-" * 30)
    original_cols = list(supply_frames["salary"].columns)
    print(f"Before: {original_cols}")
    
    rename_transform = RenameTable({"salary": "income", "NAME": "full_name"})
    supply_frames = rename_transform.apply(supply_frames, df="salary")
    
    final_cols = list(supply_frames["salary"].columns)
    print(f"After:  {final_cols}")
    
    # Show column tracking info
    log_info = rename_transform.log_info
    print(f"Tracked input columns:  {log_info.input_columns['salary']}")
    print(f"Tracked output columns: {log_info.output_columns['salary']}")
    print(f"Input variables: {log_info.input_variables}")
    print(f"Output variables: {log_info.output_variables}\n")
    
    # Example 3: JoinTable
    print("3. JoinTable Transform")
    print("-" * 30)
    left_cols = list(supply_frames["positions"].columns)
    right_cols = list(supply_frames["salary"].columns)
    print(f"Left table (positions):  {left_cols}")
    print(f"Right table (salary):    {right_cols}")
    
    join_transform = JoinTable("positions", "salary", "age", "inner")
    supply_frames = join_transform.apply(supply_frames, output_table="joined_data")
    
    joined_cols = list(supply_frames["joined_data"].columns)
    print(f"Joined table:            {joined_cols}")
    
    # Show column tracking info
    log_info = join_transform.log_info
    print(f"Tracked input columns:")
    for table, cols in log_info.input_columns.items():
        print(f"  {table}: {cols}")
    print(f"Tracked output columns:")
    for table, cols in log_info.output_columns.items():
        print(f"  {table}: {cols}")
    
    print(f"Input row counts: {log_info.input_row_counts}")
    print(f"Output row counts: {log_info.output_row_counts}\n")
    
    print("=== Column Tracking Benefits ===")
    print("✓ Complete visibility into schema changes")
    print("✓ Automatic tracking for transforms that modify columns")
    print("✓ Support for multi-table operations (joins, unions)")
    print("✓ Backward compatible - no breaking changes")
    print("✓ Conditional tracking - only when columns are modified")
    
    # Clean up
    spark.stop()
    print("\nDemo completed successfully! 🎉")

if __name__ == "__main__":
    demonstrate_column_tracking()