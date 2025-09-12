#!/usr/bin/env python
"""
Demonstration script showing schema validation feature for the transforms framework.

This script demonstrates the new schema validation capability that has been added
to the new sampling system (sampling_state.json) while leaving the legacy system 
(payload.json) unchanged.
"""

import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(current_dir, '..')
sys.path.append(os.path.abspath(parent_dir))

from pyspark.sql import SparkSession
from transformslib.tables.collections.supply_load import SupplyLoad

def demonstrate_schema_validation():
    """Demonstrate the new schema validation feature."""
    
    print("="*80)
    print("TRANSFORMS FRAMEWORK - SCHEMA VALIDATION DEMONSTRATION")
    print("="*80)
    
    # Create Spark session
    print("\n1. Creating Spark session...")
    spark = SparkSession.builder.master("local").appName("SchemaValidationDemo").getOrCreate()

    print("\n2. Testing NEW SAMPLING SYSTEM with schema validation ENABLED (default)")
    print("-" * 60)
    
    try:
        job_id = 1
        # New system with schema validation enabled (default behavior)
        supply_frames = SupplyLoad(job_id, spark=spark)
        
        print(f"\n✅ SUCCESS: Loaded {len(supply_frames)} tables with schema validation!")
        print("   Schema validation confirmed all expected columns are present.")
        
        # Show sample table info
        positions = supply_frames["positions"]
        print(f"   Example: 'positions' table has {len(positions.columns)} columns: {positions.columns}")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")

    print("\n3. Testing NEW SAMPLING SYSTEM with schema validation DISABLED")
    print("-" * 60)
    
    try:
        # New system with schema validation disabled
        supply_frames_no_validation = SupplyLoad(job_id, spark=spark, enable_schema_validation=False)
        
        print(f"\n✅ SUCCESS: Loaded {len(supply_frames_no_validation)} tables without schema validation!")
        print("   Data loaded normally, schema validation was skipped.")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")

    print("\n4. Key Features of Schema Validation:")
    print("-" * 60)
    print("   ✓ Validates column names match expected schema")
    print("   ✓ Reports expected vs actual data types")
    print("   ✓ Provides clear error messages for schema mismatches")
    print("   ✓ Only applies to NEW sampling system (sampling_state.json)")
    print("   ✓ Legacy system (payload.json) remains completely unchanged")
    print("   ✓ Can be easily disabled with enable_schema_validation=False")
    print("   ✓ Supports PySpark, Pandas, and Polars DataFrames")

    print("\n5. Usage Examples:")
    print("-" * 60)
    print("   # New system with schema validation (default)")
    print("   supply_frames = SupplyLoad(job_id=1, spark=spark)")
    print("")
    print("   # New system without schema validation")
    print("   supply_frames = SupplyLoad(job_id=1, spark=spark, enable_schema_validation=False)")
    print("")
    print("   # Legacy system (unchanged - no schema validation available)")
    print("   supply_frames = SupplyLoad(job_id=1, run_id=2, spark=spark)")

    spark.stop()
    
    print("\n" + "="*80)
    print("DEMONSTRATION COMPLETE")
    print("Schema validation is now available for the new sampling system!")
    print("="*80)

if __name__ == "__main__":
    demonstrate_schema_validation()