if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from transforms import DropVariable
    from pipeline_table import PipelineTable, PipelineTables

    # Create Spark session
    print("Creating Spark session")
    spark = SparkSession.builder.master("local").appName("TransformTest").getOrCreate()

    # Create a collection of PipelineTables using the new syntax
    print("Creating PipelineTables collection...")
    
    # Load multiple tables
    test_table = PipelineTable.load(spark=spark, path="test_tables/test.csv", format="csv", table_name="test_table", frame_type="pyspark")
    test2_table = PipelineTable.load(spark=spark, path="test_tables/test2.csv", format="csv", table_name="test2_table", frame_type="pyspark")
    
    # Create collection with initial tables
    tables_list = [test_table, test2_table]
    pt_collection = PipelineTables(tables_list)
    
    print(f"Collection created with {pt_collection.ntables} tables")
    print(f"Available tables: {list(pt_collection.named_tables.keys())}")
    
    # Demonstrate collection operations
    print("\n--- Collection Operations ---")
    
    # Access tables by name
    first_table = pt_collection["test_table"]
    print(f"First table columns: {first_table.columns}")
    
    # Check if table exists
    if "test2_table" in pt_collection:
        print("test2_table exists in collection")
    
    # Apply transforms to specific tables
    print("\n--- Applying Transforms ---")
    
    # Apply DropVariable transform to first table
    pt_collection = DropVariable("age")(pt_collection["test_table"])
    
    print(f"Transformed table columns: {pt_collection['test_table'].columns}")
    pt_collection["test_table"].show()
    
    # Show all tables in collection
    print("\n--- All Tables in Collection ---")
    for i, table in enumerate(pt_collection.tables):
        print(f"Table {i+1}: {table.table_name} - Columns: {table.columns}")
        table.df.show()
    
    # Save events for all tables in the collection
    print("\n--- Saving Events ---")
    pt_collection.save_events()
    
    print("PipelineTables collection test completed!")

