if __name__ == "__main__":
    import os
    import sys
    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Add the parent directory to sys.path
    parent_dir = os.path.join(current_dir, '..', "src")
    sys.path.append(os.path.abspath(parent_dir))

    from pyspark.sql import SparkSession
    from transforms.lib import DropVariable
    from tables.collections.supply_load import SupplyLoad

    # Create Spark session
    print("Creating Spark session")
    spark = SparkSession.builder.master("local").appName("TransformTest").getOrCreate()

    # load pipeline tables
    supply_frames = SupplyLoad("../test_tables/payload.json", spark=spark)
    print("Original columns:", supply_frames["test_table"].columns)

    # Instantiate DropVariable transform with new syntax
    supply_frames = DropVariable("age")(supply_frames, df="test_table")

    # Show result
    print("Transformed columns:", supply_frames["test_table"].columns)
    supply_frames["test_table"].show()

    #save table events
    supply_frames.save_events()
    
