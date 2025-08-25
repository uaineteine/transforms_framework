if __name__ == "__main__":
    import os
    import sys
    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Add the parent directory to sys.path
    parent_dir = os.path.join(current_dir, '..', "src")
    sys.path.append(os.path.abspath(parent_dir))

    from pyspark.sql import SparkSession
    from transforms.lib import DropVariable, SubsetTable, DistinctTable, RenameTable, FilterTransform
    from pyspark.sql.functions import col
    from tables.collections.supply_load import SupplyLoad

    # Create Spark session
    print("Creating Spark session")
    spark = SparkSession.builder.master("local").appName("TransformTest").getOrCreate()

    # load pipeline tables
    supply_frames = SupplyLoad("../test_tables/payload.json", spark=spark)

    # -------------------------------
    # Test 1: DropVariable on test_table
    # -------------------------------
    print("Original columns (test_table):", supply_frames["test_table"].columns)

    supply_frames = DropVariable("AGE").apply(supply_frames, df="test_table")

    print("After DropVariable (AGE) on test_table:", supply_frames["test_table"].columns)
    supply_frames["test_table"].show()

    # -------------------------------
    # Test 2: SubsetTable on test_table2
    # -------------------------------
    print("Original columns (test_table2):", supply_frames["test_table2"].columns)

    supply_frames = SubsetTable("SALARY").apply(supply_frames, df="test_table2")

    print("After SubsetTable (keep SALARY) on test_table2:", supply_frames["test_table2"].columns)
    supply_frames["test_table2"].show()

    # -------------------------------
    # Test 3: DistinctTable on test_table
    # -------------------------------
    print("Applying DistinctTable on test_table")
    supply_frames = DistinctTable().apply(supply_frames, df="test_table")

    print("After DistinctTable on test_table (all columns):")
    supply_frames["test_table"].show()

    # -------------------------------
    # Test 4: DistinctTable on test_table2
    # -------------------------------
    print("Applying DistinctTable on test_table2")
    supply_frames = DistinctTable().apply(supply_frames, df="test_table2")

    print("After DistinctTable on test_table2 (all columns):")
    supply_frames["test_table2"].show()

    # -------------------------------
    # Test 5: RenameTable on test_table
    # -------------------------------
    print("Original columns (test_table):", supply_frames["test_table2"].columns)

    supply_frames = RenameTable({"SALARY": "INCOME"}).apply(supply_frames, df="test_table2")

    print("After RenameTable (SALARY -> INCOME) on test_table2:", supply_frames["test_table2"].columns)
    supply_frames["test_table2"].show()

    # -------------------------------
    # Test 6: FilterTransform on test_table
    # -------------------------------
    print("Applying FilterTransform (INCOME > 600) on test_table2")

    filter_transform = FilterTransform(condition_map={
        "pyspark": lambda df: df.filter(col("INCOME") >= 600)
    })

    supply_frames = filter_transform.apply(supply_frames, df="test_table2")

    print("After FilterTransform (INCOME > 600) on test_table2:")
    supply_frames["test_table2"].show()

    # save table events
    supply_frames.save_events()
