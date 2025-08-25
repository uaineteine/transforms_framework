if __name__ == "__main__":
    import os
    import sys
    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Add the parent directory to sys.path
    parent_dir = os.path.join(current_dir, '..', "src")
    sys.path.append(os.path.abspath(parent_dir))


    #---TEMPLATE STARTS HERE---
    from pyspark.sql import SparkSession
    from transforms.lib import *
    from pyspark.sql.functions import col
    from tables.collections.supply_load import SupplyLoad

    # Create Spark session
    print("Creating Spark session")
    spark = SparkSession.builder.master("local").appName("TransformTest").getOrCreate()

    # load pipeline tables
    supply_frames = SupplyLoad("../test_tables/payload.json", spark=spark)

    # -------------------------------
    # Test 1: PartitionByValue on SALARY for test_table2
    # -------------------------------
    print("Partitioning test_table2 by SALARY")
    partition_transform = PartitionByValue("SALARY")
    supply_frames = partition_transform.apply(supply_frames, df="test_table2")
    partitioned = supply_frames.select_by_names("test_table2_*")
    for t in partitioned.tables:
        print(f"Table: {t.table_name}")
        t.show()

    # -------------------------------
    # Test 2: DropVariable on test_table
    # -------------------------------
    print("Original columns (test_table):", supply_frames["test_table"].columns)

    supply_frames = DropVariable("VAR").apply(supply_frames, df="test_table")

    print("After DropVariable (VAR) on test_table:", supply_frames["test_table"].columns)
    supply_frames["test_table"].show()

    # -------------------------------
    # Test 3: SubsetTable on test_table2
    # -------------------------------
    print("Original columns (test_table2):", supply_frames["test_table2"].columns)

    supply_frames = SubsetTable(["SALARY", "AGE"]).apply(supply_frames, df="test_table2")

    print("After SubsetTable (keep SALARY) on test_table2:", supply_frames["test_table2"].columns)
    supply_frames["test_table2"].show()

    # -------------------------------
    # Test 4: DistinctTable on test_table
    # -------------------------------
    print("Applying DistinctTable on test_table")
    supply_frames = DistinctTable().apply(supply_frames, df="test_table")

    print("After DistinctTable on test_table (all columns):")
    supply_frames["test_table"].show()

    # -------------------------------
    # Test 5: DistinctTable on test_table2
    # -------------------------------
    print("Applying DistinctTable on test_table2")
    supply_frames = DistinctTable().apply(supply_frames, df="test_table2")

    print("After DistinctTable on test_table2 (all columns):")
    supply_frames["test_table2"].show()

    # -------------------------------
    # Test 6: RenameTable on test_table2
    # -------------------------------
    print("Original columns (test_table2):", supply_frames["test_table2"].columns)

    supply_frames = RenameTable({"SALARY": "INCOME"}).apply(supply_frames, df="test_table2")

    print("After RenameTable (SALARY -> INCOME) on test_table2:", supply_frames["test_table2"].columns)
    supply_frames["test_table2"].show()

    # -------------------------------
    # Test 7: FilterTransform on test_table2
    # -------------------------------
    print("Applying FilterTransform (INCOME > 600) on test_table2")

    filter_transform = FilterTransform(condition_map={
        "pyspark": lambda df: df.filter(col("INCOME") >= 600)
    })

    supply_frames = filter_transform.apply(supply_frames, df="test_table2")

    print("After FilterTransform (INCOME > 600) on test_table2:")
    supply_frames["test_table2"].show()

    # -------------------------------
    # Test 8: JoinTable on test_table1 and test_table2
    # -------------------------------
    print("Joining test_table1 and test_table2 on AGE")

    join_transform = JoinTable(
        left_table="test_table",
        right_table="test_table2",
        join_columns="AGE",
        join_type="inner"
    )

    supply_frames = join_transform.apply(supply_frames, output_table="joined_table")

    print("After JoinTable (test_table1 inner join test_table2 on AGE):")
    supply_frames["joined_table"].show()

    # save table events
    supply_frames.save_events()
