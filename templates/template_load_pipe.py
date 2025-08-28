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
    # Test 1: PartitionByValue on SALARY for salary
    # -------------------------------
    print("Partitioning salary by SALARY")
    partition_transform = PartitionByValue("SALARY")
    supply_frames = partition_transform.apply(supply_frames, df="salary")
    partitioned = supply_frames.select_by_names("salary_*")
    for t in partitioned.tables:
        print(f"Table: {t.table_name}")
        t.show()

    # -------------------------------
    # Test 2: DropVariable on position
    # -------------------------------
    print("Original columns (position):", supply_frames["position"].columns)

    supply_frames = DropVariable("VAR").apply(supply_frames, df="position")

    print("After DropVariable (VAR) on position:", supply_frames["position"].columns)
    supply_frames["position"].show()

    # -------------------------------
    # Test 3: SubsetTable on salary
    # -------------------------------
    print("Original columns (salary):", supply_frames["salary"].columns)

    supply_frames = SubsetTable(["SALARY", "AGE"]).apply(supply_frames, df="salary")

    print("After SubsetTable (keep SALARY) on salary:", supply_frames["salary"].columns)
    supply_frames["salary"].show()

    # -------------------------------
    # Test 4: DistinctTable on position
    # -------------------------------
    print("Applying DistinctTable on position")
    supply_frames = DistinctTable().apply(supply_frames, df="position")

    print("After DistinctTable on position (all columns):")
    supply_frames["position"].show()

    # -------------------------------
    # Test 5: DistinctTable on salary
    # -------------------------------
    print("Applying DistinctTable on salary")
    supply_frames = DistinctTable().apply(supply_frames, df="salary")

    print("After DistinctTable on salary (all columns):")
    supply_frames["salary"].show()

    # -------------------------------
    # Test 6: RenameTable on salary
    # -------------------------------
    print("Original columns (salary):", supply_frames["salary"].columns)

    supply_frames = RenameTable({"SALARY": "INCOME"}).apply(supply_frames, df="salary")

    print("After RenameTable (SALARY -> INCOME) on salary:", supply_frames["salary"].columns)
    supply_frames["salary"].show()

    # -------------------------------
    # Test 7: ComplexFilter on salary
    # -------------------------------
    print("Applying ComplexFilter (INCOME > 600) on salary")
    
    #TODO 
    #make a simple filter type vs a complex one

    filter_transform = ComplexFilter(condition_map={
        "pyspark": lambda df: df.filter(col("INCOME") >= 600)
    })

    supply_frames = filter_transform.apply(supply_frames, df="salary")

    print("After ComplexFilter (INCOME > 600) on salary:")
    supply_frames["salary"].show()

    # -------------------------------
    # Test 8: JoinTable on position and salary
    # -------------------------------
    print("Joining position and salary on AGE")

    join_transform = JoinTable(
        left_table="position",
        right_table="salary",
        join_columns="AGE",
        join_type="inner"
    )

    supply_frames = join_transform.apply(supply_frames, output_table="joined_table")

    print("After JoinTable (position inner join salary on AGE):")
    supply_frames["joined_table"].show()

    #
    # Test 9: SimpleFilter on the joined table
    #
    print("Applying SimpleFilter (INCOME > 600) on joined_table")
    supply_frames = SimpleFilter(column="INCOME", op=">", value=600).apply(supply_frames, df="joined_table")

    print("After SimpleFilter:")
    supply_frames["joined_table"].show()

    # save table events
    supply_frames.save_events()
