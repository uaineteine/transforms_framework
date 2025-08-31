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
    from transformslib.transforms.atomiclib import *
    from pyspark.sql.functions import col
    from transformslib.tables.collections.supply_load import SupplyLoad

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
    # Test 2: DropVariable on positions
    # -------------------------------
    print("Original columns (positions):", supply_frames["positions"].columns)

    supply_frames = DropVariable("VAR").apply(supply_frames, df="positions")

    print("After DropVariable (VAR) on positions:", supply_frames["positions"].columns)
    supply_frames["positions"].show()

    # -------------------------------
    # Test 3: SubsetTable on salary
    # -------------------------------
    print("Original columns (salary):", supply_frames["salary"].columns)

    supply_frames = SubsetTable(["SALARY", "AGE"]).apply(supply_frames, df="salary")

    print("After SubsetTable (keep SALARY) on salary:", supply_frames["salary"].columns)
    supply_frames["salary"].show()

    # -------------------------------
    # Test 4: DistinctTable on positions
    # -------------------------------
    print("Applying DistinctTable on positions")
    supply_frames = DistinctTable().apply(supply_frames, df="positions")

    print("After DistinctTable on positions (all columns):")
    supply_frames["positions"].show()

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
    print("Applying ComplexFilter (INCOME >= 600) on salary")
    
    #TODO 
    #make a simple filter type vs a complex one

    filter_transform = ComplexFilter(condition_map={
        "pyspark": lambda df: df.filter(col("INCOME") >= 600)
    })

    supply_frames = filter_transform.apply(supply_frames, df="salary")

    print("After ComplexFilter (INCOME > 600) on salary:")
    supply_frames["salary"].show()

    # -------------------------------
    # Test 8: JoinTable on positions and salary
    # -------------------------------
    print("Joining positions and salary on AGE")

    join_transform = JoinTable(
        left_table="positions",
        right_table="salary",
        join_columns="AGE",
        join_type="inner"
    )

    supply_frames = join_transform.apply(supply_frames, output_table="example_join")

    print("After JoinTable (positions inner join salary on AGE):")
    supply_frames["example_join"].show()

    # -------------------------------
    # Test 9: SimpleFilter on the joined table
    # -------------------------------
    print("Applying SimpleFilter (INCOME > 600) on example_join")
    supply_frames = SimpleFilter(column="INCOME", op=">", value=600).apply(supply_frames, df="example_join")

    print("After SimpleFilter:")
    supply_frames["example_join"].show()

    # -------------------------------
    # Test 10: Concatenate variables
    # -------------------------------
    print("Concatenating variables")
    supply_frames = ConcatColumns(variables_to_concat=["AGE", "SKILL"], sep="_").apply(supply_frames, df="example_join", output_var="CONCATTED")
    print("After ConcatColumns (AGE, SKILL -> CONCATTED) on example_join:")
    supply_frames["example_join"].show()

    # -------------------------------
    # Test 11: ReplaceByCondition
    # -------------------------------
    print("Replacing values in INCOME where INCOME >= 610 with 600")
    supply_frames = ReplaceByCondition(
        column="INCOME",
        op=">=",
        value=610,
        replacement=600
    ).apply(supply_frames, df="example_join")

    print("After ReplaceByCondition (INCOME >= 610 -> 600):")
    supply_frames["example_join"].show()

    # save table output tables
    supply_frames.save_all("../test_tables/output", spark=spark)
