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
    from transformslib.transforms import *
    from pyspark.sql.functions import col
    from transformslib.tables.collections import SupplyLoad
    
    #tmp
    from pyspark.sql.functions import to_date

    # Create Spark session
    print("Creating Spark session")
    spark = SparkSession.builder.master("local").appName("TransformTest").getOrCreate()

    # load pipeline tables
    job_id = 1
    #run_id = 1
    
    supply_frames = SupplyLoad(job_id, spark=spark, use_test_path=True) #sample_rows=xyz
    
    listatomic()

    listmacro()

    # -------------------------------
    # Test 1: PartitionByValue on SALARY for salary
    # -------------------------------
    print("Partitioning salary by salary")
    partition_transform = PartitionByValue("salary")
    supply_frames = partition_transform.apply(supply_frames, df="salary")
    partitioned = supply_frames.select_by_names("salary_*")
    for t in partitioned.tables:
        print(f"Table: {t.table_name}")
        t.show()

    # -------------------------------
    # Test 2: DropVariable on positions
    # -------------------------------
    print("Original columns (positions):", supply_frames["positions"].columns)

    supply_frames = DropVariable("var").apply(supply_frames, df="positions")

    print("After DropVariable (var) on positions:", supply_frames["positions"].columns)
    supply_frames["positions"].show()

    # -------------------------------
    # Test 3: SubsetTable on salary
    # -------------------------------
    print("Original columns (salary):", supply_frames["salary"].columns)

    supply_frames = SubsetTable(["salary", "age"]).apply(supply_frames, df="salary")

    print("After SubsetTable (keep salary) on salary:", supply_frames["salary"].columns)
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

    supply_frames = RenameTable({"salary": "income"}).apply(supply_frames, df="salary")

    print("After RenameTable (salary -> income) on salary:", supply_frames["salary"].columns)
    supply_frames["salary"].show()

    # -------------------------------
    # Test 7: ComplexFilter on salary
    # -------------------------------
    print("Applying ComplexFilter (income >= 600) on salary")
    
    #TODO 
    #make a simple filter type vs a complex one

    filter_transform = ComplexFilter(condition_map={
        "pyspark": lambda df: df.filter(col("income") >= 600)
    })

    supply_frames = filter_transform.apply(supply_frames, df="salary")

    print("After ComplexFilter (income > 600) on salary:")
    supply_frames["salary"].show()

    # -------------------------------
    # Test 8: JoinTable on positions and salary
    # -------------------------------
    print("Joining positions and salary on age")

    join_transform = JoinTable(
        left_table="positions",
        right_table="salary",
        join_columns="age",
        join_type="inner"
    )

    supply_frames = join_transform.apply(supply_frames, output_table="example_join")

    print("After JoinTable (positions inner join salary on age):")
    supply_frames["example_join"].show()

    # -------------------------------
    # Test 9: SimpleFilter on the joined table
    # -------------------------------
    print("Applying SimpleFilter (income > 600) on example_join")
    supply_frames = SimpleFilter(column="income", op=">", value=600).apply(supply_frames, df="example_join")

    print("After SimpleFilter:")
    supply_frames["example_join"].show()

    # -------------------------------
    # Test 10: Concatenate variables
    # -------------------------------
    print("Concatenating variables")
    supply_frames = ConcatColumns(variables_to_concat=["age", "skill"], sep="_").apply(supply_frames, df="example_join", output_var="concatted")
    print("After ConcatColumns (age, skill -> concatted) on example_join:")
    supply_frames["example_join"].show()

    # -------------------------------
    # Test 11: ReplaceByCondition
    # -------------------------------
    print("Replacing values in income where income >= 610 with 600")
    supply_frames = ReplaceByCondition(
        column="income",
        op=">=",
        value=610,
        replacement=600
    ).apply(supply_frames, df="example_join")

    print("After ReplaceByCondition (income >= 610 -> 600):")
    supply_frames["example_join"].show()

    # -------------------------------
    # Test 12: explode some table
    # -------------------------------
    
    print("Exploding an array type test")
    supply_frames = ExplodeColumn("var3", " ", True).apply(supply_frames, df="array_like")
    supply_frames["array_like"].show()

    # -------------------------------
    # Test 13: RoundNumber
    # -------------------------------
    print("Rounding a number to 3 decimal places")
    supply_frames = RoundNumber("value2", 3).apply(supply_frames, df="decimal_table")
    supply_frames["decimal_table"].show()

    # -------------------------------
    # Test 14: TruncateDate
    # -------------------------------
    print("Truncating a date to year and month levels")
    # Convert string to date
    supply_frames["date_table"].df = supply_frames["date_table"].df.withColumn("event_date", to_date("event_date", "yyyy-MM-dd"))
    
    supply_frames = TruncateDate("event_date", "month").apply(supply_frames, df="date_table")
    supply_frames["date_table"].show()
    supply_frames = TruncateDate("event_date", "year").apply(supply_frames, df="date_table")
    supply_frames["date_table"].show()
    #print(supply_frames["date_table"].dtypes)

    # -------------------------------
    # Test 15: Sorting
    # -------------------------------
    print("Sorting the date_table by event_date")
    supply_frames = SortTable(by="event_date", ascending=True).apply(supply_frames, df="date_table")
    supply_frames["date_table"].show()

    # -------------------------------
    # Test 16: ForceCase
    # -------------------------------
    print("Applying ForceCase (upper) on var2 column")
    supply_frames = ForceCase("var2", "upper").apply(supply_frames, df="array_like")
    print("After ForceCase (upper):")
    supply_frames["array_like"].show()

    # -------------------------------
    # Test 17: TrimWhitespace
    # -------------------------------
    print("Applying TrimWhitespace on name column")
    supply_frames = TrimWhitespace("name").apply(supply_frames, df="location")
    print("After TrimWhitespace:")
    supply_frames["location"].show()

    # -------------------------------
    # Test 18: TopBottomCoding
    #   -------------------------------
    print("Applying TopBottomCode macro to salary column")
    print("Original salary data:")
    supply_frames["salary"].show()
    print("Salary column statistics:")
    supply_frames["salary"].df.describe(["salary"]).show()

    # -------------------------------
    # Apply TopBottomCode macro to salary table
    # -------------------------------
    print("Setting minimum value to 450 and maximum value to 650")
    
    # Create TopBottomCode macro instance
    topbottom_macro = TopBottomCode(
        input_tables=supply_frames,
        input_variables=["salary"],  # Apply coding to salary column
        max_value=650,               # Cap values above 650
        min_value=450                # Floor values below 450
    )
    
    # Apply the macro transformation to the salary table
    supply_frames = topbottom_macro.apply(df="salary")
    
    print("After TopBottomCode transformation:")
    supply_frames["salary"].show()
    print("Salary column statistics after transformation:")
    supply_frames["salary"].df.describe(["salary"]).show()

    # -------------------------------
    # Repeated tests to continue
    # -------------------------------

    print("example of final table going through some tests")
    join_transform = JoinTable(
        left_table="location",
        right_table="state",
        join_columns="city",
        join_type="outer"
    )
    print("first join complete")
    supply_frames = join_transform.apply(supply_frames, output_table="location")
    supply_frames = DistinctTable().apply(supply_frames, df="location")

    join_transform = JoinTable(
        left_table="location",
        right_table="example_join",
        join_columns="name",
        join_type="outer"
    )
    supply_frames = join_transform.apply(supply_frames, output_table="super_table")
    print("second join complete")
    
    supply_frames = TrimWhitespace("name").apply(supply_frames, df="super_table")
    supply_frames = ForceCase("name", "upper").apply(supply_frames, df="super_table")
    
    supply_frames["super_table"].show()

    # save table output tables
    supply_frames.save_all(f"../test_tables/jobs/prod/job_{job_id}/output", spark=spark, use_local_path=True)
